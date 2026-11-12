//! Safe persistence for the small JSON files under `~/.tablio`.
//!
//! `connections.json` holds database and SSH passwords, so every write
//! here is:
//!
//! - **Serialized** through one process-wide async mutex so two concurrent
//!   read-modify-write cycles (e.g. saving two connections at once) cannot
//!   lose each other's update.
//! - **Atomic**: the new content goes to a temp file in the same directory,
//!   is fsynced, then renamed over the old file. A crash mid-write leaves
//!   either the old or the new file, never a truncated one.
//! - **Private**: files are created `0600` and the directory `0700` on
//!   unix; existing files/directories are tightened on every write.
//! - **Non-destructive**: an unparseable file is an error, never treated as
//!   an empty list that would then overwrite it. Entries this build cannot
//!   decode (e.g. written by a newer version with a new enum variant) are
//!   kept verbatim, and unknown fields of an updated entry are preserved.

use serde_json::Value;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Guards every read-modify-write of a file under `~/.tablio`.
pub static STORE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// `~/.tablio`, without creating it.
pub fn tablio_dir() -> Result<PathBuf, String> {
    Ok(dirs::home_dir()
        .ok_or("Cannot determine home directory")?
        .join(".tablio"))
}

/// Create `dir` (and parents) if needed and restrict it to the owner.
pub fn ensure_private_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let meta = std::fs::metadata(dir)?;
        if meta.permissions().mode() & 0o777 != 0o700 {
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
        }
    }
    Ok(())
}

/// Atomically replace `path` with `bytes`, creating the parent directory
/// with owner-only permissions and the file with mode `0600`.
pub fn write_atomic_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    ensure_private_dir(dir)?;
    // `NamedTempFile` is created with mode 0600 on unix.
    let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
    tmp.write_all(bytes)?;
    tmp.as_file().sync_all()?;
    tmp.persist(path).map_err(|e| e.error)?;
    #[cfg(unix)]
    {
        // Make the rename itself durable.
        if let Ok(d) = std::fs::File::open(dir) {
            let _ = d.sync_all();
        }
    }
    Ok(())
}

/// Read a JSON array file as raw values. A missing file is an empty list;
/// anything that is not a JSON array is an error.
pub fn read_entries(path: &Path) -> Result<Vec<Value>, String> {
    let data = match std::fs::read_to_string(path) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(format!("Failed to read {}: {}", path.display(), e)),
    };
    if data.trim().is_empty() {
        return Ok(Vec::new());
    }
    match serde_json::from_str::<Value>(&data) {
        Ok(Value::Array(items)) => Ok(items),
        Ok(_) => Err(format!(
            "{} is not a JSON array; refusing to modify it. Fix or move the file and try again.",
            path.display()
        )),
        Err(e) => Err(format!(
            "{} could not be parsed ({}); refusing to overwrite it. Fix or move the file and try again.",
            path.display(),
            e
        )),
    }
}

/// Serialize `entries` and atomically write them to `path`.
pub fn write_entries(path: &Path, entries: &[Value]) -> Result<(), String> {
    let json = serde_json::to_string_pretty(entries).map_err(|e| e.to_string())?;
    write_atomic_private(path, json.as_bytes())
        .map_err(|e| format!("Failed to write {}: {}", path.display(), e))
}

fn entry_id(v: &Value) -> Option<&str> {
    v.get("id").and_then(Value::as_str)
}

/// Insert `new` or replace the entry with the same `id`. Fields of the old
/// entry that `new` does not know about (written by a newer build) are kept.
pub fn upsert_entry(entries: &mut Vec<Value>, new: Value) {
    let id = entry_id(&new).map(str::to_owned);
    let pos = id
        .as_deref()
        .and_then(|id| entries.iter().position(|e| entry_id(e) == Some(id)));
    match pos {
        Some(pos) => {
            let merged = match (std::mem::take(&mut entries[pos]), new) {
                (Value::Object(mut old), Value::Object(new)) => {
                    for (k, v) in new {
                        old.insert(k, v);
                    }
                    Value::Object(old)
                }
                (_, new) => new,
            };
            entries[pos] = merged;
        }
        None => entries.push(new),
    }
}

/// Remove every entry whose `id` equals `id`.
pub fn remove_entry(entries: &mut Vec<Value>, id: &str) {
    entries.retain(|e| entry_id(e) != Some(id));
}

/// Decode every entry that this build understands. Entries that fail to
/// decode are skipped (and logged) but stay in the file untouched.
pub fn decode_entries<T: serde::de::DeserializeOwned>(path: &Path, entries: Vec<Value>) -> Vec<T> {
    entries
        .into_iter()
        .enumerate()
        .filter_map(|(i, v)| {
            let id = entry_id(&v).unwrap_or("?").to_owned();
            match serde_json::from_value::<T>(v) {
                Ok(t) => Some(t),
                Err(e) => {
                    log::warn!(
                        "Skipping entry #{} (id {}) in {}: {}. It is kept in the file.",
                        i,
                        id,
                        path.display(),
                        e
                    );
                    None
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn missing_file_reads_as_empty() {
        let dir = tempfile::tempdir().unwrap();
        assert!(read_entries(&dir.path().join("nope.json"))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn corrupt_file_is_an_error_and_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("c.json");
        std::fs::write(&p, "[{\"id\": \"a\"").unwrap();
        let err = read_entries(&p).unwrap_err();
        assert!(err.contains("refusing to overwrite"), "{err}");
        assert_eq!(std::fs::read_to_string(&p).unwrap(), "[{\"id\": \"a\"");

        std::fs::write(&p, "{\"id\": \"a\"}").unwrap();
        assert!(read_entries(&p).is_err(), "non-array must be rejected");
    }

    #[test]
    fn upsert_preserves_unknown_fields_and_other_entries() {
        let mut entries = vec![
            json!({"id": "a", "name": "old", "future_field": 7}),
            json!({"id": "b", "db_type": "some_future_db"}),
        ];
        upsert_entry(&mut entries, json!({"id": "a", "name": "new"}));
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0]["name"], "new");
        assert_eq!(entries[0]["future_field"], 7);
        assert_eq!(entries[1]["db_type"], "some_future_db");

        upsert_entry(&mut entries, json!({"id": "c"}));
        assert_eq!(entries.len(), 3);
        remove_entry(&mut entries, "a");
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|e| e["id"] != "a"));
    }

    #[test]
    fn atomic_write_replaces_and_is_private() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("store");
        let p = sub.join("f.json");
        write_entries(&p, &[json!({"id": "x"})]).unwrap();
        write_entries(&p, &[json!({"id": "y"})]).unwrap();
        let back = read_entries(&p).unwrap();
        assert_eq!(back, vec![json!({"id": "y"})]);
        // No stray temp files left behind.
        assert_eq!(std::fs::read_dir(&sub).unwrap().count(), 1);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let fmode = std::fs::metadata(&p).unwrap().permissions().mode() & 0o777;
            let dmode = std::fs::metadata(&sub).unwrap().permissions().mode() & 0o777;
            assert_eq!(fmode, 0o600);
            assert_eq!(dmode, 0o700);
        }
    }

    #[cfg(unix)]
    #[test]
    fn existing_loose_permissions_are_tightened() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("loose");
        std::fs::create_dir(&sub).unwrap();
        std::fs::set_permissions(&sub, std::fs::Permissions::from_mode(0o755)).unwrap();
        let p = sub.join("f.json");
        std::fs::write(&p, "[]").unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o644)).unwrap();
        write_entries(&p, &[]).unwrap();
        assert_eq!(
            std::fs::metadata(&p).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(&sub).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }

    #[derive(serde::Deserialize, Debug)]
    struct Strict {
        #[allow(dead_code)]
        id: String,
        kind: Kind,
    }
    #[derive(serde::Deserialize, Debug, PartialEq)]
    #[serde(rename_all = "lowercase")]
    enum Kind {
        Known,
    }

    #[test]
    fn decode_skips_undecodable_entries() {
        let dir = tempfile::tempdir().unwrap();
        let entries = vec![
            json!({"id": "a", "kind": "known"}),
            json!({"id": "b", "kind": "from_the_future"}),
        ];
        let decoded: Vec<Strict> = decode_entries(&dir.path().join("x"), entries);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].kind, Kind::Known);
    }
}
