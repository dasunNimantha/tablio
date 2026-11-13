use crate::db::pool::PoolManager;
use crate::models::*;
use crate::util::json_store::{self, STORE_LOCK};
use crate::util::secrets::{self, SecretField, SecretStore};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tauri::State;

/// Strip any secret from `config` that a driver or library may have echoed
/// back in an error message (e.g. a connection URL), in both its raw and
/// URL-encoded form.
pub(crate) fn redact_secrets(message: String, config: &ConnectionConfig) -> String {
    let mut out = message;
    for secret in [&config.password, &config.ssh_password] {
        // Very short values would mangle ordinary words in the message
        // while protecting next to nothing.
        if secret.chars().count() < 3 {
            continue;
        }
        out = out.replace(secret.as_str(), "****");
        let encoded = urlencoding::encode(secret);
        if encoded != secret.as_str() {
            out = out.replace(encoded.as_ref(), "****");
        }
    }
    out
}

#[tauri::command]
pub async fn test_connection(config: ConnectionConfig) -> Result<bool, String> {
    PoolManager::test_connection(&config)
        .await
        .map_err(|e| redact_secrets(e.to_string(), &config))
}

#[tauri::command]
pub async fn connect(
    pool: State<'_, Arc<PoolManager>>,
    config: ConnectionConfig,
) -> Result<String, String> {
    let for_redaction = config.clone();
    pool.connect(config)
        .await
        .map_err(|e| redact_secrets(e.to_string(), &for_redaction))
}

#[tauri::command]
pub async fn disconnect(
    pool: State<'_, Arc<PoolManager>>,
    connection_id: String,
) -> Result<(), String> {
    pool.disconnect(&connection_id)
        .await
        .map_err(|e| e.to_string())
}

// Persistence of saved connections.
//
// `connections.json` keeps everything except the secrets (`password`,
// `ssh_password`), which go to the OS credential store. For each secret
// field the file records a marker (`password_in_keychain`, ...):
//
// - marker true: the value is in the credential store; the field in the
//   file is blank.
// - marker false/absent: the value (possibly empty) is the field in the
//   file. This is both the legacy format and the fallback when the
//   credential store is unavailable, so a broken keychain never loses a
//   password or makes saving/loading fail.
//
// Plaintext secrets found on load are moved to the store and blanked in
// the file, but only once the store write was read back successfully.
// The markers exist only on disk: the frontend, pools and exports always
// see full `ConnectionConfig`s with real secrets.

fn connections_file() -> Result<PathBuf, String> {
    Ok(json_store::tablio_dir()?.join("connections.json"))
}

fn secret_of(config: &ConnectionConfig, field: SecretField) -> &str {
    match field {
        SecretField::Password => &config.password,
        SecretField::SshPassword => &config.ssh_password,
    }
}

fn marker_set(entry: &Value, field: SecretField) -> bool {
    entry.get(field.marker_key()).and_then(Value::as_bool) == Some(true)
}

/// Store `value` and read it back. True only if the store now verifiably
/// holds it; on failure a possibly half-written value is removed again.
fn put_verified(store: &dyn SecretStore, id: &str, field: SecretField, value: &str) -> bool {
    let result = store
        .set(id, field, value)
        .and_then(|()| store.get(id, field))
        .and_then(|back| match back {
            Some(back) if back == value => Ok(()),
            _ => Err("value read back from the credential store does not match".into()),
        });
    match result {
        Ok(()) => true,
        Err(e) => {
            log::warn!(
                "Could not store the {} of connection {} in the OS credential store ({}); \
                 keeping it in connections.json instead.",
                field.json_key(),
                id,
                e
            );
            let _ = store.delete(id, field);
            false
        }
    }
}

fn load_from(path: &Path, store: &dyn SecretStore) -> Result<Vec<ConnectionConfig>, String> {
    let mut entries = json_store::read_entries(path)?;
    let mut on_disk = entries.clone();
    let mut migrated = false;

    for (entry, disk) in entries.iter_mut().zip(on_disk.iter_mut()) {
        let Some(id) = entry.get("id").and_then(Value::as_str).map(str::to_owned) else {
            continue;
        };
        // Entries this build can't decode are kept verbatim (see json_store).
        let migratable = serde_json::from_value::<ConnectionConfig>(entry.clone()).is_ok();
        let (Value::Object(entry), Value::Object(disk)) = (entry, disk) else {
            continue;
        };
        for field in SecretField::ALL {
            let key = field.json_key();
            if disk.get(field.marker_key()).and_then(Value::as_bool) == Some(true) {
                // Prefer the stored value; if it can't be read, whatever the
                // file holds (normally blank) is all we have.
                match store.get(&id, field) {
                    Ok(Some(v)) => {
                        entry.insert(key.into(), Value::String(v));
                    }
                    Ok(None) => log::warn!(
                        "The {} of connection {} is missing from the OS credential store.",
                        key,
                        id
                    ),
                    Err(e) => log::warn!(
                        "Could not read the {} of connection {} from the OS credential store: {}",
                        key,
                        id,
                        e
                    ),
                }
            } else if let Some(plain) = disk.get(key).and_then(Value::as_str).filter(|_| migratable)
            {
                if !plain.is_empty() && put_verified(store, &id, field, plain) {
                    disk.insert(key.into(), Value::String(String::new()));
                    disk.insert(field.marker_key().into(), Value::Bool(true));
                    migrated = true;
                }
            }
        }
    }

    if migrated {
        // Not fatal: the store already holds the secrets and the file still
        // has them in plaintext, so the next load simply migrates again.
        if let Err(e) = json_store::write_entries(path, &on_disk) {
            log::warn!("Could not remove migrated passwords from connections.json: {e}");
        }
    }
    Ok(json_store::decode_entries(path, entries))
}

fn save_to(path: &Path, config: &ConnectionConfig, store: &dyn SecretStore) -> Result<(), String> {
    let mut entries = json_store::read_entries(path)?;
    let previous = entries
        .iter()
        .find(|e| e.get("id").and_then(Value::as_str) == Some(config.id.as_str()));
    let mut value = serde_json::to_value(config).map_err(|e| e.to_string())?;

    for field in SecretField::ALL {
        let secret = secret_of(config, field);
        let was_stored = previous.is_some_and(|p| marker_set(p, field));
        let stored = if secret.is_empty() {
            match store.delete(&config.id, field) {
                Ok(()) => false,
                Err(e) => {
                    // If the old value can't be removed, keep pointing at it:
                    // a load while the store was down hands the frontend an
                    // empty secret, and re-saving that must not orphan the
                    // real one.
                    if was_stored {
                        log::warn!(
                            "Could not remove the {} of connection {} from the OS credential \
                             store ({}); keeping the stored value.",
                            field.json_key(),
                            config.id,
                            e
                        );
                    }
                    was_stored
                }
            }
        } else {
            put_verified(store, &config.id, field, secret)
        };
        if stored {
            value[field.json_key()] = Value::String(String::new());
        }
        value[field.marker_key()] = Value::Bool(stored);
    }

    json_store::upsert_entry(&mut entries, value);
    json_store::write_entries(path, &entries)
}

fn delete_from(path: &Path, connection_id: &str, store: &dyn SecretStore) -> Result<(), String> {
    let mut entries = json_store::read_entries(path)?;
    let before = entries.len();
    json_store::remove_entry(&mut entries, connection_id);
    if entries.len() != before {
        json_store::write_entries(path, &entries)?;
    }
    for field in SecretField::ALL {
        if let Err(e) = store.delete(connection_id, field) {
            log::warn!(
                "Could not remove the {} of deleted connection {} from the OS credential store: {}",
                field.json_key(),
                connection_id,
                e
            );
        }
    }
    Ok(())
}

/// Run a store operation on a blocking thread: credential store calls
/// block (D-Bus round trips, keychain unlock prompts).
async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, String> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| e.to_string())?
}

#[tauri::command]
pub async fn save_connection(config: ConnectionConfig) -> Result<(), String> {
    let path = connections_file()?;
    let _guard = STORE_LOCK.lock().await;
    blocking(move || save_to(&path, &config, secrets::os_store())).await
}

#[tauri::command]
pub async fn delete_connection(connection_id: String) -> Result<(), String> {
    let path = connections_file()?;
    let _guard = STORE_LOCK.lock().await;
    blocking(move || delete_from(&path, &connection_id, secrets::os_store())).await
}

#[tauri::command]
pub async fn load_connections() -> Result<Vec<ConnectionConfig>, String> {
    let path = connections_file()?;
    let _guard = STORE_LOCK.lock().await;
    blocking(move || load_from(&path, secrets::os_store())).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::secrets::MemoryStore;

    fn cfg(id: &str, name: &str) -> ConnectionConfig {
        ConnectionConfig {
            id: id.into(),
            name: name.into(),
            password: "s3cr3t/p@ss".into(),
            ..Default::default()
        }
    }

    #[test]
    fn save_replaces_by_id_and_keeps_others() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoryStore::new();
        let p = dir.path().join(".tablio").join("connections.json");
        save_to(&p, &cfg("a", "A"), &store).unwrap();
        save_to(&p, &cfg("b", "B"), &store).unwrap();
        save_to(&p, &cfg("a", "A2"), &store).unwrap();
        let all = load_from(&p, &store).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].name, "A2");
        assert_eq!(all[1].name, "B");
        delete_from(&p, "a", &store).unwrap();
        assert_eq!(load_from(&p, &store).unwrap().len(), 1);
    }

    #[test]
    fn unparseable_file_is_an_error_and_never_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoryStore::new();
        let p = dir.path().join("connections.json");
        let original = "[{\"id\": \"a\", \"name\": ";
        std::fs::write(&p, original).unwrap();
        assert!(save_to(&p, &cfg("b", "B"), &store).is_err());
        assert!(delete_from(&p, "a", &store).is_err());
        assert!(load_from(&p, &store).is_err());
        assert_eq!(std::fs::read_to_string(&p).unwrap(), original);
    }

    #[test]
    fn entries_from_newer_build_are_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoryStore::new();
        let p = dir.path().join("connections.json");
        let mut known = serde_json::to_value(cfg("a", "A")).unwrap();
        known["field_from_future"] = serde_json::json!("keep");
        let mut future = serde_json::to_value(cfg("z", "Z")).unwrap();
        future["db_type"] = serde_json::json!("database_from_the_future");
        std::fs::write(
            &p,
            serde_json::to_string(&vec![known, future.clone()]).unwrap(),
        )
        .unwrap();

        // The undecodable entry is hidden from the list...
        let listed = load_from(&p, &store).unwrap();
        assert_eq!(listed.len(), 1);
        // ...but survives a save of another entry, and the unknown field of
        // the updated entry survives too.
        save_to(&p, &cfg("a", "A-renamed"), &store).unwrap();
        let raw = json_store::read_entries(&p).unwrap();
        assert_eq!(raw.len(), 2);
        assert_eq!(raw[0]["name"], "A-renamed");
        assert_eq!(raw[0]["field_from_future"], "keep");
        assert_eq!(raw[1], future);
    }

    #[test]
    fn redact_secrets_masks_raw_and_url_encoded_passwords() {
        let mut c = cfg("a", "A");
        c.ssh_password = "sshpw".into();
        let msg = format!(
            "failed: postgres://u:{}@h/db, also {} and sshpw",
            urlencoding::encode(&c.password),
            c.password
        );
        let out = redact_secrets(msg, &c);
        assert!(!out.contains("s3cr3t"), "{out}");
        assert!(!out.contains("sshpw"), "{out}");
        assert!(out.contains("****"));
    }

    fn with_ssh(id: &str) -> ConnectionConfig {
        ConnectionConfig {
            ssh_enabled: true,
            ssh_password: "ssh-s3cret".into(),
            ..cfg(id, id)
        }
    }

    fn raw(p: &Path) -> Vec<Value> {
        json_store::read_entries(p).unwrap()
    }

    fn file_text(p: &Path) -> String {
        std::fs::read_to_string(p).unwrap()
    }

    /// Accepts writes but drops them, so read-back verification fails.
    struct LossyStore;
    impl SecretStore for LossyStore {
        fn get(&self, _: &str, _: SecretField) -> Result<Option<String>, String> {
            Ok(None)
        }
        fn set(&self, _: &str, _: SecretField, _: &str) -> Result<(), String> {
            Ok(())
        }
        fn delete(&self, _: &str, _: SecretField) -> Result<(), String> {
            Ok(())
        }
    }

    #[test]
    fn secrets_roundtrip_through_the_store_and_never_hit_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoryStore::new();
        let p = dir.path().join("connections.json");
        save_to(&p, &with_ssh("a"), &store).unwrap();

        let text = file_text(&p);
        assert!(!text.contains("s3cr3t"), "{text}");
        let entry = &raw(&p)[0];
        assert_eq!(entry["password"], "");
        assert_eq!(entry["ssh_password"], "");
        assert_eq!(entry["password_in_keychain"], true);
        assert_eq!(entry["ssh_password_in_keychain"], true);
        assert_eq!(store.len(), 2);

        let loaded = load_from(&p, &store).unwrap();
        assert_eq!(loaded[0].password, "s3cr3t/p@ss");
        assert_eq!(loaded[0].ssh_password, "ssh-s3cret");

        // Re-saving a loaded config (rename, duplicate under a new id)
        // keeps working with the real secrets.
        let mut dup = loaded[0].clone();
        dup.id = "b".into();
        dup.name = "copy".into();
        save_to(&p, &dup, &store).unwrap();
        let loaded = load_from(&p, &store).unwrap();
        assert_eq!(loaded.len(), 2);
        assert!(loaded.iter().all(|c| c.password == "s3cr3t/p@ss"));
        assert!(!file_text(&p).contains("s3cr3t"));
    }

    #[test]
    fn clearing_a_secret_removes_it_from_the_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoryStore::new();
        let p = dir.path().join("connections.json");
        save_to(&p, &with_ssh("a"), &store).unwrap();
        let mut c = with_ssh("a");
        c.ssh_password.clear();
        save_to(&p, &c, &store).unwrap();
        assert_eq!(store.len(), 1);
        assert_eq!(raw(&p)[0]["ssh_password_in_keychain"], false);
        assert_eq!(load_from(&p, &store).unwrap()[0].ssh_password, "");
    }

    #[test]
    fn plaintext_secrets_are_migrated_on_load() {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoryStore::new();
        let p = dir.path().join("connections.json");
        // Legacy file: secrets in plaintext, no markers.
        let legacy = vec![
            serde_json::to_value(with_ssh("a")).unwrap(),
            serde_json::to_value(cfg("b", "B")).unwrap(),
        ];
        json_store::write_entries(&p, &legacy).unwrap();

        let loaded = load_from(&p, &store).unwrap();
        assert_eq!(loaded[0].password, "s3cr3t/p@ss");
        assert_eq!(loaded[0].ssh_password, "ssh-s3cret");
        assert_eq!(loaded[1].password, "s3cr3t/p@ss");

        assert!(!file_text(&p).contains("s3cr3t"));
        let entries = raw(&p);
        assert_eq!(entries[0]["password_in_keychain"], true);
        assert_eq!(entries[0]["ssh_password_in_keychain"], true);
        assert_eq!(entries[1]["password_in_keychain"], true);
        // `b` had no SSH password: nothing to migrate.
        assert!(entries[1].get("ssh_password_in_keychain").is_none());
        assert_eq!(store.len(), 3);

        // Loading again reads from the store.
        let again = load_from(&p, &store).unwrap();
        assert_eq!(again[0].ssh_password, "ssh-s3cret");
    }

    #[test]
    fn migration_keeps_plaintext_when_the_store_fails_or_lies() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("connections.json");
        let legacy = vec![serde_json::to_value(with_ssh("a")).unwrap()];
        json_store::write_entries(&p, &legacy).unwrap();
        let before = file_text(&p);

        let failing = MemoryStore::new();
        failing.set_failing(true);
        let loaded = load_from(&p, &failing).unwrap();
        assert_eq!(loaded[0].password, "s3cr3t/p@ss");
        assert_eq!(file_text(&p), before, "file must be untouched");

        let loaded = load_from(&p, &LossyStore).unwrap();
        assert_eq!(loaded[0].ssh_password, "ssh-s3cret");
        assert_eq!(file_text(&p), before, "unverified write must not blank");

        // Once the store works, the next load migrates.
        let store = MemoryStore::new();
        assert_eq!(load_from(&p, &store).unwrap()[0].password, "s3cr3t/p@ss");
        assert!(!file_text(&p).contains("s3cr3t"));
    }

    #[test]
    fn save_falls_back_to_the_file_when_the_store_fails() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("connections.json");
        let store = MemoryStore::new();
        store.set_failing(true);
        save_to(&p, &with_ssh("a"), &store).unwrap();
        let entry = &raw(&p)[0];
        assert_eq!(entry["password"], "s3cr3t/p@ss");
        assert_eq!(entry["password_in_keychain"], false);
        assert_eq!(entry["ssh_password_in_keychain"], false);
        let loaded = load_from(&p, &store).unwrap();
        assert_eq!(loaded[0].password, "s3cr3t/p@ss");
        assert_eq!(loaded[0].ssh_password, "ssh-s3cret");

        save_to(&p, &cfg("b", "B"), &LossyStore).unwrap();
        assert_eq!(raw(&p)[1]["password"], "s3cr3t/p@ss");
        assert_eq!(raw(&p)[1]["password_in_keychain"], false);
    }

    #[test]
    fn store_outage_after_migration_never_orphans_the_secret() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("connections.json");
        let store = MemoryStore::new();
        save_to(&p, &cfg("a", "A"), &store).unwrap();

        // Store goes away: the connection still loads, just without its
        // password, and re-saving that (e.g. a rename) keeps the reference.
        store.set_failing(true);
        let mut loaded = load_from(&p, &store).unwrap();
        assert_eq!(loaded[0].password, "");
        loaded[0].name = "renamed".into();
        save_to(&p, &loaded[0], &store).unwrap();
        assert_eq!(raw(&p)[0]["password_in_keychain"], true);

        store.set_failing(false);
        let back = load_from(&p, &store).unwrap();
        assert_eq!(back[0].name, "renamed");
        assert_eq!(back[0].password, "s3cr3t/p@ss");
    }

    #[test]
    fn delete_removes_store_entries() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("connections.json");
        let store = MemoryStore::new();
        save_to(&p, &with_ssh("a"), &store).unwrap();
        save_to(&p, &with_ssh("b"), &store).unwrap();
        assert_eq!(store.len(), 4);
        delete_from(&p, "a", &store).unwrap();
        assert_eq!(store.len(), 2);
        assert_eq!(store.get("a", SecretField::Password).unwrap(), None);
        assert_eq!(load_from(&p, &store).unwrap()[0].id, "b");

        // A failing store doesn't block deleting the connection itself.
        store.set_failing(true);
        delete_from(&p, "b", &store).unwrap();
        assert!(raw(&p).is_empty());
    }
}
