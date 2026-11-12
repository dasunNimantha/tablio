use crate::db::pool::PoolManager;
use crate::models::*;
use crate::util::json_store::{self, STORE_LOCK};
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

fn connections_file() -> Result<PathBuf, String> {
    Ok(json_store::tablio_dir()?.join("connections.json"))
}

fn load_from(path: &Path) -> Result<Vec<ConnectionConfig>, String> {
    let entries = json_store::read_entries(path)?;
    Ok(json_store::decode_entries(path, entries))
}

fn save_to(path: &Path, config: &ConnectionConfig) -> Result<(), String> {
    let mut entries = json_store::read_entries(path)?;
    let value = serde_json::to_value(config).map_err(|e| e.to_string())?;
    json_store::upsert_entry(&mut entries, value);
    json_store::write_entries(path, &entries)
}

fn delete_from(path: &Path, connection_id: &str) -> Result<(), String> {
    let mut entries = json_store::read_entries(path)?;
    let before = entries.len();
    json_store::remove_entry(&mut entries, connection_id);
    if entries.len() == before {
        return Ok(());
    }
    json_store::write_entries(path, &entries)
}

#[tauri::command]
pub async fn save_connection(config: ConnectionConfig) -> Result<(), String> {
    let path = connections_file()?;
    let _guard = STORE_LOCK.lock().await;
    save_to(&path, &config)
}

#[tauri::command]
pub async fn delete_connection(connection_id: String) -> Result<(), String> {
    let path = connections_file()?;
    let _guard = STORE_LOCK.lock().await;
    delete_from(&path, &connection_id)
}

#[tauri::command]
pub async fn load_connections() -> Result<Vec<ConnectionConfig>, String> {
    let path = connections_file()?;
    let _guard = STORE_LOCK.lock().await;
    load_from(&path)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let p = dir.path().join(".tablio").join("connections.json");
        save_to(&p, &cfg("a", "A")).unwrap();
        save_to(&p, &cfg("b", "B")).unwrap();
        save_to(&p, &cfg("a", "A2")).unwrap();
        let all = load_from(&p).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].name, "A2");
        assert_eq!(all[1].name, "B");
        delete_from(&p, "a").unwrap();
        assert_eq!(load_from(&p).unwrap().len(), 1);
    }

    #[test]
    fn unparseable_file_is_an_error_and_never_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("connections.json");
        let original = "[{\"id\": \"a\", \"name\": ";
        std::fs::write(&p, original).unwrap();
        assert!(save_to(&p, &cfg("b", "B")).is_err());
        assert!(delete_from(&p, "a").is_err());
        assert!(load_from(&p).is_err());
        assert_eq!(std::fs::read_to_string(&p).unwrap(), original);
    }

    #[test]
    fn entries_from_newer_build_are_preserved() {
        let dir = tempfile::tempdir().unwrap();
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
        let listed = load_from(&p).unwrap();
        assert_eq!(listed.len(), 1);
        // ...but survives a save of another entry, and the unknown field of
        // the updated entry survives too.
        save_to(&p, &cfg("a", "A-renamed")).unwrap();
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
}
