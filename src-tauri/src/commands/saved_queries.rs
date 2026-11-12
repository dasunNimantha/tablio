use crate::models::*;
use crate::util::json_store::{self, STORE_LOCK};
use std::path::{Path, PathBuf};

fn queries_file() -> Result<PathBuf, String> {
    Ok(json_store::tablio_dir()?.join("saved_queries.json"))
}

fn load_from(path: &Path) -> Result<Vec<SavedQuery>, String> {
    let entries = json_store::read_entries(path)?;
    Ok(json_store::decode_entries(path, entries))
}

fn save_to(path: &Path, query: &SavedQuery) -> Result<(), String> {
    let mut entries = json_store::read_entries(path)?;
    let value = serde_json::to_value(query).map_err(|e| e.to_string())?;
    json_store::upsert_entry(&mut entries, value);
    json_store::write_entries(path, &entries)
}

fn delete_from(path: &Path, query_id: &str) -> Result<(), String> {
    let mut entries = json_store::read_entries(path)?;
    let before = entries.len();
    json_store::remove_entry(&mut entries, query_id);
    if entries.len() == before {
        return Ok(());
    }
    json_store::write_entries(path, &entries)
}

#[tauri::command]
pub async fn load_saved_queries() -> Result<Vec<SavedQuery>, String> {
    let path = queries_file()?;
    let _guard = STORE_LOCK.lock().await;
    load_from(&path)
}

#[tauri::command]
pub async fn save_query(query: SavedQuery) -> Result<(), String> {
    let path = queries_file()?;
    let _guard = STORE_LOCK.lock().await;
    save_to(&path, &query)
}

#[tauri::command]
pub async fn delete_saved_query(query_id: String) -> Result<(), String> {
    let path = queries_file()?;
    let _guard = STORE_LOCK.lock().await;
    delete_from(&path, &query_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(id: &str, name: &str) -> SavedQuery {
        SavedQuery {
            id: id.into(),
            name: name.into(),
            sql: "select 1".into(),
            connection_id: None,
            database: None,
            created_at: 1,
            updated_at: 2,
        }
    }

    #[test]
    fn save_update_delete_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join(".tablio").join("saved_queries.json");
        save_to(&p, &q("a", "first")).unwrap();
        save_to(&p, &q("b", "second")).unwrap();
        save_to(&p, &q("a", "renamed")).unwrap();
        let all = load_from(&p).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].name, "renamed");
        delete_from(&p, "a").unwrap();
        let all = load_from(&p).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].id, "b");
    }

    #[test]
    fn corrupt_file_is_not_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("saved_queries.json");
        std::fs::write(&p, "not json").unwrap();
        assert!(save_to(&p, &q("a", "x")).is_err());
        assert!(delete_from(&p, "a").is_err());
        assert!(load_from(&p).is_err());
        assert_eq!(std::fs::read_to_string(&p).unwrap(), "not json");
    }
}
