use crate::models::*;
use std::path::PathBuf;

fn queries_file() -> Result<PathBuf, String> {
    let home = dirs::home_dir().ok_or("Cannot determine home directory")?;
    Ok(home.join(".dbstudio").join("saved_queries.json"))
}

fn ensure_dir() -> Result<(), String> {
    let path = queries_file()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    Ok(())
}

#[tauri::command]
pub async fn load_saved_queries() -> Result<Vec<SavedQuery>, String> {
    let path = queries_file()?;
    if !path.exists() {
        return Ok(vec![]);
    }
    let data = std::fs::read_to_string(&path).map_err(|e| e.to_string())?;
    let queries: Vec<SavedQuery> = serde_json::from_str(&data).map_err(|e| e.to_string())?;
    Ok(queries)
}

#[tauri::command]
pub async fn save_query(query: SavedQuery) -> Result<(), String> {
    ensure_dir()?;
    let mut queries = load_saved_queries().await.unwrap_or_default();
    if let Some(pos) = queries.iter().position(|q| q.id == query.id) {
        queries[pos] = query;
    } else {
        queries.push(query);
    }
    let data = serde_json::to_string_pretty(&queries).map_err(|e| e.to_string())?;
    std::fs::write(queries_file()?, data).map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn delete_saved_query(query_id: String) -> Result<(), String> {
    let mut queries = load_saved_queries().await.unwrap_or_default();
    queries.retain(|q| q.id != query_id);
    let data = serde_json::to_string_pretty(&queries).map_err(|e| e.to_string())?;
    std::fs::write(queries_file()?, data).map_err(|e| e.to_string())
}
