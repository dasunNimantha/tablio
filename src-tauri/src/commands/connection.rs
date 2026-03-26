use crate::db::pool::PoolManager;
use crate::models::*;
use std::sync::Arc;
use tauri::State;

#[tauri::command]
pub async fn test_connection(config: ConnectionConfig) -> Result<bool, String> {
    PoolManager::test_connection(&config)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn connect(
    pool: State<'_, Arc<PoolManager>>,
    config: ConnectionConfig,
) -> Result<String, String> {
    pool.connect(config).await.map_err(|e| e.to_string())
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

#[tauri::command]
pub async fn save_connection(config: ConnectionConfig) -> Result<(), String> {
    let config_dir = dirs::home_dir()
        .ok_or("Cannot determine home directory")?
        .join(".tablio");
    std::fs::create_dir_all(&config_dir).map_err(|e| e.to_string())?;
    let path = config_dir.join("connections.json");

    let mut connections: Vec<ConnectionConfig> = if path.exists() {
        let data = std::fs::read_to_string(&path).map_err(|e| e.to_string())?;
        serde_json::from_str(&data).unwrap_or_default()
    } else {
        vec![]
    };

    if let Some(pos) = connections.iter().position(|c| c.id == config.id) {
        connections[pos] = config;
    } else {
        connections.push(config);
    }

    let json = serde_json::to_string_pretty(&connections).map_err(|e| e.to_string())?;
    std::fs::write(&path, json).map_err(|e| e.to_string())?;
    Ok(())
}

#[tauri::command]
pub async fn delete_connection(connection_id: String) -> Result<(), String> {
    let config_dir = dirs::home_dir()
        .ok_or("Cannot determine home directory")?
        .join(".tablio");
    let path = config_dir.join("connections.json");

    if !path.exists() {
        return Ok(());
    }

    let data = std::fs::read_to_string(&path).map_err(|e| e.to_string())?;
    let mut connections: Vec<ConnectionConfig> = serde_json::from_str(&data).unwrap_or_default();
    connections.retain(|c| c.id != connection_id);

    let json = serde_json::to_string_pretty(&connections).map_err(|e| e.to_string())?;
    std::fs::write(&path, json).map_err(|e| e.to_string())?;
    Ok(())
}

#[tauri::command]
pub async fn load_connections() -> Result<Vec<ConnectionConfig>, String> {
    let config_dir = dirs::home_dir()
        .ok_or("Cannot determine home directory")?
        .join(".tablio");
    let path = config_dir.join("connections.json");

    if !path.exists() {
        return Ok(vec![]);
    }

    let data = std::fs::read_to_string(&path).map_err(|e| e.to_string())?;
    serde_json::from_str(&data).map_err(|e| e.to_string())
}
