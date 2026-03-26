use crate::db::pool::PoolManager;
use crate::models::*;
use std::sync::Arc;
use tauri::State;

#[tauri::command]
pub async fn list_roles(
    pool: State<'_, Arc<PoolManager>>,
    connection_id: String,
) -> Result<Vec<RoleInfo>, String> {
    let driver = pool
        .get_driver(&connection_id)
        .await
        .map_err(|e| e.to_string())?;
    driver.list_roles().await.map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn create_role(
    pool: State<'_, Arc<PoolManager>>,
    request: CreateRoleRequest,
) -> Result<(), String> {
    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;
    driver
        .create_role(&request)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn drop_role(
    pool: State<'_, Arc<PoolManager>>,
    request: DropRoleRequest,
) -> Result<(), String> {
    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;
    driver
        .drop_role(&request.name)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn alter_role(
    pool: State<'_, Arc<PoolManager>>,
    request: AlterRoleRequest,
) -> Result<(), String> {
    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;
    driver.alter_role(&request).await.map_err(|e| e.to_string())
}
