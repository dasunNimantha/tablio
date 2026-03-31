use crate::db::pool::PoolManager;
use crate::models::*;
use std::sync::Arc;
use tauri::State;

#[tauri::command]
pub async fn execute_query(
    pool: State<'_, Arc<PoolManager>>,
    request: ExecuteQueryRequest,
) -> Result<QueryResult, String> {
    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;
    driver
        .execute_query(&request.database, &request.sql)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn explain_query(
    pool: State<'_, Arc<PoolManager>>,
    request: ExecuteQueryRequest,
) -> Result<ExplainResult, String> {
    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;
    driver
        .explain_query(&request.database, &request.sql)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn validate_query(
    pool: State<'_, Arc<PoolManager>>,
    request: ExecuteQueryRequest,
) -> Result<Option<ValidationError>, String> {
    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;
    driver
        .validate_query(&request.database, &request.sql)
        .await
        .map_err(|e| e.to_string())
}
