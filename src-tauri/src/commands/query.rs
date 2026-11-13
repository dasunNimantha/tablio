use crate::db::cancel::CancelRegistry;
use crate::db::pool::PoolManager;
use crate::models::*;
use std::sync::Arc;
use tauri::State;

#[tauri::command]
pub async fn execute_query(
    pool: State<'_, Arc<PoolManager>>,
    cancels: State<'_, Arc<CancelRegistry>>,
    request: ExecuteQueryRequest,
) -> Result<QueryResult, String> {
    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;
    match request.request_id.as_deref() {
        Some(id) => {
            // Unregistered when dropped, however the run ends.
            let registration = cancels.register(id);
            driver
                .execute_query_cancellable(
                    &request.database,
                    &request.sql,
                    registration.token.clone(),
                )
                .await
                .map_err(|e| e.to_string())
        }
        None => driver
            .execute_query(&request.database, &request.sql)
            .await
            .map_err(|e| e.to_string()),
    }
}

/// Cancel the console run started with `request_id`. Returns whether it
/// was still running; the execute call itself then resolves to the
/// "Query cancelled" error once the driver has cleaned up.
#[tauri::command]
pub async fn cancel_execution(
    cancels: State<'_, Arc<CancelRegistry>>,
    request_id: String,
) -> Result<bool, String> {
    Ok(cancels.cancel(&request_id))
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
