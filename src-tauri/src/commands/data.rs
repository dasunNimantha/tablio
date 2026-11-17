use crate::db::pool::PoolManager;
use crate::models::*;
use std::sync::Arc;
use tauri::State;

#[tauri::command]
pub async fn fetch_rows(
    pool: State<'_, Arc<PoolManager>>,
    request: FetchRowsRequest,
) -> Result<TableData, String> {
    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;
    if !request.count {
        // Same page fetch, minus the COUNT(*) (a full scan on large
        // tables, and on Cassandra).
        let columns = driver
            .list_columns(&request.database, &request.schema, &request.table)
            .await
            .map_err(|e| e.to_string())?;
        let mut data = driver
            .fetch_export_page(
                &request.database,
                &request.schema,
                &request.table,
                &columns,
                request.offset,
                request.limit,
                request.sort,
                request.filter,
            )
            .await
            .map_err(|e| e.to_string())?;
        data.total_rows = -1;
        return Ok(data);
    }
    driver
        .fetch_rows(
            &request.database,
            &request.schema,
            &request.table,
            request.offset,
            request.limit,
            request.sort,
            request.filter,
        )
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn apply_changes(
    pool: State<'_, Arc<PoolManager>>,
    changes: DataChanges,
) -> Result<(), String> {
    let driver = pool
        .get_driver(&changes.connection_id)
        .await
        .map_err(|e| e.to_string())?;
    driver
        .apply_changes(&changes)
        .await
        .map_err(|e| e.to_string())
}
