use crate::db::pool::PoolManager;
use crate::export;
use crate::models::*;
use std::sync::Arc;
use tauri::State;

#[tauri::command]
pub async fn export_table_data(
    pool: State<'_, Arc<PoolManager>>,
    request: ExportRequest,
) -> Result<String, String> {
    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;

    let data = driver
        .fetch_rows(
            &request.database,
            &request.schema,
            &request.table,
            0,
            1_000_000,
            None,
            request.filter,
        )
        .await
        .map_err(|e| e.to_string())?;

    let columns: Vec<String> = data.columns.iter().map(|c| c.name.clone()).collect();
    let content = match request.format.as_str() {
        "csv" => export::to_csv(&columns, &data.rows),
        "json" => export::to_json(&columns, &data.rows),
        "sql" => export::to_sql_inserts(&request.table, &columns, &data.rows),
        _ => return Err(format!("Unsupported format: {}", request.format)),
    };

    Ok(content)
}

#[tauri::command]
pub async fn export_table_to_file(
    pool: State<'_, Arc<PoolManager>>,
    request: ExportRequest,
    file_path: String,
) -> Result<(), String> {
    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;

    let data = driver
        .fetch_rows(
            &request.database,
            &request.schema,
            &request.table,
            0,
            1_000_000,
            None,
            request.filter,
        )
        .await
        .map_err(|e| e.to_string())?;

    let columns: Vec<String> = data.columns.iter().map(|c| c.name.clone()).collect();
    let content = match request.format.as_str() {
        "csv" => export::to_csv(&columns, &data.rows),
        "json" => export::to_json(&columns, &data.rows),
        "sql" => export::to_sql_inserts(&request.table, &columns, &data.rows),
        _ => return Err(format!("Unsupported format: {}", request.format)),
    };

    std::fs::write(&file_path, content).map_err(|e| format!("Failed to write file: {}", e))?;

    Ok(())
}

#[tauri::command]
pub async fn export_query_result(request: ExportResultRequest) -> Result<String, String> {
    let table_name = request
        .table_name
        .unwrap_or_else(|| "query_result".to_string());
    let content = match request.format.as_str() {
        "csv" => export::to_csv(&request.columns, &request.rows),
        "json" => export::to_json(&request.columns, &request.rows),
        "sql" => export::to_sql_inserts(&table_name, &request.columns, &request.rows),
        _ => return Err(format!("Unsupported format: {}", request.format)),
    };
    Ok(content)
}

#[tauri::command]
pub async fn export_query_result_to_file(
    request: ExportResultRequest,
    file_path: String,
) -> Result<(), String> {
    let table_name = request
        .table_name
        .unwrap_or_else(|| "query_result".to_string());
    let content = match request.format.as_str() {
        "csv" => export::to_csv(&request.columns, &request.rows),
        "json" => export::to_json(&request.columns, &request.rows),
        "sql" => export::to_sql_inserts(&table_name, &request.columns, &request.rows),
        _ => return Err(format!("Unsupported format: {}", request.format)),
    };

    std::fs::write(&file_path, content).map_err(|e| format!("Failed to write file: {}", e))?;

    Ok(())
}
