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
            Vec::new(),
            request.filter,
        )
        .await
        .map_err(|e| e.to_string())?;

    let columns: Vec<String> = data.columns.iter().map(|c| c.name.clone()).collect();
    let content = match request.format.as_str() {
        "csv" => export::to_csv(&columns, &data.rows),
        "json" => export::to_json(&columns, &data.rows),
        "sql" => export::to_sql_inserts(
            Some(request.schema.as_str()),
            &request.table,
            &columns,
            &data.rows,
        ),
        // xlsx is binary; the text-IPC variant of this command would
        // require base64 round-tripping. The frontend should call
        // `export_table_to_file` instead, which writes the bytes
        // straight to disk.
        "xlsx" => return Err("xlsx export must use export_table_to_file".to_string()),
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
    let file_path = crate::util::path::expand_tilde(&file_path);

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
            Vec::new(),
            request.filter,
        )
        .await
        .map_err(|e| e.to_string())?;

    let columns: Vec<String> = data.columns.iter().map(|c| c.name.clone()).collect();

    if request.format.as_str() == "xlsx" {
        let bytes = export::to_xlsx(&columns, &data.rows, &request.table)
            .map_err(|e| format!("xlsx encoding failed: {e}"))?;
        std::fs::write(&file_path, bytes).map_err(|e| format!("Failed to write file: {}", e))?;
        return Ok(());
    }

    let content = match request.format.as_str() {
        "csv" => export::to_csv(&columns, &data.rows),
        "json" => export::to_json(&columns, &data.rows),
        "sql" => export::to_sql_inserts(
            Some(request.schema.as_str()),
            &request.table,
            &columns,
            &data.rows,
        ),
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
        // No schema available from the query-result path -- the caller
        // doesn't know which logical table the rows belong to.
        "sql" => export::to_sql_inserts(None, &table_name, &request.columns, &request.rows),
        "xlsx" => return Err("xlsx export must use export_query_result_to_file".to_string()),
        _ => return Err(format!("Unsupported format: {}", request.format)),
    };
    Ok(content)
}

#[tauri::command]
pub async fn export_query_result_to_file(
    request: ExportResultRequest,
    file_path: String,
) -> Result<(), String> {
    let file_path = crate::util::path::expand_tilde(&file_path);

    let table_name = request
        .table_name
        .unwrap_or_else(|| "query_result".to_string());

    if request.format.as_str() == "xlsx" {
        let bytes = export::to_xlsx(&request.columns, &request.rows, &table_name)
            .map_err(|e| format!("xlsx encoding failed: {e}"))?;
        std::fs::write(&file_path, bytes).map_err(|e| format!("Failed to write file: {}", e))?;
        return Ok(());
    }

    let content = match request.format.as_str() {
        "csv" => export::to_csv(&request.columns, &request.rows),
        "json" => export::to_json(&request.columns, &request.rows),
        "sql" => export::to_sql_inserts(None, &table_name, &request.columns, &request.rows),
        _ => return Err(format!("Unsupported format: {}", request.format)),
    };

    std::fs::write(&file_path, content).map_err(|e| format!("Failed to write file: {}", e))?;

    Ok(())
}
