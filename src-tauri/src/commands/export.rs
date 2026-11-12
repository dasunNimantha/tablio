use crate::db::pool::PoolManager;
use crate::db::DatabaseDriver;
use crate::export;
use crate::models::*;
use std::sync::Arc;
use tauri::State;

/// Rows fetched per round trip when paging through a table.
const PAGE_SIZE: u64 = 50_000;

/// Upper bound for exports that have to travel back over IPC as one
/// string, and for tables that can't be paged deterministically.
const MAX_UNPAGED_ROWS: u64 = 1_000_000;

/// Fetch every row matched by `request`, or fail; never truncate silently.
///
/// With a primary key the table is paged in PK order, which is stable
/// across pages. Without one (or on Cassandra, which can't sort by
/// arbitrary columns) OFFSET paging could skip or repeat rows, so a single
/// fetch is used and the export fails if it would exceed
/// [`MAX_UNPAGED_ROWS`]. `max_rows` caps the total for callers that must
/// hold everything in one IPC payload.
async fn fetch_all_rows(
    driver: &Arc<dyn DatabaseDriver>,
    db_type: &DbType,
    request: &ExportRequest,
    max_rows: Option<u64>,
) -> Result<TableData, String> {
    let pk_sort: Vec<SortSpec> = if matches!(db_type, DbType::Cassandra) {
        Vec::new()
    } else {
        driver
            .list_columns(&request.database, &request.schema, &request.table)
            .await
            .map(|cols| {
                cols.into_iter()
                    .filter(|c| c.is_primary_key)
                    .map(|c| SortSpec {
                        column: c.name,
                        direction: SortDirection::Asc,
                    })
                    .collect()
            })
            .unwrap_or_default()
    };

    let too_many = |limit: u64| {
        format!(
            "'{}' has more than {} matching rows; the export was stopped instead of being \
             truncated. Narrow it down with a filter{}.",
            request.table,
            limit,
            if pk_sort.is_empty() {
                " (tables without a primary key can't be paged reliably)"
            } else {
                ", or export to a file"
            }
        )
    };

    if pk_sort.is_empty() {
        let limit = max_rows.unwrap_or(MAX_UNPAGED_ROWS).min(MAX_UNPAGED_ROWS);
        let data = driver
            .fetch_rows(
                &request.database,
                &request.schema,
                &request.table,
                0,
                limit + 1,
                Vec::new(),
                request.filter.clone(),
            )
            .await
            .map_err(|e| e.to_string())?;
        if data.rows.len() as u64 > limit {
            return Err(too_many(limit));
        }
        return Ok(data);
    }

    let mut offset = 0u64;
    let mut all: Option<TableData> = None;
    loop {
        let page = driver
            .fetch_rows(
                &request.database,
                &request.schema,
                &request.table,
                offset,
                PAGE_SIZE,
                pk_sort.clone(),
                request.filter.clone(),
            )
            .await
            .map_err(|e| e.to_string())?;
        let got = page.rows.len() as u64;
        offset += got;
        match all.as_mut() {
            None => all = Some(page),
            Some(acc) => acc.rows.extend(page.rows),
        }
        if let Some(max) = max_rows {
            if offset > max {
                return Err(too_many(max));
            }
        }
        if got < PAGE_SIZE {
            break;
        }
    }
    let mut data = all.expect("at least one page fetched");
    data.offset = 0;
    data.limit = offset;
    Ok(data)
}

fn column_names(data: &TableData) -> Vec<String> {
    data.columns.iter().map(|c| c.name.clone()).collect()
}

fn numeric_flags(data: &TableData) -> Vec<bool> {
    data.columns
        .iter()
        .map(|c| export::is_numeric_type(&c.data_type))
        .collect()
}

#[tauri::command]
pub async fn export_table_data(
    pool: State<'_, Arc<PoolManager>>,
    request: ExportRequest,
) -> Result<String, String> {
    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;
    let db_type = pool
        .get_config(&request.connection_id)
        .await
        .map(|c| c.db_type)
        .unwrap_or_default();

    // xlsx is binary; the text-IPC variant of this command would require
    // base64 round-tripping. The frontend should call
    // `export_table_to_file` instead, which writes the bytes straight to
    // disk.
    if !matches!(request.format.as_str(), "csv" | "json" | "sql") {
        return Err(match request.format.as_str() {
            "xlsx" => "xlsx export must use export_table_to_file".to_string(),
            other => format!("Unsupported format: {}", other),
        });
    }

    let data = fetch_all_rows(&driver, &db_type, &request, Some(MAX_UNPAGED_ROWS)).await?;
    let columns = column_names(&data);
    let content = match request.format.as_str() {
        "csv" => export::to_csv(&columns, &data.rows),
        "json" => export::to_json(&columns, &data.rows),
        _ => export::to_sql_inserts_typed(
            Some(request.schema.as_str()),
            &request.table,
            &columns,
            &numeric_flags(&data),
            &data.rows,
        ),
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

    if !matches!(request.format.as_str(), "csv" | "json" | "sql" | "xlsx") {
        return Err(format!("Unsupported format: {}", request.format));
    }

    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;
    let db_type = pool
        .get_config(&request.connection_id)
        .await
        .map(|c| c.db_type)
        .unwrap_or_default();

    let data = fetch_all_rows(&driver, &db_type, &request, None).await?;
    let columns = column_names(&data);

    if request.format.as_str() == "xlsx" {
        let bytes = export::to_xlsx(&columns, &data.rows, &request.table)
            .map_err(|e| format!("xlsx encoding failed: {e}"))?;
        std::fs::write(&file_path, bytes).map_err(|e| format!("Failed to write file: {}", e))?;
        return Ok(());
    }

    let content = match request.format.as_str() {
        "csv" => export::to_csv(&columns, &data.rows),
        "json" => export::to_json(&columns, &data.rows),
        _ => export::to_sql_inserts_typed(
            Some(request.schema.as_str()),
            &request.table,
            &columns,
            &numeric_flags(&data),
            &data.rows,
        ),
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
