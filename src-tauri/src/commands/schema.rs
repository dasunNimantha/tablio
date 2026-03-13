use crate::db::pool::PoolManager;
use crate::models::*;
use std::sync::Arc;
use tauri::State;

#[tauri::command]
pub async fn get_ddl(
    pool: State<'_, Arc<PoolManager>>,
    request: GetDdlRequest,
) -> Result<String, String> {
    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;
    driver
        .get_ddl(&request.database, &request.schema, &request.object_name, &request.object_type)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn create_table(
    pool: State<'_, Arc<PoolManager>>,
    request: CreateTableRequest,
) -> Result<(), String> {
    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;
    driver
        .create_table(&request.database, &request.schema, &request.table_name, &request.columns)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn list_databases(
    pool: State<'_, Arc<PoolManager>>,
    connection_id: String,
) -> Result<Vec<DatabaseInfo>, String> {
    let driver = pool.get_driver(&connection_id).await.map_err(|e| e.to_string())?;
    driver.list_databases().await.map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn list_schemas(
    pool: State<'_, Arc<PoolManager>>,
    connection_id: String,
    database: String,
) -> Result<Vec<SchemaInfo>, String> {
    let driver = pool.get_driver(&connection_id).await.map_err(|e| e.to_string())?;
    driver.list_schemas(&database).await.map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn list_tables(
    pool: State<'_, Arc<PoolManager>>,
    connection_id: String,
    database: String,
    schema: String,
) -> Result<Vec<TableInfo>, String> {
    let driver = pool.get_driver(&connection_id).await.map_err(|e| e.to_string())?;
    driver.list_tables(&database, &schema).await.map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn list_columns(
    pool: State<'_, Arc<PoolManager>>,
    connection_id: String,
    database: String,
    schema: String,
    table: String,
) -> Result<Vec<ColumnInfo>, String> {
    let driver = pool.get_driver(&connection_id).await.map_err(|e| e.to_string())?;
    driver
        .list_columns(&database, &schema, &table)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn list_indexes(
    pool: State<'_, Arc<PoolManager>>,
    connection_id: String,
    database: String,
    schema: String,
    table: String,
) -> Result<Vec<IndexInfo>, String> {
    let driver = pool.get_driver(&connection_id).await.map_err(|e| e.to_string())?;
    driver
        .list_indexes(&database, &schema, &table)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn list_foreign_keys(
    pool: State<'_, Arc<PoolManager>>,
    connection_id: String,
    database: String,
    schema: String,
    table: String,
) -> Result<Vec<ForeignKeyInfo>, String> {
    let driver = pool.get_driver(&connection_id).await.map_err(|e| e.to_string())?;
    driver
        .list_foreign_keys(&database, &schema, &table)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn alter_table(
    pool: State<'_, Arc<PoolManager>>,
    request: AlterTableRequest,
) -> Result<(), String> {
    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;
    driver
        .alter_table(&request.database, &request.schema, &request.table_name, &request.operations)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn list_functions(
    pool: State<'_, Arc<PoolManager>>,
    connection_id: String,
    database: String,
    schema: String,
) -> Result<Vec<FunctionInfo>, String> {
    let driver = pool.get_driver(&connection_id).await.map_err(|e| e.to_string())?;
    driver
        .list_functions(&database, &schema)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn list_triggers(
    pool: State<'_, Arc<PoolManager>>,
    connection_id: String,
    database: String,
    schema: String,
    table: String,
) -> Result<Vec<TriggerInfo>, String> {
    let driver = pool.get_driver(&connection_id).await.map_err(|e| e.to_string())?;
    driver
        .list_triggers(&database, &schema, &table)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn get_table_stats(
    pool: State<'_, Arc<PoolManager>>,
    connection_id: String,
    database: String,
    schema: String,
    table: String,
) -> Result<TableStats, String> {
    let driver = pool.get_driver(&connection_id).await.map_err(|e| e.to_string())?;
    driver
        .get_table_stats(&database, &schema, &table)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn import_data(
    pool: State<'_, Arc<PoolManager>>,
    request: ImportDataRequest,
) -> Result<u64, String> {
    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;
    driver
        .import_data(&request.database, &request.schema, &request.table, &request.columns, &request.rows)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn drop_object(
    pool: State<'_, Arc<PoolManager>>,
    request: DropObjectRequest,
) -> Result<(), String> {
    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;
    driver
        .drop_object(&request.database, &request.schema, &request.object_name, &request.object_type)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn truncate_table(
    pool: State<'_, Arc<PoolManager>>,
    request: TruncateTableRequest,
) -> Result<(), String> {
    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;
    driver
        .truncate_table(&request.database, &request.schema, &request.table_name)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn get_server_activity(
    pool: State<'_, Arc<PoolManager>>,
    request: ServerActivityRequest,
) -> Result<Vec<ServerActivity>, String> {
    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;
    driver
        .get_server_activity()
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn cancel_query(
    pool: State<'_, Arc<PoolManager>>,
    request: CancelQueryRequest,
) -> Result<(), String> {
    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;
    driver
        .cancel_query(&request.pid)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn get_database_stats(
    pool: State<'_, Arc<PoolManager>>,
    request: ServerActivityRequest,
) -> Result<DatabaseStats, String> {
    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;
    driver
        .get_database_stats()
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn get_locks(
    pool: State<'_, Arc<PoolManager>>,
    request: ServerActivityRequest,
) -> Result<Vec<LockInfo>, String> {
    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;
    driver
        .get_locks()
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn get_server_config(
    pool: State<'_, Arc<PoolManager>>,
    request: ServerActivityRequest,
) -> Result<Vec<ServerConfigEntry>, String> {
    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;
    driver
        .get_server_config()
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn get_query_stats(
    pool: State<'_, Arc<PoolManager>>,
    request: QueryStatsRequest,
) -> Result<QueryStatsResponse, String> {
    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;
    driver
        .get_query_stats()
        .await
        .map_err(|e| e.to_string())
}
