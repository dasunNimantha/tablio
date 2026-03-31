pub mod commands;
pub mod db;
pub mod export;
pub mod models;

use commands::backup::*;
use commands::connection::*;
use commands::data::*;
use commands::export::*;
use commands::query::*;
use commands::roles::*;
use commands::saved_queries::*;
use commands::schema::*;
use commands::system::*;
use db::pool::PoolManager;
use std::sync::Arc;

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    let pool_manager = Arc::new(PoolManager::new());

    tauri::Builder::default()
        .plugin(tauri_plugin_fs::init())
        .plugin(tauri_plugin_dialog::init())
        .manage(pool_manager)
        .invoke_handler(tauri::generate_handler![
            test_connection,
            connect,
            disconnect,
            save_connection,
            delete_connection,
            load_connections,
            list_databases,
            list_schemas,
            list_tables,
            list_columns,
            list_indexes,
            list_foreign_keys,
            list_functions,
            list_triggers,
            get_table_stats,
            execute_query,
            explain_query,
            validate_query,
            fetch_rows,
            apply_changes,
            get_ddl,
            create_table,
            alter_table,
            drop_object,
            truncate_table,
            import_data,
            get_server_activity,
            get_database_stats,
            get_locks,
            get_server_config,
            cancel_query,
            get_query_stats,
            export_table_data,
            export_table_to_file,
            export_query_result,
            export_query_result_to_file,
            load_saved_queries,
            save_query,
            delete_saved_query,
            list_roles,
            create_role,
            drop_role,
            alter_role,
            backup_database,
            restore_database,
            dump_and_restore,
            get_app_resource_usage,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
