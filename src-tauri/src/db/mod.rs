pub mod cockroachdb;
pub mod mariadb;
pub mod mysql;
pub mod mysql_common;
pub mod pg_common;
pub mod pool;
pub mod postgres;
pub mod sqlite;
pub mod tidb;

use crate::models::*;
use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
#[allow(clippy::too_many_arguments)]
pub trait DatabaseDriver: Send + Sync {
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>>;
    async fn list_schemas(&self, database: &str) -> Result<Vec<SchemaInfo>>;
    async fn list_tables(&self, database: &str, schema: &str) -> Result<Vec<TableInfo>>;
    async fn list_columns(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ColumnInfo>>;
    async fn list_indexes(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<IndexInfo>>;
    async fn list_foreign_keys(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>>;
    async fn list_functions(&self, database: &str, schema: &str) -> Result<Vec<FunctionInfo>>;
    async fn list_triggers(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<TriggerInfo>>;
    async fn get_table_stats(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<TableStats>;
    async fn fetch_rows(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        offset: u64,
        limit: u64,
        sort: Option<SortSpec>,
        filter: Option<String>,
    ) -> Result<TableData>;
    async fn execute_query(&self, database: &str, sql: &str) -> Result<QueryResult>;
    async fn explain_query(&self, database: &str, sql: &str) -> Result<ExplainResult>;
    async fn get_ddl(
        &self,
        database: &str,
        schema: &str,
        object_name: &str,
        object_type: &str,
    ) -> Result<String>;
    async fn apply_changes(&self, changes: &DataChanges) -> Result<()>;
    async fn create_table(
        &self,
        database: &str,
        schema: &str,
        table_name: &str,
        columns: &[ColumnDefinition],
    ) -> Result<()>;
    async fn alter_table(
        &self,
        database: &str,
        schema: &str,
        table_name: &str,
        operations: &[AlterTableOperation],
    ) -> Result<()>;
    async fn drop_object(
        &self,
        database: &str,
        schema: &str,
        object_name: &str,
        object_type: &str,
    ) -> Result<()>;
    async fn truncate_table(&self, database: &str, schema: &str, table_name: &str) -> Result<()>;
    async fn import_data(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        columns: &[String],
        rows: &[Vec<serde_json::Value>],
    ) -> Result<u64>;
    async fn get_server_activity(&self) -> Result<Vec<ServerActivity>>;
    async fn get_database_stats(&self) -> Result<DatabaseStats>;
    async fn get_locks(&self) -> Result<Vec<LockInfo>>;
    async fn get_server_config(&self) -> Result<Vec<ServerConfigEntry>>;
    async fn get_query_stats(&self) -> Result<QueryStatsResponse>;
    async fn cancel_query(&self, pid: &str) -> Result<()>;
    async fn list_roles(&self) -> Result<Vec<RoleInfo>>;
    async fn create_role(&self, req: &CreateRoleRequest) -> Result<()>;
    async fn drop_role(&self, name: &str) -> Result<()>;
    async fn alter_role(&self, req: &AlterRoleRequest) -> Result<()>;
    async fn test_connection(&self) -> Result<bool>;
}
