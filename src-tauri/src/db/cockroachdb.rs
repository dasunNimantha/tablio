use anyhow::Result;
use async_trait::async_trait;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use std::time::Instant;

use crate::db::pg_common::*;
use crate::db::DatabaseDriver;
use crate::models::*;

pub struct CockroachdbDriver {
    pool: PgPool,
}

impl CockroachdbDriver {
    pub async fn connect(config: &ConnectionConfig) -> Result<Self> {
        let ssl_mode = if config.ssl { "require" } else { "disable" };
        let url = format!(
            "postgres://{}:{}@{}:{}/{}?sslmode={}",
            config.user, config.password, config.host, config.port, config.database, ssl_mode
        );
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl DatabaseDriver for CockroachdbDriver {
    // Shared PG-wire methods
    async fn list_roles(&self) -> Result<Vec<RoleInfo>> {
        pg_list_roles(&self.pool).await
    }
    async fn create_role(&self, req: &CreateRoleRequest) -> Result<()> {
        pg_create_role(&self.pool, req).await
    }
    async fn drop_role(&self, name: &str) -> Result<()> {
        pg_drop_role(&self.pool, name).await
    }
    async fn alter_role(&self, req: &AlterRoleRequest) -> Result<()> {
        pg_alter_role(&self.pool, req).await
    }
    async fn test_connection(&self) -> Result<bool> {
        pg_test_connection(&self.pool).await
    }
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        pg_list_databases(&self.pool).await
    }
    async fn list_schemas(&self, database: &str) -> Result<Vec<SchemaInfo>> {
        pg_list_schemas(&self.pool, database).await
    }
    async fn list_tables(&self, database: &str, schema: &str) -> Result<Vec<TableInfo>> {
        pg_list_tables(&self.pool, database, schema).await
    }
    async fn list_indexes(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<IndexInfo>> {
        pg_list_indexes(&self.pool, database, schema, table).await
    }
    async fn list_foreign_keys(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        pg_list_foreign_keys(&self.pool, database, schema, table).await
    }
    async fn list_functions(&self, database: &str, schema: &str) -> Result<Vec<FunctionInfo>> {
        pg_list_functions(&self.pool, database, schema).await
    }
    async fn execute_query(&self, database: &str, sql: &str) -> Result<QueryResult> {
        pg_execute_query(&self.pool, database, sql).await
    }
    async fn get_ddl(
        &self,
        database: &str,
        schema: &str,
        object_name: &str,
        object_type: &str,
    ) -> Result<String> {
        pg_get_ddl(&self.pool, database, schema, object_name, object_type).await
    }
    async fn create_table(
        &self,
        database: &str,
        schema: &str,
        table_name: &str,
        columns: &[ColumnDefinition],
    ) -> Result<()> {
        pg_create_table(&self.pool, database, schema, table_name, columns).await
    }
    async fn alter_table(
        &self,
        database: &str,
        schema: &str,
        table_name: &str,
        operations: &[AlterTableOperation],
    ) -> Result<()> {
        pg_alter_table(&self.pool, database, schema, table_name, operations).await
    }
    async fn import_data(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        columns: &[String],
        rows: &[Vec<serde_json::Value>],
    ) -> Result<u64> {
        pg_import_data(&self.pool, database, schema, table, columns, rows).await
    }
    async fn drop_object(
        &self,
        database: &str,
        schema: &str,
        object_name: &str,
        object_type: &str,
    ) -> Result<()> {
        pg_drop_object(&self.pool, database, schema, object_name, object_type).await
    }
    async fn truncate_table(&self, database: &str, schema: &str, table_name: &str) -> Result<()> {
        pg_truncate_table(&self.pool, database, schema, table_name).await
    }
    async fn get_server_activity(&self) -> Result<Vec<ServerActivity>> {
        pg_get_server_activity(&self.pool).await
    }
    async fn cancel_query(&self, pid: &str) -> Result<()> {
        pg_cancel_query(&self.pool, pid).await
    }
    async fn apply_changes(&self, changes: &DataChanges) -> Result<()> {
        pg_apply_changes(&self.pool, changes).await
    }

    // -----------------------------------------------------------------------
    // CockroachDB-specific implementations
    // -----------------------------------------------------------------------

    async fn list_columns(
        &self,
        _database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ColumnInfo>> {
        let rows = sqlx::query(
            "SELECT c.column_name, c.data_type, c.is_nullable, c.column_default, c.ordinal_position, \
                    c.is_identity, c.identity_generation, c.is_generated, \
                    c.character_maximum_length, c.numeric_precision, c.numeric_scale, \
                    CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_pk \
             FROM information_schema.columns c \
             LEFT JOIN ( \
                 SELECT ku.column_name \
                 FROM information_schema.table_constraints tc \
                 JOIN information_schema.key_column_usage ku \
                     ON tc.constraint_name = ku.constraint_name \
                     AND tc.table_schema = ku.table_schema \
                 WHERE tc.constraint_type = 'PRIMARY KEY' \
                   AND tc.table_schema = $1 AND tc.table_name = $2 \
             ) pk ON pk.column_name = c.column_name \
             WHERE c.table_schema = $1 AND c.table_name = $2 \
             ORDER BY c.ordinal_position",
        )
        .bind(schema)
        .bind(table)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|r| {
                let nullable_str: String = r.get("is_nullable");
                let default_val: Option<String> = r.try_get("column_default").ok();
                let is_identity: String = r.get("is_identity");
                let is_generated: String = r.get("is_generated");
                let has_serial_default = default_val
                    .as_deref()
                    .map(|d| d.starts_with("nextval(") || d.starts_with("unique_rowid("))
                    .unwrap_or(false);
                let raw_type: String = r.get("data_type");
                let char_max_len: Option<i32> =
                    r.try_get("character_maximum_length").ok().flatten();
                let num_precision: Option<i32> = r.try_get("numeric_precision").ok().flatten();
                let num_scale: Option<i32> = r.try_get("numeric_scale").ok().flatten();
                let data_type = if let Some(len) = char_max_len {
                    format!("{}({})", raw_type, len)
                } else if raw_type == "numeric" || raw_type == "decimal" {
                    match (num_precision, num_scale) {
                        (Some(p), Some(s)) if s > 0 => format!("{}({},{})", raw_type, p, s),
                        (Some(p), _) => format!("{}({})", raw_type, p),
                        _ => raw_type,
                    }
                } else {
                    raw_type
                };
                // CockroachDB returns ordinal_position as INT8
                let ordinal: i32 = r.get::<i64, _>("ordinal_position") as i32;
                ColumnInfo {
                    name: r.get("column_name"),
                    data_type,
                    is_nullable: nullable_str == "YES",
                    is_primary_key: r.get("is_pk"),
                    default_value: default_val,
                    ordinal_position: ordinal,
                    is_auto_generated: is_identity == "YES"
                        || is_generated == "ALWAYS"
                        || has_serial_default,
                }
            })
            .collect())
    }

    async fn fetch_rows(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        offset: u64,
        limit: u64,
        sort: Option<SortSpec>,
        filter: Option<String>,
    ) -> Result<TableData> {
        let columns = self.list_columns(database, schema, table).await?;
        pg_fetch_rows_impl(
            &self.pool, columns, schema, table, offset, limit, sort, filter,
        )
        .await
    }

    async fn list_triggers(
        &self,
        _database: &str,
        _schema: &str,
        _table: &str,
    ) -> Result<Vec<TriggerInfo>> {
        Ok(vec![])
    }

    async fn explain_query(&self, _database: &str, sql: &str) -> Result<ExplainResult> {
        let start = Instant::now();
        let explain_sql = format!("EXPLAIN {}", sql);
        let rows = sqlx::query(&explain_sql).fetch_all(&self.pool).await?;
        let elapsed = start.elapsed().as_millis() as u64;

        let raw_text = rows
            .iter()
            .filter_map(|r| r.try_get::<String, _>(0).ok())
            .collect::<Vec<_>>()
            .join("\n");

        Ok(ExplainResult {
            plan: ExplainNode {
                node_type: "CockroachDB Plan".to_string(),
                relation: None,
                startup_cost: 0.0,
                total_cost: 0.0,
                actual_time_ms: None,
                rows_estimated: 0,
                rows_actual: None,
                width: 0,
                filter: None,
                children: vec![],
            },
            raw_text,
            execution_time_ms: elapsed,
        })
    }

    async fn get_table_stats(
        &self,
        _database: &str,
        schema: &str,
        table: &str,
    ) -> Result<TableStats> {
        let sql = "SELECT c.relname AS table_name,
                   GREATEST(c.reltuples, 0)::bigint AS row_count,
                   pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
                   pg_size_pretty(pg_indexes_size(c.oid)) AS index_size,
                   pg_size_pretty(pg_relation_size(c.oid)) AS data_size
                   FROM pg_class c
                   JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = $1
                   WHERE c.relname = $2 AND c.relkind = 'r'";
        let row = sqlx::query(sql)
            .bind(schema)
            .bind(table)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Table {}.{} not found", schema, table))?;

        Ok(TableStats {
            table_name: row.get("table_name"),
            row_count: row.get("row_count"),
            total_size: row.get("total_size"),
            index_size: row.get("index_size"),
            data_size: row.get("data_size"),
            last_vacuum: None,
            last_analyze: None,
            dead_tuples: None,
            live_tuples: None,
        })
    }

    async fn get_database_stats(&self) -> Result<DatabaseStats> {
        let conn_row = sqlx::query(
            "SELECT \
                count(*) FILTER (WHERE state = 'active') AS active, \
                count(*) FILTER (WHERE state = 'idle') AS idle, \
                count(*) FILTER (WHERE state = 'idle in transaction') AS idle_tx, \
                count(*) AS total \
            FROM pg_stat_activity",
        )
        .fetch_one(&self.pool)
        .await?;

        let ts_row = sqlx::query("SELECT CAST(EXTRACT(EPOCH FROM now()) * 1000 AS FLOAT8) AS ts")
            .fetch_one(&self.pool)
            .await?;

        Ok(DatabaseStats {
            active_connections: conn_row.get::<i64, _>("active"),
            idle_connections: conn_row.get::<i64, _>("idle"),
            idle_in_transaction: conn_row.get::<i64, _>("idle_tx"),
            total_connections: conn_row.get::<i64, _>("total"),
            xact_commit: 0,
            xact_rollback: 0,
            tup_inserted: 0,
            tup_updated: 0,
            tup_deleted: 0,
            tup_fetched: 0,
            blks_read: 0,
            blks_hit: 0,
            timestamp_ms: ts_row.get::<f64, _>("ts"),
        })
    }

    async fn get_locks(&self) -> Result<Vec<LockInfo>> {
        let rows = sqlx::query(
            "SELECT l.pid, l.locktype, \
                    COALESCE(c.relname, '') AS relation, \
                    l.mode, l.granted, \
                    COALESCE(d.datname, '') AS database, \
                    COALESCE(a.usename, '') AS username, \
                    COALESCE(a.state, '') AS state, \
                    COALESCE(a.query, '') AS query, \
                    EXTRACT(EPOCH FROM (now() - a.query_start)) * 1000 AS duration_ms \
            FROM pg_locks l \
            LEFT JOIN pg_class c ON l.relation = c.oid \
            LEFT JOIN pg_stat_activity a ON l.pid = a.pid \
            LEFT JOIN pg_database d ON l.database = d.oid \
            ORDER BY l.granted, l.pid",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|row| LockInfo {
                pid: row.get::<i32, _>("pid"),
                locktype: row.get::<String, _>("locktype"),
                database: row.get::<String, _>("database"),
                relation: row.get::<String, _>("relation"),
                mode: row.get::<String, _>("mode"),
                granted: row.get::<bool, _>("granted"),
                query: row.get::<String, _>("query"),
                user: row.get::<String, _>("username"),
                state: row.get::<String, _>("state"),
                duration_ms: row.try_get::<f64, _>("duration_ms").ok(),
            })
            .collect())
    }

    async fn get_server_config(&self) -> Result<Vec<ServerConfigEntry>> {
        let rows = sqlx::query(
            "SELECT name, setting, unit, category, short_desc, context, source, pending_restart \
            FROM pg_settings ORDER BY category, name",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|row| ServerConfigEntry {
                name: row.get::<String, _>("name"),
                setting: row.try_get::<String, _>("setting").unwrap_or_default(),
                unit: row.try_get::<String, _>("unit").ok(),
                category: row
                    .try_get::<String, _>("category")
                    .unwrap_or_else(|_| "Uncategorized".to_string()),
                description: row.try_get::<String, _>("short_desc").unwrap_or_default(),
                context: row.try_get::<String, _>("context").unwrap_or_default(),
                source: row.try_get::<String, _>("source").unwrap_or_default(),
                pending_restart: row.try_get::<bool, _>("pending_restart").unwrap_or(false),
            })
            .collect())
    }

    async fn get_query_stats(&self) -> Result<QueryStatsResponse> {
        Ok(QueryStatsResponse {
            available: false,
            message: Some(
                "Query statistics (pg_stat_statements) are not available in CockroachDB. \
                Use the CockroachDB Admin UI for query insights."
                    .to_string(),
            ),
            entries: vec![],
        })
    }
}
