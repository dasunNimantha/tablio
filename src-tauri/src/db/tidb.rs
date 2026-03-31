use anyhow::Result;
use async_trait::async_trait;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{Column, MySqlPool, Row};
use std::time::{Duration, Instant};

use crate::db::mysql_common::*;
use crate::db::DatabaseDriver;
use crate::models::*;

pub struct TidbDriver {
    pool: MySqlPool,
}

impl TidbDriver {
    pub async fn connect(config: &ConnectionConfig) -> Result<Self> {
        let ssl_mode = if config.ssl {
            if config.trust_server_cert {
                "REQUIRED"
            } else {
                "VERIFY_IDENTITY"
            }
        } else {
            "PREFERRED"
        };
        let db_segment = if config.database.trim().is_empty() {
            String::new()
        } else {
            format!("/{}", urlencoding::encode(&config.database))
        };
        let url = format!(
            "mysql://{}:{}@{}:{}{}?ssl-mode={}",
            urlencoding::encode(&config.user),
            urlencoding::encode(&config.password),
            &config.host,
            config.port,
            db_segment,
            ssl_mode
        );
        let pool = MySqlPoolOptions::new()
            .max_connections(4)
            .min_connections(0)
            .idle_timeout(Duration::from_secs(1800))
            .connect(&url)
            .await?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl DatabaseDriver for TidbDriver {
    // Shared MySQL-wire methods
    async fn list_roles(&self) -> Result<Vec<RoleInfo>> {
        my_list_roles(&self.pool).await
    }
    async fn create_role(&self, req: &CreateRoleRequest) -> Result<()> {
        my_create_role(&self.pool, req).await
    }
    async fn drop_role(&self, name: &str) -> Result<()> {
        my_drop_role(&self.pool, name).await
    }
    async fn alter_role(&self, req: &AlterRoleRequest) -> Result<()> {
        my_alter_role(&self.pool, req).await
    }
    async fn test_connection(&self) -> Result<bool> {
        my_test_connection(&self.pool).await
    }
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        my_list_databases(&self.pool).await
    }
    async fn list_schemas(&self, database: &str) -> Result<Vec<SchemaInfo>> {
        my_list_schemas(&self.pool, database).await
    }
    async fn list_tables(&self, database: &str, schema: &str) -> Result<Vec<TableInfo>> {
        my_list_tables(&self.pool, database, schema).await
    }
    async fn list_columns(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ColumnInfo>> {
        my_list_columns(&self.pool, database, schema, table).await
    }
    async fn list_indexes(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<IndexInfo>> {
        my_list_indexes(&self.pool, database, schema, table).await
    }
    async fn list_foreign_keys(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        my_list_foreign_keys(&self.pool, database, schema, table).await
    }
    async fn execute_query(&self, database: &str, sql: &str) -> Result<QueryResult> {
        my_execute_query(&self.pool, database, sql).await
    }
    async fn get_ddl(
        &self,
        database: &str,
        schema: &str,
        object_name: &str,
        object_type: &str,
    ) -> Result<String> {
        my_get_ddl(&self.pool, database, schema, object_name, object_type).await
    }
    async fn create_table(
        &self,
        database: &str,
        schema: &str,
        table_name: &str,
        columns: &[ColumnDefinition],
    ) -> Result<()> {
        my_create_table(&self.pool, database, schema, table_name, columns).await
    }
    async fn alter_table(
        &self,
        database: &str,
        schema: &str,
        table_name: &str,
        operations: &[AlterTableOperation],
    ) -> Result<()> {
        my_alter_table(&self.pool, database, schema, table_name, operations).await
    }
    async fn import_data(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        columns: &[String],
        rows: &[Vec<serde_json::Value>],
    ) -> Result<u64> {
        my_import_data(&self.pool, database, schema, table, columns, rows).await
    }
    async fn drop_object(
        &self,
        database: &str,
        schema: &str,
        object_name: &str,
        object_type: &str,
    ) -> Result<()> {
        my_drop_object(&self.pool, database, schema, object_name, object_type).await
    }
    async fn truncate_table(&self, database: &str, schema: &str, table_name: &str) -> Result<()> {
        my_truncate_table(&self.pool, database, schema, table_name).await
    }
    async fn get_server_activity(&self) -> Result<Vec<ServerActivity>> {
        my_get_server_activity(&self.pool).await
    }
    async fn cancel_query(&self, pid: &str) -> Result<()> {
        my_cancel_query(&self.pool, pid).await
    }
    async fn apply_changes(&self, changes: &DataChanges) -> Result<()> {
        my_apply_changes(&self.pool, changes).await
    }
    async fn get_table_stats(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<TableStats> {
        my_get_table_stats(&self.pool, database, schema, table).await
    }
    async fn get_server_config(&self) -> Result<Vec<ServerConfigEntry>> {
        my_get_server_config(&self.pool).await
    }

    async fn fetch_rows(
        &self,
        database: &str,
        _schema: &str,
        table: &str,
        offset: u64,
        limit: u64,
        sort: Option<SortSpec>,
        filter: Option<String>,
    ) -> Result<TableData> {
        let columns = self.list_columns(database, database, table).await?;
        my_fetch_rows_impl(
            &self.pool, columns, database, table, offset, limit, sort, filter,
        )
        .await
    }

    // -----------------------------------------------------------------------
    // TiDB-specific implementations
    // -----------------------------------------------------------------------

    async fn explain_query(&self, _database: &str, sql: &str) -> Result<ExplainResult> {
        let start = Instant::now();
        // TiDB does not support EXPLAIN FORMAT=JSON — use plain EXPLAIN
        let explain_sql = format!("EXPLAIN {}", sql);
        let rows = sqlx::query(&explain_sql).fetch_all(&self.pool).await?;
        let elapsed = start.elapsed().as_millis() as u64;

        let columns: Vec<String> = rows
            .first()
            .map(|r| {
                (0..r.columns().len())
                    .map(|i| r.columns()[i].name().to_string())
                    .collect()
            })
            .unwrap_or_default();

        let mut lines = vec![columns.join("\t")];
        for row in &rows {
            let vals: Vec<String> = (0..row.columns().len())
                .map(|i| row.try_get::<String, _>(i).unwrap_or_default())
                .collect();
            lines.push(vals.join("\t"));
        }
        let raw_text = lines.join("\n");

        Ok(ExplainResult {
            plan: ExplainNode {
                node_type: "TiDB Plan".to_string(),
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

    async fn validate_query(&self, _database: &str, sql: &str) -> Result<Option<ValidationError>> {
        my_validate_query(&self.pool, sql).await
    }

    async fn list_functions(&self, _database: &str, _schema: &str) -> Result<Vec<FunctionInfo>> {
        Ok(vec![])
    }

    async fn list_triggers(
        &self,
        _database: &str,
        _schema: &str,
        _table: &str,
    ) -> Result<Vec<TriggerInfo>> {
        Ok(vec![])
    }

    async fn get_database_stats(&self) -> Result<DatabaseStats> {
        let activity = self.get_server_activity().await.unwrap_or_default();
        let active = activity
            .iter()
            .filter(|a| a.state != "Sleep" && a.state != "Daemon")
            .count() as i64;
        let idle = activity.iter().filter(|a| a.state == "Sleep").count() as i64;
        let total = activity.len() as i64;

        let status_sql = "SHOW GLOBAL STATUS WHERE Variable_name IN (\
            'Com_commit', 'Com_rollback')";
        let status_rows = sqlx::raw_sql(status_sql).fetch_all(&self.pool).await?;

        let get_status = |name: &str| -> i64 {
            status_rows
                .iter()
                .find(|r| {
                    r.try_get::<String, _>(0)
                        .ok()
                        .map(|v| v == name)
                        .unwrap_or(false)
                })
                .and_then(|r| {
                    r.try_get::<String, _>(1)
                        .ok()
                        .and_then(|v| v.parse::<i64>().ok())
                })
                .unwrap_or(0)
        };

        let ts_row = sqlx::query("SELECT CAST(UNIX_TIMESTAMP() * 1000 AS DOUBLE) AS ts")
            .fetch_one(&self.pool)
            .await?;
        let timestamp_ms: f64 = ts_row.try_get::<f64, _>("ts").unwrap_or(0.0);

        Ok(DatabaseStats {
            active_connections: active,
            idle_connections: idle,
            idle_in_transaction: 0,
            total_connections: total,
            xact_commit: get_status("Com_commit"),
            xact_rollback: get_status("Com_rollback"),
            tup_inserted: 0,
            tup_updated: 0,
            tup_deleted: 0,
            tup_fetched: 0,
            blks_read: 0,
            blks_hit: 0,
            timestamp_ms,
        })
    }

    async fn get_locks(&self) -> Result<Vec<LockInfo>> {
        let sql = "SELECT \
            trx_mysql_thread_id AS pid, \
            CAST(trx_id AS CHAR) AS lock_id, \
            IFNULL(trx_query, '') AS query, \
            IFNULL(CAST(trx_state AS CHAR), '') AS state, \
            TIMESTAMPDIFF(SECOND, trx_started, NOW()) AS duration_s \
          FROM information_schema.INNODB_TRX \
          ORDER BY trx_started";
        let rows = sqlx::raw_sql(sql).fetch_all(&self.pool).await?;

        Ok(rows
            .iter()
            .map(|r| LockInfo {
                pid: r.try_get::<i64, _>("pid").unwrap_or(0) as i32,
                locktype: r
                    .try_get::<String, _>("lock_id")
                    .unwrap_or_else(|_| "TiDB".into()),
                database: String::new(),
                relation: String::new(),
                mode: String::new(),
                granted: true,
                query: r.try_get::<String, _>("query").unwrap_or_default(),
                user: String::new(),
                state: r.try_get::<String, _>("state").unwrap_or_default(),
                duration_ms: r
                    .try_get::<i64, _>("duration_s")
                    .ok()
                    .map(|s| s as f64 * 1000.0),
            })
            .collect())
    }

    async fn get_query_stats(&self) -> Result<QueryStatsResponse> {
        Ok(QueryStatsResponse {
            available: false,
            message: Some("Query statistics are not available in TiDB. Use the TiDB Dashboard for query insights.".to_string()),
            entries: vec![],
        })
    }
}
