use anyhow::Result;
use async_trait::async_trait;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{MySqlPool, Row};
use std::time::Instant;

use crate::db::mysql_common::*;
use crate::db::DatabaseDriver;
use crate::models::*;

pub struct MysqlDriver {
    pool: MySqlPool,
}

impl MysqlDriver {
    pub async fn connect(config: &ConnectionConfig) -> Result<Self> {
        let ssl_mode = if config.ssl { "REQUIRED" } else { "PREFERRED" };
        let url = format!(
            "mysql://{}:{}@{}:{}/{}?ssl-mode={}",
            urlencoding::encode(&config.user),
            urlencoding::encode(&config.password),
            &config.host,
            config.port,
            urlencoding::encode(&config.database),
            ssl_mode
        );
        let pool = MySqlPoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await?;
        Ok(Self { pool })
    }
}

#[async_trait]
impl DatabaseDriver for MysqlDriver {
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
    async fn get_query_stats(&self) -> Result<QueryStatsResponse> {
        my_get_query_stats(&self.pool).await
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
    // MySQL-specific implementations
    // -----------------------------------------------------------------------

    async fn explain_query(&self, _database: &str, sql: &str) -> Result<ExplainResult> {
        let start = Instant::now();
        let explain_sql = format!("EXPLAIN FORMAT=JSON {}", sql);
        let row = sqlx::query(&explain_sql).fetch_one(&self.pool).await?;
        let elapsed = start.elapsed().as_millis() as u64;

        let raw_text: String = row.try_get(0)?;
        let json: serde_json::Value = serde_json::from_str(&raw_text)?;
        let query_block = json.get("query_block").unwrap_or(&serde_json::Value::Null);

        let plan = ExplainNode {
            node_type: "Query Block".to_string(),
            relation: query_block
                .get("table")
                .and_then(|t| t.get("table_name"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            startup_cost: 0.0,
            total_cost: query_block
                .get("cost_info")
                .and_then(|c| c.get("query_cost"))
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok())
                .unwrap_or(0.0),
            actual_time_ms: None,
            rows_estimated: query_block
                .get("table")
                .and_then(|t| t.get("rows_examined_per_scan"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            rows_actual: None,
            width: 0,
            filter: None,
            children: vec![],
        };

        Ok(ExplainResult {
            plan,
            raw_text: serde_json::to_string_pretty(&json)?,
            execution_time_ms: elapsed,
        })
    }

    async fn list_functions(&self, database: &str, _schema: &str) -> Result<Vec<FunctionInfo>> {
        let sql = "SELECT CAST(ROUTINE_NAME AS CHAR) AS name, \
                   CAST(ROUTINE_SCHEMA AS CHAR) AS `schema`, \
                   COALESCE(CAST(DATA_TYPE AS CHAR), '') AS return_type, \
                   CAST(ROUTINE_BODY AS CHAR) AS language, \
                   CAST(ROUTINE_TYPE AS CHAR) AS kind \
                   FROM information_schema.ROUTINES \
                   WHERE ROUTINE_SCHEMA = ? \
                   ORDER BY ROUTINE_NAME";
        let rows = sqlx::query(sql)
            .bind(database)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows
            .iter()
            .map(|r| FunctionInfo {
                name: r.get("name"),
                schema: r.get("schema"),
                return_type: r.get("return_type"),
                language: r.get("language"),
                kind: r.get("kind"),
            })
            .collect())
    }

    async fn list_triggers(
        &self,
        database: &str,
        _schema: &str,
        table: &str,
    ) -> Result<Vec<TriggerInfo>> {
        let sql = "SELECT CAST(TRIGGER_NAME AS CHAR) AS name, \
                   CAST(EVENT_OBJECT_TABLE AS CHAR) AS table_name, \
                   CAST(EVENT_MANIPULATION AS CHAR) AS event, \
                   CAST(ACTION_TIMING AS CHAR) AS timing \
                   FROM information_schema.TRIGGERS \
                   WHERE TRIGGER_SCHEMA = ? AND EVENT_OBJECT_TABLE = ? \
                   ORDER BY TRIGGER_NAME";
        let rows = sqlx::query(sql)
            .bind(database)
            .bind(table)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows
            .iter()
            .map(|r| TriggerInfo {
                name: r.get("name"),
                table_name: r.get("table_name"),
                event: r.get("event"),
                timing: r.get("timing"),
            })
            .collect())
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
            'Com_commit', 'Com_rollback', \
            'Innodb_rows_inserted', 'Innodb_rows_updated', \
            'Innodb_rows_deleted', 'Innodb_rows_read', \
            'Innodb_buffer_pool_reads', 'Innodb_buffer_pool_read_requests')";
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
            tup_inserted: get_status("Innodb_rows_inserted"),
            tup_updated: get_status("Innodb_rows_updated"),
            tup_deleted: get_status("Innodb_rows_deleted"),
            tup_fetched: get_status("Innodb_rows_read"),
            blks_read: get_status("Innodb_buffer_pool_reads"),
            blks_hit: get_status("Innodb_buffer_pool_read_requests"),
            timestamp_ms,
        })
    }

    async fn get_locks(&self) -> Result<Vec<LockInfo>> {
        let sql = "SELECT \
            r.trx_mysql_thread_id AS pid, \
            CAST(r.trx_id AS CHAR) AS lock_id, \
            IFNULL(CAST(l.OBJECT_SCHEMA AS CHAR), '') AS db, \
            IFNULL(CAST(l.OBJECT_NAME AS CHAR), '') AS relation, \
            IFNULL(CAST(l.LOCK_MODE AS CHAR), '') AS mode, \
            CASE WHEN l.LOCK_STATUS = 'GRANTED' THEN 1 ELSE 0 END AS granted, \
            IFNULL(r.trx_query, '') AS query, \
            IFNULL(CAST(p.USER AS CHAR), '') AS user, \
            IFNULL(CAST(r.trx_state AS CHAR), '') AS state, \
            TIMESTAMPDIFF(SECOND, r.trx_started, NOW()) AS duration_s \
          FROM information_schema.INNODB_TRX r \
          LEFT JOIN performance_schema.data_locks l \
            ON r.trx_id = l.ENGINE_TRANSACTION_ID \
          LEFT JOIN information_schema.PROCESSLIST p \
            ON r.trx_mysql_thread_id = p.ID \
          ORDER BY r.trx_started";
        let result = sqlx::raw_sql(sql).fetch_all(&self.pool).await;

        match result {
            Ok(rows) => Ok(rows
                .iter()
                .map(|r| LockInfo {
                    pid: r.try_get::<i64, _>("pid").unwrap_or(0) as i32,
                    locktype: r
                        .try_get::<String, _>("lock_id")
                        .unwrap_or_else(|_| "InnoDB".into()),
                    database: r.try_get::<String, _>("db").unwrap_or_default(),
                    relation: r.try_get::<String, _>("relation").unwrap_or_default(),
                    mode: r.try_get::<String, _>("mode").unwrap_or_default(),
                    granted: r.try_get::<i32, _>("granted").unwrap_or(0) == 1,
                    query: r.try_get::<String, _>("query").unwrap_or_default(),
                    user: r.try_get::<String, _>("user").unwrap_or_default(),
                    state: r.try_get::<String, _>("state").unwrap_or_default(),
                    duration_ms: r
                        .try_get::<i64, _>("duration_s")
                        .ok()
                        .map(|s| s as f64 * 1000.0),
                })
                .collect()),
            Err(_) => {
                // Fallback for MySQL < 8.0 (no performance_schema.data_locks)
                let fallback_sql = "SELECT \
                    r.trx_mysql_thread_id AS pid, \
                    CAST(r.trx_id AS CHAR) AS lock_id, \
                    IFNULL(r.trx_query, '') AS query, \
                    IFNULL(CAST(p.USER AS CHAR), '') AS user, \
                    IFNULL(CAST(r.trx_state AS CHAR), '') AS state, \
                    TIMESTAMPDIFF(SECOND, r.trx_started, NOW()) AS duration_s \
                  FROM information_schema.INNODB_TRX r \
                  LEFT JOIN information_schema.PROCESSLIST p \
                    ON r.trx_mysql_thread_id = p.ID \
                  ORDER BY r.trx_started";
                let rows = sqlx::raw_sql(fallback_sql).fetch_all(&self.pool).await?;
                Ok(rows
                    .iter()
                    .map(|r| LockInfo {
                        pid: r.try_get::<i64, _>("pid").unwrap_or(0) as i32,
                        locktype: r
                            .try_get::<String, _>("lock_id")
                            .unwrap_or_else(|_| "InnoDB".into()),
                        database: String::new(),
                        relation: String::new(),
                        mode: String::new(),
                        granted: true,
                        query: r.try_get::<String, _>("query").unwrap_or_default(),
                        user: r.try_get::<String, _>("user").unwrap_or_default(),
                        state: r.try_get::<String, _>("state").unwrap_or_default(),
                        duration_ms: r
                            .try_get::<i64, _>("duration_s")
                            .ok()
                            .map(|s| s as f64 * 1000.0),
                    })
                    .collect())
            }
        }
    }
}
