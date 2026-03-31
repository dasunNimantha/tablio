use anyhow::Result;
use async_trait::async_trait;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::RwLock;

use crate::db::pg_common::*;
use crate::db::DatabaseDriver;
use crate::models::*;

pub struct PostgresDriver {
    pool: PgPool,
    config: ConnectionConfig,
    db_pools: RwLock<HashMap<String, PgPool>>,
}

impl PostgresDriver {
    fn ssl_mode(config: &ConnectionConfig) -> &'static str {
        if config.ssl {
            if config.trust_server_cert {
                "require"
            } else {
                "verify-full"
            }
        } else {
            "prefer"
        }
    }

    fn build_url(config: &ConnectionConfig, database: &str) -> String {
        let ssl_mode = Self::ssl_mode(config);
        let db_segment = if database.trim().is_empty() {
            String::new()
        } else {
            format!("/{}", urlencoding::encode(database))
        };
        format!(
            "postgres://{}:{}@{}:{}{}?sslmode={}",
            urlencoding::encode(&config.user),
            urlencoding::encode(&config.password),
            &config.host,
            config.port,
            db_segment,
            ssl_mode
        )
    }

    pub async fn connect(config: &ConnectionConfig) -> Result<Self> {
        let initial_db = if config.database.trim().is_empty() {
            "postgres"
        } else {
            &config.database
        };
        let url = Self::build_url(config, initial_db);
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await?;
        Ok(Self {
            pool,
            config: config.clone(),
            db_pools: RwLock::new(HashMap::new()),
        })
    }

    async fn get_pool(&self, database: &str) -> Result<PgPool> {
        if database.is_empty() || database == self.config.database {
            return Ok(self.pool.clone());
        }
        {
            let pools = self.db_pools.read().await;
            if let Some(pool) = pools.get(database) {
                return Ok(pool.clone());
            }
        }
        let url = Self::build_url(&self.config, database);
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await?;
        self.db_pools
            .write()
            .await
            .insert(database.to_string(), pool.clone());
        Ok(pool)
    }
}

#[async_trait]
impl DatabaseDriver for PostgresDriver {
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
        let pool = self.get_pool(database).await?;
        pg_list_schemas(&pool, database).await
    }
    async fn list_tables(&self, database: &str, schema: &str) -> Result<Vec<TableInfo>> {
        let pool = self.get_pool(database).await?;
        pg_list_tables(&pool, database, schema).await
    }
    async fn list_indexes(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<IndexInfo>> {
        let pool = self.get_pool(database).await?;
        pg_list_indexes(&pool, database, schema, table).await
    }
    async fn list_foreign_keys(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        let pool = self.get_pool(database).await?;
        pg_list_foreign_keys(&pool, database, schema, table).await
    }
    async fn list_functions(&self, database: &str, schema: &str) -> Result<Vec<FunctionInfo>> {
        let pool = self.get_pool(database).await?;
        pg_list_functions(&pool, database, schema).await
    }
    async fn list_triggers(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<TriggerInfo>> {
        let pool = self.get_pool(database).await?;
        pg_list_triggers(&pool, database, schema, table).await
    }
    async fn execute_query(&self, database: &str, sql: &str) -> Result<QueryResult> {
        let pool = self.get_pool(database).await?;
        pg_execute_query(&pool, database, sql).await
    }
    async fn get_ddl(
        &self,
        database: &str,
        schema: &str,
        object_name: &str,
        object_type: &str,
    ) -> Result<String> {
        let pool = self.get_pool(database).await?;
        pg_get_ddl(&pool, database, schema, object_name, object_type).await
    }
    async fn create_table(
        &self,
        database: &str,
        schema: &str,
        table_name: &str,
        columns: &[ColumnDefinition],
    ) -> Result<()> {
        let pool = self.get_pool(database).await?;
        pg_create_table(&pool, database, schema, table_name, columns).await
    }
    async fn alter_table(
        &self,
        database: &str,
        schema: &str,
        table_name: &str,
        operations: &[AlterTableOperation],
    ) -> Result<()> {
        let pool = self.get_pool(database).await?;
        pg_alter_table(&pool, database, schema, table_name, operations).await
    }
    async fn import_data(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        columns: &[String],
        rows: &[Vec<serde_json::Value>],
    ) -> Result<u64> {
        let pool = self.get_pool(database).await?;
        pg_import_data(&pool, database, schema, table, columns, rows).await
    }
    async fn drop_object(
        &self,
        database: &str,
        schema: &str,
        object_name: &str,
        object_type: &str,
    ) -> Result<()> {
        let pool = self.get_pool(database).await?;
        pg_drop_object(&pool, database, schema, object_name, object_type).await
    }
    async fn truncate_table(&self, database: &str, schema: &str, table_name: &str) -> Result<()> {
        let pool = self.get_pool(database).await?;
        pg_truncate_table(&pool, database, schema, table_name).await
    }
    async fn get_server_activity(&self) -> Result<Vec<ServerActivity>> {
        pg_get_server_activity(&self.pool).await
    }
    async fn cancel_query(&self, pid: &str) -> Result<()> {
        pg_cancel_query(&self.pool, pid).await
    }
    async fn apply_changes(&self, changes: &DataChanges) -> Result<()> {
        let pool = self.get_pool(&changes.database).await?;
        pg_apply_changes(&pool, changes).await
    }

    // -----------------------------------------------------------------------
    // Postgres-specific implementations (not shared with CockroachDB)
    // -----------------------------------------------------------------------

    async fn list_columns(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ColumnInfo>> {
        let pool = self.get_pool(database).await?;
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
        .fetch_all(&pool)
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
                    .map(|d| d.starts_with("nextval("))
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
                ColumnInfo {
                    name: r.get("column_name"),
                    data_type,
                    is_nullable: nullable_str == "YES",
                    is_primary_key: r.get("is_pk"),
                    default_value: default_val,
                    ordinal_position: r.get::<i32, _>("ordinal_position"),
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
        let pool = self.get_pool(database).await?;
        let columns = self.list_columns(database, schema, table).await?;
        pg_fetch_rows_impl(&pool, columns, schema, table, offset, limit, sort, filter).await
    }

    async fn explain_query(&self, database: &str, sql: &str) -> Result<ExplainResult> {
        let pool = self.get_pool(database).await?;
        let start = Instant::now();
        let explain_sql = format!("EXPLAIN (FORMAT JSON) {}", sql);
        let row = sqlx::query(&explain_sql).fetch_one(&pool).await?;
        let elapsed = start.elapsed().as_millis() as u64;

        let raw_json: serde_json::Value = row.try_get(0)?;
        let raw_text = serde_json::to_string_pretty(&raw_json)?;
        let plan = parse_pg_explain_node(&raw_json);

        Ok(ExplainResult {
            plan,
            raw_text,
            execution_time_ms: elapsed,
        })
    }

    async fn get_table_stats(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<TableStats> {
        let pool = self.get_pool(database).await?;
        let sql = "SELECT c.relname AS table_name,
                   GREATEST(c.reltuples, 0)::bigint AS row_count,
                   pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
                   pg_size_pretty(pg_indexes_size(c.oid)) AS index_size,
                   pg_size_pretty(pg_relation_size(c.oid)) AS data_size,
                   s.last_vacuum::text AS last_vacuum,
                   s.last_analyze::text AS last_analyze,
                   s.n_dead_tup AS dead_tuples,
                   s.n_live_tup AS live_tuples
                   FROM pg_class c
                   JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = $1
                   LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
                   WHERE c.relname = $2 AND c.relkind = 'r'";
        let row = sqlx::query(sql)
            .bind(schema)
            .bind(table)
            .fetch_optional(&pool)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Table {}.{} not found", schema, table))?;

        Ok(TableStats {
            table_name: row.get("table_name"),
            row_count: row.get("row_count"),
            total_size: row.get("total_size"),
            index_size: row.get("index_size"),
            data_size: row.get("data_size"),
            last_vacuum: row.try_get::<String, _>("last_vacuum").ok(),
            last_analyze: row.try_get::<String, _>("last_analyze").ok(),
            dead_tuples: row.try_get("dead_tuples").ok(),
            live_tuples: row.try_get("live_tuples").ok(),
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

        let db_row_opt = sqlx::query(
            "SELECT COALESCE(xact_commit, 0) AS xact_commit, \
                    COALESCE(xact_rollback, 0) AS xact_rollback, \
                    COALESCE(tup_inserted, 0) AS tup_inserted, \
                    COALESCE(tup_updated, 0) AS tup_updated, \
                    COALESCE(tup_deleted, 0) AS tup_deleted, \
                    COALESCE(tup_fetched, 0) AS tup_fetched, \
                    COALESCE(blks_read, 0) AS blks_read, \
                    COALESCE(blks_hit, 0) AS blks_hit \
            FROM pg_stat_database WHERE datname = current_database()",
        )
        .fetch_optional(&self.pool)
        .await?;

        let ts_row = sqlx::query("SELECT CAST(EXTRACT(EPOCH FROM now()) * 1000 AS FLOAT8) AS ts")
            .fetch_one(&self.pool)
            .await?;

        let (
            xact_commit,
            xact_rollback,
            tup_inserted,
            tup_updated,
            tup_deleted,
            tup_fetched,
            blks_read,
            blks_hit,
        ) = if let Some(db_row) = db_row_opt {
            (
                db_row.get::<i64, _>("xact_commit"),
                db_row.get::<i64, _>("xact_rollback"),
                db_row.get::<i64, _>("tup_inserted"),
                db_row.get::<i64, _>("tup_updated"),
                db_row.get::<i64, _>("tup_deleted"),
                db_row.get::<i64, _>("tup_fetched"),
                db_row.get::<i64, _>("blks_read"),
                db_row.get::<i64, _>("blks_hit"),
            )
        } else {
            (0, 0, 0, 0, 0, 0, 0, 0)
        };

        Ok(DatabaseStats {
            active_connections: conn_row.get::<i64, _>("active"),
            idle_connections: conn_row.get::<i64, _>("idle"),
            idle_in_transaction: conn_row.get::<i64, _>("idle_tx"),
            total_connections: conn_row.get::<i64, _>("total"),
            xact_commit,
            xact_rollback,
            tup_inserted,
            tup_updated,
            tup_deleted,
            tup_fetched,
            blks_read,
            blks_hit,
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
                setting: row.get::<String, _>("setting"),
                unit: row.try_get::<String, _>("unit").ok(),
                category: row.get::<String, _>("category"),
                description: row.get::<String, _>("short_desc"),
                context: row.get::<String, _>("context"),
                source: row.get::<String, _>("source"),
                pending_restart: row.get::<bool, _>("pending_restart"),
            })
            .collect())
    }

    async fn get_query_stats(&self) -> Result<QueryStatsResponse> {
        let ext_check =
            sqlx::query("SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'")
                .fetch_optional(&self.pool)
                .await?;

        if ext_check.is_none() {
            return Ok(QueryStatsResponse {
                available: false,
                message: Some(
                    "The pg_stat_statements extension is not installed. To enable it:\n\n\
                     1. Add to postgresql.conf:\n   shared_preload_libraries = 'pg_stat_statements'\n\n\
                     2. Restart PostgreSQL\n\n\
                     3. Run in your database:\n   CREATE EXTENSION pg_stat_statements;"
                        .to_string(),
                ),
                entries: vec![],
            });
        }

        let version_row = sqlx::query("SHOW server_version_num")
            .fetch_one(&self.pool)
            .await?;
        let version_num: i32 = version_row
            .try_get::<String, _>("server_version_num")
            .unwrap_or_default()
            .parse()
            .unwrap_or(0);

        let sql = if version_num >= 130000 {
            "SELECT s.query, s.queryid, s.calls, \
                COALESCE(r.rolname, '') as username, \
                s.total_exec_time as total_exec_time_ms, \
                s.mean_exec_time as mean_exec_time_ms, \
                s.min_exec_time as min_exec_time_ms, \
                s.max_exec_time as max_exec_time_ms, \
                s.rows, s.shared_blks_hit, s.shared_blks_read, \
                CASE WHEN (s.shared_blks_hit + s.shared_blks_read) > 0 \
                     THEN s.shared_blks_hit::float / (s.shared_blks_hit + s.shared_blks_read) * 100 \
                     ELSE 0 END as cache_hit_ratio, \
                s.total_plan_time as total_plan_time_ms, \
                s.mean_plan_time as mean_plan_time_ms \
             FROM pg_stat_statements s \
             LEFT JOIN pg_roles r ON s.userid = r.oid \
             ORDER BY s.total_exec_time DESC"
        } else {
            "SELECT s.query, s.queryid, s.calls, \
                COALESCE(r.rolname, '') as username, \
                s.total_time as total_exec_time_ms, \
                s.mean_time as mean_exec_time_ms, \
                s.min_time as min_exec_time_ms, \
                s.max_time as max_exec_time_ms, \
                s.rows, s.shared_blks_hit, s.shared_blks_read, \
                CASE WHEN (s.shared_blks_hit + s.shared_blks_read) > 0 \
                     THEN s.shared_blks_hit::float / (s.shared_blks_hit + s.shared_blks_read) * 100 \
                     ELSE 0 END as cache_hit_ratio, \
                0::float as total_plan_time_ms, \
                0::float as mean_plan_time_ms \
             FROM pg_stat_statements s \
             LEFT JOIN pg_roles r ON s.userid = r.oid \
             ORDER BY s.total_time DESC"
        };

        let rows = sqlx::query(sql).fetch_all(&self.pool).await?;

        let entries = rows
            .iter()
            .map(|r| QueryStatEntry {
                query: r.try_get::<String, _>("query").unwrap_or_default(),
                queryid: r.try_get::<i64, _>("queryid").ok(),
                user: r.try_get::<String, _>("username").unwrap_or_default(),
                calls: r.try_get::<i64, _>("calls").unwrap_or(0),
                total_exec_time_ms: r.try_get::<f64, _>("total_exec_time_ms").unwrap_or(0.0),
                mean_exec_time_ms: r.try_get::<f64, _>("mean_exec_time_ms").unwrap_or(0.0),
                min_exec_time_ms: r.try_get::<f64, _>("min_exec_time_ms").unwrap_or(0.0),
                max_exec_time_ms: r.try_get::<f64, _>("max_exec_time_ms").unwrap_or(0.0),
                rows: r.try_get::<i64, _>("rows").unwrap_or(0),
                shared_blks_hit: r.try_get::<i64, _>("shared_blks_hit").unwrap_or(0),
                shared_blks_read: r.try_get::<i64, _>("shared_blks_read").unwrap_or(0),
                cache_hit_ratio: r.try_get::<f64, _>("cache_hit_ratio").unwrap_or(0.0),
                total_plan_time_ms: r.try_get::<f64, _>("total_plan_time_ms").ok(),
                mean_plan_time_ms: r.try_get::<f64, _>("mean_plan_time_ms").ok(),
            })
            .collect();

        Ok(QueryStatsResponse {
            available: true,
            message: None,
            entries,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(database: &str) -> ConnectionConfig {
        ConnectionConfig {
            id: "test".into(),
            name: "test".into(),
            db_type: DbType::Postgres,
            host: "localhost".into(),
            port: 5432,
            user: "myuser".into(),
            password: "mypass".into(),
            database: database.into(),
            color: "#000".into(),
            ssl: false,
            trust_server_cert: false,
            group: None,
            ssh_enabled: false,
            ssh_host: String::new(),
            ssh_port: 22,
            ssh_user: String::new(),
            ssh_password: String::new(),
            ssh_key_path: String::new(),
        }
    }

    #[test]
    fn build_url_with_database() {
        let config = test_config("mydb");
        let url = PostgresDriver::build_url(&config, "mydb");
        assert!(url.contains("/mydb?"));
        assert!(url.starts_with("postgres://myuser:mypass@localhost:5432/"));
    }

    #[test]
    fn build_url_empty_database() {
        let config = test_config("");
        let url = PostgresDriver::build_url(&config, "");
        assert!(!url.contains("//myuser:mypass@localhost:5432/"));
        assert!(url.contains("localhost:5432?"));
    }

    #[test]
    fn build_url_different_database() {
        let config = test_config("default_db");
        let url = PostgresDriver::build_url(&config, "other_db");
        assert!(url.contains("/other_db?"));
        assert!(!url.contains("default_db"));
    }

    #[test]
    fn build_url_encodes_special_chars() {
        let config = test_config("");
        let url = PostgresDriver::build_url(&config, "my db");
        assert!(url.contains("/my%20db?"));
    }

    #[test]
    fn ssl_mode_no_ssl() {
        let config = test_config("");
        assert_eq!(PostgresDriver::ssl_mode(&config), "prefer");
    }

    #[test]
    fn ssl_mode_ssl_trusted() {
        let mut config = test_config("");
        config.ssl = true;
        config.trust_server_cert = true;
        assert_eq!(PostgresDriver::ssl_mode(&config), "require");
    }

    #[test]
    fn ssl_mode_ssl_verify() {
        let mut config = test_config("");
        config.ssl = true;
        config.trust_server_cert = false;
        assert_eq!(PostgresDriver::ssl_mode(&config), "verify-full");
    }

    fn format_column_data_type(
        raw_type: &str,
        char_max_len: Option<i32>,
        num_precision: Option<i32>,
        num_scale: Option<i32>,
    ) -> String {
        if let Some(len) = char_max_len {
            format!("{}({})", raw_type, len)
        } else if raw_type == "numeric" || raw_type == "decimal" {
            match (num_precision, num_scale) {
                (Some(p), Some(s)) if s > 0 => format!("{}({},{})", raw_type, p, s),
                (Some(p), _) => format!("{}({})", raw_type, p),
                _ => raw_type.to_string(),
            }
        } else {
            raw_type.to_string()
        }
    }

    #[test]
    fn column_type_varchar_with_length() {
        assert_eq!(
            format_column_data_type("character varying", Some(50), None, None),
            "character varying(50)"
        );
    }
    #[test]
    fn column_type_no_length() {
        assert_eq!(format_column_data_type("text", None, None, None), "text");
    }
    #[test]
    fn column_type_numeric_precision_and_scale() {
        assert_eq!(
            format_column_data_type("numeric", None, Some(10), Some(2)),
            "numeric(10,2)"
        );
    }
    #[test]
    fn column_type_numeric_precision_only() {
        assert_eq!(
            format_column_data_type("numeric", None, Some(10), Some(0)),
            "numeric(10)"
        );
    }
    #[test]
    fn column_type_numeric_no_modifiers() {
        assert_eq!(
            format_column_data_type("numeric", None, None, None),
            "numeric"
        );
    }
    #[test]
    fn column_type_boolean_unchanged() {
        assert_eq!(
            format_column_data_type("boolean", None, None, None),
            "boolean"
        );
    }
}
