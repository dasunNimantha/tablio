use anyhow::Result;
use async_trait::async_trait;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Column, PgPool, Row, TypeInfo};
use std::time::Instant;

use crate::db::DatabaseDriver;
use crate::models::*;

pub struct PostgresDriver {
    pool: PgPool,
}

impl PostgresDriver {
    pub async fn connect(config: &ConnectionConfig) -> Result<Self> {
        let ssl_mode = if config.ssl { "require" } else { "prefer" };
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

fn pg_row_to_json_values(row: &sqlx::postgres::PgRow, col_count: usize) -> Vec<serde_json::Value> {
    let mut values = Vec::with_capacity(col_count);
    for i in 0..col_count {
        let col = row.column(i);
        let type_name = col.type_info().name();
        let val: serde_json::Value = match type_name {
            "BOOL" => row
                .try_get::<bool, _>(i)
                .ok()
                .map(serde_json::Value::Bool)
                .unwrap_or(serde_json::Value::Null),
            "INT2" => row
                .try_get::<i16, _>(i)
                .ok()
                .map(|v| serde_json::Value::Number(v.into()))
                .unwrap_or(serde_json::Value::Null),
            "INT4" => row
                .try_get::<i32, _>(i)
                .ok()
                .map(|v| serde_json::Value::Number(v.into()))
                .unwrap_or(serde_json::Value::Null),
            "INT8" => row
                .try_get::<i64, _>(i)
                .ok()
                .map(|v| serde_json::Value::Number(v.into()))
                .unwrap_or(serde_json::Value::Null),
            "FLOAT4" => row
                .try_get::<f32, _>(i)
                .ok()
                .and_then(|v| serde_json::Number::from_f64(v as f64))
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            "FLOAT8" | "NUMERIC" => row
                .try_get::<f64, _>(i)
                .ok()
                .and_then(|v| serde_json::Number::from_f64(v))
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            "JSON" | "JSONB" => row
                .try_get::<serde_json::Value, _>(i)
                .unwrap_or(serde_json::Value::Null),
            _ => row
                .try_get::<String, _>(i)
                .ok()
                .map(serde_json::Value::String)
                .unwrap_or(serde_json::Value::Null),
        };
        values.push(val);
    }
    values
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Rejects filter strings that could inject SQL (e.g. statement terminator or comments).
fn filter_is_unsafe(filter: &str) -> bool {
    let s = filter.trim();
    s.contains(';') || s.contains("--") || s.contains("/*") || s.contains("*/")
}

/// Rejects type or default fragments that could inject SQL.
fn sql_fragment_is_unsafe(s: &str) -> bool {
    s.contains(';') || s.contains("--") || s.contains("/*") || s.contains("*/") || s.contains('\'')
}

fn json_to_sql_literal(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        _ => format!("'{}'", val.to_string().replace('\'', "''")),
    }
}

#[async_trait]
impl DatabaseDriver for PostgresDriver {
    async fn list_roles(&self) -> Result<Vec<RoleInfo>> {
        let sql = "SELECT r.rolname, r.rolsuper, r.rolcanlogin, r.rolcreatedb, r.rolcreaterole, \
                   r.rolreplication, COALESCE(r.rolconnlimit, -1)::int4 as rolconnlimit, \
                   r.rolvaliduntil::text, \
                   COALESCE(array_agg(grp.rolname) FILTER (WHERE grp.rolname IS NOT NULL), ARRAY[]::text[]) as member_of \
                   FROM pg_roles r \
                   LEFT JOIN pg_auth_members am ON am.member = r.oid \
                   LEFT JOIN pg_roles grp ON grp.oid = am.roleid \
                   GROUP BY r.oid, r.rolname, r.rolsuper, r.rolcanlogin, r.rolcreatedb, r.rolcreaterole, \
                            r.rolreplication, r.rolconnlimit, r.rolvaliduntil \
                   ORDER BY r.rolname";
        let rows = sqlx::query(sql).fetch_all(&self.pool).await?;

        Ok(rows
            .iter()
            .map(|r| {
                let member_of: Vec<String> = r.try_get("member_of").unwrap_or_default();
                RoleInfo {
                    name: r.get("rolname"),
                    is_superuser: r.get("rolsuper"),
                    can_login: r.get("rolcanlogin"),
                    can_create_db: r.get("rolcreatedb"),
                    can_create_role: r.get("rolcreaterole"),
                    is_replication: r.get("rolreplication"),
                    connection_limit: r.get("rolconnlimit"),
                    valid_until: r.try_get("rolvaliduntil").ok(),
                    member_of,
                }
            })
            .collect())
    }

    async fn create_role(&self, req: &CreateRoleRequest) -> Result<()> {
        let mut options: Vec<String> = Vec::new();
        options.push(if req.is_superuser {
            "SUPERUSER".to_string()
        } else {
            "NOSUPERUSER".to_string()
        });
        options.push(if req.can_login {
            "LOGIN".to_string()
        } else {
            "NOLOGIN".to_string()
        });
        options.push(if req.can_create_db {
            "CREATEDB".to_string()
        } else {
            "NOCREATEDB".to_string()
        });
        options.push(if req.can_create_role {
            "CREATEROLE".to_string()
        } else {
            "NOCREATEROLE".to_string()
        });
        options.push(format!("CONNECTION LIMIT {}", req.connection_limit));
        if let Some(ref pwd) = req.password {
            options.push(format!("PASSWORD '{}'", pwd.replace('\'', "''")));
        }
        if let Some(ref v) = req.valid_until {
            options.push(format!("VALID UNTIL '{}'", v.replace('\'', "''")));
        }
        let sql = format!(
            "CREATE ROLE {} WITH {}",
            quote_ident(&req.name),
            options.join(" ")
        );
        sqlx::query(&sql).execute(&self.pool).await?;
        Ok(())
    }

    async fn drop_role(&self, name: &str) -> Result<()> {
        let sql = format!("DROP ROLE IF EXISTS {}", quote_ident(name));
        sqlx::query(&sql).execute(&self.pool).await?;
        Ok(())
    }

    async fn alter_role(&self, req: &AlterRoleRequest) -> Result<()> {
        let mut options: Vec<String> = Vec::new();
        if let Some(v) = req.is_superuser {
            options.push(if v { "SUPERUSER".to_string() } else { "NOSUPERUSER".to_string() });
        }
        if let Some(v) = req.can_login {
            options.push(if v { "LOGIN".to_string() } else { "NOLOGIN".to_string() });
        }
        if let Some(v) = req.can_create_db {
            options.push(if v { "CREATEDB".to_string() } else { "NOCREATEDB".to_string() });
        }
        if let Some(v) = req.can_create_role {
            options.push(if v { "CREATEROLE".to_string() } else { "NOCREATEROLE".to_string() });
        }
        if let Some(v) = req.connection_limit {
            options.push(format!("CONNECTION LIMIT {}", v));
        }
        if let Some(ref pwd) = req.password {
            options.push(format!("PASSWORD '{}'", pwd.replace('\'', "''")));
        }
        if let Some(ref v) = req.valid_until {
            options.push(format!("VALID UNTIL '{}'", v.replace('\'', "''")));
        }
        if options.is_empty() {
            return Ok(());
        }
        let sql = format!(
            "ALTER ROLE {} WITH {}",
            quote_ident(&req.name),
            options.join(" ")
        );
        sqlx::query(&sql).execute(&self.pool).await?;
        Ok(())
    }

    async fn test_connection(&self) -> Result<bool> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(true)
    }

    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        let rows = sqlx::query(
            "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|r| DatabaseInfo {
                name: r.get("datname"),
            })
            .collect())
    }

    async fn list_schemas(&self, _database: &str) -> Result<Vec<SchemaInfo>> {
        let rows = sqlx::query(
            "SELECT schema_name FROM information_schema.schemata \
             WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') \
             ORDER BY schema_name",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|r| SchemaInfo {
                name: r.get("schema_name"),
            })
            .collect())
    }

    async fn list_tables(&self, _database: &str, schema: &str) -> Result<Vec<TableInfo>> {
        let rows = sqlx::query(
            "SELECT t.table_name, t.table_type, \
                    COALESCE(c.reltuples::bigint, 0) as row_estimate \
             FROM information_schema.tables t \
             LEFT JOIN pg_class c ON c.relname = t.table_name \
             LEFT JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema \
             WHERE t.table_schema = $1 \
             ORDER BY t.table_name",
        )
        .bind(schema)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|r| TableInfo {
                name: r.get("table_name"),
                schema: schema.to_string(),
                table_type: r.get("table_type"),
                row_count_estimate: r.try_get("row_estimate").ok(),
            })
            .collect())
    }

    async fn list_columns(
        &self,
        _database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ColumnInfo>> {
        let rows = sqlx::query(
            "SELECT c.column_name, c.data_type, c.is_nullable, c.column_default, c.ordinal_position, \
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
                ColumnInfo {
                    name: r.get("column_name"),
                    data_type: r.get("data_type"),
                    is_nullable: nullable_str == "YES",
                    is_primary_key: r.get("is_pk"),
                    default_value: r.try_get("column_default").ok(),
                    ordinal_position: r.get("ordinal_position"),
                }
            })
            .collect())
    }

    async fn list_indexes(
        &self,
        _database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<IndexInfo>> {
        let sql = "SELECT i.relname AS index_name, \
                   ix.indisunique AS is_unique, \
                   am.amname AS index_type, \
                   array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) AS columns \
                   FROM pg_index ix \
                   JOIN pg_class t ON t.oid = ix.indrelid \
                   JOIN pg_class i ON i.oid = ix.indexrelid \
                   JOIN pg_namespace n ON n.oid = t.relnamespace \
                   JOIN pg_am am ON am.oid = i.relam \
                   JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey) \
                   WHERE n.nspname = $1 AND t.relname = $2 \
                   GROUP BY i.relname, ix.indisunique, am.amname \
                   ORDER BY i.relname";
        let rows = sqlx::query(sql)
            .bind(schema)
            .bind(table)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows
            .iter()
            .map(|r| {
                let cols: Vec<String> = r.get("columns");
                IndexInfo {
                    name: r.get("index_name"),
                    columns: cols,
                    is_unique: r.get("is_unique"),
                    index_type: r.get("index_type"),
                }
            })
            .collect())
    }

    async fn list_foreign_keys(
        &self,
        _database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        let sql = "SELECT tc.constraint_name, kcu.column_name, \
                   ccu.table_name AS foreign_table, ccu.column_name AS foreign_column, \
                   rc.delete_rule, rc.update_rule \
                   FROM information_schema.table_constraints tc \
                   JOIN information_schema.key_column_usage kcu \
                       ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema \
                   JOIN information_schema.constraint_column_usage ccu \
                       ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema \
                   JOIN information_schema.referential_constraints rc \
                       ON rc.constraint_name = tc.constraint_name AND rc.constraint_schema = tc.table_schema \
                   WHERE tc.constraint_type = 'FOREIGN KEY' \
                       AND tc.table_schema = $1 AND tc.table_name = $2 \
                   ORDER BY tc.constraint_name, kcu.ordinal_position";
        let rows = sqlx::query(sql)
            .bind(schema)
            .bind(table)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows
            .iter()
            .map(|r| ForeignKeyInfo {
                name: r.get("constraint_name"),
                column: r.get("column_name"),
                referenced_table: r.get("foreign_table"),
                referenced_column: r.get("foreign_column"),
                on_delete: r.get("delete_rule"),
                on_update: r.get("update_rule"),
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
        if let Some(ref f) = filter {
            if !f.trim().is_empty() && filter_is_unsafe(f) {
                anyhow::bail!("Filter contains invalid characters (; -- /* */)");
            }
        }

        let columns = self.list_columns(database, schema, table).await?;

        let where_clause = filter
            .filter(|f| !f.trim().is_empty())
            .map(|f| format!("WHERE {}", f))
            .unwrap_or_default();

        let order_clause = sort
            .map(|s| {
                let dir = match s.direction {
                    SortDirection::Asc => "ASC",
                    SortDirection::Desc => "DESC",
                };
                format!("ORDER BY {} {}", quote_ident(&s.column), dir)
            })
            .unwrap_or_default();

        let count_sql = format!(
            "SELECT COUNT(*) as cnt FROM {}.{} {}",
            quote_ident(schema),
            quote_ident(table),
            where_clause
        );
        let count_row = sqlx::query(&count_sql).fetch_one(&self.pool).await?;
        let total_rows: i64 = count_row.get("cnt");

        let sql = format!(
            "SELECT * FROM {}.{} {} {} LIMIT {} OFFSET {}",
            quote_ident(schema),
            quote_ident(table),
            where_clause,
            order_clause,
            limit,
            offset
        );

        let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;
        let col_count = columns.len();
        let data_rows: Vec<Vec<serde_json::Value>> = rows
            .iter()
            .map(|r| pg_row_to_json_values(r, col_count))
            .collect();

        Ok(TableData {
            columns,
            rows: data_rows,
            total_rows,
            offset,
            limit,
        })
    }

    async fn execute_query(&self, _database: &str, sql: &str) -> Result<QueryResult> {
        let start = Instant::now();
        let trimmed = sql.trim().to_uppercase();
        let is_select = trimmed.starts_with("SELECT")
            || trimmed.starts_with("WITH")
            || trimmed.starts_with("SHOW")
            || trimmed.starts_with("EXPLAIN");

        if is_select {
            let rows = sqlx::query(sql).fetch_all(&self.pool).await?;
            let elapsed = start.elapsed().as_millis() as u64;

            let columns: Vec<String> = if rows.is_empty() {
                vec![]
            } else {
                rows[0]
                    .columns()
                    .iter()
                    .map(|c| c.name().to_string())
                    .collect()
            };

            let col_count = columns.len();
            let data_rows: Vec<Vec<serde_json::Value>> = rows
                .iter()
                .map(|r| pg_row_to_json_values(r, col_count))
                .collect();

            Ok(QueryResult {
                columns,
                rows: data_rows,
                rows_affected: 0,
                execution_time_ms: elapsed,
                is_select: true,
            })
        } else {
            let result = sqlx::query(sql).execute(&self.pool).await?;
            let elapsed = start.elapsed().as_millis() as u64;

            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: result.rows_affected(),
                execution_time_ms: elapsed,
                is_select: false,
            })
        }
    }

    async fn explain_query(&self, _database: &str, sql: &str) -> Result<ExplainResult> {
        let start = Instant::now();
        let explain_sql = format!("EXPLAIN (ANALYZE, FORMAT JSON) {}", sql);
        let row = sqlx::query(&explain_sql)
            .fetch_one(&self.pool)
            .await?;
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

    async fn get_ddl(
        &self,
        _database: &str,
        schema: &str,
        object_name: &str,
        object_type: &str,
    ) -> Result<String> {
        match object_type.to_uppercase().as_str() {
            "VIEW" => {
                let sql = "SELECT pg_get_viewdef($1::regclass, true) AS def";
                let fq = format!("{}.{}", schema, object_name);
                let row = sqlx::query(sql).bind(&fq).fetch_one(&self.pool).await?;
                let def: String = row.get("def");
                Ok(format!(
                    "CREATE OR REPLACE VIEW {}.{} AS\n{}",
                    quote_ident(schema),
                    quote_ident(object_name),
                    def
                ))
            }
            _ => {
                let cols_sql = "SELECT column_name, data_type, is_nullable, column_default, \
                    character_maximum_length \
                    FROM information_schema.columns \
                    WHERE table_schema = $1 AND table_name = $2 \
                    ORDER BY ordinal_position";
                let cols = sqlx::query(cols_sql)
                    .bind(schema)
                    .bind(object_name)
                    .fetch_all(&self.pool)
                    .await?;

                let pk_sql = "SELECT ku.column_name \
                    FROM information_schema.table_constraints tc \
                    JOIN information_schema.key_column_usage ku \
                        ON tc.constraint_name = ku.constraint_name \
                        AND tc.table_schema = ku.table_schema \
                    WHERE tc.constraint_type = 'PRIMARY KEY' \
                        AND tc.table_schema = $1 AND tc.table_name = $2 \
                    ORDER BY ku.ordinal_position";
                let pk_rows = sqlx::query(pk_sql)
                    .bind(schema)
                    .bind(object_name)
                    .fetch_all(&self.pool)
                    .await?;
                let pk_cols: Vec<String> = pk_rows.iter().map(|r| r.get("column_name")).collect();

                let mut col_defs = Vec::new();
                for col in &cols {
                    let name: String = col.get("column_name");
                    let dtype: String = col.get("data_type");
                    let nullable: String = col.get("is_nullable");
                    let default: Option<String> = col.try_get("column_default").ok();
                    let max_len: Option<i32> = col.try_get("character_maximum_length").ok();

                    let type_str = if let Some(len) = max_len {
                        format!("{}({})", dtype, len)
                    } else {
                        dtype
                    };

                    let mut def = format!("    {} {}", quote_ident(&name), type_str);
                    if nullable == "NO" {
                        def.push_str(" NOT NULL");
                    }
                    if let Some(d) = default {
                        def.push_str(&format!(" DEFAULT {}", d));
                    }
                    col_defs.push(def);
                }

                if !pk_cols.is_empty() {
                    let pk_str = pk_cols
                        .iter()
                        .map(|c| quote_ident(c))
                        .collect::<Vec<_>>()
                        .join(", ");
                    col_defs.push(format!("    PRIMARY KEY ({})", pk_str));
                }

                Ok(format!(
                    "CREATE TABLE {}.{} (\n{}\n);",
                    quote_ident(schema),
                    quote_ident(object_name),
                    col_defs.join(",\n")
                ))
            }
        }
    }

    async fn create_table(
        &self,
        _database: &str,
        schema: &str,
        table_name: &str,
        columns: &[ColumnDefinition],
    ) -> Result<()> {
        if columns.is_empty() {
            anyhow::bail!("At least one column is required");
        }
        for col in columns {
            if sql_fragment_is_unsafe(&col.data_type) {
                anyhow::bail!("Invalid character in data type for column {}", col.name);
            }
            if let Some(d) = &col.default_value {
                if !d.is_empty() && sql_fragment_is_unsafe(d) {
                    anyhow::bail!("Invalid character in default value for column {}", col.name);
                }
            }
        }

        let pk_cols: Vec<&ColumnDefinition> = columns.iter().filter(|c| c.is_primary_key).collect();
        let mut col_defs = Vec::new();
        for col in columns {
            let mut def = format!("{} {}", quote_ident(&col.name), col.data_type);
            if !col.is_nullable {
                def.push_str(" NOT NULL");
            }
            if let Some(d) = &col.default_value {
                if !d.is_empty() {
                    def.push_str(&format!(" DEFAULT {}", d));
                }
            }
            col_defs.push(def);
        }
        if !pk_cols.is_empty() {
            let pk_str = pk_cols
                .iter()
                .map(|c| quote_ident(&c.name))
                .collect::<Vec<_>>()
                .join(", ");
            col_defs.push(format!("PRIMARY KEY ({})", pk_str));
        }
        let sql = format!(
            "CREATE TABLE {}.{} (\n    {}\n)",
            quote_ident(schema),
            quote_ident(table_name),
            col_defs.join(",\n    ")
        );
        sqlx::query(&sql).execute(&self.pool).await?;
        Ok(())
    }

    async fn list_functions(
        &self,
        _database: &str,
        schema: &str,
    ) -> Result<Vec<FunctionInfo>> {
        let sql = "SELECT p.proname AS name, n.nspname AS schema, t.typname AS return_type, l.lanname AS language,
                   CASE p.prokind
                       WHEN 'f' THEN 'function'
                       WHEN 'p' THEN 'procedure'
                       WHEN 'a' THEN 'aggregate'
                       WHEN 'w' THEN 'window'
                       ELSE 'function'
                   END AS kind
                   FROM pg_proc p
                   JOIN pg_namespace n ON n.oid = p.pronamespace
                   JOIN pg_type t ON t.oid = p.prorettype
                   JOIN pg_language l ON l.oid = p.prolang
                   WHERE n.nspname = $1
                     AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
                   ORDER BY p.proname";
        let rows = sqlx::query(sql)
            .bind(schema)
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
        _database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<TriggerInfo>> {
        let sql = "SELECT trigger_name, event_object_table AS table_name,
                   event_manipulation AS event, action_timing AS timing
                   FROM information_schema.triggers
                   WHERE trigger_schema = $1 AND event_object_table = $2
                   ORDER BY trigger_name";
        let rows = sqlx::query(sql)
            .bind(schema)
            .bind(table)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows
            .iter()
            .map(|r| TriggerInfo {
                name: r.get("trigger_name"),
                table_name: r.get("table_name"),
                event: r.get("event"),
                timing: r.get("timing"),
            })
            .collect())
    }

    async fn get_table_stats(
        &self,
        _database: &str,
        schema: &str,
        table: &str,
    ) -> Result<TableStats> {
        let sql = "SELECT c.relname AS table_name,
                   COALESCE(s.n_live_tup, 0)::bigint AS row_count,
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
            .fetch_optional(&self.pool)
            .await?;

        let row = row.ok_or_else(|| anyhow::anyhow!("Table {}.{} not found", schema, table))?;

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

    async fn alter_table(
        &self,
        _database: &str,
        schema: &str,
        table_name: &str,
        operations: &[AlterTableOperation],
    ) -> Result<()> {
        for op in operations {
            match op {
                AlterTableOperation::ChangeColumnType { new_type, .. } => {
                    if sql_fragment_is_unsafe(new_type) {
                        anyhow::bail!("Invalid character in column type");
                    }
                }
                AlterTableOperation::SetDefault {
                    default_value: Some(d),
                    ..
                } if !d.is_empty() => {
                    if sql_fragment_is_unsafe(d) {
                        anyhow::bail!("Invalid character in default value");
                    }
                }
                _ => {}
            }
        }

        let mut current_table = table_name.to_string();

        for op in operations {
            let table_ref = format!("{}.{}", quote_ident(schema), quote_ident(&current_table));

            let sql = match op {
                AlterTableOperation::AddColumn { column } => {
                    let mut def = format!(
                        "ALTER TABLE {} ADD COLUMN {} {}",
                        table_ref,
                        quote_ident(&column.name),
                        column.data_type
                    );
                    if !column.is_nullable {
                        def.push_str(" NOT NULL");
                    }
                    if let Some(d) = &column.default_value {
                        if !d.is_empty() {
                            def.push_str(&format!(" DEFAULT {}", d));
                        }
                    }
                    def
                }
                AlterTableOperation::DropColumn { column_name } => {
                    format!(
                        "ALTER TABLE {} DROP COLUMN {}",
                        table_ref,
                        quote_ident(column_name)
                    )
                }
                AlterTableOperation::RenameColumn { old_name, new_name } => {
                    format!(
                        "ALTER TABLE {} RENAME COLUMN {} TO {}",
                        table_ref,
                        quote_ident(old_name),
                        quote_ident(new_name)
                    )
                }
                AlterTableOperation::ChangeColumnType {
                    column_name,
                    new_type,
                } => {
                    format!(
                        "ALTER TABLE {} ALTER COLUMN {} TYPE {}",
                        table_ref,
                        quote_ident(column_name),
                        new_type
                    )
                }
                AlterTableOperation::SetNullable {
                    column_name,
                    nullable,
                } => {
                    let action = if *nullable { "DROP NOT NULL" } else { "SET NOT NULL" };
                    format!(
                        "ALTER TABLE {} ALTER COLUMN {} {}",
                        table_ref,
                        quote_ident(column_name),
                        action
                    )
                }
                AlterTableOperation::SetDefault {
                    column_name,
                    default_value,
                } => {
                    let action = match default_value {
                        Some(d) if !d.is_empty() => format!("SET DEFAULT {}", d),
                        _ => "DROP DEFAULT".to_string(),
                    };
                    format!(
                        "ALTER TABLE {} ALTER COLUMN {} {}",
                        table_ref,
                        quote_ident(column_name),
                        action
                    )
                }
                AlterTableOperation::RenameTable { new_name } => {
                    current_table = new_name.clone();
                    format!("ALTER TABLE {} RENAME TO {}", table_ref, quote_ident(new_name))
                }
            };
            sqlx::query(&sql).execute(&self.pool).await?;
        }

        Ok(())
    }

    async fn import_data(
        &self,
        _database: &str,
        schema: &str,
        table: &str,
        columns: &[String],
        rows: &[Vec<serde_json::Value>],
    ) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        let table_ref = format!("{}.{}", quote_ident(schema), quote_ident(table));
        let col_list: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
        let col_str = col_list.join(", ");

        const BATCH_SIZE: usize = 500;
        let mut total_inserted: u64 = 0;
        let mut tx = self.pool.begin().await?;

        for chunk in rows.chunks(BATCH_SIZE) {
            let mut values_list = Vec::with_capacity(chunk.len());
            for row in chunk {
                let vals: Vec<String> = row
                    .iter()
                    .map(|v| json_to_sql_literal(v))
                    .collect();
                values_list.push(format!("({})", vals.join(", ")));
            }
            let values_str = values_list.join(", ");
            let sql = format!(
                "INSERT INTO {} ({}) VALUES {}",
                table_ref,
                col_str,
                values_str
            );
            let result = sqlx::query(&sql).execute(&mut *tx).await?;
            total_inserted += result.rows_affected();
        }

        tx.commit().await?;
        Ok(total_inserted)
    }

    async fn drop_object(
        &self,
        _database: &str,
        schema: &str,
        object_name: &str,
        object_type: &str,
    ) -> Result<()> {
        let kind = match object_type.to_uppercase().as_str() {
            "VIEW" => "VIEW",
            _ => "TABLE",
        };
        let sql = format!(
            "DROP {} IF EXISTS {}.{} CASCADE",
            kind,
            quote_ident(schema),
            quote_ident(object_name)
        );
        sqlx::query(&sql).execute(&self.pool).await?;
        Ok(())
    }

    async fn truncate_table(
        &self,
        _database: &str,
        schema: &str,
        table_name: &str,
    ) -> Result<()> {
        let sql = format!(
            "TRUNCATE TABLE {}.{} CASCADE",
            quote_ident(schema),
            quote_ident(table_name)
        );
        sqlx::query(&sql).execute(&self.pool).await?;
        Ok(())
    }

    async fn get_server_activity(&self) -> Result<Vec<ServerActivity>> {
        let rows = sqlx::query(
            "SELECT pid, usename, datname, state, query, \
             EXTRACT(EPOCH FROM (now() - query_start)) * 1000 as duration_ms, \
             client_addr::text \
             FROM pg_stat_activity \
             WHERE state IS NOT NULL AND pid <> pg_backend_pid() \
             ORDER BY query_start DESC NULLS LAST"
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|r| ServerActivity {
                pid: r.try_get::<i32, _>("pid").map(|v| v.to_string()).unwrap_or_default(),
                user: r.try_get::<String, _>("usename").unwrap_or_default(),
                database: r.try_get::<String, _>("datname").unwrap_or_default(),
                state: r.try_get::<String, _>("state").unwrap_or_default(),
                query: r.try_get::<String, _>("query").unwrap_or_default(),
                duration_ms: r.try_get::<f64, _>("duration_ms").ok(),
                client_addr: r.try_get::<String, _>("client_addr").unwrap_or_default(),
            })
            .collect())
    }

    async fn cancel_query(&self, pid: &str) -> Result<()> {
        let pid_int: i32 = pid.parse()?;
        sqlx::query("SELECT pg_cancel_backend($1)")
            .bind(pid_int)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn apply_changes(&self, changes: &DataChanges) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let fq_table = format!(
            "{}.{}",
            quote_ident(&changes.schema),
            quote_ident(&changes.table)
        );

        for update in &changes.updates {
            let set_clause = format!(
                "{} = {}",
                quote_ident(&update.column_name),
                json_to_sql_literal(&update.new_value)
            );
            let where_clause: Vec<String> = update
                .primary_key_values
                .iter()
                .map(|(col, val)| {
                    format!("{} = {}", quote_ident(col), json_to_sql_literal(val))
                })
                .collect();
            let sql = format!(
                "UPDATE {} SET {} WHERE {}",
                fq_table,
                set_clause,
                where_clause.join(" AND ")
            );
            sqlx::query(&sql).execute(&mut *tx).await?;
        }

        for insert in &changes.inserts {
            let cols: Vec<String> = insert.values.iter().map(|(c, _)| quote_ident(c)).collect();
            let vals: Vec<String> = insert
                .values
                .iter()
                .map(|(_, v)| json_to_sql_literal(v))
                .collect();
            let sql = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                fq_table,
                cols.join(", "),
                vals.join(", ")
            );
            sqlx::query(&sql).execute(&mut *tx).await?;
        }

        for delete in &changes.deletes {
            let where_clause: Vec<String> = delete
                .primary_key_values
                .iter()
                .map(|(col, val)| {
                    format!("{} = {}", quote_ident(col), json_to_sql_literal(val))
                })
                .collect();
            let sql = format!(
                "DELETE FROM {} WHERE {}",
                fq_table,
                where_clause.join(" AND ")
            );
            sqlx::query(&sql).execute(&mut *tx).await?;
        }

        tx.commit().await?;
        Ok(())
    }
}

fn parse_pg_explain_node(json: &serde_json::Value) -> ExplainNode {
    let plan = if let Some(arr) = json.as_array() {
        arr.first()
            .and_then(|v| v.get("Plan"))
            .unwrap_or(&serde_json::Value::Null)
    } else if let Some(p) = json.get("Plan") {
        p
    } else {
        json
    };

    parse_pg_plan_node(plan)
}

fn parse_pg_plan_node(plan: &serde_json::Value) -> ExplainNode {
    let children = plan
        .get("Plans")
        .and_then(|p| p.as_array())
        .map(|arr| arr.iter().map(parse_pg_plan_node).collect())
        .unwrap_or_default();

    ExplainNode {
        node_type: plan
            .get("Node Type")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown")
            .to_string(),
        relation: plan
            .get("Relation Name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        startup_cost: plan
            .get("Startup Cost")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0),
        total_cost: plan
            .get("Total Cost")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0),
        actual_time_ms: plan
            .get("Actual Total Time")
            .and_then(|v| v.as_f64()),
        rows_estimated: plan
            .get("Plan Rows")
            .and_then(|v| v.as_u64())
            .unwrap_or(0),
        rows_actual: plan.get("Actual Rows").and_then(|v| v.as_u64()),
        width: plan
            .get("Plan Width")
            .and_then(|v| v.as_u64())
            .unwrap_or(0),
        filter: plan
            .get("Filter")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        children,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_ident() {
        assert_eq!(quote_ident("col"), r#""col""#);
        assert_eq!(quote_ident(r#"col"umn"#), r#""col""umn""#);
    }

    #[test]
    fn test_filter_is_unsafe() {
        assert!(!filter_is_unsafe(""));
        assert!(!filter_is_unsafe(r#""id" = 1"#));
        assert!(filter_is_unsafe("x; DROP TABLE t"));
        assert!(filter_is_unsafe("x -- comment"));
        assert!(filter_is_unsafe("x /* comment */"));
    }

    #[test]
    fn test_sql_fragment_is_unsafe() {
        assert!(!sql_fragment_is_unsafe("integer"));
        assert!(!sql_fragment_is_unsafe("varchar(255)"));
        assert!(sql_fragment_is_unsafe("int); DROP TABLE t; --"));
        assert!(sql_fragment_is_unsafe("default 'x'"));
    }

    #[test]
    fn test_json_to_sql_literal() {
        assert_eq!(json_to_sql_literal(&serde_json::Value::Null), "NULL");
        assert_eq!(
            json_to_sql_literal(&serde_json::Value::Bool(true)),
            "true"
        );
        assert_eq!(
            json_to_sql_literal(&serde_json::Value::Number(42i64.into())),
            "42"
        );
        assert_eq!(
            json_to_sql_literal(&serde_json::Value::String("O'Brien".into())),
            "'O''Brien'"
        );
    }
}
