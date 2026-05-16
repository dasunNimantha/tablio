use anyhow::Result;
use async_trait::async_trait;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Column, Row, SqlitePool, TypeInfo};
use std::time::{Duration, Instant};

use crate::db::DatabaseDriver;
use crate::models::*;
use crate::util::path::expand_tilde;

pub struct SqliteDriver {
    pool: SqlitePool,
}

impl SqliteDriver {
    pub async fn connect(config: &ConnectionConfig) -> Result<Self> {
        // The user-typed value can be either a bare filesystem path
        // (most common, e.g. `~/database.db` or `./foo.db`) or an
        // explicit `sqlite:` URL with query params or `sqlite::memory:`.
        // Tilde-expand the path form so shell conventions work in the
        // GUI; leave URLs alone because their syntax is sqlx's domain
        // (apart from a tilde in the path component).
        let (url, resolved_for_error) = if let Some(rest) = config.database.strip_prefix("sqlite:")
        {
            // Preserve `?mode=...` and similar URL params verbatim.
            let (path_part, query_part) = match rest.find('?') {
                Some(idx) => (&rest[..idx], &rest[idx..]),
                None => (rest, ""),
            };
            let expanded = expand_tilde(path_part);
            let url = format!("sqlite:{}{}", expanded, query_part);
            (url, expanded)
        } else {
            let resolved = expand_tilde(&config.database);
            (format!("sqlite:{}", resolved), resolved)
        };

        // Defer existence + permission checking to sqlx so we report
        // the actual OS error (ENOENT, EACCES, network share offline,
        // sandbox denial, etc.) instead of guessing. Wrap with the
        // resolved path so users can tell whether tilde expansion
        // worked, especially when the input differed from what was
        // ultimately opened.
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .min_connections(0)
            .idle_timeout(Duration::from_secs(1800))
            .connect(&url)
            .await
            .map_err(|e| {
                if resolved_for_error != config.database {
                    anyhow::anyhow!(
                        "Failed to open SQLite database at {} (resolved from \"{}\"): {}",
                        resolved_for_error,
                        config.database,
                        e
                    )
                } else {
                    anyhow::anyhow!(
                        "Failed to open SQLite database at {}: {}",
                        resolved_for_error,
                        e
                    )
                }
            })?;
        Ok(Self { pool })
    }
}

fn sqlite_row_to_json_values(
    row: &sqlx::sqlite::SqliteRow,
    col_count: usize,
) -> Vec<serde_json::Value> {
    let mut values = Vec::with_capacity(col_count);
    for i in 0..col_count {
        let col = row.column(i);
        let type_name = col.type_info().name();
        let val: serde_json::Value = match type_name {
            "BOOLEAN" => row
                .try_get::<bool, _>(i)
                .ok()
                .map(serde_json::Value::Bool)
                .unwrap_or(serde_json::Value::Null),
            "INTEGER" => row
                .try_get::<i64, _>(i)
                .ok()
                .map(|v| serde_json::Value::Number(v.into()))
                .unwrap_or(serde_json::Value::Null),
            "REAL" => row
                .try_get::<f64, _>(i)
                .ok()
                .and_then(serde_json::Number::from_f64)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            "BLOB" => row
                .try_get::<Option<Vec<u8>>, _>(i)
                .ok()
                .flatten()
                .map(|b| {
                    let hex_str: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
                    serde_json::Value::String(format!("X'{}'", hex_str))
                })
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

fn json_to_sql_literal(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        _ => format!("'{}'", val.to_string().replace('\'', "''")),
    }
}

/// Strict — types/operators must never legitimately contain `'`.
fn sql_fragment_is_unsafe(s: &str) -> bool {
    s.contains(';') || s.contains("--") || s.contains("/*") || s.contains("*/") || s.contains('\'')
}

/// Same as `sql_fragment_is_unsafe` but allows `'` so legitimate
/// string-literal defaults survive validation.
fn sql_default_is_unsafe(s: &str) -> bool {
    s.contains(';') || s.contains("--") || s.contains("/*") || s.contains("*/")
}

/// Mirrors `pg_common::filter_is_unsafe` so SQLite rejects the same set
/// of patterns as the Postgres path: statement terminator `;`, comment
/// markers `--` / `/* */`, nested `(SELECT ...)` subqueries, and
/// `UNION SELECT` shape changes.
fn filter_is_unsafe(filter: &str) -> bool {
    let s = filter.trim();
    if s.contains(';') || s.contains("--") || s.contains("/*") || s.contains("*/") {
        return true;
    }
    let u = s.to_uppercase();
    if u.contains("(SELECT") {
        return true;
    }
    u.contains(" UNION SELECT")
        || u.contains(" UNION ALL SELECT")
        || u.contains(" UNION DISTINCT SELECT")
}

fn filter_unsafe_reason(filter: &str) -> Option<&'static str> {
    let s = filter.trim();
    if s.contains(';') {
        return Some("statement terminator (`;`)");
    }
    if s.contains("--") || s.contains("/*") || s.contains("*/") {
        return Some("SQL comment markers (`--` / `/* */`)");
    }
    let u = s.to_uppercase();
    if u.contains("(SELECT") {
        return Some("nested `(SELECT ...)` subquery");
    }
    if u.contains(" UNION SELECT")
        || u.contains(" UNION ALL SELECT")
        || u.contains(" UNION DISTINCT SELECT")
    {
        return Some("`UNION SELECT` clause");
    }
    None
}

fn format_bytes(bytes: i64) -> String {
    const KB: i64 = 1024;
    const MB: i64 = KB * 1024;
    const GB: i64 = MB * 1024;
    if bytes < KB {
        format!("{} B", bytes)
    } else if bytes < MB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else if bytes < GB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    }
}

#[async_trait]
impl DatabaseDriver for SqliteDriver {
    async fn list_roles(&self) -> Result<Vec<RoleInfo>> {
        Ok(vec![])
    }

    async fn create_role(&self, _req: &CreateRoleRequest) -> Result<()> {
        Err(anyhow::anyhow!("Roles are not supported in SQLite"))
    }

    async fn drop_role(&self, _name: &str) -> Result<()> {
        Err(anyhow::anyhow!("Roles are not supported in SQLite"))
    }

    async fn alter_role(&self, _req: &AlterRoleRequest) -> Result<()> {
        Err(anyhow::anyhow!("Roles are not supported in SQLite"))
    }

    async fn test_connection(&self) -> Result<bool> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(true)
    }

    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        Ok(vec![DatabaseInfo {
            name: "main".to_string(),
        }])
    }

    async fn list_schemas(&self, _database: &str) -> Result<Vec<SchemaInfo>> {
        Ok(vec![SchemaInfo {
            name: "main".to_string(),
        }])
    }

    async fn list_tables(&self, _database: &str, _schema: &str) -> Result<Vec<TableInfo>> {
        let rows = sqlx::query(
            "SELECT name, type FROM sqlite_master \
             WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' \
             ORDER BY name",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(|r| {
                let ttype: String = r.get("type");
                TableInfo {
                    name: r.get("name"),
                    schema: "main".to_string(),
                    table_type: if ttype == "table" {
                        "BASE TABLE".to_string()
                    } else {
                        "VIEW".to_string()
                    },
                    row_count_estimate: None,
                    ..Default::default()
                }
            })
            .collect())
    }

    async fn list_columns(
        &self,
        _database: &str,
        _schema: &str,
        table: &str,
    ) -> Result<Vec<ColumnInfo>> {
        let sql = format!("PRAGMA table_info({})", quote_ident(table));
        let rows = sqlx::query(sqlx::AssertSqlSafe(&*sql))
            .fetch_all(&self.pool)
            .await?;

        Ok(rows
            .iter()
            .map(|r| {
                let pk: i32 = r.get("pk");
                let col_type: String = r.get("type");
                let is_rowid_alias = pk > 0 && col_type.to_uppercase().contains("INTEGER");
                ColumnInfo {
                    name: r.get("name"),
                    data_type: col_type,
                    is_nullable: {
                        let notnull: i32 = r.get("notnull");
                        notnull == 0
                    },
                    is_primary_key: pk > 0,
                    default_value: r.try_get::<String, _>("dflt_value").ok(),
                    ordinal_position: r.get("cid"),
                    is_auto_generated: is_rowid_alias,
                }
            })
            .collect())
    }

    async fn list_indexes(
        &self,
        _database: &str,
        _schema: &str,
        table: &str,
    ) -> Result<Vec<IndexInfo>> {
        let sql = format!("PRAGMA index_list({})", quote_ident(table));
        let rows = sqlx::query(sqlx::AssertSqlSafe(&*sql))
            .fetch_all(&self.pool)
            .await?;

        let mut indexes = Vec::new();
        for r in &rows {
            let name: String = r.get("name");
            let unique: i32 = r.get("unique");
            let origin: String = r
                .try_get::<String, _>("origin")
                .unwrap_or_else(|_| "c".into());

            let info_sql = format!("PRAGMA index_info({})", quote_ident(&name));
            let info_rows = sqlx::query(sqlx::AssertSqlSafe(&*info_sql))
                .fetch_all(&self.pool)
                .await?;
            let columns: Vec<String> = info_rows.iter().map(|ir| ir.get("name")).collect();

            indexes.push(IndexInfo {
                name,
                columns,
                is_unique: unique != 0,
                index_type: if origin == "pk" {
                    "PRIMARY".into()
                } else {
                    "BTREE".into()
                },
            });
        }
        Ok(indexes)
    }

    async fn list_foreign_keys(
        &self,
        _database: &str,
        _schema: &str,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        let rows = sqlx::query("SELECT * FROM pragma_foreign_key_list(?)")
            .bind(table)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows
            .iter()
            .map(|r| {
                let id: i32 = r.get("id");
                ForeignKeyInfo {
                    name: format!("fk_{}", id),
                    column: r.get("from"),
                    referenced_table: r.get("table"),
                    referenced_column: r.get("to"),
                    on_delete: r
                        .try_get::<String, _>("on_delete")
                        .unwrap_or_else(|_| "NO ACTION".into()),
                    on_update: r
                        .try_get::<String, _>("on_update")
                        .unwrap_or_else(|_| "NO ACTION".into()),
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
        if let Some(ref f) = filter {
            if !f.trim().is_empty() {
                if let Some(reason) = filter_unsafe_reason(f) {
                    anyhow::bail!("Filter rejected: {}", reason);
                }
                debug_assert!(!filter_is_unsafe(f));
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
            .unwrap_or_else(|| {
                let pk_cols: Vec<String> = columns
                    .iter()
                    .filter(|c| c.is_primary_key)
                    .map(|c| quote_ident(&c.name))
                    .collect();
                if pk_cols.is_empty() {
                    String::new()
                } else {
                    format!("ORDER BY {}", pk_cols.join(", "))
                }
            });

        let count_sql = format!(
            "SELECT COUNT(*) as cnt FROM {} {}",
            quote_ident(table),
            where_clause
        );
        let count_row = sqlx::query(sqlx::AssertSqlSafe(&*count_sql))
            .fetch_one(&self.pool)
            .await?;
        let total_rows: i64 = count_row.get("cnt");

        let sql = format!(
            "SELECT * FROM {} {} {} LIMIT {} OFFSET {}",
            quote_ident(table),
            where_clause,
            order_clause,
            limit,
            offset
        );

        let rows = sqlx::query(sqlx::AssertSqlSafe(&*sql))
            .fetch_all(&self.pool)
            .await?;
        let col_count = columns.len();
        let data_rows: Vec<Vec<serde_json::Value>> = rows
            .iter()
            .map(|r| sqlite_row_to_json_values(r, col_count))
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
            || trimmed.starts_with("PRAGMA")
            || trimmed.starts_with("EXPLAIN");

        if is_select {
            let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
                .fetch_all(&self.pool)
                .await?;
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
                .map(|r| sqlite_row_to_json_values(r, col_count))
                .collect();

            Ok(QueryResult {
                columns,
                rows: data_rows,
                rows_affected: 0,
                execution_time_ms: elapsed,
                is_select: true,
            })
        } else {
            let result = sqlx::query(sqlx::AssertSqlSafe(sql))
                .execute(&self.pool)
                .await?;
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
        let explain_sql = format!("EXPLAIN QUERY PLAN {}", sql);
        let rows = sqlx::query(sqlx::AssertSqlSafe(&*explain_sql))
            .fetch_all(&self.pool)
            .await?;
        let elapsed = start.elapsed().as_millis() as u64;

        let mut raw_lines = Vec::new();
        for row in &rows {
            let detail: String = row.try_get("detail").unwrap_or_default();
            raw_lines.push(detail);
        }
        let raw_text = raw_lines.join("\n");

        let node_type = raw_lines
            .first()
            .cloned()
            .unwrap_or_else(|| "Query Plan".to_string());
        let plan = ExplainNode {
            node_type,
            relation: None,
            startup_cost: 0.0,
            total_cost: 0.0,
            actual_time_ms: None,
            rows_estimated: 0,
            rows_actual: None,
            width: 0,
            filter: None,
            children: raw_lines
                .iter()
                .skip(1)
                .map(|line| ExplainNode {
                    node_type: line.clone(),
                    relation: None,
                    startup_cost: 0.0,
                    total_cost: 0.0,
                    actual_time_ms: None,
                    rows_estimated: 0,
                    rows_actual: None,
                    width: 0,
                    filter: None,
                    children: vec![],
                })
                .collect(),
        };

        Ok(ExplainResult {
            plan,
            raw_text,
            execution_time_ms: elapsed,
        })
    }

    async fn validate_query(&self, _database: &str, sql: &str) -> Result<Option<ValidationError>> {
        if sql.trim().is_empty() {
            return Ok(Some(ValidationError {
                message: "Empty query".to_string(),
                position: None,
            }));
        }
        use sqlx::Executor;
        match self
            .pool
            .prepare(
                <sqlx::AssertSqlSafe<&str> as sqlx::SqlSafeStr>::into_sql_str(sqlx::AssertSqlSafe(
                    sql,
                )),
            )
            .await
        {
            Ok(_) => Ok(None),
            Err(e) => {
                let message = if let Some(db_err) = e.as_database_error() {
                    db_err.message().to_string()
                } else {
                    e.to_string()
                };
                Ok(Some(ValidationError {
                    message,
                    position: None,
                }))
            }
        }
    }

    async fn get_ddl(
        &self,
        _database: &str,
        _schema: &str,
        object_name: &str,
        _object_type: &str,
    ) -> Result<String> {
        let sql = "SELECT sql FROM sqlite_master WHERE name = ?";
        let row = sqlx::query(sql)
            .bind(object_name)
            .fetch_one(&self.pool)
            .await?;
        let ddl: String = row.try_get("sql")?;
        Ok(ddl)
    }

    async fn create_table(
        &self,
        _database: &str,
        _schema: &str,
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
                if !d.is_empty() && sql_default_is_unsafe(d) {
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
            "CREATE TABLE {} (\n    {}\n)",
            quote_ident(table_name),
            col_defs.join(",\n    ")
        );
        sqlx::query(sqlx::AssertSqlSafe(&*sql))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_functions(&self, _database: &str, _schema: &str) -> Result<Vec<FunctionInfo>> {
        // SQLite doesn't have user-defined functions accessible via SQL
        Ok(vec![])
    }

    async fn list_triggers(
        &self,
        _database: &str,
        _schema: &str,
        table: &str,
    ) -> Result<Vec<TriggerInfo>> {
        let sql = "SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger' AND tbl_name = ? ORDER BY name";
        let rows = sqlx::query(sql).bind(table).fetch_all(&self.pool).await?;

        let triggers: Vec<TriggerInfo> = rows
            .iter()
            .map(|r| {
                let name: String = r.get("name");
                let tbl_name: String = r.get("tbl_name");
                let sql_def: Option<String> = r.try_get("sql").ok();

                let (event, timing) = sql_def
                    .as_ref()
                    .map(|s| {
                        let s = s.to_uppercase();
                        let timing = if s.contains("BEFORE ") {
                            "BEFORE"
                        } else if s.contains("AFTER ") {
                            "AFTER"
                        } else if s.contains("INSTEAD OF ") {
                            "INSTEAD OF"
                        } else {
                            "UNKNOWN"
                        };
                        let event = if s.contains(" INSERT ") {
                            "INSERT"
                        } else if s.contains(" DELETE ") {
                            "DELETE"
                        } else if s.contains(" UPDATE ") {
                            "UPDATE"
                        } else {
                            "UNKNOWN"
                        };
                        (event.to_string(), timing.to_string())
                    })
                    .unwrap_or_else(|| ("UNKNOWN".to_string(), "UNKNOWN".to_string()));

                TriggerInfo {
                    name,
                    table_name: tbl_name,
                    event,
                    timing,
                }
            })
            .collect();

        Ok(triggers)
    }

    async fn get_table_stats(
        &self,
        _database: &str,
        _schema: &str,
        table: &str,
    ) -> Result<TableStats> {
        let count_sql = format!("SELECT COUNT(*) as cnt FROM {}", quote_ident(table));
        let count_row = sqlx::query(sqlx::AssertSqlSafe(&*count_sql))
            .fetch_one(&self.pool)
            .await?;
        let row_count: i64 = count_row.get("cnt");

        let size_row = sqlx::query(
            "SELECT (SELECT page_count FROM pragma_page_count('main')) * (SELECT page_size FROM pragma_page_size('main')) AS total_bytes",
        )
        .fetch_one(&self.pool)
        .await?;
        let total_bytes: i64 = size_row.try_get("total_bytes").unwrap_or(0);
        let total_size = format_bytes(total_bytes);

        Ok(TableStats {
            table_name: table.to_string(),
            row_count,
            total_size: total_size.clone(),
            index_size: total_size.clone(),
            data_size: total_size,
            last_vacuum: None,
            last_analyze: None,
            dead_tuples: None,
            live_tuples: Some(row_count),
        })
    }

    async fn alter_table(
        &self,
        _database: &str,
        _schema: &str,
        table_name: &str,
        operations: &[AlterTableOperation],
    ) -> Result<()> {
        for op in operations {
            if let AlterTableOperation::AddColumn { column } = op {
                if sql_fragment_is_unsafe(&column.data_type) {
                    anyhow::bail!("Invalid character in data type for column {}", column.name);
                }
                if let Some(d) = &column.default_value {
                    if !d.is_empty() && sql_default_is_unsafe(d) {
                        anyhow::bail!(
                            "Invalid character in default value for column {}",
                            column.name
                        );
                    }
                }
            }
        }

        let mut current_table = table_name.to_string();

        for op in operations {
            let table_ref = quote_ident(&current_table);

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
                AlterTableOperation::ChangeColumnType { .. }
                | AlterTableOperation::SetNullable { .. }
                | AlterTableOperation::SetDefault { .. } => {
                    return Err(anyhow::anyhow!(
                        "SQLite does not support this ALTER TABLE operation"
                    ));
                }
                AlterTableOperation::RenameTable { new_name } => {
                    current_table = new_name.clone();
                    format!(
                        "ALTER TABLE {} RENAME TO {}",
                        table_ref,
                        quote_ident(new_name)
                    )
                }
            };
            sqlx::query(sqlx::AssertSqlSafe(&*sql))
                .execute(&self.pool)
                .await?;
        }

        Ok(())
    }

    async fn import_data(
        &self,
        _database: &str,
        _schema: &str,
        table: &str,
        columns: &[String],
        rows: &[Vec<serde_json::Value>],
    ) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        let table_ref = quote_ident(table);
        let col_list: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
        let col_str = col_list.join(", ");

        const BATCH_SIZE: usize = 500;
        let mut total_inserted: u64 = 0;
        let mut tx = self.pool.begin().await?;

        for chunk in rows.chunks(BATCH_SIZE) {
            let mut values_list = Vec::with_capacity(chunk.len());
            for row in chunk {
                let vals: Vec<String> = row.iter().map(json_to_sql_literal).collect();
                values_list.push(format!("({})", vals.join(", ")));
            }
            let values_str = values_list.join(", ");
            let sql = format!(
                "INSERT INTO {} ({}) VALUES {}",
                table_ref, col_str, values_str
            );
            let result = sqlx::query(sqlx::AssertSqlSafe(&*sql))
                .execute(&mut *tx)
                .await?;
            total_inserted += result.rows_affected();
        }

        tx.commit().await?;
        Ok(total_inserted)
    }

    async fn drop_object(
        &self,
        _database: &str,
        _schema: &str,
        object_name: &str,
        object_type: &str,
    ) -> Result<()> {
        let kind = match object_type.to_uppercase().as_str() {
            "VIEW" => "VIEW",
            _ => "TABLE",
        };
        let sql = format!("DROP {} IF EXISTS {}", kind, quote_ident(object_name));
        sqlx::query(sqlx::AssertSqlSafe(&*sql))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn truncate_table(&self, _database: &str, _schema: &str, table_name: &str) -> Result<()> {
        let sql = format!("DELETE FROM {}", quote_ident(table_name));
        sqlx::query(sqlx::AssertSqlSafe(&*sql))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_server_activity(&self) -> Result<Vec<ServerActivity>> {
        Ok(vec![])
    }

    async fn get_database_stats(&self) -> Result<DatabaseStats> {
        Ok(DatabaseStats {
            active_connections: 0,
            idle_connections: 0,
            idle_in_transaction: 0,
            total_connections: 1,
            xact_commit: 0,
            xact_rollback: 0,
            tup_inserted: 0,
            tup_updated: 0,
            tup_deleted: 0,
            tup_fetched: 0,
            blks_read: 0,
            blks_hit: 0,
            timestamp_ms: 0.0,
        })
    }

    async fn get_locks(&self) -> Result<Vec<LockInfo>> {
        Ok(vec![])
    }

    async fn get_server_config(&self) -> Result<Vec<ServerConfigEntry>> {
        Ok(vec![])
    }

    async fn get_query_stats(&self) -> Result<QueryStatsResponse> {
        Ok(QueryStatsResponse {
            available: false,
            kind: QueryStatsKind::EngineUnsupported,
            message: Some("Query statistics are not supported for SQLite.".to_string()),
            entries: vec![],
        })
    }

    async fn cancel_query(&self, _pid: &str) -> Result<()> {
        Ok(())
    }

    async fn apply_changes(&self, changes: &DataChanges) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        for update in &changes.updates {
            if update.primary_key_values.is_empty() {
                anyhow::bail!("Cannot update row: no primary key values provided");
            }
            let set_clause = format!(
                "{} = {}",
                quote_ident(&update.column_name),
                json_to_sql_literal(&update.new_value)
            );
            let where_clause: Vec<String> = update
                .primary_key_values
                .iter()
                .map(|(col, val)| format!("{} = {}", quote_ident(col), json_to_sql_literal(val)))
                .collect();
            let sql = format!(
                "UPDATE {} SET {} WHERE {}",
                quote_ident(&changes.table),
                set_clause,
                where_clause.join(" AND ")
            );
            sqlx::query(sqlx::AssertSqlSafe(&*sql))
                .execute(&mut *tx)
                .await?;
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
                quote_ident(&changes.table),
                cols.join(", "),
                vals.join(", ")
            );
            sqlx::query(sqlx::AssertSqlSafe(&*sql))
                .execute(&mut *tx)
                .await?;
        }

        for delete in &changes.deletes {
            if delete.primary_key_values.is_empty() {
                anyhow::bail!("Cannot delete row: no primary key values provided");
            }
            let where_clause: Vec<String> = delete
                .primary_key_values
                .iter()
                .map(|(col, val)| format!("{} = {}", quote_ident(col), json_to_sql_literal(val)))
                .collect();
            let sql = format!(
                "DELETE FROM {} WHERE {}",
                quote_ident(&changes.table),
                where_clause.join(" AND ")
            );
            sqlx::query(sqlx::AssertSqlSafe(&*sql))
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_safe_empty() {
        assert!(!filter_is_unsafe(""));
    }

    #[test]
    fn filter_safe_expression() {
        assert!(!filter_is_unsafe(r#""id" = 1"#));
        assert!(!filter_is_unsafe(r#""name" LIKE '%test%'"#));
    }

    #[test]
    fn filter_unsafe_semicolon() {
        assert!(filter_is_unsafe("x; DROP TABLE t"));
    }

    #[test]
    fn filter_unsafe_line_comment() {
        assert!(filter_is_unsafe("x -- comment"));
    }

    #[test]
    fn filter_unsafe_block_comment() {
        assert!(filter_is_unsafe("x /* evil */"));
        assert!(filter_is_unsafe("x */"));
    }

    #[test]
    fn quote_ident_simple() {
        assert_eq!(quote_ident("col"), r#""col""#);
    }

    #[test]
    fn quote_ident_with_double_quote() {
        assert_eq!(quote_ident(r#"col"umn"#), r#""col""umn""#);
    }

    #[test]
    fn quote_ident_empty() {
        assert_eq!(quote_ident(""), r#""""#);
    }

    #[test]
    fn json_to_sql_null() {
        assert_eq!(json_to_sql_literal(&serde_json::Value::Null), "NULL");
    }

    #[test]
    fn json_to_sql_bool_maps_to_integer() {
        assert_eq!(json_to_sql_literal(&serde_json::Value::Bool(true)), "1");
        assert_eq!(json_to_sql_literal(&serde_json::Value::Bool(false)), "0");
    }

    #[test]
    fn json_to_sql_number() {
        assert_eq!(
            json_to_sql_literal(&serde_json::Value::Number(42i64.into())),
            "42"
        );
    }

    #[test]
    fn json_to_sql_string_escapes() {
        assert_eq!(
            json_to_sql_literal(&serde_json::Value::String("O'Brien".into())),
            "'O''Brien'"
        );
    }

    #[test]
    fn json_to_sql_empty_string() {
        assert_eq!(
            json_to_sql_literal(&serde_json::Value::String("".into())),
            "''"
        );
    }

    #[test]
    fn format_bytes_small() {
        assert_eq!(format_bytes(500), "500 B");
    }

    #[test]
    fn format_bytes_kb() {
        assert_eq!(format_bytes(2048), "2.0 KB");
    }

    #[test]
    fn format_bytes_mb() {
        assert_eq!(format_bytes(1048576), "1.0 MB");
    }

    #[test]
    fn format_bytes_gb() {
        assert_eq!(format_bytes(1073741824), "1.0 GB");
    }

    #[test]
    fn format_bytes_zero() {
        assert_eq!(format_bytes(0), "0 B");
    }

    #[test]
    fn sql_fragment_safe_types() {
        assert!(!sql_fragment_is_unsafe("integer"));
        assert!(!sql_fragment_is_unsafe("varchar(255)"));
        assert!(!sql_fragment_is_unsafe("decimal(10,2)"));
        assert!(!sql_fragment_is_unsafe("boolean"));
        assert!(!sql_fragment_is_unsafe("TEXT"));
    }

    #[test]
    fn sql_fragment_safe_empty() {
        assert!(!sql_fragment_is_unsafe(""));
    }

    #[test]
    fn sql_fragment_unsafe_semicolon() {
        assert!(sql_fragment_is_unsafe("int); DROP TABLE t; --"));
    }

    #[test]
    fn sql_fragment_unsafe_single_quote() {
        assert!(sql_fragment_is_unsafe("default 'x'"));
    }

    #[test]
    fn sql_fragment_unsafe_line_comment() {
        assert!(sql_fragment_is_unsafe("int --evil"));
    }

    #[test]
    fn sql_fragment_unsafe_block_comment() {
        assert!(sql_fragment_is_unsafe("int /* evil */"));
    }

    #[test]
    fn sql_fragment_unsafe_block_comment_close() {
        assert!(sql_fragment_is_unsafe("int */"));
    }

    // -----------------------------------------------------------------------
    // Regression tests for fixes applied to prevent future issues
    // -----------------------------------------------------------------------

    #[test]
    fn with_cte_treated_as_select() {
        let trimmed = "WITH cte AS (SELECT 1) SELECT * FROM cte"
            .trim()
            .to_uppercase();
        let is_select = trimmed.starts_with("SELECT")
            || trimmed.starts_with("WITH")
            || trimmed.starts_with("PRAGMA")
            || trimmed.starts_with("EXPLAIN");
        assert!(is_select);
    }

    #[test]
    fn with_lowercase_treated_as_select() {
        let trimmed = "  with recursive t as (select 1) select * from t"
            .trim()
            .to_uppercase();
        let is_select = trimmed.starts_with("SELECT")
            || trimmed.starts_with("WITH")
            || trimmed.starts_with("PRAGMA")
            || trimmed.starts_with("EXPLAIN");
        assert!(is_select);
    }

    #[test]
    fn insert_not_treated_as_select() {
        let trimmed = "INSERT INTO t VALUES (1)".trim().to_uppercase();
        let is_select = trimmed.starts_with("SELECT")
            || trimmed.starts_with("WITH")
            || trimmed.starts_with("PRAGMA")
            || trimmed.starts_with("EXPLAIN");
        assert!(!is_select);
    }

    #[test]
    fn filter_unsafe_union_injection() {
        assert!(filter_is_unsafe("1=1 UNION SELECT * FROM sqlite_master--"));
    }

    #[test]
    fn filter_safe_between() {
        assert!(!filter_is_unsafe("\"price\" BETWEEN 10 AND 100"));
    }

    #[test]
    fn quote_ident_prevents_injection() {
        let evil = r#""; DROP TABLE users; --"#;
        let quoted = quote_ident(evil);
        assert!(quoted.starts_with('"'));
        assert!(quoted.ends_with('"'));
        // Inner `"` is doubled so the payload cannot close the identifier early.
        assert_eq!(quoted, "\"\"\"; DROP TABLE users; --\"");
    }

    #[test]
    fn json_to_sql_string_with_backslash() {
        let val = json_to_sql_literal(&serde_json::Value::String("path\\to\\file".into()));
        assert_eq!(val, "'path\\to\\file'");
    }

    #[test]
    fn json_to_sql_string_injection_attempt() {
        let val = json_to_sql_literal(&serde_json::Value::String("'; DROP TABLE t; --".into()));
        assert_eq!(val, "'''; DROP TABLE t; --'");
    }

    // -----------------------------------------------------------------------
    // Cross-driver parity: the SQLite filter check now matches Postgres.
    // Previously it only blocked `;`, `--`, `/*`, `*/`, so `UNION SELECT`
    // and `(SELECT ...)` payloads slipped through.
    // -----------------------------------------------------------------------

    #[test]
    fn filter_unsafe_unionless_subquery_now_blocked() {
        assert!(filter_is_unsafe("1=1 OR (SELECT 1)"));
        assert!(filter_is_unsafe("(SELECT count(*) FROM sqlite_master) > 0"));
    }

    #[test]
    fn filter_unsafe_union_without_comments_now_blocked() {
        assert!(filter_is_unsafe("1=1 UNION SELECT * FROM sqlite_master"));
        assert!(filter_is_unsafe("1=1 UNION ALL SELECT 1"));
        assert!(filter_is_unsafe("1=1 UNION DISTINCT SELECT 1"));
    }

    #[test]
    fn filter_unsafe_reason_descriptive_messages() {
        assert_eq!(
            filter_unsafe_reason("x; DROP TABLE t"),
            Some("statement terminator (`;`)")
        );
        assert_eq!(
            filter_unsafe_reason("(SELECT 1)"),
            Some("nested `(SELECT ...)` subquery")
        );
        assert!(filter_unsafe_reason("\"id\" = 1").is_none());
    }

    #[test]
    fn sql_default_allows_string_literals_sqlite() {
        // Same regression class as pg_common / mysql_common.
        assert!(!sql_default_is_unsafe("'pending'"));
        assert!(!sql_default_is_unsafe("'O''Brien'"));
        assert!(!sql_default_is_unsafe("CURRENT_TIMESTAMP"));
    }

    #[test]
    fn sql_default_still_blocks_injection() {
        assert!(sql_default_is_unsafe("'x'; DROP TABLE users"));
        assert!(sql_default_is_unsafe("'x' -- comment"));
        assert!(sql_default_is_unsafe("'x' /* comment */"));
    }

    // -----------------------------------------------------------------------
    // SqliteDriver::connect — regression tests for issue #106
    // (tilde paths and other path-form handling).
    // -----------------------------------------------------------------------

    fn sqlite_config(database: &str) -> ConnectionConfig {
        ConnectionConfig {
            id: "test".into(),
            db_type: DbType::Sqlite,
            database: database.into(),
            ..Default::default()
        }
    }

    /// Tests that mutate `$HOME` must run one at a time. cargo runs
    /// tests in parallel by default and `dirs::home_dir()` reads the
    /// env var lazily, so without serialisation a parallel test can
    /// flip `$HOME` mid-`connect()` and break tilde resolution.
    #[cfg(unix)]
    fn home_env_lock() -> &'static tokio::sync::Mutex<()> {
        use std::sync::OnceLock;
        static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    /// Run `body` with `$HOME` retargeted to `home_dir`, restoring the
    /// previous value before returning. The lock guarantees no
    /// sibling test can race on `$HOME` while we're inside.
    #[cfg(unix)]
    async fn with_home<F, Fut, T>(home_dir: &std::path::Path, body: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let _guard = home_env_lock().lock().await;
        let original = std::env::var_os("HOME");
        unsafe {
            std::env::set_var("HOME", home_dir);
        }
        let result = body().await;
        unsafe {
            match original {
                Some(v) => std::env::set_var("HOME", v),
                None => std::env::remove_var("HOME"),
            }
        }
        result
    }

    /// Helper: open + run `SELECT 1` so we can assert connect+query
    /// succeed without needing `Debug` on `SqliteDriver`.
    async fn connect_and_select_one(cfg: &ConnectionConfig) -> Result<i64> {
        let driver = SqliteDriver::connect(cfg).await?;
        let row: (i64,) = sqlx::query_as("SELECT 1").fetch_one(&driver.pool).await?;
        Ok(row.0)
    }

    /// `connect` opens an existing file when given an absolute path.
    /// Establishes the baseline before exercising the path-rewriting
    /// branches.
    #[tokio::test]
    async fn connect_opens_existing_absolute_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("baseline.db");
        // Touch the file so sqlx (with default `create_if_missing=false`)
        // will find it.
        std::fs::File::create(&path).unwrap();

        let cfg = sqlite_config(path.to_str().unwrap());
        let result = connect_and_select_one(&cfg).await;
        match result {
            Ok(v) => assert_eq!(v, 1),
            Err(e) => panic!("should open existing absolute-path SQLite file: {e:#}"),
        }
    }

    /// `connect` expands a leading `~/...` against `$HOME`. We retarget
    /// `$HOME` to a tempdir so the test never touches the real home,
    /// then assert that the file opens via the tilde path.
    #[cfg(unix)]
    #[tokio::test]
    async fn connect_expands_tilde_home_path() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("tilde-home.db");
        std::fs::File::create(&db_path).unwrap();

        let result = with_home(dir.path(), || async {
            let cfg = sqlite_config("~/tilde-home.db");
            connect_and_select_one(&cfg).await
        })
        .await;

        match result {
            Ok(v) => assert_eq!(v, 1),
            Err(e) => panic!("tilde path should expand and open: {e:#}"),
        }
    }

    /// A non-existent path surfaces an error that mentions both the
    /// resolved filesystem path and the original user input, so the
    /// dialog can show a useful diagnostic.
    #[tokio::test]
    async fn connect_missing_file_error_mentions_resolved_path() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does-not-exist.db");

        let cfg = sqlite_config(missing.to_str().unwrap());
        let err = match SqliteDriver::connect(&cfg).await {
            Ok(_) => panic!("missing file should fail to open"),
            Err(e) => e,
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("Failed to open SQLite database"),
            "expected wrapped error, got: {msg}"
        );
        assert!(
            msg.contains(missing.to_str().unwrap()),
            "expected resolved path in error, got: {msg}"
        );
    }

    /// When the input differs from the resolved path (e.g. a tilde was
    /// expanded), the error must show *both* so the user can tell
    /// whether shell-style expansion ran as expected.
    #[cfg(unix)]
    #[tokio::test]
    async fn connect_missing_tilde_path_error_shows_both_forms() {
        let dir = tempfile::tempdir().unwrap();

        let result = with_home(dir.path(), || async {
            let cfg = sqlite_config("~/never-created.db");
            SqliteDriver::connect(&cfg).await
        })
        .await;

        let err = match result {
            Ok(_) => panic!("missing tilde path should fail"),
            Err(e) => e,
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains("~/never-created.db"),
            "error should echo original input, got: {msg}"
        );
        assert!(
            msg.contains("never-created.db") && msg.contains("resolved from"),
            "error should mention resolved path, got: {msg}"
        );
    }

    /// `sqlite::memory:` URLs must continue to open in-memory databases
    /// and not be mistaken for a non-existent file path.
    #[tokio::test]
    async fn connect_memory_url_opens_in_memory_db() {
        let cfg = sqlite_config("sqlite::memory:");
        let driver = match SqliteDriver::connect(&cfg).await {
            Ok(d) => d,
            Err(e) => panic!("in-memory SQLite URL should open: {e:#}"),
        };
        sqlx::query("CREATE TABLE t (x INTEGER)")
            .execute(&driver.pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO t VALUES (1), (2)")
            .execute(&driver.pool)
            .await
            .unwrap();
        let row: (i64,) = sqlx::query_as("SELECT count(*) FROM t")
            .fetch_one(&driver.pool)
            .await
            .unwrap();
        assert_eq!(row.0, 2);
    }

    /// Query-string params on `sqlite:` URLs (e.g. `?mode=rwc`) must
    /// reach sqlx untouched so users can opt into create-if-missing
    /// or read-only behaviour.
    #[tokio::test]
    async fn connect_url_with_query_string_creates_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rwc.db");
        // Note: file does NOT exist beforehand.
        let url = format!("sqlite:{}?mode=rwc", path.to_string_lossy());

        let cfg = sqlite_config(&url);
        match connect_and_select_one(&cfg).await {
            Ok(v) => assert_eq!(v, 1),
            Err(e) => panic!("?mode=rwc should create the file: {e:#}"),
        }
        assert!(
            path.exists(),
            "?mode=rwc should have created the database file"
        );
    }

    /// Tilde inside the path component of an `sqlite:` URL is also
    /// expanded — we don't want the URL form to be a footgun where the
    /// path form works but `sqlite:~/foo` doesn't.
    #[cfg(unix)]
    #[tokio::test]
    async fn connect_url_with_tilde_in_path_is_expanded() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("url-tilde.db");
        std::fs::File::create(&db_path).unwrap();

        let result = with_home(dir.path(), || async {
            let cfg = sqlite_config("sqlite:~/url-tilde.db");
            connect_and_select_one(&cfg).await
        })
        .await;

        match result {
            Ok(v) => assert_eq!(v, 1),
            Err(e) => panic!("tilde inside sqlite: URL should expand and open: {e:#}"),
        }
    }
}
