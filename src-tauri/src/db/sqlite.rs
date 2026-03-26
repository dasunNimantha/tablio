use anyhow::Result;
use async_trait::async_trait;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Column, Row, SqlitePool, TypeInfo};
use std::time::Instant;

use crate::db::DatabaseDriver;
use crate::models::*;

pub struct SqliteDriver {
    pool: SqlitePool,
}

impl SqliteDriver {
    pub async fn connect(config: &ConnectionConfig) -> Result<Self> {
        let url = if config.database.starts_with("sqlite:") {
            config.database.clone()
        } else {
            format!("sqlite:{}", config.database)
        };
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await?;
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

fn sql_fragment_is_unsafe(s: &str) -> bool {
    s.contains(';') || s.contains("--") || s.contains("/*") || s.contains("*/") || s.contains('\'')
}

fn filter_is_unsafe(filter: &str) -> bool {
    let s = filter.trim();
    s.contains(';') || s.contains("--") || s.contains("/*") || s.contains("*/")
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
        let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;

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
        let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;

        let mut indexes = Vec::new();
        for r in &rows {
            let name: String = r.get("name");
            let unique: i32 = r.get("unique");
            let origin: String = r
                .try_get::<String, _>("origin")
                .unwrap_or_else(|_| "c".into());

            let info_sql = format!("PRAGMA index_info({})", quote_ident(&name));
            let info_rows = sqlx::query(&info_sql).fetch_all(&self.pool).await?;
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
        let sql = format!("PRAGMA foreign_key_list({})", quote_ident(table));
        let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;

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
        let count_row = sqlx::query(&count_sql).fetch_one(&self.pool).await?;
        let total_rows: i64 = count_row.get("cnt");

        let sql = format!(
            "SELECT * FROM {} {} {} LIMIT {} OFFSET {}",
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
            || trimmed.starts_with("PRAGMA")
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
        let explain_sql = format!("EXPLAIN QUERY PLAN {}", sql);
        let rows = sqlx::query(&explain_sql).fetch_all(&self.pool).await?;
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
            "CREATE TABLE {} (\n    {}\n)",
            quote_ident(table_name),
            col_defs.join(",\n    ")
        );
        sqlx::query(&sql).execute(&self.pool).await?;
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
        let count_row = sqlx::query(&count_sql).fetch_one(&self.pool).await?;
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
                    if !d.is_empty() && sql_fragment_is_unsafe(d) {
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
            sqlx::query(&sql).execute(&self.pool).await?;
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
            let result = sqlx::query(&sql).execute(&mut *tx).await?;
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
        sqlx::query(&sql).execute(&self.pool).await?;
        Ok(())
    }

    async fn truncate_table(&self, _database: &str, _schema: &str, table_name: &str) -> Result<()> {
        let sql = format!("DELETE FROM {}", quote_ident(table_name));
        sqlx::query(&sql).execute(&self.pool).await?;
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
                quote_ident(&changes.table),
                cols.join(", "),
                vals.join(", ")
            );
            sqlx::query(&sql).execute(&mut *tx).await?;
        }

        for delete in &changes.deletes {
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
            sqlx::query(&sql).execute(&mut *tx).await?;
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
}
