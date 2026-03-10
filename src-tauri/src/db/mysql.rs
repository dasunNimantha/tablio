use anyhow::Result;
use async_trait::async_trait;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{Column, MySqlPool, Row, TypeInfo};
use std::time::Instant;

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
            config.user, config.password, config.host, config.port, config.database, ssl_mode
        );
        let pool = MySqlPoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await?;
        Ok(Self { pool })
    }
}

fn mysql_row_to_json_values(
    row: &sqlx::mysql::MySqlRow,
    col_count: usize,
) -> Vec<serde_json::Value> {
    let mut values = Vec::with_capacity(col_count);
    for i in 0..col_count {
        let col = row.column(i);
        let type_name = col.type_info().name();
        let val: serde_json::Value = match type_name {
            "BOOLEAN" | "TINYINT(1)" => row
                .try_get::<bool, _>(i)
                .ok()
                .map(serde_json::Value::Bool)
                .unwrap_or(serde_json::Value::Null),
            "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" => row
                .try_get::<i32, _>(i)
                .ok()
                .map(|v| serde_json::Value::Number(v.into()))
                .unwrap_or(serde_json::Value::Null),
            "BIGINT" => row
                .try_get::<i64, _>(i)
                .ok()
                .map(|v| serde_json::Value::Number(v.into()))
                .unwrap_or(serde_json::Value::Null),
            "FLOAT" => row
                .try_get::<f32, _>(i)
                .ok()
                .and_then(|v| serde_json::Number::from_f64(v as f64))
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            "DOUBLE" | "DECIMAL" => row
                .try_get::<f64, _>(i)
                .ok()
                .and_then(|v| serde_json::Number::from_f64(v))
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            "JSON" => row
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
    format!("`{}`", name.replace('`', "``"))
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

fn filter_is_unsafe(filter: &str) -> bool {
    let s = filter.trim();
    s.contains(';') || s.contains("--") || s.contains("/*") || s.contains("*/")
}

fn format_bytes(bytes: Option<i64>) -> String {
    let bytes = match bytes {
        Some(b) if b >= 0 => b as u64,
        _ => 0,
    };
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;
    if bytes >= TB {
        format!("{:.1} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.0} kB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

#[async_trait]
impl DatabaseDriver for MysqlDriver {
    async fn list_roles(&self) -> Result<Vec<RoleInfo>> {
        Ok(vec![])
    }

    async fn create_role(&self, _req: &CreateRoleRequest) -> Result<()> {
        Err(anyhow::anyhow!("Role management not yet implemented for MySQL"))
    }

    async fn drop_role(&self, _name: &str) -> Result<()> {
        Err(anyhow::anyhow!("Role management not yet implemented for MySQL"))
    }

    async fn alter_role(&self, _req: &AlterRoleRequest) -> Result<()> {
        Err(anyhow::anyhow!("Role management not yet implemented for MySQL"))
    }

    async fn test_connection(&self) -> Result<bool> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(true)
    }

    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        let rows = sqlx::query("SHOW DATABASES")
            .fetch_all(&self.pool)
            .await?;

        Ok(rows
            .iter()
            .map(|r| DatabaseInfo {
                name: r.get::<String, _>(0),
            })
            .collect())
    }

    async fn list_schemas(&self, database: &str) -> Result<Vec<SchemaInfo>> {
        Ok(vec![SchemaInfo {
            name: database.to_string(),
        }])
    }

    async fn list_tables(&self, database: &str, _schema: &str) -> Result<Vec<TableInfo>> {
        let sql = format!(
            "SELECT TABLE_NAME, TABLE_TYPE, TABLE_ROWS \
             FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = '{}'
             ORDER BY TABLE_NAME",
            database.replace('\'', "''")
        );
        let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;

        Ok(rows
            .iter()
            .map(|r| TableInfo {
                name: r.get("TABLE_NAME"),
                schema: database.to_string(),
                table_type: r.get("TABLE_TYPE"),
                row_count_estimate: r.try_get::<i64, _>("TABLE_ROWS").ok(),
            })
            .collect())
    }

    async fn list_columns(
        &self,
        database: &str,
        _schema: &str,
        table: &str,
    ) -> Result<Vec<ColumnInfo>> {
        let sql = "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY, \
                   COLUMN_DEFAULT, ORDINAL_POSITION \
                   FROM information_schema.COLUMNS \
                   WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
                   ORDER BY ORDINAL_POSITION";
        let rows = sqlx::query(sql)
            .bind(database)
            .bind(table)
            .fetch_all(&self.pool)
            .await?;

        Ok(rows
            .iter()
            .map(|r| {
                let nullable_str: String = r.get("IS_NULLABLE");
                let key: String = r.get("COLUMN_KEY");
                ColumnInfo {
                    name: r.get("COLUMN_NAME"),
                    data_type: r.get("DATA_TYPE"),
                    is_nullable: nullable_str == "YES",
                    is_primary_key: key == "PRI",
                    default_value: r.try_get("COLUMN_DEFAULT").ok(),
                    ordinal_position: r.get("ORDINAL_POSITION"),
                }
            })
            .collect())
    }

    async fn list_indexes(
        &self,
        database: &str,
        _schema: &str,
        table: &str,
    ) -> Result<Vec<IndexInfo>> {
        let sql = format!("SHOW INDEX FROM {}.{}", quote_ident(database), quote_ident(table));
        let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;

        let mut index_map: std::collections::HashMap<String, IndexInfo> = std::collections::HashMap::new();
        for r in &rows {
            let name: String = r.get("Key_name");
            let col: String = r.get("Column_name");
            let non_unique: i64 = r.try_get::<i64, _>("Non_unique").unwrap_or(1);
            let idx_type: String = r.try_get::<String, _>("Index_type").unwrap_or_else(|_| "BTREE".into());
            index_map
                .entry(name.clone())
                .and_modify(|e| e.columns.push(col.clone()))
                .or_insert_with(|| IndexInfo {
                    name,
                    columns: vec![col],
                    is_unique: non_unique == 0,
                    index_type: idx_type,
                });
        }
        let mut result: Vec<IndexInfo> = index_map.into_values().collect();
        result.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(result)
    }

    async fn list_foreign_keys(
        &self,
        database: &str,
        _schema: &str,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        let sql = "SELECT CONSTRAINT_NAME, COLUMN_NAME, \
                   REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME \
                   FROM information_schema.KEY_COLUMN_USAGE \
                   WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
                   AND REFERENCED_TABLE_NAME IS NOT NULL \
                   ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION";
        let rows = sqlx::query(sql)
            .bind(database)
            .bind(table)
            .fetch_all(&self.pool)
            .await?;

        let fk_rules_sql = "SELECT CONSTRAINT_NAME, DELETE_RULE, UPDATE_RULE \
                            FROM information_schema.REFERENTIAL_CONSTRAINTS \
                            WHERE CONSTRAINT_SCHEMA = ? AND TABLE_NAME = ?";
        let rule_rows = sqlx::query(fk_rules_sql)
            .bind(database)
            .bind(table)
            .fetch_all(&self.pool)
            .await?;
        let rules: std::collections::HashMap<String, (String, String)> = rule_rows
            .iter()
            .map(|r| {
                let name: String = r.get("CONSTRAINT_NAME");
                let del: String = r.get("DELETE_RULE");
                let upd: String = r.get("UPDATE_RULE");
                (name, (del, upd))
            })
            .collect();

        Ok(rows
            .iter()
            .map(|r| {
                let name: String = r.get("CONSTRAINT_NAME");
                let (on_del, on_upd) = rules
                    .get(&name)
                    .cloned()
                    .unwrap_or(("RESTRICT".into(), "RESTRICT".into()));
                ForeignKeyInfo {
                    name,
                    column: r.get("COLUMN_NAME"),
                    referenced_table: r.get("REFERENCED_TABLE_NAME"),
                    referenced_column: r.get("REFERENCED_COLUMN_NAME"),
                    on_delete: on_del,
                    on_update: on_upd,
                }
            })
            .collect())
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
        if let Some(ref f) = filter {
            if !f.trim().is_empty() && filter_is_unsafe(f) {
                anyhow::bail!("Filter contains invalid characters (; -- /* */)");
            }
        }

        let columns = self.list_columns(database, database, table).await?;

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
            quote_ident(database),
            quote_ident(table),
            where_clause
        );
        let count_row = sqlx::query(&count_sql).fetch_one(&self.pool).await?;
        let total_rows: i64 = count_row.get("cnt");

        let sql = format!(
            "SELECT * FROM {}.{} {} {} LIMIT {} OFFSET {}",
            quote_ident(database),
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
            .map(|r| mysql_row_to_json_values(r, col_count))
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
            || trimmed.starts_with("SHOW")
            || trimmed.starts_with("DESCRIBE")
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
                .map(|r| mysql_row_to_json_values(r, col_count))
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
        let explain_sql = format!("EXPLAIN FORMAT=JSON {}", sql);
        let row = sqlx::query(&explain_sql)
            .fetch_one(&self.pool)
            .await?;
        let elapsed = start.elapsed().as_millis() as u64;

        let raw_text: String = row.try_get(0)?;
        let json: serde_json::Value = serde_json::from_str(&raw_text)?;

        let query_block = json
            .get("query_block")
            .unwrap_or(&serde_json::Value::Null);

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

    async fn get_ddl(
        &self,
        database: &str,
        _schema: &str,
        object_name: &str,
        object_type: &str,
    ) -> Result<String> {
        let sql = match object_type.to_uppercase().as_str() {
            "VIEW" => format!("SHOW CREATE VIEW {}.{}", quote_ident(database), quote_ident(object_name)),
            _ => format!("SHOW CREATE TABLE {}.{}", quote_ident(database), quote_ident(object_name)),
        };
        let row = sqlx::query(&sql).fetch_one(&self.pool).await?;
        let ddl: String = row.try_get(1)?;
        Ok(ddl)
    }

    async fn create_table(
        &self,
        database: &str,
        _schema: &str,
        table_name: &str,
        columns: &[ColumnDefinition],
    ) -> Result<()> {
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
            quote_ident(database),
            quote_ident(table_name),
            col_defs.join(",\n    ")
        );
        sqlx::query(&sql).execute(&self.pool).await?;
        Ok(())
    }

    async fn list_functions(
        &self,
        database: &str,
        _schema: &str,
    ) -> Result<Vec<FunctionInfo>> {
        let sql = "SELECT ROUTINE_NAME AS name, ROUTINE_SCHEMA AS schema,
                   COALESCE(DATA_TYPE, '') AS return_type, ROUTINE_BODY AS language, ROUTINE_TYPE AS kind
                   FROM information_schema.ROUTINES
                   WHERE ROUTINE_SCHEMA = ?
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
        let sql = "SELECT TRIGGER_NAME AS name, EVENT_OBJECT_TABLE AS table_name,
                   EVENT_MANIPULATION AS event, ACTION_TIMING AS timing
                   FROM information_schema.TRIGGERS
                   WHERE TRIGGER_SCHEMA = ? AND EVENT_OBJECT_TABLE = ?
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

    async fn get_table_stats(
        &self,
        database: &str,
        _schema: &str,
        table: &str,
    ) -> Result<TableStats> {
        let sql = "SELECT TABLE_NAME, TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH
                   FROM information_schema.TABLES
                   WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
        let row = sqlx::query(sql)
            .bind(database)
            .bind(table)
            .fetch_optional(&self.pool)
            .await?;

        let row = row.ok_or_else(|| anyhow::anyhow!("Table {}.{} not found", database, table))?;

        let table_name: String = row.get("TABLE_NAME");
        let table_rows: Option<i64> = row.try_get("TABLE_ROWS").ok();
        let data_length: Option<i64> = row.try_get("DATA_LENGTH").ok();
        let index_length: Option<i64> = row.try_get("INDEX_LENGTH").ok();

        let data_bytes = data_length.unwrap_or(0);
        let index_bytes = index_length.unwrap_or(0);
        let total_bytes = data_bytes + index_bytes;

        Ok(TableStats {
            table_name: table_name.clone(),
            row_count: table_rows.unwrap_or(0),
            total_size: format_bytes(Some(total_bytes)),
            index_size: format_bytes(index_length),
            data_size: format_bytes(data_length),
            last_vacuum: None,
            last_analyze: None,
            dead_tuples: None,
            live_tuples: table_rows,
        })
    }

    async fn alter_table(
        &self,
        database: &str,
        _schema: &str,
        table_name: &str,
        operations: &[AlterTableOperation],
    ) -> Result<()> {
        let mut current_table = table_name.to_string();

        for op in operations {
            let table_ref = format!(
                "{}.{}",
                quote_ident(database),
                quote_ident(&current_table)
            );

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
                        "ALTER TABLE {} MODIFY COLUMN {} {}",
                        table_ref,
                        quote_ident(column_name),
                        new_type
                    )
                }
                AlterTableOperation::SetNullable {
                    column_name,
                    nullable,
                } => {
                    let col_sql = "SELECT COLUMN_TYPE FROM information_schema.COLUMNS
                                   WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?";
                    let row = sqlx::query(col_sql)
                        .bind(database)
                        .bind(&current_table)
                        .bind(column_name)
                        .fetch_optional(&self.pool)
                        .await?;
                    let col_type: String = row
                        .ok_or_else(|| {
                            anyhow::anyhow!("Column {} not found in table {}", column_name, current_table)
                        })?
                        .get("COLUMN_TYPE");
                    let null_part = if *nullable { "" } else { " NOT NULL" };
                    format!(
                        "ALTER TABLE {} MODIFY COLUMN {} {}{}",
                        table_ref,
                        quote_ident(column_name),
                        col_type,
                        null_part
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
                    let new_ref = format!(
                        "{}.{}",
                        quote_ident(database),
                        quote_ident(new_name)
                    );
                    current_table = new_name.clone();
                    format!("RENAME TABLE {} TO {}", table_ref, new_ref)
                }
            };
            sqlx::query(&sql).execute(&self.pool).await?;
        }

        Ok(())
    }

    async fn import_data(
        &self,
        database: &str,
        _schema: &str,
        table: &str,
        columns: &[String],
        rows: &[Vec<serde_json::Value>],
    ) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        let table_ref = format!(
            "{}.{}",
            quote_ident(database),
            quote_ident(table)
        );
        let col_list: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
        let col_str = col_list.join(", ");

        const BATCH_SIZE: usize = 500;
        let mut total_inserted: u64 = 0;
        let mut tx = self.pool.begin().await?;

        for chunk in rows.chunks(BATCH_SIZE) {
            let mut values_list = Vec::with_capacity(chunk.len());
            for row in chunk {
                let vals: Vec<String> = row.iter().map(|v| json_to_sql_literal(v)).collect();
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
        database: &str,
        _schema: &str,
        object_name: &str,
        object_type: &str,
    ) -> Result<()> {
        let kind = match object_type.to_uppercase().as_str() {
            "VIEW" => "VIEW",
            _ => "TABLE",
        };
        let sql = format!(
            "DROP {} IF EXISTS {}.{}",
            kind,
            quote_ident(database),
            quote_ident(object_name)
        );
        sqlx::query(&sql).execute(&self.pool).await?;
        Ok(())
    }

    async fn truncate_table(
        &self,
        database: &str,
        _schema: &str,
        table_name: &str,
    ) -> Result<()> {
        let sql = format!(
            "TRUNCATE TABLE {}.{}",
            quote_ident(database),
            quote_ident(table_name)
        );
        sqlx::query(&sql).execute(&self.pool).await?;
        Ok(())
    }

    async fn get_server_activity(&self) -> Result<Vec<ServerActivity>> {
        let rows = sqlx::query("SHOW PROCESSLIST")
            .fetch_all(&self.pool)
            .await?;

        Ok(rows
            .iter()
            .map(|r| ServerActivity {
                pid: r.try_get::<i64, _>("Id").map(|v| v.to_string()).unwrap_or_default(),
                user: r.try_get::<String, _>("User").unwrap_or_default(),
                database: r.try_get::<String, _>("db").unwrap_or_default(),
                state: r.try_get::<String, _>("Command").unwrap_or_default(),
                query: r.try_get::<String, _>("Info").unwrap_or_default(),
                duration_ms: r.try_get::<i32, _>("Time").ok().map(|t| t as f64 * 1000.0),
                client_addr: r.try_get::<String, _>("Host").unwrap_or_default(),
            })
            .collect())
    }

    async fn cancel_query(&self, pid: &str) -> Result<()> {
        let sql = format!("KILL QUERY {}", pid);
        sqlx::query(&sql).execute(&self.pool).await?;
        Ok(())
    }

    async fn apply_changes(&self, changes: &DataChanges) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let fq_table = format!(
            "{}.{}",
            quote_ident(&changes.database),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_is_unsafe() {
        assert!(!filter_is_unsafe(""));
        assert!(!filter_is_unsafe("`id` = 1"));
        assert!(filter_is_unsafe("x; DROP TABLE t"));
        assert!(filter_is_unsafe("x -- comment"));
    }
}
