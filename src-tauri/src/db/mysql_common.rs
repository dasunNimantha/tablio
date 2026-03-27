use anyhow::{anyhow, Result};
use sqlx::{Column, MySqlPool, Row, TypeInfo};
use std::time::Instant;

use crate::models::*;

pub fn mysql_row_to_json_values(
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
            "FLOAT" | "DOUBLE" => row
                .try_get::<f64, _>(i)
                .ok()
                .and_then(serde_json::Number::from_f64)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            "DECIMAL" | "NEWDECIMAL" => row
                .try_get::<rust_decimal::Decimal, _>(i)
                .ok()
                .map(|d| {
                    use rust_decimal::prelude::ToPrimitive;
                    d.to_f64()
                        .and_then(serde_json::Number::from_f64)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::String(d.to_string()))
                })
                .unwrap_or(serde_json::Value::Null),
            "JSON" => row
                .try_get::<serde_json::Value, _>(i)
                .unwrap_or(serde_json::Value::Null),
            "DATETIME" | "TIMESTAMP" => row
                .try_get::<chrono::NaiveDateTime, _>(i)
                .ok()
                .map(|dt| serde_json::Value::String(dt.format("%Y-%m-%d %H:%M:%S").to_string()))
                .unwrap_or(serde_json::Value::Null),
            "DATE" => row
                .try_get::<chrono::NaiveDate, _>(i)
                .ok()
                .map(|d| serde_json::Value::String(d.format("%Y-%m-%d").to_string()))
                .unwrap_or(serde_json::Value::Null),
            "TIME" => row
                .try_get::<chrono::NaiveTime, _>(i)
                .ok()
                .map(|t| serde_json::Value::String(t.format("%H:%M:%S").to_string()))
                .unwrap_or(serde_json::Value::Null),
            "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BINARY" | "VARBINARY" => row
                .try_get::<Vec<u8>, _>(i)
                .ok()
                .map(|b| {
                    let hex_str: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
                    serde_json::Value::String(format!("0x{}", hex_str))
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

pub fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

pub fn json_to_sql_literal(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        _ => format!("'{}'", val.to_string().replace('\'', "''")),
    }
}

pub fn sql_fragment_is_unsafe(s: &str) -> bool {
    s.contains(';') || s.contains("--") || s.contains("/*") || s.contains("*/") || s.contains('\'')
}

pub fn mysql_variable_category(name: &str) -> &str {
    if name.starts_with("innodb_") {
        "InnoDB"
    } else if name.starts_with("performance_schema") {
        "Performance Schema"
    } else if name.starts_with("ssl_") || name.starts_with("tls_") {
        "SSL / TLS"
    } else if name.starts_with("log_")
        || name.starts_with("binlog_")
        || name.starts_with("relay_log")
    {
        "Logging"
    } else if name.starts_with("max_") || name.starts_with("net_") || name.starts_with("wait_") {
        "Limits & Timeouts"
    } else if name.starts_with("character_set") || name.starts_with("collation") {
        "Character Sets"
    } else if name.starts_with("slave_")
        || name.starts_with("replica_")
        || name.starts_with("gtid_")
    {
        "Replication"
    } else if name.starts_with("optimizer_")
        || name.starts_with("sort_")
        || name.starts_with("join_")
        || name.starts_with("tmp_")
    {
        "Optimizer"
    } else {
        "General"
    }
}

pub fn filter_is_unsafe(filter: &str) -> bool {
    let s = filter.trim();
    s.contains(';') || s.contains("--") || s.contains("/*") || s.contains("*/")
}

pub fn format_bytes(bytes: Option<i64>) -> String {
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

// ---------------------------------------------------------------------------
// Shared async methods (MySQL-wire compatible databases)
// ---------------------------------------------------------------------------

pub async fn my_list_roles(_pool: &MySqlPool) -> Result<Vec<RoleInfo>> {
    Ok(vec![])
}

pub async fn my_create_role(_pool: &MySqlPool, _req: &CreateRoleRequest) -> Result<()> {
    Err(anyhow!("Role management not yet implemented for MySQL"))
}

pub async fn my_drop_role(_pool: &MySqlPool, _name: &str) -> Result<()> {
    Err(anyhow!("Role management not yet implemented for MySQL"))
}

pub async fn my_alter_role(_pool: &MySqlPool, _req: &AlterRoleRequest) -> Result<()> {
    Err(anyhow!("Role management not yet implemented for MySQL"))
}

pub async fn my_test_connection(pool: &MySqlPool) -> Result<bool> {
    sqlx::query("SELECT 1").execute(pool).await?;
    Ok(true)
}

pub async fn my_list_databases(pool: &MySqlPool) -> Result<Vec<DatabaseInfo>> {
    let rows = sqlx::query(
        "SELECT CAST(SCHEMA_NAME AS CHAR) AS SCHEMA_NAME \
         FROM information_schema.SCHEMATA ORDER BY SCHEMA_NAME",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| DatabaseInfo {
            name: r.get::<String, _>("SCHEMA_NAME"),
        })
        .collect())
}

pub async fn my_list_schemas(_pool: &MySqlPool, database: &str) -> Result<Vec<SchemaInfo>> {
    Ok(vec![SchemaInfo {
        name: database.to_string(),
    }])
}

pub async fn my_list_tables(
    pool: &MySqlPool,
    database: &str,
    _schema: &str,
) -> Result<Vec<TableInfo>> {
    let rows = sqlx::query(
        "SELECT CAST(TABLE_NAME AS CHAR) AS TABLE_NAME, \
         CAST(TABLE_TYPE AS CHAR) AS TABLE_TYPE, TABLE_ROWS \
         FROM information_schema.TABLES \
         WHERE TABLE_SCHEMA = ? \
         ORDER BY TABLE_NAME",
    )
    .bind(database)
    .fetch_all(pool)
    .await?;

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

pub async fn my_list_columns(
    pool: &MySqlPool,
    database: &str,
    _schema: &str,
    table: &str,
) -> Result<Vec<ColumnInfo>> {
    let sql = "SELECT CAST(COLUMN_NAME AS CHAR) AS COLUMN_NAME, \
               CAST(DATA_TYPE AS CHAR) AS DATA_TYPE, \
               CAST(IS_NULLABLE AS CHAR) AS IS_NULLABLE, \
               CAST(COLUMN_KEY AS CHAR) AS COLUMN_KEY, \
               CAST(COLUMN_DEFAULT AS CHAR) AS COLUMN_DEFAULT, \
               CAST(ORDINAL_POSITION AS SIGNED) AS ORDINAL_POSITION, \
               CAST(EXTRA AS CHAR) AS EXTRA \
               FROM information_schema.COLUMNS \
               WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
               ORDER BY ORDINAL_POSITION";
    let rows = sqlx::query(sql)
        .bind(database)
        .bind(table)
        .fetch_all(pool)
        .await?;

    Ok(rows
        .iter()
        .map(|r| {
            let nullable_str: String = r.get("IS_NULLABLE");
            let key: String = r.get("COLUMN_KEY");
            let extra: String = r.try_get("EXTRA").unwrap_or_default();
            ColumnInfo {
                name: r.get("COLUMN_NAME"),
                data_type: r.get("DATA_TYPE"),
                is_nullable: nullable_str == "YES",
                is_primary_key: key == "PRI",
                default_value: r.try_get("COLUMN_DEFAULT").ok(),
                ordinal_position: r.get("ORDINAL_POSITION"),
                is_auto_generated: extra.contains("auto_increment")
                    || extra.contains("VIRTUAL GENERATED")
                    || extra.contains("STORED GENERATED"),
            }
        })
        .collect())
}

pub async fn my_list_indexes(
    pool: &MySqlPool,
    database: &str,
    _schema: &str,
    table: &str,
) -> Result<Vec<IndexInfo>> {
    let sql = format!(
        "SHOW INDEX FROM {}.{}",
        quote_ident(database),
        quote_ident(table)
    );
    let rows = sqlx::query(&sql).fetch_all(pool).await?;

    let mut index_map: std::collections::HashMap<String, IndexInfo> =
        std::collections::HashMap::new();
    for r in &rows {
        let name: String = r.get("Key_name");
        let col: String = r.get("Column_name");
        let non_unique: i64 = r.try_get::<i64, _>("Non_unique").unwrap_or(1);
        let idx_type: String = r
            .try_get::<String, _>("Index_type")
            .unwrap_or_else(|_| "BTREE".into());
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

pub async fn my_list_foreign_keys(
    pool: &MySqlPool,
    database: &str,
    _schema: &str,
    table: &str,
) -> Result<Vec<ForeignKeyInfo>> {
    let sql = "SELECT CAST(CONSTRAINT_NAME AS CHAR) AS CONSTRAINT_NAME, \
               CAST(COLUMN_NAME AS CHAR) AS COLUMN_NAME, \
               CAST(REFERENCED_TABLE_NAME AS CHAR) AS REFERENCED_TABLE_NAME, \
               CAST(REFERENCED_COLUMN_NAME AS CHAR) AS REFERENCED_COLUMN_NAME \
               FROM information_schema.KEY_COLUMN_USAGE \
               WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? \
               AND REFERENCED_TABLE_NAME IS NOT NULL \
               ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION";
    let rows = sqlx::query(sql)
        .bind(database)
        .bind(table)
        .fetch_all(pool)
        .await?;

    let fk_rules_sql = "SELECT CAST(CONSTRAINT_NAME AS CHAR) AS CONSTRAINT_NAME, \
                        CAST(DELETE_RULE AS CHAR) AS DELETE_RULE, \
                        CAST(UPDATE_RULE AS CHAR) AS UPDATE_RULE \
                        FROM information_schema.REFERENTIAL_CONSTRAINTS \
                        WHERE CONSTRAINT_SCHEMA = ? AND TABLE_NAME = ?";
    let rule_rows = sqlx::query(fk_rules_sql)
        .bind(database)
        .bind(table)
        .fetch_all(pool)
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

#[allow(clippy::too_many_arguments)]
pub async fn my_fetch_rows_impl(
    pool: &MySqlPool,
    columns: Vec<ColumnInfo>,
    database: &str,
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
        "SELECT COUNT(*) as cnt FROM {}.{} {}",
        quote_ident(database),
        quote_ident(table),
        where_clause
    );
    let count_row = sqlx::query(&count_sql).fetch_one(pool).await?;
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

    let rows = sqlx::query(&sql).fetch_all(pool).await?;
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

pub async fn my_execute_query(pool: &MySqlPool, _database: &str, sql: &str) -> Result<QueryResult> {
    let start = Instant::now();
    let trimmed = sql.trim().to_uppercase();
    let is_select = trimmed.starts_with("SELECT")
        || trimmed.starts_with("SHOW")
        || trimmed.starts_with("DESCRIBE")
        || trimmed.starts_with("EXPLAIN");

    if is_select {
        let rows = sqlx::query(sql).fetch_all(pool).await?;
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
        let result = sqlx::raw_sql(sql).execute(pool).await?;
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

pub async fn my_get_ddl(
    pool: &MySqlPool,
    database: &str,
    _schema: &str,
    object_name: &str,
    object_type: &str,
) -> Result<String> {
    let sql = match object_type.to_uppercase().as_str() {
        "VIEW" => format!(
            "SHOW CREATE VIEW {}.{}",
            quote_ident(database),
            quote_ident(object_name)
        ),
        _ => format!(
            "SHOW CREATE TABLE {}.{}",
            quote_ident(database),
            quote_ident(object_name)
        ),
    };
    let row = sqlx::query(&sql).fetch_one(pool).await?;
    let ddl: String = row.try_get(1)?;
    Ok(ddl)
}

pub async fn my_create_table(
    pool: &MySqlPool,
    database: &str,
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
        "CREATE TABLE {}.{} (\n    {}\n)",
        quote_ident(database),
        quote_ident(table_name),
        col_defs.join(",\n    ")
    );
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn my_alter_table(
    pool: &MySqlPool,
    database: &str,
    _schema: &str,
    table_name: &str,
    operations: &[AlterTableOperation],
) -> Result<()> {
    for op in operations {
        match op {
            AlterTableOperation::AddColumn { column } => {
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
        let table_ref = format!("{}.{}", quote_ident(database), quote_ident(&current_table));
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
                let col_sql = "SELECT CAST(COLUMN_TYPE AS CHAR) AS COLUMN_TYPE \
                               FROM information_schema.COLUMNS \
                               WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?";
                let row = sqlx::query(col_sql)
                    .bind(database)
                    .bind(&current_table)
                    .bind(column_name)
                    .fetch_optional(pool)
                    .await?;
                let col_type: String = row
                    .ok_or_else(|| {
                        anyhow!(
                            "Column {} not found in table {}",
                            column_name,
                            current_table
                        )
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
                let new_ref = format!("{}.{}", quote_ident(database), quote_ident(new_name));
                current_table = new_name.clone();
                format!("RENAME TABLE {} TO {}", table_ref, new_ref)
            }
        };
        sqlx::query(&sql).execute(pool).await?;
    }

    Ok(())
}

pub async fn my_import_data(
    pool: &MySqlPool,
    database: &str,
    _schema: &str,
    table: &str,
    columns: &[String],
    rows: &[Vec<serde_json::Value>],
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }

    let table_ref = format!("{}.{}", quote_ident(database), quote_ident(table));
    let col_list: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
    let col_str = col_list.join(", ");

    const BATCH_SIZE: usize = 500;
    let mut total_inserted: u64 = 0;
    let mut tx = pool.begin().await?;

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

pub async fn my_drop_object(
    pool: &MySqlPool,
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
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn my_truncate_table(
    pool: &MySqlPool,
    database: &str,
    _schema: &str,
    table_name: &str,
) -> Result<()> {
    let sql = format!(
        "TRUNCATE TABLE {}.{}",
        quote_ident(database),
        quote_ident(table_name)
    );
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn my_get_server_activity(pool: &MySqlPool) -> Result<Vec<ServerActivity>> {
    let rows = sqlx::query("SHOW PROCESSLIST").fetch_all(pool).await?;

    Ok(rows
        .iter()
        .map(|r| ServerActivity {
            pid: r
                .try_get::<i64, _>("Id")
                .map(|v| v.to_string())
                .unwrap_or_default(),
            user: r.try_get::<String, _>("User").unwrap_or_default(),
            database: r.try_get::<String, _>("db").unwrap_or_default(),
            state: r.try_get::<String, _>("Command").unwrap_or_default(),
            query: r.try_get::<String, _>("Info").unwrap_or_default(),
            duration_ms: r.try_get::<i32, _>("Time").ok().map(|t| t as f64 * 1000.0),
            client_addr: r.try_get::<String, _>("Host").unwrap_or_default(),
        })
        .collect())
}

pub async fn my_cancel_query(pool: &MySqlPool, pid: &str) -> Result<()> {
    let pid_num: u64 = pid
        .parse()
        .map_err(|_| anyhow!("Invalid PID: must be numeric"))?;
    let sql = format!("KILL QUERY {}", pid_num);
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn my_apply_changes(pool: &MySqlPool, changes: &DataChanges) -> Result<()> {
    let mut tx = pool.begin().await?;
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
            .map(|(col, val)| format!("{} = {}", quote_ident(col), json_to_sql_literal(val)))
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
            .map(|(col, val)| format!("{} = {}", quote_ident(col), json_to_sql_literal(val)))
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

pub async fn my_get_table_stats(
    pool: &MySqlPool,
    database: &str,
    _schema: &str,
    table: &str,
) -> Result<TableStats> {
    let meta_sql = "SELECT CAST(TABLE_NAME AS CHAR) AS TABLE_NAME, \
               DATA_LENGTH, INDEX_LENGTH \
               FROM information_schema.TABLES \
               WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";
    let meta_row = sqlx::query(meta_sql)
        .bind(database)
        .bind(table)
        .fetch_optional(pool)
        .await?;
    let meta_row = meta_row.ok_or_else(|| anyhow!("Table {}.{} not found", database, table))?;

    let table_name: String = meta_row.get("TABLE_NAME");
    let data_length: Option<i64> = meta_row.try_get("DATA_LENGTH").ok();
    let index_length: Option<i64> = meta_row.try_get("INDEX_LENGTH").ok();

    let count_sql = format!(
        "SELECT COUNT(*) AS cnt FROM {}.{}",
        quote_ident(database),
        quote_ident(table)
    );
    let count_row = sqlx::query(&count_sql).fetch_one(pool).await?;
    let exact_count: i64 = count_row.try_get("cnt").unwrap_or(0);

    let data_bytes = data_length.unwrap_or(0);
    let index_bytes = index_length.unwrap_or(0);
    let total_bytes = data_bytes + index_bytes;

    Ok(TableStats {
        table_name: table_name.clone(),
        row_count: exact_count,
        total_size: format_bytes(Some(total_bytes)),
        index_size: format_bytes(index_length),
        data_size: format_bytes(data_length),
        last_vacuum: None,
        last_analyze: None,
        dead_tuples: None,
        live_tuples: Some(exact_count),
    })
}

pub async fn my_get_server_config(pool: &MySqlPool) -> Result<Vec<ServerConfigEntry>> {
    let rows = sqlx::raw_sql("SHOW VARIABLES").fetch_all(pool).await?;

    Ok(rows
        .iter()
        .map(|r| {
            let name: String = r.try_get::<String, _>(0).unwrap_or_default();
            let value: String = r.try_get::<String, _>(1).unwrap_or_default();
            let category = mysql_variable_category(&name).to_string();
            ServerConfigEntry {
                name,
                setting: value,
                unit: None,
                category,
                description: String::new(),
                context: "dynamic".into(),
                source: String::new(),
                pending_restart: false,
            }
        })
        .collect())
}

pub async fn my_get_query_stats(_pool: &MySqlPool) -> Result<QueryStatsResponse> {
    Ok(QueryStatsResponse {
        available: false,
        message: Some("Query statistics are not supported for MySQL. Use performance_schema for similar functionality.".to_string()),
        entries: vec![],
    })
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
        assert!(!filter_is_unsafe("`id` = 1"));
        assert!(!filter_is_unsafe("`name` LIKE '%test%'"));
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
        assert_eq!(quote_ident("col"), "`col`");
    }
    #[test]
    fn quote_ident_with_backtick() {
        assert_eq!(quote_ident("my`col"), "`my``col`");
    }
    #[test]
    fn quote_ident_empty() {
        assert_eq!(quote_ident(""), "``");
    }
    #[test]
    fn json_to_sql_null() {
        assert_eq!(json_to_sql_literal(&serde_json::Value::Null), "NULL");
    }
    #[test]
    fn json_to_sql_bool() {
        assert_eq!(json_to_sql_literal(&serde_json::Value::Bool(true)), "true");
        assert_eq!(
            json_to_sql_literal(&serde_json::Value::Bool(false)),
            "false"
        );
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
    fn format_bytes_none() {
        assert_eq!(format_bytes(None), "0 B");
    }
    #[test]
    fn format_bytes_negative() {
        assert_eq!(format_bytes(Some(-100)), "0 B");
    }
    #[test]
    fn format_bytes_zero() {
        assert_eq!(format_bytes(Some(0)), "0 B");
    }
    #[test]
    fn format_bytes_kb() {
        assert_eq!(format_bytes(Some(2048)), "2 kB");
    }
    #[test]
    fn format_bytes_mb() {
        assert_eq!(format_bytes(Some(1048576)), "1.0 MB");
    }
    #[test]
    fn format_bytes_gb() {
        assert_eq!(format_bytes(Some(1073741824)), "1.0 GB");
    }
    #[test]
    fn format_bytes_tb() {
        assert_eq!(format_bytes(Some(1099511627776)), "1.0 TB");
    }
    #[test]
    fn sql_fragment_safe_types() {
        assert!(!sql_fragment_is_unsafe("integer"));
        assert!(!sql_fragment_is_unsafe("varchar(255)"));
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
}
