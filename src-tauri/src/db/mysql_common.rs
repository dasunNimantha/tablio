use anyhow::{anyhow, Result};
use sqlx::{Column, MySqlPool, Row, TypeInfo};
use std::time::Instant;

use crate::db::cancel::{
    cancelled_error, run_interruptible, unless_cancelled, CancellationToken, Interruptible,
};
use crate::models::*;
use crate::util::sql::{
    ensure_row_matched, is_numeric_type_name, numeric_literal, script_transaction_state,
    split_sql_statements_for, statement_is_select_like, ScriptTxn, SqlDialect,
};

/// Returns true when the server supports `ALTER TABLE ... RENAME COLUMN ... TO ...`.
///
/// This is the preferred rename syntax because it preserves every column
/// attribute (`AUTO_INCREMENT`, `ON UPDATE CURRENT_TIMESTAMP`, generated
/// expressions, character set, collation, comment) without us having to
/// enumerate them. It became valid syntax on:
///
/// - MySQL 8.0.3 (released 2017-09)
/// - MariaDB 10.5.2 (released 2020-07)
/// - TiDB (all supported releases parse it)
///
/// Older servers — notably the MySQL 5.7 still in our nightly matrix and
/// MariaDB 10.4 / earlier — reject it with a 1064 syntax error, so we
/// detect the version once per call and fall back to `CHANGE COLUMN` only
/// when we have to. We deliberately use `SELECT VERSION()` (universally
/// supported) rather than `@@version_compile_id` or similar so this works
/// on every flavour of the MySQL wire protocol.
async fn rename_column_supported(pool: &MySqlPool) -> Result<bool> {
    let row = sqlx::query("SELECT VERSION() AS v").fetch_one(pool).await?;
    let version: String = row.try_get("v").unwrap_or_default();
    Ok(version_supports_rename_column(&version))
}

/// Pure version-string check, split out so it is unit-testable without a
/// live MySQL connection. `version` is whatever `SELECT VERSION()`
/// returns, e.g. `"8.0.36"`, `"5.7.44"`, `"10.5.2-MariaDB"`, `"5.7.25-TiDB-v6.5.0"`.
fn version_supports_rename_column(version: &str) -> bool {
    // MariaDB always advertises itself in the version string after the
    // first dash, e.g. `10.5.2-MariaDB-1:10.5.2+maria~focal`. We
    // identify it on a case-insensitive substring match because the
    // suffix has shifted across releases.
    let is_mariadb = version.to_ascii_lowercase().contains("mariadb");

    // TiDB encodes the upstream-MySQL-compatibility version at the
    // start (`5.7.25-TiDB-v6.5.0`). Its parser accepts RENAME COLUMN
    // regardless of that prefix, so short-circuit there.
    if version.to_ascii_lowercase().contains("tidb") {
        return true;
    }

    // Strip anything after the first non-version character (`-`, `_`,
    // ` `) to leave just the dotted numeric prefix.
    let numeric: String = version
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    let mut parts = numeric.split('.').filter_map(|p| p.parse::<u32>().ok());
    let major = parts.next().unwrap_or(0);
    let minor = parts.next().unwrap_or(0);
    let patch = parts.next().unwrap_or(0);

    if is_mariadb {
        // MariaDB 10.5.2+
        (major, minor, patch) >= (10, 5, 2)
    } else {
        // MySQL 8.0.3+
        (major, minor, patch) >= (8, 0, 3)
    }
}

#[cfg(test)]
mod version_tests {
    use super::version_supports_rename_column as v;

    #[test]
    fn mysql_modern_supported() {
        assert!(v("8.0.36"));
        assert!(v("8.0.3"));
        assert!(v("8.4.0"));
        assert!(v("9.0.0"));
    }

    #[test]
    fn mysql_legacy_unsupported() {
        assert!(!v("5.7.44"));
        assert!(!v("5.6.51"));
        assert!(!v("8.0.2"));
    }

    #[test]
    fn mariadb_modern_supported() {
        assert!(v("10.5.2-MariaDB"));
        assert!(v("10.6.16-MariaDB-1:10.6.16+maria~ubu2204"));
        assert!(v("11.4.2-MariaDB"));
    }

    #[test]
    fn mariadb_legacy_unsupported() {
        assert!(!v("10.4.34-MariaDB"));
        assert!(!v("10.5.1-MariaDB"));
        assert!(!v("10.3.39-MariaDB"));
    }

    #[test]
    fn tidb_always_supported() {
        assert!(v("5.7.25-TiDB-v6.5.0"));
        assert!(v("8.0.11-TiDB-v7.5.0"));
    }

    #[test]
    fn empty_or_garbage_falls_back_to_change_column() {
        // Unknown servers err on the side of the universally supported
        // `CHANGE COLUMN` form rather than risking a 1064.
        assert!(!v(""));
        assert!(!v("not-a-version"));
    }
}

/// Convert a MySQL / MariaDB / TiDB row into JSON cells.
///
/// Rows should be fetched with the text protocol (`sqlx::raw_sql`): then
/// every value arrives as the server's text, which keeps DECIMAL exact
/// and lets values that the typed decoders reject (TIME beyond
/// 00:00-24:00, zero dates, YEAR, ...) fall back to that text instead
/// of rendering as NULL. Never panics on a column-count mismatch.
pub fn mysql_row_to_json_values(
    row: &sqlx::mysql::MySqlRow,
    col_count: usize,
) -> Vec<serde_json::Value> {
    let width = row.len();
    (0..col_count)
        .map(|i| {
            if i < width {
                mysql_cell_to_json(row, i)
            } else {
                serde_json::Value::Null
            }
        })
        .collect()
}

fn hex_json(prefix: &str, bytes: &[u8]) -> serde_json::Value {
    let hex_str: String = bytes.iter().map(|byte| format!("{:02x}", byte)).collect();
    serde_json::Value::String(format!("{}{}", prefix, hex_str))
}

fn mysql_cell_to_json(row: &sqlx::mysql::MySqlRow, i: usize) -> serde_json::Value {
    use crate::util::sql::{exact_numeric_text_to_json, i64_to_json, u64_to_json};
    use serde_json::Value;
    use sqlx::ValueRef;

    let raw = match row.try_get_raw(i) {
        Ok(raw) => raw,
        Err(_) => return Value::Null,
    };
    if raw.is_null() {
        return Value::Null;
    }
    let type_name = raw.type_info().name().to_string();
    // Raw value bytes (the server's text for text-protocol rows).
    let bytes = || row.try_get_unchecked::<Vec<u8>, _>(i).ok();
    let text = || bytes().and_then(|b| String::from_utf8(b).ok());

    let decoded: Option<Value> = match type_name.as_str() {
        "BOOLEAN" => row.try_get::<bool, _>(i).ok().map(Value::Bool),
        "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "BIGINT" => {
            row.try_get::<i64, _>(i).ok().map(i64_to_json)
        }
        "TINYINT UNSIGNED" | "SMALLINT UNSIGNED" | "MEDIUMINT UNSIGNED" | "INT UNSIGNED"
        | "BIGINT UNSIGNED" => row.try_get::<u64, _>(i).ok().map(u64_to_json),
        "FLOAT" | "DOUBLE" => row
            .try_get::<f64, _>(i)
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number),
        "DECIMAL" => text().map(|t| exact_numeric_text_to_json(&t)).or_else(|| {
            row.try_get::<rust_decimal::Decimal, _>(i)
                .ok()
                .map(|d| exact_numeric_text_to_json(&d.to_string()))
        }),
        "YEAR" => text()
            .and_then(|t| t.trim().parse::<i64>().ok())
            .map(|y| Value::Number(y.into())),
        "BIT" => bytes().map(|b| {
            if b.len() <= 8 {
                let v = b
                    .iter()
                    .fold(0u64, |acc, byte| (acc << 8) | u64::from(*byte));
                u64_to_json(v)
            } else {
                hex_json("0x", &b)
            }
        }),
        "JSON" => row.try_get::<serde_json::Value, _>(i).ok(),
        "DATETIME" | "TIMESTAMP" => row
            .try_get::<chrono::NaiveDateTime, _>(i)
            .ok()
            .map(|dt| Value::String(dt.format("%Y-%m-%d %H:%M:%S%.f").to_string())),
        "DATE" => row
            .try_get::<chrono::NaiveDate, _>(i)
            .ok()
            .map(|d| Value::String(d.format("%Y-%m-%d").to_string())),
        "TIME" => row
            .try_get::<chrono::NaiveTime, _>(i)
            .ok()
            .map(|t| Value::String(t.format("%H:%M:%S%.f").to_string())),
        "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BINARY" | "VARBINARY" | "GEOMETRY" => {
            bytes().map(|b| hex_json("0x", &b))
        }
        _ => None,
    };
    if let Some(v) = decoded {
        return v;
    }

    // Everything else (strings, ENUM/SET, and values a typed decoder
    // rejected such as `0000-00-00` or `-838:59:59`): the server text,
    // or hex when it is not valid UTF-8.
    match bytes() {
        Some(b) => match String::from_utf8(b) {
            Ok(s) => Value::String(s),
            Err(e) => hex_json("0x", e.as_bytes()),
        },
        None => Value::String(format!("<{} value>", type_name)),
    }
}

pub fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

pub fn json_to_sql_literal(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => {
            format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
        }
        _ => format!(
            "'{}'",
            val.to_string().replace('\\', "\\\\").replace('\'', "''")
        ),
    }
}

/// Strict check for SQL fragments that should never contain string
/// literals (data types, operator names, etc). Bans `'` here because
/// types never legitimately need one. For `DEFAULT` clauses use
/// `sql_default_is_unsafe` instead so string defaults survive.
pub fn sql_fragment_is_unsafe(s: &str) -> bool {
    s.contains(';') || s.contains("--") || s.contains("/*") || s.contains("*/") || s.contains('\'')
}

/// Same as `sql_fragment_is_unsafe` but allows `'` so legitimate
/// string-literal defaults (e.g. `DEFAULT 'pending'`) aren't blocked.
/// Mirrors `pg_common::sql_default_is_unsafe`.
pub fn sql_default_is_unsafe(s: &str) -> bool {
    s.contains(';') || s.contains("--") || s.contains("/*") || s.contains("*/")
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

/// MySQL/MariaDB/TiDB filter safety check. Mirrors the strict
/// PostgreSQL version (`pg_common::filter_is_unsafe`) so the rejection
/// matrix is identical across drivers — see the doc comment there for
/// the rationale and the full list of patterns blocked.
pub fn filter_is_unsafe(filter: &str) -> bool {
    filter_unsafe_reason(filter).is_some()
}

/// Mirrors `pg_common::filter_unsafe_reason` so MySQL-family drivers
/// can produce the same precise error messages.
pub fn filter_unsafe_reason(filter: &str) -> Option<&'static str> {
    crate::util::sql::filter_unsafe_reason_for(filter, crate::util::sql::SqlDialect::MySql)
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
            ..Default::default()
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
    let rows = sqlx::query(sqlx::AssertSqlSafe(&*sql))
        .fetch_all(pool)
        .await?;

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
    sort: Vec<SortSpec>,
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

    let where_clause = filter
        .filter(|f| !f.trim().is_empty())
        .map(|f| format!("WHERE {}", f))
        .unwrap_or_default();

    let order_clause = if !sort.is_empty() {
        // Multi-column sort (issue #57): compose every requested
        // sort in priority order, comma-separated.
        let parts: Vec<String> = sort
            .iter()
            .map(|s| {
                let dir = match s.direction {
                    SortDirection::Asc => "ASC",
                    SortDirection::Desc => "DESC",
                };
                format!("{} {}", quote_ident(&s.column), dir)
            })
            .collect();
        format!("ORDER BY {}", parts.join(", "))
    } else {
        // No explicit sort — fall back to the PK columns for
        // stable pagination.
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
    };

    let count_sql = format!(
        "SELECT COUNT(*) as cnt FROM {}.{} {}",
        quote_ident(database),
        quote_ident(table),
        where_clause
    );
    let count_row = sqlx::query(sqlx::AssertSqlSafe(&*count_sql))
        .fetch_one(pool)
        .await?;
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

    // Text protocol keeps DECIMAL exact and lets out-of-range values
    // (TIME > 24h, zero dates) fall back to the server text.
    let rows = sqlx::raw_sql(sqlx::AssertSqlSafe(&*sql))
        .fetch_all(pool)
        .await?;
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

pub async fn my_execute_query(pool: &MySqlPool, database: &str, sql: &str) -> Result<QueryResult> {
    my_execute_query_cancellable(pool, database, sql, false, CancellationToken::new()).await
}

/// Ask the server to stop the statement running on connection `id`.
/// `KILL QUERY` leaves the connection itself open (unlike `KILL`), so
/// the pinned connection can still run its cleanup. Runs on another
/// pooled connection; errors are ignored because the caller retries and
/// finally discards the connection anyway.
async fn my_interrupt(pool: MySqlPool, id: u64) {
    let sql = format!("KILL QUERY {}", id);
    let _ = sqlx::raw_sql(sqlx::AssertSqlSafe(sql.as_str()))
        .execute(&pool)
        .await;
}

/// `cancellable` fetches the connection id up front (one extra round
/// trip) so `token` can interrupt the statement server-side.
pub async fn my_execute_query_cancellable(
    pool: &MySqlPool,
    database: &str,
    sql: &str,
    cancellable: bool,
    token: CancellationToken,
) -> Result<QueryResult> {
    let start = Instant::now();

    // Multi-statement support: see the matching comment in
    // `pg_common::pg_execute_query`. The same model applies here —
    // run all leading statements as exec-only, then run the LAST
    // statement based on its own shape so e.g.
    // `CREATE VIEW v ...; SELECT * FROM v;` returns rows from the
    // SELECT instead of `rows_affected = 0`.
    //
    // Everything runs on a single pinned connection so session
    // variables (`SET @x = ...;`) and temp tables survive between
    // statements.
    let statements = split_sql_statements_for(sql, SqlDialect::MySql);
    if statements.is_empty() {
        return Err(anyhow::anyhow!("Query is empty"));
    }

    let mut conn = unless_cancelled(pool.acquire(), &token).await?;
    let connection_id: Option<u64> = if cancellable {
        let row = sqlx::raw_sql("SELECT CONNECTION_ID()")
            .fetch_one(&mut *conn)
            .await?;
        Some(row.try_get::<u64, _>(0)?)
    } else {
        None
    };

    // Run against the database selected in the console. The pooled
    // connection's default database is restored afterwards so other
    // users of the pool are unaffected (also covers a `USE` inside the
    // script itself).
    let script_uses_db = statements
        .iter()
        .any(|s| statement_is_select_like(s, &["USE "]));
    let wants_db = !database.trim().is_empty();
    let original_db: Option<Option<String>> = if wants_db || script_uses_db {
        let row = sqlx::raw_sql("SELECT DATABASE()")
            .fetch_one(&mut *conn)
            .await?;
        Some(row.try_get::<Option<String>, _>(0).unwrap_or(None))
    } else {
        None
    };
    let mut switched = false;
    if wants_db {
        let current = original_db.clone().flatten();
        if current.as_deref() != Some(database) {
            let use_sql = format!("USE {}", quote_ident(database));
            sqlx::raw_sql(sqlx::AssertSqlSafe(use_sql.as_str()))
                .execute(&mut *conn)
                .await?;
            switched = true;
        }
    }

    let run = my_run_statements(&mut conn, &statements, start, &token);
    let interrupt = || {
        let pool = pool.clone();
        async move {
            if let Some(id) = connection_id {
                my_interrupt(pool, id).await;
            }
        }
    };
    let outcome = match run_interruptible(run, &token, interrupt).await {
        Interruptible::Done(r) | Interruptible::Interrupted(r) => r,
        Interruptible::Abandoned => {
            // Dropped mid-protocol: the connection cannot be trusted.
            conn.close_on_drop();
            return Err(cancelled_error());
        }
    };
    let cancelled = token.is_cancelled();

    // Never hand a connection back to the pool inside a transaction
    // (sqlx does not roll back a user-issued BEGIN / START TRANSACTION)
    // or with session state that changes how later queries behave.
    let txn = script_transaction_state(&statements);
    let left_open = outcome.is_ok() && txn == ScriptTxn::LeftOpen;
    if txn != ScriptTxn::Untouched
        && (outcome.is_err() || left_open || cancelled)
        && sqlx::raw_sql("ROLLBACK").execute(&mut *conn).await.is_err()
    {
        conn.close_on_drop();
    }
    let dirty_session = statements.iter().any(|s| {
        statement_is_select_like(
            s,
            &[
                "SET AUTOCOMMIT",
                "SET @@AUTOCOMMIT",
                "SET SESSION AUTOCOMMIT",
                "LOCK TABLE",
                "XA ",
            ],
        )
    });
    if dirty_session {
        conn.close_on_drop();
    }
    if cancelled {
        // A `KILL QUERY` that lands late would interrupt whatever the
        // next borrower runs on this connection: retire it. Closing also
        // makes restoring the default database below unnecessary.
        conn.close_on_drop();
        return Err(cancelled_error());
    }
    if switched || script_uses_db {
        match original_db.flatten() {
            Some(db) => {
                let use_sql = format!("USE {}", quote_ident(&db));
                if sqlx::raw_sql(sqlx::AssertSqlSafe(use_sql.as_str()))
                    .execute(&mut *conn)
                    .await
                    .is_err()
                {
                    conn.close_on_drop();
                }
            }
            // There is no way to "un-USE" back to no default database.
            None => conn.close_on_drop(),
        }
    }

    if left_open {
        anyhow::bail!(
            "The script left a transaction open; it was rolled back. \
             Include COMMIT (or ROLLBACK) in the same run."
        );
    }
    outcome
}

async fn my_run_statements(
    conn: &mut sqlx::pool::PoolConnection<sqlx::MySql>,
    statements: &[String],
    start: Instant,
    token: &CancellationToken,
) -> Result<QueryResult> {
    let Some((last, leading)) = statements.split_last() else {
        return Err(anyhow::anyhow!("Query is empty"));
    };

    // A cancel between statements stops before the next one starts.
    for stmt in leading {
        if token.is_cancelled() {
            return Err(cancelled_error());
        }
        sqlx::raw_sql(sqlx::AssertSqlSafe(stmt.as_str()))
            .execute(&mut **conn)
            .await?;
    }
    if token.is_cancelled() {
        return Err(cancelled_error());
    }

    let last = last.as_str();
    let last_is_select = statement_is_select_like(
        last,
        &[
            "SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "WITH", "TABLE", "VALUES",
        ],
    );

    if last_is_select {
        // Text protocol: exact DECIMALs and server text for values the
        // typed decoders cannot represent.
        let rows = sqlx::raw_sql(sqlx::AssertSqlSafe(last))
            .fetch_all(&mut **conn)
            .await?;
        let elapsed = start.elapsed().as_millis() as u64;
        let columns: Vec<String> = rows
            .first()
            .map(|r| r.columns().iter().map(|c| c.name().to_string()).collect())
            .unwrap_or_default();
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
        let result = sqlx::raw_sql(sqlx::AssertSqlSafe(last))
            .execute(&mut **conn)
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
    let row = sqlx::query(sqlx::AssertSqlSafe(&*sql))
        .fetch_one(pool)
        .await?;
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
            // String defaults like `DEFAULT 'pending'` are valid SQL
            // and must survive validation; only block statement
            // terminators / comment markers.
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
        "CREATE TABLE {}.{} (\n    {}\n)",
        quote_ident(database),
        quote_ident(table_name),
        col_defs.join(",\n    ")
    );
    sqlx::query(sqlx::AssertSqlSafe(&*sql))
        .execute(pool)
        .await?;
    Ok(())
}

#[allow(clippy::collapsible_match)]
/// A column's current definition, read from `information_schema`, so
/// `MODIFY COLUMN` / `CHANGE COLUMN` (which replace the whole
/// definition) can be rebuilt with only the intended attribute changed.
#[derive(Debug, Clone, Default)]
pub struct MyColumnDef {
    pub column_type: String,
    pub is_nullable: bool,
    pub default: Option<String>,
    pub extra: String,
    pub comment: String,
    pub charset: Option<String>,
    pub collation: Option<String>,
    pub generation_expression: Option<String>,
}

async fn server_is_mariadb(pool: &MySqlPool) -> bool {
    match sqlx::query("SELECT VERSION() AS v").fetch_one(pool).await {
        Ok(row) => row
            .try_get::<String, _>("v")
            .map(|v| v.to_ascii_lowercase().contains("mariadb"))
            .unwrap_or(false),
        Err(_) => false,
    }
}

async fn my_fetch_column_def(
    pool: &MySqlPool,
    database: &str,
    table: &str,
    column: &str,
) -> Result<MyColumnDef> {
    let sql = "SELECT CAST(COLUMN_TYPE AS CHAR) AS COLUMN_TYPE, \
                      CAST(IS_NULLABLE AS CHAR) AS IS_NULLABLE, \
                      CAST(COLUMN_DEFAULT AS CHAR) AS COLUMN_DEFAULT, \
                      CAST(EXTRA AS CHAR) AS EXTRA, \
                      CAST(COLUMN_COMMENT AS CHAR) AS COLUMN_COMMENT, \
                      CAST(CHARACTER_SET_NAME AS CHAR) AS CHARACTER_SET_NAME, \
                      CAST(COLLATION_NAME AS CHAR) AS COLLATION_NAME, \
                      CAST(GENERATION_EXPRESSION AS CHAR) AS GENERATION_EXPRESSION \
               FROM information_schema.COLUMNS \
               WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?";
    let row = sqlx::query(sql)
        .bind(database)
        .bind(table)
        .bind(column)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| anyhow!("Column {} not found in table {}", column, table))?;
    let opt = |name: &str| -> Option<String> {
        row.try_get::<Option<String>, _>(name)
            .ok()
            .flatten()
            .filter(|s| !s.is_empty())
    };
    Ok(MyColumnDef {
        column_type: row.try_get("COLUMN_TYPE")?,
        is_nullable: opt("IS_NULLABLE")
            .map(|s| s.eq_ignore_ascii_case("YES"))
            .unwrap_or(true),
        default: row
            .try_get::<Option<String>, _>("COLUMN_DEFAULT")
            .ok()
            .flatten(),
        extra: opt("EXTRA").unwrap_or_default(),
        comment: opt("COLUMN_COMMENT").unwrap_or_default(),
        charset: opt("CHARACTER_SET_NAME"),
        collation: opt("COLLATION_NAME"),
        generation_expression: opt("GENERATION_EXPRESSION"),
    })
}

/// Escape text for a MySQL single-quoted string literal (the server
/// treats backslash as an escape by default).
fn my_quote_string(s: &str) -> String {
    format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"))
}

/// `CURRENT_TIMESTAMP`, `current_timestamp(3)`, `NOW()`, ... — the
/// temporal defaults that are written without quotes or parentheses.
fn is_current_timestamp_expr(s: &str) -> bool {
    let u = s.trim().to_ascii_uppercase();
    let base = u.split('(').next().unwrap_or("").trim();
    matches!(
        base,
        "CURRENT_TIMESTAMP"
            | "NOW"
            | "LOCALTIME"
            | "LOCALTIMESTAMP"
            | "CURRENT_DATE"
            | "CURRENT_TIME"
    ) && (!u.contains('(') || u.ends_with(')'))
}

/// Render the `DEFAULT ...` clause for an `information_schema`
/// COLUMN_DEFAULT value.
///
/// * MariaDB (10.2.7+) already stores the default as SQL: literals
///   quoted (`'pending'`), expressions bare (`current_timestamp()`),
///   `NULL` for an explicit NULL default — so it is used verbatim.
/// * MySQL / TiDB store literals unquoted (`pending`); expression
///   defaults are flagged with `DEFAULT_GENERATED` in EXTRA and need
///   parentheses unless they are a CURRENT_TIMESTAMP form.
fn render_default(def: &MyColumnDef, mariadb: bool) -> Option<String> {
    let d = def.default.as_ref()?;
    if mariadb {
        // MariaDB reports a nullable column without a default as
        // `NULL`; that is the implicit default anyway, and it must not
        // be carried over when the column becomes NOT NULL.
        if d.trim().eq_ignore_ascii_case("NULL") {
            return None;
        }
        return Some(format!("DEFAULT {}", d));
    }
    let lower_extra = def.extra.to_ascii_lowercase();
    if is_current_timestamp_expr(d) {
        return Some(format!("DEFAULT {}", d));
    }
    if lower_extra.contains("default_generated") {
        let expr = d.trim();
        if expr.starts_with('(') && expr.ends_with(')') {
            return Some(format!("DEFAULT {}", expr));
        }
        return Some(format!("DEFAULT ({})", expr));
    }
    // Bit-value literals are reported as `b'101'`.
    if (d.starts_with("b'") || d.starts_with("B'")) && d.ends_with('\'') && d.len() >= 3 {
        return Some(format!("DEFAULT {}", d));
    }
    Some(format!("DEFAULT {}", my_quote_string(d)))
}

/// `ON UPDATE CURRENT_TIMESTAMP[(n)]` from EXTRA, if present.
fn extract_on_update(extra: &str) -> Option<String> {
    let lower = extra.to_ascii_lowercase();
    let idx = lower.find("on update ")?;
    let rest = &extra[idx + "on update ".len()..];
    let expr = rest.split_whitespace().next()?;
    Some(format!("ON UPDATE {}", expr))
}

fn type_is_character(column_type: &str) -> bool {
    let lower = column_type.trim().to_ascii_lowercase();
    [
        "char",
        "varchar",
        "tinytext",
        "text",
        "mediumtext",
        "longtext",
        "enum",
        "set",
    ]
    .iter()
    .any(|t| {
        lower == *t
            || lower.starts_with(&format!("{}(", t))
            || lower.starts_with(&format!("{} ", t))
    })
}

/// Rebuild a full column definition (everything after the column name
/// in `MODIFY` / `CHANGE COLUMN`). Charset/collation are only carried
/// over when both the old and new type are character types and the new
/// type does not spell out its own.
pub fn render_column_definition(def: &MyColumnDef, mariadb: bool) -> String {
    let mut out = def.column_type.clone();
    let lower_type = def.column_type.to_ascii_lowercase();
    let type_sets_charset = lower_type.contains("character set")
        || lower_type.contains("charset")
        || lower_type.contains("collate");
    if type_is_character(&def.column_type) && !type_sets_charset {
        if let Some(cs) = &def.charset {
            out.push_str(&format!(" CHARACTER SET {}", cs));
        }
        if let Some(coll) = &def.collation {
            out.push_str(&format!(" COLLATE {}", coll));
        }
    }
    let lower_extra = def.extra.to_ascii_lowercase();
    if let Some(expr) = &def.generation_expression {
        let kind = if lower_extra.contains("stored") || lower_extra.contains("persistent") {
            "STORED"
        } else {
            "VIRTUAL"
        };
        out.push_str(&format!(" GENERATED ALWAYS AS ({}) {}", expr, kind));
        // MariaDB rejects NULL / NOT NULL on generated columns.
        if !mariadb && !def.is_nullable {
            out.push_str(" NOT NULL");
        }
    } else {
        out.push_str(if def.is_nullable {
            " NULL"
        } else {
            " NOT NULL"
        });
        if let Some(d) = render_default(def, mariadb) {
            out.push(' ');
            out.push_str(&d);
        }
        if let Some(on_update) = extract_on_update(&def.extra) {
            out.push(' ');
            out.push_str(&on_update);
        }
        if lower_extra.contains("auto_increment") {
            out.push_str(" AUTO_INCREMENT");
        }
    }
    if lower_extra.contains("invisible") {
        out.push_str(" INVISIBLE");
    }
    if !def.comment.is_empty() {
        out.push_str(&format!(" COMMENT {}", my_quote_string(&def.comment)));
    }
    out
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
                    if !d.is_empty() && sql_default_is_unsafe(d) {
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
            } if !d.is_empty() && sql_default_is_unsafe(d) => {
                anyhow::bail!("Invalid character in default value");
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
                // Choose the rename syntax based on what the server
                // supports. `ALTER TABLE ... RENAME COLUMN ... TO ...`
                // is the right tool because it preserves *every*
                // column attribute (EXTRA: AUTO_INCREMENT / ON UPDATE
                // CURRENT_TIMESTAMP, generated-column expressions,
                // CHARACTER_SET, COLLATION, COMMENT) without us having
                // to enumerate them. It is, however, only valid on:
                //   - MySQL 8.0.3+
                //   - MariaDB 10.5.2+
                //   - TiDB (all supported releases)
                // Older servers (notably MySQL 5.7 plus MariaDB 10.4
                // and earlier) reject it with a 1064 syntax error, so
                // we fall back to the universal
                // `CHANGE COLUMN <old> <new> <type>` form there.
                // `CHANGE COLUMN` requires us to spell the column
                // definition out, so it is rebuilt from
                // INFORMATION_SCHEMA by `render_column_definition`.
                if rename_column_supported(pool).await? {
                    format!(
                        "ALTER TABLE {} RENAME COLUMN {} TO {}",
                        table_ref,
                        quote_ident(old_name),
                        quote_ident(new_name)
                    )
                } else {
                    // `CHANGE COLUMN` replaces the whole definition, so
                    // rebuild it from information_schema to keep every
                    // attribute (default, AUTO_INCREMENT, ON UPDATE,
                    // comment, charset/collation).
                    let def = my_fetch_column_def(pool, database, &current_table, old_name).await?;
                    format!(
                        "ALTER TABLE {} CHANGE COLUMN {} {} {}",
                        table_ref,
                        quote_ident(old_name),
                        quote_ident(new_name),
                        render_column_definition(&def, server_is_mariadb(pool).await)
                    )
                }
            }
            AlterTableOperation::ChangeColumnType {
                column_name,
                new_type,
            } => {
                // MODIFY replaces the full definition: change only the
                // type and keep nullability, default, AUTO_INCREMENT,
                // ON UPDATE, comment and (for character types)
                // charset/collation.
                let mut def =
                    my_fetch_column_def(pool, database, &current_table, column_name).await?;
                def.column_type = new_type.clone();
                format!(
                    "ALTER TABLE {} MODIFY COLUMN {} {}",
                    table_ref,
                    quote_ident(column_name),
                    render_column_definition(&def, server_is_mariadb(pool).await)
                )
            }
            AlterTableOperation::SetNullable {
                column_name,
                nullable,
            } => {
                let mut def =
                    my_fetch_column_def(pool, database, &current_table, column_name).await?;
                def.is_nullable = *nullable;
                format!(
                    "ALTER TABLE {} MODIFY COLUMN {} {}",
                    table_ref,
                    quote_ident(column_name),
                    render_column_definition(&def, server_is_mariadb(pool).await)
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
        sqlx::query(sqlx::AssertSqlSafe(&*sql))
            .execute(pool)
            .await?;
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
        let result = sqlx::query(sqlx::AssertSqlSafe(&*sql))
            .execute(&mut *tx)
            .await?;
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
    sqlx::query(sqlx::AssertSqlSafe(&*sql))
        .execute(pool)
        .await?;
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
    sqlx::query(sqlx::AssertSqlSafe(&*sql))
        .execute(pool)
        .await?;
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
    sqlx::query(sqlx::AssertSqlSafe(&*sql))
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn my_validate_query(pool: &MySqlPool, sql: &str) -> Result<Option<ValidationError>> {
    if sql.trim().is_empty() {
        return Ok(Some(ValidationError {
            message: "Empty query".to_string(),
            position: None,
        }));
    }
    use sqlx::Executor;
    match pool
        .prepare(
            <sqlx::AssertSqlSafe<&str> as sqlx::SqlSafeStr>::into_sql_str(sqlx::AssertSqlSafe(sql)),
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

/// `COLUMN_NAME -> COLUMN_TYPE` for one table.
async fn my_column_types(
    pool: &MySqlPool,
    database: &str,
    table: &str,
) -> Result<std::collections::HashMap<String, String>> {
    let rows = sqlx::query(
        "SELECT CAST(COLUMN_NAME AS CHAR) AS n, CAST(COLUMN_TYPE AS CHAR) AS t \
         FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
    )
    .bind(database)
    .bind(table)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .iter()
        .filter_map(|r| {
            let n: String = r.try_get("n").ok()?;
            let t: String = r.try_get("t").ok()?;
            Some((n, t))
        })
        .collect())
}

/// Like `json_to_sql_literal`, but a string value for a numeric column
/// is emitted as a bare literal once validated as a plain number.
pub fn typed_sql_literal(data_type: Option<&str>, val: &serde_json::Value) -> String {
    if let (Some(t), serde_json::Value::String(s)) = (data_type, val) {
        if is_numeric_type_name(t) {
            if let Some(n) = numeric_literal(s) {
                return n.to_string();
            }
        }
    }
    json_to_sql_literal(val)
}

pub async fn my_apply_changes(pool: &MySqlPool, changes: &DataChanges) -> Result<()> {
    // Column types decide how string values are written: a string for an
    // integer/decimal column (big ids the frontend cannot hold as a JS
    // number) must be an unquoted literal, because MySQL compares a
    // BIGINT column with a quoted string as DOUBLE and can match the
    // wrong row.
    let col_types: std::collections::HashMap<String, String> =
        my_column_types(pool, &changes.database, &changes.table)
            .await
            .unwrap_or_default();
    let lit = |col: &str, val: &serde_json::Value| -> String {
        typed_sql_literal(col_types.get(col).map(String::as_str), val)
    };
    let mut tx = pool.begin().await?;
    let fq_table = format!(
        "{}.{}",
        quote_ident(&changes.database),
        quote_ident(&changes.table)
    );

    for update in &changes.updates {
        if update.primary_key_values.is_empty() {
            anyhow::bail!("Cannot update row: no primary key values provided");
        }
        let set_clause = format!(
            "{} = {}",
            quote_ident(&update.column_name),
            lit(&update.column_name, &update.new_value)
        );
        let where_clause: Vec<String> = update
            .primary_key_values
            .iter()
            .map(|(col, val)| format!("{} = {}", quote_ident(col), lit(col, val)))
            .collect();
        let sql = format!(
            "UPDATE {} SET {} WHERE {}",
            fq_table,
            set_clause,
            where_clause.join(" AND ")
        );
        // sqlx connects with CLIENT_FOUND_ROWS, so rows_affected counts
        // matched rows: an UPDATE that writes identical values still
        // reports 1 and is not mistaken for a missing row.
        let result = sqlx::query(sqlx::AssertSqlSafe(&*sql))
            .execute(&mut *tx)
            .await?;
        ensure_row_matched(result.rows_affected(), "update", &update.primary_key_values)?;
    }

    for insert in &changes.inserts {
        let cols: Vec<String> = insert.values.iter().map(|(c, _)| quote_ident(c)).collect();
        let vals: Vec<String> = insert.values.iter().map(|(c, v)| lit(c, v)).collect();
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            fq_table,
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
            .map(|(col, val)| format!("{} = {}", quote_ident(col), lit(col, val)))
            .collect();
        let sql = format!(
            "DELETE FROM {} WHERE {}",
            fq_table,
            where_clause.join(" AND ")
        );
        let result = sqlx::query(sqlx::AssertSqlSafe(&*sql))
            .execute(&mut *tx)
            .await?;
        ensure_row_matched(result.rows_affected(), "delete", &delete.primary_key_values)?;
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
    let count_row = sqlx::query(sqlx::AssertSqlSafe(&*count_sql))
        .fetch_one(pool)
        .await?;
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

/// Per-query stats sourced from the always-on (since MySQL 5.6 / MariaDB 10.0)
/// `performance_schema.events_statements_summary_by_digest` view.
///
/// Failure modes the UI cares about (all surface as
/// `kind = MysqlPerfSchemaDisabled`):
/// - performance_schema is compiled-out or `performance_schema = OFF`,
/// - the `*_summary_by_digest` instrument / consumer is disabled,
/// - the connecting user lacks `SELECT` on `performance_schema`.
///
/// Time fields in `performance_schema` are reported in **picoseconds**, so we
/// divide by `1_000_000_000` to land in milliseconds for the shared
/// `QueryStatEntry` shape.
///
/// MySQL has no shared-buffer / cache-hit equivalent, so `shared_blks_*` and
/// `cache_hit_ratio` are reported as honest zeros rather than fabricated
/// numbers. Plan times are `None` for the same reason. The 32-byte hex
/// `DIGEST` column doesn't fit in `i64`, so `queryid` stays `None` — the
/// `query` text is the de-facto identifier in the UI.
pub async fn my_get_query_stats(pool: &MySqlPool) -> Result<QueryStatsResponse> {
    if let Err(e) =
        sqlx::query("SELECT 1 FROM performance_schema.events_statements_summary_by_digest LIMIT 1")
            .fetch_optional(pool)
            .await
    {
        return Ok(QueryStatsResponse {
            available: false,
            kind: QueryStatsKind::MysqlPerfSchemaDisabled,
            message: Some(format!(
                "Performance Schema digest table is not readable: {e}.\n\n\
                 To enable query statistics:\n\n\
                 1. In your my.cnf / my.ini under [mysqld]:\n   performance_schema = ON\n\n\
                 2. Restart the server.\n\n\
                 3. Grant the connecting user read access:\n   \
                 GRANT SELECT ON performance_schema.* TO '<user>'@'<host>';"
            )),
            entries: vec![],
        });
    }

    // `1000000000.0` (vs `1000000000`) forces float division — without it MySQL
    // truncates to integer milliseconds and we lose sub-ms precision on cheap
    // queries.
    let sql = "SELECT \
            COALESCE(DIGEST_TEXT, '')                            AS query, \
            COALESCE(SCHEMA_NAME, '')                            AS user, \
            CAST(COUNT_STAR AS SIGNED)                           AS calls, \
            (SUM_TIMER_WAIT / 1000000000.0)                      AS total_exec_time_ms, \
            (AVG_TIMER_WAIT / 1000000000.0)                      AS mean_exec_time_ms, \
            (MIN_TIMER_WAIT / 1000000000.0)                      AS min_exec_time_ms, \
            (MAX_TIMER_WAIT / 1000000000.0)                      AS max_exec_time_ms, \
            CAST(SUM_ROWS_SENT AS SIGNED)                        AS rows_sent \
         FROM performance_schema.events_statements_summary_by_digest \
         WHERE DIGEST_TEXT IS NOT NULL \
         ORDER BY SUM_TIMER_WAIT DESC \
         LIMIT 200";

    let rows = sqlx::query(sql).fetch_all(pool).await?;

    let entries = rows
        .iter()
        .map(|r| QueryStatEntry {
            query: r.try_get::<String, _>("query").unwrap_or_default(),
            queryid: None,
            user: r.try_get::<String, _>("user").unwrap_or_default(),
            calls: r.try_get::<i64, _>("calls").unwrap_or(0),
            total_exec_time_ms: r.try_get::<f64, _>("total_exec_time_ms").unwrap_or(0.0),
            mean_exec_time_ms: r.try_get::<f64, _>("mean_exec_time_ms").unwrap_or(0.0),
            min_exec_time_ms: r.try_get::<f64, _>("min_exec_time_ms").unwrap_or(0.0),
            max_exec_time_ms: r.try_get::<f64, _>("max_exec_time_ms").unwrap_or(0.0),
            rows: r.try_get::<i64, _>("rows_sent").unwrap_or(0),
            shared_blks_hit: 0,
            shared_blks_read: 0,
            cache_hit_ratio: 0.0,
            total_plan_time_ms: None,
            mean_plan_time_ms: None,
        })
        .collect();

    Ok(QueryStatsResponse {
        available: true,
        kind: QueryStatsKind::Available,
        message: None,
        entries,
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

    // -----------------------------------------------------------------------
    // Regression tests for fixes applied to prevent future issues
    // -----------------------------------------------------------------------

    #[test]
    fn json_to_sql_string_escapes_backslash() {
        let val = json_to_sql_literal(&serde_json::Value::String("path\\to\\file".into()));
        assert_eq!(val, "'path\\\\to\\\\file'");
    }

    #[test]
    fn json_to_sql_string_backslash_quote_injection() {
        let val = json_to_sql_literal(&serde_json::Value::String("\\' OR 1=1 --".into()));
        assert_eq!(val, "'\\\\'' OR 1=1 --'");
        assert!(!val.contains("\\' "));
    }

    #[test]
    fn json_to_sql_string_backslash_at_end() {
        let val = json_to_sql_literal(&serde_json::Value::String("test\\".into()));
        assert_eq!(val, "'test\\\\'");
    }

    #[test]
    fn json_to_sql_array_escapes_backslash() {
        let val = serde_json::json!(["a\\b"]);
        let lit = json_to_sql_literal(&val);
        assert!(lit.contains("\\\\"));
    }

    #[test]
    fn filter_unsafe_union_injection() {
        assert!(filter_is_unsafe("1=1 UNION SELECT * FROM mysql.user--"));
    }

    #[test]
    fn filter_unsafe_nested_comment() {
        assert!(filter_is_unsafe("id = 1 /* "));
        assert!(filter_is_unsafe("id = 1 */"));
    }

    #[test]
    fn filter_safe_normal_operators() {
        assert!(!filter_is_unsafe("`age` >= 18 AND `status` = 'active'"));
        assert!(!filter_is_unsafe("`price` BETWEEN 10 AND 100"));
    }

    // -----------------------------------------------------------------------
    // Cross-driver parity: MySQL's filter_is_unsafe used to only block
    // `;`, `--`, `/*`, `*/` (no UNION / subquery checks), so a payload
    // like `1=1 UNION SELECT password FROM mysql.user` was accepted
    // when it would have been rejected on the Postgres driver. After
    // the unification it must reject the same set of patterns.
    // -----------------------------------------------------------------------

    #[test]
    fn filter_unsafe_unionless_subquery_now_blocked() {
        assert!(filter_is_unsafe("1=1 OR (SELECT 1)"));
        assert!(filter_is_unsafe("(SELECT count(*) FROM mysql.user) > 0"));
    }

    #[test]
    fn filter_unsafe_union_without_comments_now_blocked() {
        // Previously slipped through because there's no `;`, `--`, `/*`, `*/`.
        assert!(filter_is_unsafe("1=1 UNION SELECT user FROM mysql.user"));
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
            filter_unsafe_reason("1=1 UNION SELECT 1"),
            Some("`UNION SELECT` clause")
        );
        assert!(filter_unsafe_reason("`id` = 1").is_none());
    }

    #[test]
    fn sql_default_allows_string_literals_mysql() {
        // Same regression class as pg_common: legitimate `DEFAULT 'val'`
        // must not be rejected for containing a single quote.
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

    #[test]
    fn quote_ident_prevents_injection() {
        let evil = "`; DROP TABLE users; --";
        let quoted = quote_ident(evil);
        assert_eq!(quoted, "```; DROP TABLE users; --`");
    }

    #[test]
    fn sql_fragment_unsafe_double_dash() {
        assert!(sql_fragment_is_unsafe("text--evil"));
    }

    #[test]
    fn mysql_validation_error_no_position() {
        let err = ValidationError {
            message: "You have an error in your SQL syntax".to_string(),
            position: None,
        };
        assert!(err.position.is_none());
        assert!(err.message.contains("SQL syntax"));
    }

    #[test]
    fn mysql_validation_error_serialization() {
        let err = ValidationError {
            message: "Table 'db.unknown' doesn't exist".to_string(),
            position: None,
        };
        let json = serde_json::to_string(&err).unwrap();
        let deserialized: ValidationError = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.message, err.message);
        assert!(deserialized.position.is_none());
    }
}

#[cfg(test)]
mod column_definition_tests {
    use super::*;

    fn def(t: &str) -> MyColumnDef {
        MyColumnDef {
            column_type: t.into(),
            is_nullable: true,
            ..Default::default()
        }
    }

    #[test]
    fn mysql_literal_default_is_quoted() {
        let mut d = def("varchar(20)");
        d.is_nullable = false;
        d.default = Some("it's".into());
        d.comment = "a 'c'".into();
        assert_eq!(
            render_column_definition(&d, false),
            "varchar(20) NOT NULL DEFAULT 'it''s' COMMENT 'a ''c'''"
        );
    }

    #[test]
    fn mariadb_default_is_verbatim_and_null_default_dropped() {
        let mut d = def("varchar(20)");
        d.default = Some("'pending'".into());
        assert_eq!(
            render_column_definition(&d, true),
            "varchar(20) NULL DEFAULT 'pending'"
        );
        d.default = Some("NULL".into());
        d.is_nullable = false;
        assert_eq!(render_column_definition(&d, true), "varchar(20) NOT NULL");
    }

    #[test]
    fn expression_defaults_and_extra_flags() {
        let mut d = def("timestamp");
        d.default = Some("CURRENT_TIMESTAMP".into());
        d.extra = "DEFAULT_GENERATED on update CURRENT_TIMESTAMP".into();
        assert_eq!(
            render_column_definition(&d, false),
            "timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
        );
        let mut e = def("varchar(36)");
        e.default = Some("uuid()".into());
        e.extra = "DEFAULT_GENERATED".into();
        assert_eq!(
            render_column_definition(&e, false),
            "varchar(36) NULL DEFAULT (uuid())"
        );
        let mut a = def("bigint");
        a.is_nullable = false;
        a.extra = "auto_increment".into();
        assert_eq!(
            render_column_definition(&a, false),
            "bigint NOT NULL AUTO_INCREMENT"
        );
    }

    #[test]
    fn charset_only_for_character_types() {
        let mut d = def("varchar(50)");
        d.charset = Some("utf8mb4".into());
        d.collation = Some("utf8mb4_bin".into());
        assert_eq!(
            render_column_definition(&d, false),
            "varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL"
        );
        d.column_type = "int".into();
        assert_eq!(render_column_definition(&d, false), "int NULL");
    }
}
