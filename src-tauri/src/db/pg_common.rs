use anyhow::Result;
use sqlx::{Column, PgPool, Row, TypeInfo};
use std::time::Instant;

use crate::models::*;

pub fn pg_row_to_json_values(
    row: &sqlx::postgres::PgRow,
    col_count: usize,
) -> Vec<serde_json::Value> {
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
                .map(|v| {
                    let f = v as f64;
                    if f.is_finite() {
                        serde_json::Number::from_f64(f)
                            .map(serde_json::Value::Number)
                            .unwrap_or(serde_json::Value::String(v.to_string()))
                    } else {
                        serde_json::Value::String(v.to_string())
                    }
                })
                .unwrap_or(serde_json::Value::Null),
            "FLOAT8" => row
                .try_get::<f64, _>(i)
                .ok()
                .map(|v| {
                    if v.is_finite() {
                        serde_json::Number::from_f64(v)
                            .map(serde_json::Value::Number)
                            .unwrap_or(serde_json::Value::String(v.to_string()))
                    } else {
                        serde_json::Value::String(v.to_string())
                    }
                })
                .unwrap_or(serde_json::Value::Null),
            "NUMERIC" => row
                .try_get::<rust_decimal::Decimal, _>(i)
                .ok()
                .map(|d| {
                    use rust_decimal::prelude::ToPrimitive;
                    // to_f64() can lose precision for large or high-scale decimals; string fallback preserves exact value.
                    d.to_f64()
                        .and_then(serde_json::Number::from_f64)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::String(d.to_string()))
                })
                .unwrap_or(serde_json::Value::Null),
            "JSON" | "JSONB" => row
                .try_get::<serde_json::Value, _>(i)
                .unwrap_or(serde_json::Value::Null),
            "TIMESTAMP" | "TIMESTAMPTZ" => row
                .try_get::<chrono::NaiveDateTime, _>(i)
                .ok()
                .map(|dt| serde_json::Value::String(dt.format("%Y-%m-%d %H:%M:%S").to_string()))
                .or_else(|| {
                    row.try_get::<chrono::DateTime<chrono::Utc>, _>(i)
                        .ok()
                        .map(|dt| serde_json::Value::String(dt.to_rfc3339()))
                })
                .unwrap_or(serde_json::Value::Null),
            "DATE" => row
                .try_get::<chrono::NaiveDate, _>(i)
                .ok()
                .map(|d| serde_json::Value::String(d.format("%Y-%m-%d").to_string()))
                .unwrap_or(serde_json::Value::Null),
            "TIME" | "TIMETZ" => row
                .try_get::<chrono::NaiveTime, _>(i)
                .ok()
                .map(|t| serde_json::Value::String(t.format("%H:%M:%S").to_string()))
                .unwrap_or(serde_json::Value::Null),
            "UUID" => row
                .try_get::<uuid::Uuid, _>(i)
                .ok()
                .map(|u| serde_json::Value::String(u.to_string()))
                .unwrap_or(serde_json::Value::Null),
            "BYTEA" => row
                .try_get::<Vec<u8>, _>(i)
                .ok()
                .map(|b| {
                    let hex_str: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
                    serde_json::Value::String(format!("\\x{}", hex_str))
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
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub fn filter_is_unsafe(filter: &str) -> bool {
    let s = filter.trim();
    if s.contains(';') || s.contains("--") || s.contains("/*") || s.contains("*/") {
        return true;
    }
    let u = s.to_uppercase();
    if u.contains("(SELECT") {
        return true;
    }
    // UNION-based injection (narrow patterns to avoid false positives e.g. `'a UNION b'`)
    u.contains(" UNION SELECT")
        || u.contains(" UNION ALL SELECT")
        || u.contains(" UNION DISTINCT SELECT")
}

pub fn sql_fragment_is_unsafe(s: &str) -> bool {
    s.contains(';') || s.contains("--") || s.contains("/*") || s.contains("*/") || s.contains('\'')
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

pub fn parse_pg_explain_node(json: &serde_json::Value) -> ExplainNode {
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
        actual_time_ms: plan.get("Actual Total Time").and_then(|v| v.as_f64()),
        rows_estimated: plan.get("Plan Rows").and_then(|v| v.as_u64()).unwrap_or(0),
        rows_actual: plan.get("Actual Rows").and_then(|v| v.as_u64()),
        width: plan.get("Plan Width").and_then(|v| v.as_u64()).unwrap_or(0),
        filter: plan
            .get("Filter")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        children,
    }
}

// ---------------------------------------------------------------------------
// Shared async methods (PG-wire compatible databases)
// ---------------------------------------------------------------------------

pub async fn pg_list_roles(pool: &PgPool) -> Result<Vec<RoleInfo>> {
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
    let rows = sqlx::query(sql).fetch_all(pool).await?;

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

pub async fn pg_create_role(pool: &PgPool, req: &CreateRoleRequest) -> Result<()> {
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
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn pg_drop_role(pool: &PgPool, name: &str) -> Result<()> {
    let sql = format!("DROP ROLE IF EXISTS {}", quote_ident(name));
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn pg_alter_role(pool: &PgPool, req: &AlterRoleRequest) -> Result<()> {
    let mut options: Vec<String> = Vec::new();
    if let Some(v) = req.is_superuser {
        options.push(if v {
            "SUPERUSER".to_string()
        } else {
            "NOSUPERUSER".to_string()
        });
    }
    if let Some(v) = req.can_login {
        options.push(if v {
            "LOGIN".to_string()
        } else {
            "NOLOGIN".to_string()
        });
    }
    if let Some(v) = req.can_create_db {
        options.push(if v {
            "CREATEDB".to_string()
        } else {
            "NOCREATEDB".to_string()
        });
    }
    if let Some(v) = req.can_create_role {
        options.push(if v {
            "CREATEROLE".to_string()
        } else {
            "NOCREATEROLE".to_string()
        });
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
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn pg_test_connection(pool: &PgPool) -> Result<bool> {
    sqlx::query("SELECT 1").execute(pool).await?;
    Ok(true)
}

pub async fn pg_list_databases(pool: &PgPool) -> Result<Vec<DatabaseInfo>> {
    let rows =
        sqlx::query("SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname")
            .fetch_all(pool)
            .await?;

    Ok(rows
        .iter()
        .map(|r| DatabaseInfo {
            name: r.get("datname"),
        })
        .collect())
}

pub async fn pg_list_schemas(pool: &PgPool, _database: &str) -> Result<Vec<SchemaInfo>> {
    let rows = sqlx::query(
        "SELECT schema_name FROM information_schema.schemata \
         WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') \
         ORDER BY schema_name",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| SchemaInfo {
            name: r.get("schema_name"),
        })
        .collect())
}

pub async fn pg_list_tables(
    pool: &PgPool,
    _database: &str,
    schema: &str,
) -> Result<Vec<TableInfo>> {
    let rows = sqlx::query(
        "SELECT DISTINCT ON (t.table_name) t.table_name, t.table_type, \
                COALESCE(c.reltuples::bigint, 0) as row_estimate \
         FROM information_schema.tables t \
         LEFT JOIN pg_class c ON c.relname = t.table_name AND c.relkind IN ('r', 'v', 'm', 'f', 'p') \
         LEFT JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema \
         WHERE t.table_schema = $1 \
         ORDER BY t.table_name",
    )
    .bind(schema)
    .fetch_all(pool)
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

pub async fn pg_list_indexes(
    pool: &PgPool,
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
        .fetch_all(pool)
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

pub async fn pg_list_foreign_keys(
    pool: &PgPool,
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
        .fetch_all(pool)
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

pub async fn pg_list_functions(
    pool: &PgPool,
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
    let rows = sqlx::query(sql).bind(schema).fetch_all(pool).await?;

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

pub async fn pg_list_triggers(
    pool: &PgPool,
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
        .fetch_all(pool)
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

#[allow(clippy::too_many_arguments)]
pub async fn pg_fetch_rows_impl(
    pool: &PgPool,
    columns: Vec<ColumnInfo>,
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
        quote_ident(schema),
        quote_ident(table),
        where_clause
    );
    let count_row = sqlx::query(&count_sql).fetch_one(pool).await?;
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

    let rows = sqlx::query(&sql).fetch_all(pool).await?;
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

pub async fn pg_execute_query(pool: &PgPool, _database: &str, sql: &str) -> Result<QueryResult> {
    let start = Instant::now();
    let trimmed = sql.trim().to_uppercase();
    let is_select = trimmed.starts_with("SELECT")
        || trimmed.starts_with("WITH")
        || trimmed.starts_with("SHOW")
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
        let result = sqlx::query(sql).execute(pool).await?;
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

pub async fn pg_get_ddl(
    pool: &PgPool,
    _database: &str,
    schema: &str,
    object_name: &str,
    object_type: &str,
) -> Result<String> {
    match object_type.to_uppercase().as_str() {
        "VIEW" => {
            let sql = "SELECT pg_get_viewdef((quote_ident($1::text) || '.' || quote_ident($2::text))::regclass, true) AS def";
            let row = sqlx::query(sql)
                .bind(schema)
                .bind(object_name)
                .fetch_one(pool)
                .await?;
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
                .fetch_all(pool)
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
                .fetch_all(pool)
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

pub async fn pg_create_table(
    pool: &PgPool,
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
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn pg_alter_table(
    pool: &PgPool,
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
                let action = if *nullable {
                    "DROP NOT NULL"
                } else {
                    "SET NOT NULL"
                };
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
                format!(
                    "ALTER TABLE {} RENAME TO {}",
                    table_ref,
                    quote_ident(new_name)
                )
            }
        };
        sqlx::query(&sql).execute(pool).await?;
    }

    Ok(())
}

pub async fn pg_import_data(
    pool: &PgPool,
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

fn pg_drop_object_sql_kind(object_type: &str) -> &'static str {
    match object_type.to_uppercase().as_str() {
        "VIEW" => "VIEW",
        "FUNCTION" => "FUNCTION",
        "PROCEDURE" => "PROCEDURE",
        "MATERIALIZED VIEW" => "MATERIALIZED VIEW",
        _ => "TABLE",
    }
}

pub async fn pg_drop_object(
    pool: &PgPool,
    _database: &str,
    schema: &str,
    object_name: &str,
    object_type: &str,
) -> Result<()> {
    let kind = pg_drop_object_sql_kind(object_type);
    let sql = format!(
        "DROP {} IF EXISTS {}.{} CASCADE",
        kind,
        quote_ident(schema),
        quote_ident(object_name)
    );
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn pg_truncate_table(
    pool: &PgPool,
    _database: &str,
    schema: &str,
    table_name: &str,
) -> Result<()> {
    let sql = format!(
        "TRUNCATE TABLE {}.{} CASCADE",
        quote_ident(schema),
        quote_ident(table_name)
    );
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn pg_get_server_activity(pool: &PgPool) -> Result<Vec<ServerActivity>> {
    let rows = sqlx::query(
        "SELECT pid, usename, datname, state, query, \
         EXTRACT(EPOCH FROM (now() - query_start)) * 1000 as duration_ms, \
         client_addr::text \
         FROM pg_stat_activity \
         WHERE state IS NOT NULL AND pid <> pg_backend_pid() \
         ORDER BY query_start DESC NULLS LAST",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .iter()
        .map(|r| ServerActivity {
            pid: r
                .try_get::<i32, _>("pid")
                .map(|v| v.to_string())
                .unwrap_or_default(),
            user: r.try_get::<String, _>("usename").unwrap_or_default(),
            database: r.try_get::<String, _>("datname").unwrap_or_default(),
            state: r.try_get::<String, _>("state").unwrap_or_default(),
            query: r.try_get::<String, _>("query").unwrap_or_default(),
            duration_ms: r.try_get::<f64, _>("duration_ms").ok(),
            client_addr: r.try_get::<String, _>("client_addr").unwrap_or_default(),
        })
        .collect())
}

pub async fn pg_cancel_query(pool: &PgPool, pid: &str) -> Result<()> {
    let pid_int: i32 = pid.parse()?;
    sqlx::query("SELECT pg_cancel_backend($1)")
        .bind(pid_int)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn pg_validate_query(pool: &PgPool, sql: &str) -> Result<Option<ValidationError>> {
    if sql.trim().is_empty() {
        return Ok(Some(ValidationError {
            message: "Empty query".to_string(),
            position: None,
        }));
    }
    use sqlx::Executor;
    match pool.prepare(sql).await {
        Ok(_) => Ok(None),
        Err(e) => {
            let mut position: Option<usize> = None;
            let message = if let Some(db_err) = e.as_database_error() {
                if let Some(pg_pos) = db_err.try_downcast_ref::<sqlx::postgres::PgDatabaseError>() {
                    position = pg_pos.position().and_then(|p| match p {
                        sqlx::postgres::PgErrorPosition::Original(offset) => Some(offset),
                        _ => None,
                    });
                }
                db_err.message().to_string()
            } else {
                e.to_string()
            };
            Ok(Some(ValidationError { message, position }))
        }
    }
}

pub async fn pg_apply_changes(pool: &PgPool, changes: &DataChanges) -> Result<()> {
    let mut tx = pool.begin().await?;
    let fq_table = format!(
        "{}.{}",
        quote_ident(&changes.schema),
        quote_ident(&changes.table)
    );

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
            fq_table,
            where_clause.join(" AND ")
        );
        sqlx::query(&sql).execute(&mut *tx).await?;
    }

    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn quote_ident_spaces() {
        assert_eq!(quote_ident("my column"), r#""my column""#);
    }
    #[test]
    fn quote_ident_keywords() {
        assert_eq!(quote_ident("select"), r#""select""#);
        assert_eq!(quote_ident("table"), r#""table""#);
    }
    #[test]
    fn quote_ident_multiple_quotes() {
        assert_eq!(quote_ident(r#"a""b"#), r#""a""""b""#);
    }

    #[test]
    fn filter_safe_empty() {
        assert!(!filter_is_unsafe(""));
    }
    #[test]
    fn filter_safe_simple_expression() {
        assert!(!filter_is_unsafe(r#""id" = 1"#));
        assert!(!filter_is_unsafe(r#""name" LIKE '%test%'"#));
        assert!(!filter_is_unsafe(r#""status" IN ('active', 'idle')"#));
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
        assert!(filter_is_unsafe("x /* comment */"));
        assert!(filter_is_unsafe("/**/"));
    }
    #[test]
    fn filter_safe_with_whitespace() {
        assert!(!filter_is_unsafe("  \"id\" > 10  "));
    }

    #[test]
    fn sql_fragment_safe_types() {
        assert!(!sql_fragment_is_unsafe("integer"));
        assert!(!sql_fragment_is_unsafe("varchar(255)"));
        assert!(!sql_fragment_is_unsafe("timestamp with time zone"));
        assert!(!sql_fragment_is_unsafe("numeric(10, 2)"));
        assert!(!sql_fragment_is_unsafe("boolean"));
    }
    #[test]
    fn sql_fragment_unsafe_injection_with_semicolon() {
        assert!(sql_fragment_is_unsafe("int); DROP TABLE t; --"));
    }
    #[test]
    fn sql_fragment_unsafe_single_quote() {
        assert!(sql_fragment_is_unsafe("default 'x'"));
    }
    #[test]
    fn sql_fragment_unsafe_block_comment() {
        assert!(sql_fragment_is_unsafe("int /* evil */"));
    }
    #[test]
    fn sql_fragment_safe_empty() {
        assert!(!sql_fragment_is_unsafe(""));
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
    fn json_to_sql_integer() {
        assert_eq!(
            json_to_sql_literal(&serde_json::Value::Number(42i64.into())),
            "42"
        );
    }
    #[test]
    fn json_to_sql_float() {
        let n = serde_json::Number::from_f64(3.14).unwrap();
        assert_eq!(json_to_sql_literal(&serde_json::Value::Number(n)), "3.14");
    }
    #[test]
    fn json_to_sql_string_simple() {
        assert_eq!(
            json_to_sql_literal(&serde_json::Value::String("hello".into())),
            "'hello'"
        );
    }
    #[test]
    fn json_to_sql_string_escapes_quotes() {
        assert_eq!(
            json_to_sql_literal(&serde_json::Value::String("O'Brien".into())),
            "'O''Brien'"
        );
    }

    // -----------------------------------------------------------------------
    // Regression tests for fixes applied to prevent future issues
    // -----------------------------------------------------------------------

    #[test]
    fn filter_unsafe_union_injection() {
        assert!(filter_is_unsafe("1=1 UNION SELECT * FROM pg_shadow--"));
    }

    #[test]
    fn filter_unsafe_stacked_query() {
        assert!(filter_is_unsafe("1=1; DROP TABLE users"));
    }

    #[test]
    fn filter_unsafe_nested_comment() {
        assert!(filter_is_unsafe("id = 1 /* "));
        assert!(filter_is_unsafe("id = 1 */"));
    }

    #[test]
    fn filter_safe_normal_operators() {
        assert!(!filter_is_unsafe("\"age\" >= 18 AND \"status\" = 'active'"));
        assert!(!filter_is_unsafe("\"price\" BETWEEN 10 AND 100"));
        assert!(!filter_is_unsafe("\"name\" IS NOT NULL"));
    }

    #[test]
    fn json_to_sql_string_multiple_quotes() {
        assert_eq!(
            json_to_sql_literal(&serde_json::Value::String("it''s a 'test'".into())),
            "'it''''s a ''test'''"
        );
    }

    #[test]
    fn json_to_sql_string_with_backslash() {
        let val = json_to_sql_literal(&serde_json::Value::String("path\\to\\file".into()));
        assert_eq!(val, "'path\\to\\file'");
    }

    #[test]
    fn json_to_sql_array_serializes() {
        let val = serde_json::json!(["a", "b"]);
        let lit = json_to_sql_literal(&val);
        assert!(lit.starts_with('\''));
        assert!(lit.ends_with('\''));
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
    fn sql_fragment_unsafe_double_dash() {
        assert!(sql_fragment_is_unsafe("text--evil"));
    }

    #[test]
    fn sql_fragment_unsafe_comment_start() {
        assert!(sql_fragment_is_unsafe("int/*"));
    }

    #[test]
    fn sql_fragment_unsafe_comment_end() {
        assert!(sql_fragment_is_unsafe("int*/"));
    }

    #[test]
    fn filter_unsafe_subquery_attempt() {
        assert!(filter_is_unsafe("1=1) OR (SELECT password FROM pg_shadow)"));
    }

    #[test]
    fn pg_drop_object_kind_function_and_materialized_view() {
        assert_eq!(pg_drop_object_sql_kind("function"), "FUNCTION");
        assert_eq!(
            pg_drop_object_sql_kind("Materialized View"),
            "MATERIALIZED VIEW"
        );
        assert_eq!(pg_drop_object_sql_kind("view"), "VIEW");
        assert_eq!(pg_drop_object_sql_kind("procedure"), "PROCEDURE");
        assert_eq!(pg_drop_object_sql_kind("table"), "TABLE");
        assert_eq!(pg_drop_object_sql_kind("unknown"), "TABLE");
    }

    /// If primary key values were omitted, UPDATE/DELETE would emit `WHERE ` with no predicates.
    #[test]
    fn empty_primary_key_values_would_emit_bad_where_clause() {
        let fq_table = format!("{}.{}", quote_ident("public"), quote_ident("users"));
        let set_clause = format!(
            "{} = {}",
            quote_ident("name"),
            json_to_sql_literal(&serde_json::json!("x"))
        );
        let where_clause: Vec<String> = vec![];
        let sql = format!(
            "UPDATE {} SET {} WHERE {}",
            fq_table,
            set_clause,
            where_clause.join(" AND ")
        );
        assert_eq!(sql, r#"UPDATE "public"."users" SET "name" = 'x' WHERE "#);
    }

    #[test]
    fn validation_error_none_position() {
        let err = ValidationError {
            message: "syntax error".to_string(),
            position: None,
        };
        assert_eq!(err.message, "syntax error");
        assert!(err.position.is_none());
    }

    #[test]
    fn validation_error_with_position() {
        let err = ValidationError {
            message: "column does not exist".to_string(),
            position: Some(42),
        };
        assert_eq!(err.message, "column does not exist");
        assert_eq!(err.position, Some(42));
    }

    #[test]
    fn validation_error_serialization_roundtrip() {
        let err = ValidationError {
            message: "syntax error at or near \"SELCT\"".to_string(),
            position: Some(1),
        };
        let json = serde_json::to_string(&err).unwrap();
        let deserialized: ValidationError = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.message, err.message);
        assert_eq!(deserialized.position, err.position);
    }

    #[test]
    fn validation_error_serialization_null_position() {
        let err = ValidationError {
            message: "some error".to_string(),
            position: None,
        };
        let json = serde_json::to_string(&err).unwrap();
        assert!(json.contains(r#""position":null"#));
        let deserialized: ValidationError = serde_json::from_str(&json).unwrap();
        assert!(deserialized.position.is_none());
    }

    #[test]
    fn validation_error_position_zero() {
        let err = ValidationError {
            message: "error at start".to_string(),
            position: Some(0),
        };
        assert_eq!(err.position, Some(0));
    }

    #[test]
    fn validation_error_long_message() {
        let long_msg = "a".repeat(500);
        let err = ValidationError {
            message: long_msg.clone(),
            position: Some(100),
        };
        assert_eq!(err.message.len(), 500);
        assert_eq!(err.position, Some(100));
    }

    #[test]
    fn validation_error_message_with_special_chars() {
        let err = ValidationError {
            message: r#"syntax error at or near "FROM" (SQLSTATE 42601)"#.to_string(),
            position: Some(15),
        };
        let json = serde_json::to_string(&err).unwrap();
        let deserialized: ValidationError = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.message, err.message);
        assert_eq!(deserialized.position, Some(15));
    }
}
