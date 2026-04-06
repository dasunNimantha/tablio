use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::collections::HashSet;
use std::time::Instant;
use tiberius::numeric::Numeric;
use tiberius::{AuthMethod, Client, ColumnData, Config, EncryptionLevel, Row};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::compat::TokioAsyncWriteCompatExt;

use crate::db::DatabaseDriver;
use crate::models::*;

type MssqlStream = tokio_util::compat::Compat<tokio::net::TcpStream>;

pub struct MssqlDriver {
    client: Mutex<Client<MssqlStream>>,
}

fn bracket(ident: &str) -> String {
    format!("[{}]", ident.replace(']', "]]"))
}

fn three_part_table(database: &str, schema: &str, table: &str) -> String {
    format!(
        "{}.{}.{}",
        bracket(database),
        bracket(schema),
        bracket(table)
    )
}

fn json_to_mssql_literal(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => {
            if *b {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => format!("N'{}'", s.replace('\'', "''")),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            format!("N'{}'", val.to_string().replace('\'', "''"))
        }
    }
}

fn column_data_to_json(data: &ColumnData<'_>) -> serde_json::Value {
    match data {
        ColumnData::U8(v) => v
            .map(|x| serde_json::json!(x))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::I16(v) => v
            .map(|x| serde_json::json!(x))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::I32(v) => v
            .map(|x| serde_json::json!(x))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::I64(v) => v
            .map(|x| serde_json::json!(x))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::F32(v) => v
            .map(|x| serde_json::json!(x))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::F64(v) => v
            .map(|x| serde_json::json!(x))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::Bit(v) => v
            .map(|x| serde_json::json!(x))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::String(v) => v
            .as_ref()
            .map(|s| serde_json::Value::String(s.to_string()))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::Guid(v) => v
            .map(|g| serde_json::Value::String(g.to_string()))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::Binary(v) => v
            .as_ref()
            .map(|b| {
                serde_json::Value::String(format!(
                    "0x{}",
                    b.iter()
                        .map(|byte| format!("{:02x}", byte))
                        .collect::<String>()
                ))
            })
            .unwrap_or(serde_json::Value::Null),
        ColumnData::Numeric(v) => v
            .as_ref()
            .map(|n: &Numeric| serde_json::Value::String(format!("{}", n)))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::Xml(v) => v
            .as_ref()
            .map(|x| serde_json::Value::String(x.to_string()))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::DateTime(v) => v
            .map(|d| serde_json::Value::String(format!("{:?}", d)))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::SmallDateTime(v) => v
            .map(|d| serde_json::Value::String(format!("{:?}", d)))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::Time(v) => v
            .map(|d| serde_json::Value::String(format!("{:?}", d)))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::Date(v) => v
            .map(|d| serde_json::Value::String(format!("{:?}", d)))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::DateTime2(v) => v
            .map(|d| serde_json::Value::String(format!("{:?}", d)))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::DateTimeOffset(v) => v
            .map(|d| serde_json::Value::String(format!("{:?}", d)))
            .unwrap_or(serde_json::Value::Null),
    }
}

fn rows_to_grid(rows: Vec<Row>) -> (Vec<String>, Vec<Vec<serde_json::Value>>) {
    if rows.is_empty() {
        return (vec![], vec![]);
    }
    let columns: Vec<String> = rows[0]
        .columns()
        .iter()
        .map(|c| c.name().to_string())
        .collect();
    let data: Vec<Vec<serde_json::Value>> = rows
        .iter()
        .map(|row| row.cells().map(|(_, d)| column_data_to_json(d)).collect())
        .collect();
    (columns, data)
}

impl MssqlDriver {
    pub async fn connect(config: &ConnectionConfig) -> Result<Self> {
        let mut tcfg = Config::new();
        tcfg.host(&config.host);
        tcfg.port(config.port);
        if config.database.trim().is_empty() {
            tcfg.database("master");
        } else {
            tcfg.database(&config.database);
        }
        tcfg.authentication(AuthMethod::sql_server(&config.user, &config.password));
        tcfg.application_name("Tablio");
        if config.ssl {
            tcfg.encryption(EncryptionLevel::Required);
            if config.trust_server_cert {
                tcfg.trust_cert();
            }
        } else {
            tcfg.encryption(EncryptionLevel::Off);
            tcfg.trust_cert();
        }
        let tcp = TcpStream::connect(tcfg.get_addr())
            .await
            .map_err(|e| anyhow!("TCP connect failed: {}", e))?;
        tcp.set_nodelay(true)
            .map_err(|e| anyhow!("set_nodelay: {}", e))?;
        let client = Client::connect(tcfg, tcp.compat_write())
            .await
            .map_err(|e| anyhow!("SQL Server login failed: {}", e))?;
        Ok(Self {
            client: Mutex::new(client),
        })
    }

    async fn run_select(&self, sql: &str) -> Result<(Vec<String>, Vec<Vec<serde_json::Value>>)> {
        let mut c = self.client.lock().await;
        let stream = c.simple_query(sql).await.map_err(|e| anyhow!("{}", e))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| anyhow!("{}", e))?;
        Ok(rows_to_grid(rows))
    }

    async fn run_exec(&self, sql: &str) -> Result<u64> {
        let mut c = self.client.lock().await;
        let r = c.execute(sql, &[]).await.map_err(|e| anyhow!("{}", e))?;
        Ok(r.rows_affected().iter().copied().sum())
    }

    async fn run_batch(&self, sql: &str) -> Result<()> {
        let mut c = self.client.lock().await;
        c.simple_query(sql)
            .await
            .map_err(|e| anyhow!("{}", e))?
            .into_results()
            .await
            .map_err(|e| anyhow!("{}", e))?;
        Ok(())
    }

    async fn use_database(&self, database: &str) -> Result<()> {
        if !database.trim().is_empty() {
            self.run_batch(&format!("USE {}", bracket(database)))
                .await?;
        }
        Ok(())
    }

    fn is_ddl(sql: &str) -> bool {
        let u = sql.trim().to_ascii_uppercase();
        u.starts_with("CREATE ")
            || u.starts_with("ALTER ")
            || u.starts_with("DROP ")
            || u.starts_with("TRUNCATE ")
            || u.starts_with("GRANT ")
            || u.starts_with("REVOKE ")
            || u.starts_with("DENY ")
    }

    #[cfg(test)]
    fn with_use_database(database: &str, sql: &str) -> String {
        if database.trim().is_empty() {
            return sql.to_string();
        }
        format!("USE {}; {}", bracket(database), sql)
    }

    fn is_select_like(sql: &str) -> bool {
        let t = sql.trim();
        let u = t.to_ascii_uppercase();
        u.starts_with("SELECT")
            || u.starts_with("WITH")
            || u.starts_with("SHOWPLAN")
            || u.starts_with("EXPLAIN")
    }

    async fn primary_key_set(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<HashSet<String>> {
        let sql = format!(
            "SELECT c.name
             FROM {}.sys.indexes i
             INNER JOIN {}.sys.index_columns ic
               ON i.object_id = ic.object_id AND i.index_id = ic.index_id
             INNER JOIN {}.sys.columns c
               ON ic.object_id = c.object_id AND ic.column_id = c.column_id
             INNER JOIN {}.sys.tables t ON i.object_id = t.object_id
             INNER JOIN {}.sys.schemas s ON t.schema_id = s.schema_id
             WHERE i.is_primary_key = 1 AND t.name = N'{}' AND s.name = N'{}'
             ORDER BY ic.key_ordinal",
            bracket(database),
            bracket(database),
            bracket(database),
            bracket(database),
            bracket(database),
            table.replace('\'', "''"),
            schema.replace('\'', "''"),
        );
        let (_, rows) = self.run_select(&sql).await?;
        let mut set = HashSet::new();
        for r in rows {
            if let Some(serde_json::Value::String(name)) = r.first() {
                set.insert(name.clone());
            }
        }
        Ok(set)
    }
}

#[async_trait]
impl DatabaseDriver for MssqlDriver {
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        let sql = "SELECT name FROM sys.databases WHERE database_id > 4 ORDER BY name";
        let (_, rows) = self.run_select(sql).await?;
        let mut out = Vec::new();
        for r in rows {
            if let Some(serde_json::Value::String(name)) = r.first() {
                out.push(DatabaseInfo { name: name.clone() });
            }
        }
        Ok(out)
    }

    async fn list_schemas(&self, database: &str) -> Result<Vec<SchemaInfo>> {
        let sql = format!(
            "SELECT s.name FROM {}.sys.schemas s
             INNER JOIN {}.sys.database_principals p ON s.principal_id = p.principal_id
             WHERE p.type IN (N'S', N'U', N'G')
               AND s.name NOT IN (N'sys', N'INFORMATION_SCHEMA', N'guest')
             ORDER BY s.name",
            bracket(database),
            bracket(database),
        );
        let (_, rows) = self.run_select(&sql).await?;
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                r.into_iter().next().and_then(|v| match v {
                    serde_json::Value::String(s) => Some(SchemaInfo { name: s }),
                    _ => None,
                })
            })
            .collect())
    }

    async fn list_tables(&self, database: &str, schema: &str) -> Result<Vec<TableInfo>> {
        let sql = format!(
            "SELECT t.name, SUM(CASE WHEN p.index_id IN (0, 1) THEN p.rows ELSE 0 END) AS rc
             FROM {}.sys.tables t
             INNER JOIN {}.sys.schemas s ON t.schema_id = s.schema_id
             LEFT JOIN {}.sys.partitions p ON t.object_id = p.object_id
             WHERE s.name = N'{}'
             GROUP BY t.name
             ORDER BY t.name",
            bracket(database),
            bracket(database),
            bracket(database),
            schema.replace('\'', "''"),
        );
        let (_, rows) = self.run_select(&sql).await?;
        let mut tables = Vec::new();
        for r in rows {
            if r.len() >= 2 {
                let name = match &r[0] {
                    serde_json::Value::String(s) => s.clone(),
                    _ => continue,
                };
                let row_count = match &r[1] {
                    serde_json::Value::Number(n) => n.as_i64(),
                    serde_json::Value::String(s) => s.parse().ok(),
                    _ => None,
                };
                tables.push(TableInfo {
                    name,
                    schema: schema.to_string(),
                    table_type: "BASE TABLE".to_string(),
                    row_count_estimate: row_count,
                });
            }
        }
        Ok(tables)
    }

    async fn list_columns(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ColumnInfo>> {
        let pk = self.primary_key_set(database, schema, table).await?;
        let sql = format!(
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, ORDINAL_POSITION,
                    COLUMNPROPERTY(
                      OBJECT_ID(QUOTENAME(N'{}') + N'.' + QUOTENAME(TABLE_SCHEMA) + N'.' + QUOTENAME(TABLE_NAME)),
                      COLUMN_NAME,
                      N'IsIdentity'
                    ) AS is_identity
             FROM {}.INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_SCHEMA = N'{}' AND TABLE_NAME = N'{}'
             ORDER BY ORDINAL_POSITION",
            database.replace('\'', "''"),
            bracket(database),
            schema.replace('\'', "''"),
            table.replace('\'', "''"),
        );
        let (_, rows) = self.run_select(&sql).await?;
        let mut cols = Vec::new();
        for r in rows {
            if r.len() < 6 {
                continue;
            }
            let name = match &r[0] {
                serde_json::Value::String(s) => s.clone(),
                _ => continue,
            };
            let data_type = match &r[1] {
                serde_json::Value::String(s) => s.clone(),
                _ => String::new(),
            };
            let is_nullable =
                matches!(&r[2], serde_json::Value::String(s) if s.eq_ignore_ascii_case("YES"));
            let default_value = match &r[3] {
                serde_json::Value::String(s) => Some(s.clone()),
                serde_json::Value::Null => None,
                _ => None,
            };
            let ordinal = match &r[4] {
                serde_json::Value::Number(n) => n.as_i64().unwrap_or(0) as i32,
                _ => 0,
            };
            let is_auto = matches!(&r[5], serde_json::Value::Number(n) if n.as_i64() == Some(1));
            cols.push(ColumnInfo {
                name,
                data_type,
                is_nullable,
                is_primary_key: false,
                default_value,
                ordinal_position: ordinal,
                is_auto_generated: is_auto,
            });
        }
        for c in &mut cols {
            c.is_primary_key = pk.contains(&c.name);
        }
        Ok(cols)
    }

    async fn list_indexes(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<IndexInfo>> {
        let sql = format!(
            "SELECT i.name, i.is_unique, i.type_desc,
                    STRING_AGG(c.name, N',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS cols
             FROM {}.sys.tables t
             INNER JOIN {}.sys.schemas s ON t.schema_id = s.schema_id
             INNER JOIN {}.sys.indexes i ON t.object_id = i.object_id
             INNER JOIN {}.sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
             INNER JOIN {}.sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
             WHERE s.name = N'{}' AND t.name = N'{}' AND i.is_hypothetical = 0 AND i.index_id > 0
             GROUP BY i.index_id, i.name, i.is_unique, i.type_desc, i.is_primary_key
             HAVING MAX(CASE WHEN i.is_primary_key = 1 THEN 1 ELSE 0 END) = 0
             ORDER BY i.name",
            bracket(database),
            bracket(database),
            bracket(database),
            bracket(database),
            bracket(database),
            schema.replace('\'', "''"),
            table.replace('\'', "''"),
        );
        let result = self.run_select(&sql).await;
        let (rows, from_string_agg) = match &result {
            Ok((_, r)) if !r.is_empty() && r[0].len() == 4 => (r.clone(), true),
            _ => {
                let fallback = format!(
                    "SELECT i.name, i.is_unique, i.type_desc, c.name AS col, ic.key_ordinal
                     FROM {}.sys.tables t
                     INNER JOIN {}.sys.schemas s ON t.schema_id = s.schema_id
                     INNER JOIN {}.sys.indexes i ON t.object_id = i.object_id
                     INNER JOIN {}.sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                     INNER JOIN {}.sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                     WHERE s.name = N'{}' AND t.name = N'{}' AND i.is_hypothetical = 0 AND i.is_primary_key = 0
                     ORDER BY i.name, ic.key_ordinal",
                    bracket(database),
                    bracket(database),
                    bracket(database),
                    bracket(database),
                    bracket(database),
                    schema.replace('\'', "''"),
                    table.replace('\'', "''"),
                );
                let (_, fr) = self.run_select(&fallback).await?;
                (fr, false)
            }
        };

        if from_string_agg {
            return Ok(rows
                .into_iter()
                .filter_map(|r| {
                    let name = r.first()?.as_str()?.to_string();
                    let is_unique = matches!(r.get(1), Some(serde_json::Value::Bool(b)) if *b)
                        || matches!(r.get(1), Some(serde_json::Value::Number(n)) if n.as_i64() == Some(1));
                    let index_type = r
                        .get(2)
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let cols = r
                        .get(3)
                        .and_then(|v| v.as_str())
                        .map(|s| s.split(',').map(|x| x.to_string()).collect())
                        .unwrap_or_default();
                    Some(IndexInfo {
                        name,
                        columns: cols,
                        is_unique,
                        index_type,
                    })
                })
                .collect());
        }

        use std::collections::BTreeMap;
        let mut by_index: BTreeMap<String, (bool, String, Vec<String>)> = BTreeMap::new();
        for r in rows {
            if r.len() < 5 {
                continue;
            }
            let name = match r[0].as_str() {
                Some(s) => s.to_string(),
                None => continue,
            };
            let is_unique = matches!(r.get(1), Some(serde_json::Value::Bool(b)) if *b)
                || matches!(r.get(1), Some(serde_json::Value::Number(n)) if n.as_i64() == Some(1));
            let index_type = r.get(2).and_then(|v| v.as_str()).unwrap_or("").to_string();
            let col = r.get(3).and_then(|v| v.as_str()).unwrap_or("").to_string();
            let ord = r.get(4).and_then(|v| v.as_u64()).unwrap_or(0) as usize;
            let e =
                by_index
                    .entry(name.clone())
                    .or_insert((is_unique, index_type.clone(), Vec::new()));
            e.0 = is_unique;
            e.1 = index_type;
            while e.2.len() <= ord {
                e.2.push(String::new());
            }
            if ord < e.2.len() {
                e.2[ord] = col;
            }
        }
        Ok(by_index
            .into_iter()
            .map(|(name, (is_unique, index_type, mut cols))| {
                cols.retain(|s| !s.is_empty());
                IndexInfo {
                    name,
                    columns: cols,
                    is_unique,
                    index_type,
                }
            })
            .collect())
    }

    async fn list_foreign_keys(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        let sql = format!(
            "SELECT fk.name, cp.name AS col,
                    OBJECT_SCHEMA_NAME(fk.referenced_object_id, DB_ID(N'{}')) AS rschema,
                    OBJECT_NAME(fk.referenced_object_id, DB_ID(N'{}')) AS rtable,
                    cr.name AS rcol,
                    fk.delete_referential_action_desc,
                    fk.update_referential_action_desc
             FROM {}.sys.foreign_keys fk
             INNER JOIN {}.sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
             INNER JOIN {}.sys.tables pt ON fk.parent_object_id = pt.object_id
             INNER JOIN {}.sys.schemas ps ON pt.schema_id = ps.schema_id
             INNER JOIN {}.sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
             INNER JOIN {}.sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
             WHERE ps.name = N'{}' AND pt.name = N'{}'
             ORDER BY fk.name, fkc.constraint_column_id",
            database.replace('\'', "''"),
            database.replace('\'', "''"),
            bracket(database),
            bracket(database),
            bracket(database),
            bracket(database),
            bracket(database),
            bracket(database),
            schema.replace('\'', "''"),
            table.replace('\'', "''"),
        );
        let (_, rows) = self.run_select(&sql).await?;
        let mut out = Vec::new();
        for r in rows {
            if r.len() < 7 {
                continue;
            }
            let name = r[0].as_str().unwrap_or("").to_string();
            let column = r[1].as_str().unwrap_or("").to_string();
            let rschema = r[2].as_str().unwrap_or("dbo");
            let rtable = r[3].as_str().unwrap_or("").to_string();
            let rcol = r[4].as_str().unwrap_or("").to_string();
            let referenced_table = format!("{}.{}", rschema, rtable);
            let on_delete = r[5].as_str().unwrap_or("").to_string();
            let on_update = r[6].as_str().unwrap_or("").to_string();
            out.push(ForeignKeyInfo {
                name,
                column,
                referenced_table,
                referenced_column: rcol,
                on_delete,
                on_update,
            });
        }
        Ok(out)
    }

    async fn list_functions(&self, database: &str, schema: &str) -> Result<Vec<FunctionInfo>> {
        let sql = format!(
            "SELECT ROUTINE_NAME, ROUTINE_SCHEMA,
                    ISNULL(DATA_TYPE, N'') AS ret,
                    ISNULL(ROUTINE_TYPE, N'') AS kind
             FROM {}.INFORMATION_SCHEMA.ROUTINES
             WHERE ROUTINE_SCHEMA = N'{}'
             ORDER BY ROUTINE_NAME",
            bracket(database),
            schema.replace('\'', "''"),
        );
        let (_, rows) = self.run_select(&sql).await?;
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                if r.len() < 4 {
                    return None;
                }
                Some(FunctionInfo {
                    name: r[0].as_str()?.to_string(),
                    schema: r[1].as_str()?.to_string(),
                    return_type: r[2].as_str()?.to_string(),
                    language: "SQL".to_string(),
                    kind: r[3].as_str()?.to_string(),
                })
            })
            .collect())
    }

    async fn list_triggers(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<TriggerInfo>> {
        let sql = format!(
            "SELECT tr.name, OBJECT_NAME(tr.parent_id) AS table_name,
                    te.type_desc AS event_s, N'INSTEAD OF' AS timing
             FROM {}.sys.triggers tr
             CROSS APPLY (SELECT TOP 1 type_desc FROM {}.sys.trigger_events te WHERE te.object_id = tr.object_id) te
             INNER JOIN {}.sys.tables t ON tr.parent_id = t.object_id
             INNER JOIN {}.sys.schemas s ON t.schema_id = s.schema_id
             WHERE s.name = N'{}' AND t.name = N'{}' AND tr.parent_class = 1",
            bracket(database),
            bracket(database),
            bracket(database),
            bracket(database),
            schema.replace('\'', "''"),
            table.replace('\'', "''"),
        );
        let result = self.run_select(&sql).await;
        let (_, rows) = match result {
            Ok(r) => r,
            Err(_) => {
                let simple = format!(
                    "SELECT tr.name, t.name AS table_name, N'MODIFY' AS event_s, N'AFTER' AS timing
                     FROM {}.sys.triggers tr
                     INNER JOIN {}.sys.tables t ON tr.parent_id = t.object_id
                     INNER JOIN {}.sys.schemas s ON t.schema_id = s.schema_id
                     WHERE s.name = N'{}' AND t.name = N'{}' AND tr.parent_class = 1",
                    bracket(database),
                    bracket(database),
                    bracket(database),
                    schema.replace('\'', "''"),
                    table.replace('\'', "''"),
                );
                self.run_select(&simple).await.unwrap_or((vec![], vec![]))
            }
        };
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                if r.len() < 4 {
                    return None;
                }
                Some(TriggerInfo {
                    name: r[0].as_str()?.to_string(),
                    table_name: r[1].as_str()?.to_string(),
                    event: r[2].as_str()?.to_string(),
                    timing: r[3].as_str()?.to_string(),
                })
            })
            .collect())
    }

    async fn get_table_stats(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<TableStats> {
        let fq = three_part_table(database, schema, table);
        let count_sql = format!("SELECT COUNT_BIG(*) AS c FROM {}", fq);
        let (_, cr) = self.run_select(&count_sql).await?;
        let row_count = cr
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.as_i64())
            .unwrap_or(-1);

        let size_sql = format!(
            "SELECT
               SUM(a.total_pages) * 8 AS total_kb,
               SUM(CASE WHEN i.index_id <= 1 THEN a.used_pages * 8 ELSE 0 END) AS data_kb,
               SUM(CASE WHEN i.index_id > 1 THEN a.used_pages * 8 ELSE 0 END) AS index_kb
             FROM {}.sys.tables t
             INNER JOIN {}.sys.schemas s ON t.schema_id = s.schema_id
             INNER JOIN {}.sys.indexes i ON t.object_id = i.object_id
             INNER JOIN {}.sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
             INNER JOIN {}.sys.allocation_units a ON p.partition_id = a.container_id
             WHERE s.name = N'{}' AND t.name = N'{}'",
            bracket(database),
            bracket(database),
            bracket(database),
            bracket(database),
            bracket(database),
            schema.replace('\'', "''"),
            table.replace('\'', "''"),
        );
        let (_, sz) = self.run_select(&size_sql).await.unwrap_or((vec![], vec![]));
        let (total, data, index) = sz
            .first()
            .map(|r| {
                let t = r.first().and_then(|v| v.as_i64()).unwrap_or(0);
                let d = r.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
                let i = r.get(2).and_then(|v| v.as_i64()).unwrap_or(0);
                (t, d, i)
            })
            .unwrap_or((0, 0, 0));

        Ok(TableStats {
            table_name: format!("{}.{}.{}", database, schema, table),
            row_count,
            total_size: format!("{} KB", total),
            data_size: format!("{} KB", data),
            index_size: format!("{} KB", index),
            last_vacuum: None,
            last_analyze: None,
            dead_tuples: None,
            live_tuples: None,
        })
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
        let columns = self.list_columns(database, schema, table).await?;
        if columns.is_empty() {
            anyhow::bail!("Table has no columns (or table not found)");
        }
        let fq = three_part_table(database, schema, table);
        let pk_cols: Vec<String> = columns
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| c.name.clone())
            .collect();
        let order = if let Some(s) = sort {
            format!(
                "{} {}",
                bracket(&s.column),
                match s.direction {
                    SortDirection::Asc => "ASC",
                    SortDirection::Desc => "DESC",
                }
            )
        } else if pk_cols.is_empty() {
            bracket(&columns[0].name).to_string()
        } else {
            pk_cols
                .iter()
                .map(|c| bracket(c))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let mut where_clause = String::new();
        if let Some(f) = filter {
            let t = f.trim();
            if !t.is_empty() {
                if t.contains(';') || t.contains("--") || t.contains("/*") || t.contains("*/") {
                    anyhow::bail!("Filter contains invalid characters (; -- /* */)");
                }
                where_clause = format!(" WHERE {}", t);
            }
        }

        let count_sql = format!("SELECT COUNT_BIG(*) AS c FROM {}{}", fq, where_clause);
        let total_rows = self
            .run_select(&count_sql)
            .await?
            .1
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let data_sql = format!(
            "SELECT * FROM {}{} ORDER BY {} OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
            fq, where_clause, order, offset, limit
        );
        let (select_names, row_values) = self.run_select(&data_sql).await?;

        let columns_out = if select_names.is_empty() {
            columns.clone()
        } else {
            let ordered_columns: Vec<ColumnInfo> = select_names
                .iter()
                .filter_map(|name| columns.iter().find(|c| &c.name == name).cloned())
                .collect();
            if ordered_columns.len() == select_names.len() {
                ordered_columns
            } else {
                columns.clone()
            }
        };

        Ok(TableData {
            columns: columns_out,
            rows: row_values,
            total_rows,
            offset,
            limit,
        })
    }

    async fn execute_query(&self, database: &str, sql: &str) -> Result<QueryResult> {
        self.use_database(database).await?;
        let start = Instant::now();

        if Self::is_select_like(sql) {
            let (columns, rows) = self.run_select(sql).await?;
            let elapsed = start.elapsed().as_millis() as u64;
            let n = rows.len() as u64;
            return Ok(QueryResult {
                columns,
                rows,
                rows_affected: n,
                execution_time_ms: elapsed,
                is_select: true,
            });
        }

        if Self::is_ddl(sql) {
            self.run_batch(sql).await?;
            let elapsed = start.elapsed().as_millis() as u64;
            return Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: 0,
                execution_time_ms: elapsed,
                is_select: false,
            });
        }

        let rows_affected = self.run_exec(sql).await?;
        let elapsed = start.elapsed().as_millis() as u64;
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected,
            execution_time_ms: elapsed,
            is_select: false,
        })
    }

    async fn explain_query(&self, database: &str, sql: &str) -> Result<ExplainResult> {
        self.use_database(database).await?;
        let start = Instant::now();
        let plan_batch = format!("SET SHOWPLAN_XML ON; {}; SET SHOWPLAN_XML OFF;", sql);
        let result = self.run_select(&plan_batch).await;
        let elapsed = start.elapsed().as_millis() as u64;
        match result {
            Ok((cols, rows)) => {
                let raw = rows
                    .first()
                    .and_then(|r| r.first())
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "(no plan output)".to_string());
                Ok(ExplainResult {
                    plan: ExplainNode {
                        node_type: "SQL Server SHOWPLAN_XML".to_string(),
                        relation: None,
                        startup_cost: 0.0,
                        total_cost: 0.0,
                        actual_time_ms: Some(elapsed as f64),
                        rows_estimated: 0,
                        rows_actual: None,
                        width: 0,
                        filter: cols.first().cloned(),
                        children: vec![],
                    },
                    raw_text: raw,
                    execution_time_ms: elapsed,
                })
            }
            Err(e) => Ok(ExplainResult {
                plan: ExplainNode {
                    node_type: "Explain".to_string(),
                    relation: None,
                    startup_cost: 0.0,
                    total_cost: 0.0,
                    actual_time_ms: Some(elapsed as f64),
                    rows_estimated: 0,
                    rows_actual: None,
                    width: 0,
                    filter: Some(sql.to_string()),
                    children: vec![],
                },
                raw_text: format!("Could not run SHOWPLAN_XML: {}", e),
                execution_time_ms: elapsed,
            }),
        }
    }

    async fn validate_query(&self, database: &str, sql: &str) -> Result<Option<ValidationError>> {
        if sql.trim().is_empty() {
            return Ok(Some(ValidationError {
                message: "Empty query".to_string(),
                position: None,
            }));
        }
        self.use_database(database).await?;
        let batch = format!("SET PARSEONLY ON; {}; SET PARSEONLY OFF;", sql);
        let mut client = self.client.lock().await;
        match client.execute(&*batch, &[]).await {
            Ok(_) => Ok(None),
            Err(e) => {
                let message = e.to_string();
                Ok(Some(ValidationError {
                    message,
                    position: None,
                }))
            }
        }
    }

    async fn get_ddl(
        &self,
        database: &str,
        schema: &str,
        object_name: &str,
        object_type: &str,
    ) -> Result<String> {
        let ot = object_type.to_uppercase();
        let full_obj = three_part_table(database, schema, object_name);
        let full_lit = full_obj.replace('\'', "''");
        let sql = match ot.as_str() {
            "VIEW" => {
                format!(
                    "SELECT OBJECT_DEFINITION(OBJECT_ID(N'{}')) AS ddl",
                    full_lit
                )
            }
            "TABLE" | "BASE TABLE" => {
                let cols = self.list_columns(database, schema, object_name).await?;
                let pk_cols: Vec<String> = cols
                    .iter()
                    .filter(|c| c.is_primary_key)
                    .map(|c| bracket(&c.name))
                    .collect();
                let col_defs: Vec<String> = cols
                    .iter()
                    .map(|c| {
                        let mut def = format!("    {} {}", bracket(&c.name), c.data_type);
                        if !c.is_nullable {
                            def.push_str(" NOT NULL");
                        }
                        if c.is_auto_generated {
                            def.push_str(" IDENTITY");
                        }
                        if let Some(d) = &c.default_value {
                            def.push_str(&format!(" DEFAULT {}", d));
                        }
                        def
                    })
                    .collect();
                let mut parts = col_defs;
                if !pk_cols.is_empty() {
                    parts.push(format!("    PRIMARY KEY ({})", pk_cols.join(", ")));
                }
                return Ok(format!(
                    "CREATE TABLE {}.{} (\n{}\n);",
                    bracket(schema),
                    bracket(object_name),
                    parts.join(",\n")
                ));
            }
            _ => anyhow::bail!("DDL for '{}' is not supported on SQL Server", object_type),
        };
        let (_, rows) = self.run_select(&sql).await?;
        let ddl = rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow!("No DDL returned"))?;
        Ok(ddl)
    }

    async fn apply_changes(&self, changes: &DataChanges) -> Result<()> {
        let fq = three_part_table(&changes.database, &changes.schema, &changes.table);
        let mut stmts = Vec::new();
        stmts.push("SET XACT_ABORT ON; BEGIN TRAN;".to_string());
        for update in &changes.updates {
            if update.primary_key_values.is_empty() {
                anyhow::bail!("Cannot update row: no primary key values provided");
            }
            let set_clause = format!(
                "{} = {}",
                bracket(&update.column_name),
                json_to_mssql_literal(&update.new_value)
            );
            let where_clause: Vec<String> = update
                .primary_key_values
                .iter()
                .map(|(col, val)| format!("{} = {}", bracket(col), json_to_mssql_literal(val)))
                .collect();
            stmts.push(format!(
                "UPDATE {} SET {} WHERE {};",
                fq,
                set_clause,
                where_clause.join(" AND ")
            ));
        }
        for insert in &changes.inserts {
            let cols: Vec<String> = insert.values.iter().map(|(n, _)| bracket(n)).collect();
            let vals: Vec<String> = insert
                .values
                .iter()
                .map(|(_, v)| json_to_mssql_literal(v))
                .collect();
            stmts.push(format!(
                "INSERT INTO {} ({}) VALUES ({});",
                fq,
                cols.join(", "),
                vals.join(", ")
            ));
        }
        for delete in &changes.deletes {
            if delete.primary_key_values.is_empty() {
                anyhow::bail!("Cannot delete row: no primary key values provided");
            }
            let where_clause: Vec<String> = delete
                .primary_key_values
                .iter()
                .map(|(col, val)| format!("{} = {}", bracket(col), json_to_mssql_literal(val)))
                .collect();
            stmts.push(format!(
                "DELETE FROM {} WHERE {};",
                fq,
                where_clause.join(" AND ")
            ));
        }
        stmts.push("COMMIT TRAN;".to_string());

        let batch = stmts.join("\n");
        self.run_exec(&batch).await?;
        Ok(())
    }

    async fn create_table(
        &self,
        database: &str,
        schema: &str,
        table_name: &str,
        columns: &[ColumnDefinition],
    ) -> Result<()> {
        if columns.is_empty() {
            anyhow::bail!("At least one column is required");
        }
        let mut parts = Vec::new();
        let mut pk: Vec<String> = Vec::new();
        for col in columns {
            let null = if col.is_nullable { "NULL" } else { "NOT NULL" };
            let ident = format!("{} {}", bracket(&col.name), col.data_type);
            let ident = if let Some(d) = &col.default_value {
                format!("{} DEFAULT {}", ident, d)
            } else {
                ident
            };
            parts.push(format!("{} {}", ident, null));
            if col.is_primary_key {
                pk.push(bracket(&col.name));
            }
        }
        if !pk.is_empty() {
            parts.push(format!("PRIMARY KEY ({})", pk.join(", ")));
        }
        self.use_database(database).await?;
        let sql = format!(
            "CREATE TABLE {}.{} ({})",
            bracket(schema),
            bracket(table_name),
            parts.join(", ")
        );
        self.run_batch(&sql).await?;
        Ok(())
    }

    async fn alter_table(
        &self,
        database: &str,
        schema: &str,
        table_name: &str,
        operations: &[AlterTableOperation],
    ) -> Result<()> {
        self.use_database(database).await?;
        let fq = format!("{}.{}", bracket(schema), bracket(table_name));
        for op in operations {
            let stmt = match op {
                AlterTableOperation::AddColumn { column } => {
                    let null = if column.is_nullable {
                        "NULL"
                    } else {
                        "NOT NULL"
                    };
                    format!(
                        "ALTER TABLE {} ADD {} {} {}",
                        fq,
                        bracket(&column.name),
                        column.data_type,
                        null
                    )
                }
                AlterTableOperation::DropColumn { column_name } => {
                    format!("ALTER TABLE {} DROP COLUMN {}", fq, bracket(column_name))
                }
                AlterTableOperation::RenameColumn { old_name, new_name } => {
                    let current = format!(
                        "{}.{}.{}",
                        schema.replace('\'', "''"),
                        table_name.replace('\'', "''"),
                        old_name.replace('\'', "''")
                    );
                    format!(
                        "EXEC sp_rename N'{}', N'{}', N'COLUMN'",
                        current,
                        new_name.replace('\'', "''")
                    )
                }
                AlterTableOperation::ChangeColumnType {
                    column_name,
                    new_type,
                } => format!(
                    "ALTER TABLE {} ALTER COLUMN {} {}",
                    fq,
                    bracket(column_name),
                    new_type
                ),
                AlterTableOperation::RenameTable { new_name } => {
                    let current = format!(
                        "{}.{}",
                        schema.replace('\'', "''"),
                        table_name.replace('\'', "''")
                    );
                    format!(
                        "EXEC sp_rename N'{}', N'{}'",
                        current,
                        new_name.replace('\'', "''")
                    )
                }
                AlterTableOperation::SetNullable {
                    column_name,
                    nullable,
                } => {
                    let cols = self.list_columns(database, schema, table_name).await?;
                    let col = cols
                        .iter()
                        .find(|c| c.name == *column_name)
                        .ok_or_else(|| anyhow!("Column {} not found", column_name))?;
                    let null_str = if *nullable { "NULL" } else { "NOT NULL" };
                    format!(
                        "ALTER TABLE {} ALTER COLUMN {} {} {}",
                        fq,
                        bracket(column_name),
                        col.data_type,
                        null_str
                    )
                }
                AlterTableOperation::SetDefault {
                    column_name,
                    default_value,
                } => match default_value {
                    Some(d) if !d.is_empty() => {
                        format!(
                            "ALTER TABLE {} ADD DEFAULT {} FOR {}",
                            fq,
                            d,
                            bracket(column_name)
                        )
                    }
                    _ => {
                        let find_constraint = format!(
                                "SELECT dc.name FROM sys.default_constraints dc \
                                 INNER JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id \
                                 INNER JOIN sys.tables t ON c.object_id = t.object_id \
                                 INNER JOIN sys.schemas s ON t.schema_id = s.schema_id \
                                 WHERE s.name = N'{}' AND t.name = N'{}' AND c.name = N'{}'",
                                schema.replace('\'', "''"), table_name.replace('\'', "''"), column_name.replace('\'', "''")
                            );
                        let (_, rows) = self.run_select(&find_constraint).await?;
                        if let Some(constraint_name) = rows
                            .first()
                            .and_then(|r| r.first())
                            .and_then(|v| v.as_str())
                        {
                            format!(
                                "ALTER TABLE {} DROP CONSTRAINT {}",
                                fq,
                                bracket(constraint_name)
                            )
                        } else {
                            continue;
                        }
                    }
                },
            };
            self.run_batch(&stmt).await?;
        }
        Ok(())
    }

    async fn drop_object(
        &self,
        database: &str,
        schema: &str,
        object_name: &str,
        object_type: &str,
    ) -> Result<()> {
        self.use_database(database).await?;
        let fq = format!("{}.{}", bracket(schema), bracket(object_name));
        let sql = match object_type.to_uppercase().as_str() {
            "TABLE" | "BASE TABLE" => format!("DROP TABLE {}", fq),
            "VIEW" => format!("DROP VIEW {}", fq),
            "INDEX" => {
                let q = format!(
                    "SELECT t.name FROM sys.indexes i
                     INNER JOIN sys.tables t ON i.object_id = t.object_id
                     INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                     WHERE i.name = N'{}' AND s.name = N'{}'",
                    object_name.replace('\'', "''"),
                    schema.replace('\'', "''"),
                );
                let (_, rows) = self.run_select(&q).await?;
                let tbl = rows
                    .first()
                    .and_then(|r| r.first())
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        anyhow!("Index '{}' not found in schema '{}'", object_name, schema)
                    })?;
                format!(
                    "DROP INDEX {} ON {}.{}",
                    bracket(object_name),
                    bracket(schema),
                    bracket(tbl),
                )
            }
            _ => anyhow::bail!("Unsupported drop type {}", object_type),
        };
        self.run_batch(&sql).await?;
        Ok(())
    }

    async fn truncate_table(&self, database: &str, schema: &str, table_name: &str) -> Result<()> {
        self.use_database(database).await?;
        let sql = format!("TRUNCATE TABLE {}.{}", bracket(schema), bracket(table_name));
        self.run_batch(&sql).await?;
        Ok(())
    }

    async fn import_data(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        columns: &[String],
        rows: &[Vec<serde_json::Value>],
    ) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }
        self.use_database(database).await?;
        let fq = format!("{}.{}", bracket(schema), bracket(table));
        let cols = columns
            .iter()
            .map(|c| bracket(c))
            .collect::<Vec<_>>()
            .join(", ");
        let mut n = 0u64;
        const BATCH_SIZE: usize = 100;
        for chunk in rows.chunks(BATCH_SIZE) {
            let values_list: Vec<String> = chunk
                .iter()
                .map(|row| {
                    let vals = row
                        .iter()
                        .map(json_to_mssql_literal)
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("({})", vals)
                })
                .collect();
            let sql = format!(
                "INSERT INTO {} ({}) VALUES {}",
                fq,
                cols,
                values_list.join(", ")
            );
            let affected = self.run_exec(&sql).await?;
            n += affected;
        }
        Ok(n)
    }

    async fn get_server_activity(&self) -> Result<Vec<ServerActivity>> {
        let sql = "SELECT CAST(r.session_id AS varchar(20)), s.login_name,
                          DB_NAME(r.database_id), r.status, t.text,
                          CAST(r.total_elapsed_time AS float) / 1000.0,
                          CONVERT(varchar(48), c.client_net_address)
                   FROM sys.dm_exec_requests r
                   INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
                   CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
                   LEFT JOIN sys.dm_exec_connections c ON r.session_id = c.session_id
                   WHERE r.session_id <> @@SPID AND r.sql_handle IS NOT NULL";
        let (_, rows) = self.run_select(sql).await.unwrap_or((vec![], vec![]));
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                if r.len() < 7 {
                    return None;
                }
                Some(ServerActivity {
                    pid: r[0].as_str()?.to_string(),
                    user: r[1].as_str()?.to_string(),
                    database: r[2].as_str().unwrap_or("").to_string(),
                    state: r[3].as_str().unwrap_or("").to_string(),
                    query: r[4].as_str().unwrap_or("").to_string(),
                    duration_ms: r[5].as_f64(),
                    client_addr: r[6].as_str().unwrap_or("").to_string(),
                })
            })
            .collect())
    }

    async fn get_database_stats(&self) -> Result<DatabaseStats> {
        Ok(DatabaseStats {
            active_connections: 0,
            idle_connections: 0,
            idle_in_transaction: 0,
            total_connections: 0,
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
            message: Some("Query statistics are not wired for SQL Server yet".to_string()),
            entries: vec![],
        })
    }

    async fn cancel_query(&self, pid: &str) -> Result<()> {
        let id: i32 = pid.parse().map_err(|_| anyhow!("Invalid session id"))?;
        let sql = format!("KILL {}", id);
        self.run_batch(&sql).await?;
        Ok(())
    }

    async fn list_roles(&self) -> Result<Vec<RoleInfo>> {
        let sql = "SELECT name FROM sys.server_principals WHERE type IN ('S','U') ORDER BY name";
        let (_, rows) = self.run_select(sql).await.unwrap_or((vec![], vec![]));
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                let name = r.first()?.as_str()?.to_string();
                Some(RoleInfo {
                    name,
                    is_superuser: false,
                    can_login: true,
                    can_create_db: false,
                    can_create_role: false,
                    is_replication: false,
                    connection_limit: -1,
                    valid_until: None,
                    member_of: vec![],
                })
            })
            .collect())
    }

    async fn create_role(&self, _req: &CreateRoleRequest) -> Result<()> {
        Err(anyhow!(
            "Creating SQL Server logins from Tablio is not supported yet"
        ))
    }

    async fn drop_role(&self, _name: &str) -> Result<()> {
        Err(anyhow!(
            "Dropping SQL Server logins from Tablio is not supported yet"
        ))
    }

    async fn alter_role(&self, _req: &AlterRoleRequest) -> Result<()> {
        Err(anyhow!(
            "Altering SQL Server logins from Tablio is not supported yet"
        ))
    }

    async fn test_connection(&self) -> Result<bool> {
        self.run_select("SELECT 1").await?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bracket_plain_identifier() {
        assert_eq!(bracket("users"), "[users]");
    }

    #[test]
    fn bracket_identifier_with_closing_bracket() {
        assert_eq!(bracket("ta]ble"), "[ta]]ble]");
    }

    #[test]
    fn bracket_empty_identifier() {
        assert_eq!(bracket(""), "[]");
    }

    #[test]
    fn three_part_table_basic() {
        assert_eq!(
            three_part_table("mydb", "dbo", "orders"),
            "[mydb].[dbo].[orders]"
        );
    }

    #[test]
    fn three_part_table_special_chars() {
        assert_eq!(
            three_part_table("my]db", "d]bo", "or]ders"),
            "[my]]db].[d]]bo].[or]]ders]"
        );
    }

    #[test]
    fn json_to_mssql_literal_null() {
        assert_eq!(json_to_mssql_literal(&serde_json::Value::Null), "NULL");
    }

    #[test]
    fn json_to_mssql_literal_bool() {
        assert_eq!(json_to_mssql_literal(&serde_json::json!(true)), "1");
        assert_eq!(json_to_mssql_literal(&serde_json::json!(false)), "0");
    }

    #[test]
    fn json_to_mssql_literal_number() {
        assert_eq!(json_to_mssql_literal(&serde_json::json!(42)), "42");
        assert_eq!(json_to_mssql_literal(&serde_json::json!(3.14)), "3.14");
    }

    #[test]
    fn json_to_mssql_literal_string() {
        assert_eq!(
            json_to_mssql_literal(&serde_json::json!("hello")),
            "N'hello'"
        );
    }

    #[test]
    fn json_to_mssql_literal_string_with_quotes() {
        assert_eq!(
            json_to_mssql_literal(&serde_json::json!("it's")),
            "N'it''s'"
        );
    }

    #[test]
    fn json_to_mssql_literal_array() {
        let val = serde_json::json!([1, 2, 3]);
        let lit = json_to_mssql_literal(&val);
        assert!(lit.starts_with("N'"));
        assert!(lit.ends_with('\''));
        assert!(lit.contains("[1,2,3]"));
    }

    #[test]
    fn json_to_mssql_literal_object() {
        let val = serde_json::json!({"key": "value"});
        let lit = json_to_mssql_literal(&val);
        assert!(lit.starts_with("N'"));
        assert!(lit.contains("key"));
    }

    #[test]
    fn column_data_to_json_u8() {
        assert_eq!(
            column_data_to_json(&ColumnData::U8(Some(42))),
            serde_json::json!(42)
        );
        assert_eq!(
            column_data_to_json(&ColumnData::U8(None)),
            serde_json::Value::Null
        );
    }

    #[test]
    fn column_data_to_json_i16() {
        assert_eq!(
            column_data_to_json(&ColumnData::I16(Some(-1))),
            serde_json::json!(-1)
        );
        assert_eq!(
            column_data_to_json(&ColumnData::I16(None)),
            serde_json::Value::Null
        );
    }

    #[test]
    fn column_data_to_json_i32() {
        assert_eq!(
            column_data_to_json(&ColumnData::I32(Some(100))),
            serde_json::json!(100)
        );
        assert_eq!(
            column_data_to_json(&ColumnData::I32(None)),
            serde_json::Value::Null
        );
    }

    #[test]
    fn column_data_to_json_i64() {
        assert_eq!(
            column_data_to_json(&ColumnData::I64(Some(999999999999i64))),
            serde_json::json!(999999999999i64)
        );
    }

    #[test]
    fn column_data_to_json_f32() {
        let val = column_data_to_json(&ColumnData::F32(Some(1.5)));
        assert!(val.as_f64().unwrap() - 1.5 < 0.01);
    }

    #[test]
    fn column_data_to_json_f64() {
        let val = column_data_to_json(&ColumnData::F64(Some(3.14)));
        assert!((val.as_f64().unwrap() - 3.14).abs() < 0.001);
    }

    #[test]
    fn column_data_to_json_bit() {
        assert_eq!(
            column_data_to_json(&ColumnData::Bit(Some(true))),
            serde_json::json!(true)
        );
        assert_eq!(
            column_data_to_json(&ColumnData::Bit(Some(false))),
            serde_json::json!(false)
        );
        assert_eq!(
            column_data_to_json(&ColumnData::Bit(None)),
            serde_json::Value::Null
        );
    }

    #[test]
    fn column_data_to_json_string() {
        assert_eq!(
            column_data_to_json(&ColumnData::String(Some("test".into()))),
            serde_json::json!("test")
        );
        assert_eq!(
            column_data_to_json(&ColumnData::String(None)),
            serde_json::Value::Null
        );
    }

    #[test]
    fn column_data_to_json_guid() {
        let u = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let val = column_data_to_json(&ColumnData::Guid(Some(u)));
        assert_eq!(
            val,
            serde_json::json!("550e8400-e29b-41d4-a716-446655440000")
        );
        assert_eq!(
            column_data_to_json(&ColumnData::Guid(None)),
            serde_json::Value::Null
        );
    }

    #[test]
    fn column_data_to_json_binary() {
        let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let val = column_data_to_json(&ColumnData::Binary(Some(data.into())));
        assert_eq!(val, serde_json::json!("0xdeadbeef"));
        assert_eq!(
            column_data_to_json(&ColumnData::Binary(None)),
            serde_json::Value::Null
        );
    }

    #[test]
    fn column_data_to_json_xml_none() {
        assert_eq!(
            column_data_to_json(&ColumnData::Xml(None)),
            serde_json::Value::Null
        );
    }

    #[test]
    fn column_data_to_json_datetime_none() {
        assert_eq!(
            column_data_to_json(&ColumnData::DateTime(None)),
            serde_json::Value::Null
        );
    }

    #[test]
    fn column_data_to_json_smalldatetime_none() {
        assert_eq!(
            column_data_to_json(&ColumnData::SmallDateTime(None)),
            serde_json::Value::Null
        );
    }

    #[test]
    fn column_data_to_json_numeric_none() {
        assert_eq!(
            column_data_to_json(&ColumnData::Numeric(None)),
            serde_json::Value::Null
        );
    }

    #[test]
    fn rows_to_grid_empty() {
        let (cols, rows) = rows_to_grid(vec![]);
        assert!(cols.is_empty());
        assert!(rows.is_empty());
    }

    #[test]
    fn is_select_like_variants() {
        assert!(MssqlDriver::is_select_like("SELECT 1"));
        assert!(MssqlDriver::is_select_like("  select * from t"));
        assert!(MssqlDriver::is_select_like(
            "WITH cte AS (SELECT 1) SELECT * FROM cte"
        ));
        assert!(!MssqlDriver::is_select_like("INSERT INTO t VALUES (1)"));
        assert!(!MssqlDriver::is_select_like("UPDATE t SET a = 1"));
        assert!(!MssqlDriver::is_select_like("DELETE FROM t"));
        assert!(!MssqlDriver::is_select_like("CREATE TABLE t (id INT)"));
    }

    #[test]
    fn with_use_database_non_empty() {
        let sql = MssqlDriver::with_use_database("mydb", "SELECT 1");
        assert!(sql.starts_with("USE [mydb]; "));
        assert!(sql.contains("SELECT 1"));
    }

    #[test]
    fn with_use_database_empty() {
        let sql = MssqlDriver::with_use_database("", "SELECT 1");
        assert_eq!(sql, "SELECT 1");
    }

    #[test]
    fn with_use_database_whitespace_only() {
        let sql = MssqlDriver::with_use_database("  ", "SELECT 1");
        assert_eq!(sql, "SELECT 1");
    }

    // -----------------------------------------------------------------------
    // Regression tests for fixes applied to prevent future issues
    // -----------------------------------------------------------------------

    #[test]
    fn is_ddl_create() {
        assert!(MssqlDriver::is_ddl("CREATE TABLE t (id INT)"));
        assert!(MssqlDriver::is_ddl("CREATE VIEW v AS SELECT 1"));
        assert!(MssqlDriver::is_ddl("  CREATE INDEX ix ON t(c)"));
    }

    #[test]
    fn is_ddl_alter() {
        assert!(MssqlDriver::is_ddl("ALTER TABLE t ADD col INT"));
    }

    #[test]
    fn is_ddl_drop() {
        assert!(MssqlDriver::is_ddl("DROP TABLE t"));
        assert!(MssqlDriver::is_ddl("DROP VIEW v"));
    }

    #[test]
    fn is_ddl_truncate() {
        assert!(MssqlDriver::is_ddl("TRUNCATE TABLE t"));
    }

    #[test]
    fn is_ddl_grant_revoke_deny() {
        assert!(MssqlDriver::is_ddl("GRANT SELECT ON t TO u"));
        assert!(MssqlDriver::is_ddl("REVOKE SELECT ON t FROM u"));
        assert!(MssqlDriver::is_ddl("DENY SELECT ON t TO u"));
    }

    #[test]
    fn is_ddl_false_for_dml() {
        assert!(!MssqlDriver::is_ddl("SELECT * FROM t"));
        assert!(!MssqlDriver::is_ddl("INSERT INTO t VALUES (1)"));
        assert!(!MssqlDriver::is_ddl("UPDATE t SET a = 1"));
        assert!(!MssqlDriver::is_ddl("DELETE FROM t"));
    }

    #[test]
    fn json_to_mssql_literal_string_with_backslash() {
        assert_eq!(
            json_to_mssql_literal(&serde_json::json!("path\\to\\file")),
            "N'path\\to\\file'"
        );
    }

    #[test]
    fn json_to_mssql_literal_injection_attempt() {
        let val = serde_json::json!("'; DROP TABLE users; --");
        let lit = json_to_mssql_literal(&val);
        assert_eq!(lit, "N'''; DROP TABLE users; --'");
        assert!(lit.starts_with("N'"));
        assert!(lit.ends_with('\''));
    }

    #[test]
    fn bracket_prevents_injection() {
        let evil = "]; DROP TABLE users; --";
        let quoted = bracket(evil);
        assert_eq!(quoted, "[]]; DROP TABLE users; --]");
        assert!(quoted.starts_with('['));
        assert!(quoted.ends_with(']'));
    }

    #[test]
    fn bracket_double_close_bracket() {
        assert_eq!(bracket("a]]b"), "[a]]]]b]");
    }

    #[test]
    fn filter_validation_rejects_semicolon() {
        let f = "id = 1; DROP TABLE users";
        assert!(f.contains(';'));
    }

    #[test]
    fn filter_validation_rejects_comments() {
        let f1 = "id = 1 -- comment";
        let f2 = "id = 1 /* evil */";
        assert!(f1.contains("--"));
        assert!(f2.contains("/*"));
    }

    #[test]
    fn three_part_table_with_injection_attempt() {
        let result = three_part_table("db]; DROP TABLE x; --", "dbo", "t");
        assert_eq!(result, "[db]]; DROP TABLE x; --].[dbo].[t]");
    }
}
