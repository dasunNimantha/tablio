use anyhow::{anyhow, Result};
use async_trait::async_trait;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::response::query_result::QueryRowsResult;
use std::time::Instant;

use crate::db::DatabaseDriver;
use crate::models::*;

pub struct CassandraDriver {
    session: Session,
}

const SYSTEM_KEYSPACES: &[&str] = &[
    "system",
    "system_auth",
    "system_distributed",
    "system_distributed_everywhere",
    "system_schema",
    "system_traces",
    "system_views",
    "system_virtual_schema",
];

impl CassandraDriver {
    pub async fn connect(config: &ConnectionConfig) -> Result<Self> {
        let addr = format!("{}:{}", config.host, config.port);
        let mut builder = SessionBuilder::new().known_node(&addr);

        if !config.user.is_empty() {
            builder = builder.user(&config.user, &config.password);
        }

        let session = builder.build().await.map_err(|e| anyhow!("{}", e))?;
        Ok(Self { session })
    }
}

fn cql_value_to_json(col_type: &str, raw: &scylla::value::CqlValue) -> serde_json::Value {
    use scylla::value::CqlValue;
    match raw {
        CqlValue::Ascii(s) | CqlValue::Text(s) => serde_json::Value::String(s.clone()),
        CqlValue::Boolean(b) => serde_json::Value::Bool(*b),
        CqlValue::Int(i) => serde_json::json!(*i),
        CqlValue::BigInt(i) => serde_json::json!(*i),
        CqlValue::SmallInt(i) => serde_json::json!(*i),
        CqlValue::TinyInt(i) => serde_json::json!(*i),
        CqlValue::Float(f) => serde_json::json!(*f),
        CqlValue::Double(d) => serde_json::json!(*d),
        CqlValue::Uuid(u) => serde_json::Value::String(u.to_string()),
        CqlValue::Timeuuid(t) => serde_json::Value::String(t.as_ref().to_string()),
        CqlValue::Timestamp(ts) => serde_json::Value::String(format!("{}", ts.0)),
        CqlValue::Date(d) => serde_json::Value::String(format!("{}", d.0)),
        CqlValue::Time(t) => serde_json::Value::String(format!("{}", t.0)),
        CqlValue::Inet(addr) => serde_json::Value::String(addr.to_string()),
        CqlValue::Blob(b) => {
            let hex: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
            serde_json::Value::String(format!("0x{}", hex))
        }
        CqlValue::Varint(v) => serde_json::Value::String(format!("{:?}", v)),
        CqlValue::Decimal(d) => serde_json::Value::String(format!("{:?}", d)),
        CqlValue::Counter(c) => serde_json::json!(c.0),
        CqlValue::Set(items) => {
            let arr: Vec<serde_json::Value> = items
                .iter()
                .map(|v| cql_value_to_json(col_type, v))
                .collect();
            serde_json::Value::Array(arr)
        }
        CqlValue::List(items) => {
            let arr: Vec<serde_json::Value> = items
                .iter()
                .map(|v| cql_value_to_json(col_type, v))
                .collect();
            serde_json::Value::Array(arr)
        }
        CqlValue::Map(entries) => {
            let obj: serde_json::Map<String, serde_json::Value> = entries
                .iter()
                .map(|(k, v)| {
                    let key = match k {
                        CqlValue::Text(s) | CqlValue::Ascii(s) => s.clone(),
                        other => format!("{:?}", other),
                    };
                    (key, cql_value_to_json(col_type, v))
                })
                .collect();
            serde_json::Value::Object(obj)
        }
        CqlValue::Tuple(fields) => {
            let arr: Vec<serde_json::Value> = fields
                .iter()
                .map(|f| match f {
                    Some(v) => cql_value_to_json(col_type, v),
                    None => serde_json::Value::Null,
                })
                .collect();
            serde_json::Value::Array(arr)
        }
        CqlValue::UserDefinedType { fields, .. } => {
            let obj: serde_json::Map<String, serde_json::Value> = fields
                .iter()
                .map(|(name, val)| {
                    let v = match val {
                        Some(v) => cql_value_to_json(col_type, v),
                        None => serde_json::Value::Null,
                    };
                    (name.clone(), v)
                })
                .collect();
            serde_json::Value::Object(obj)
        }
        CqlValue::Empty => serde_json::Value::Null,
        _ => serde_json::Value::String(format!("{:?}", raw)),
    }
}

fn json_to_cql_literal(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(json_to_cql_literal).collect();
            format!("[{}]", items.join(", "))
        }
        serde_json::Value::Object(obj) => {
            let items: Vec<String> = obj
                .iter()
                .map(|(k, v)| format!("'{}': {}", k.replace('\'', "''"), json_to_cql_literal(v)))
                .collect();
            format!("{{{}}}", items.join(", "))
        }
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn rows_from_result(result: &QueryRowsResult) -> (Vec<String>, Vec<Vec<serde_json::Value>>) {
    let col_specs = result.column_specs();
    let columns: Vec<String> = col_specs.iter().map(|c| c.name().to_string()).collect();
    let mut rows = Vec::new();

    if let Ok(rows_iter) = result.rows::<scylla::value::Row>() {
        for row_result in rows_iter {
            if let Ok(row) = row_result {
                let json_row: Vec<serde_json::Value> = row
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(i, opt)| match opt {
                        Some(v) => {
                            let col_name = if i < columns.len() {
                                &columns[i]
                            } else {
                                ""
                            };
                            cql_value_to_json(col_name, v)
                        }
                        None => serde_json::Value::Null,
                    })
                    .collect();
                rows.push(json_row);
            }
        }
    }

    (columns, rows)
}

#[async_trait]
impl DatabaseDriver for CassandraDriver {
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        let result = self
            .session
            .query_unpaged(
                "SELECT keyspace_name FROM system_schema.keyspaces",
                &[],
            )
            .await
            .map_err(|e| anyhow!("{}", e))?
            .into_rows_result()
            .map_err(|e| anyhow!("{}", e))?;

        let mut databases = Vec::new();
        if let Ok(rows) = result.rows::<(String,)>() {
            for row in rows {
                if let Ok((name,)) = row {
                    if !SYSTEM_KEYSPACES.contains(&name.as_str()) {
                        databases.push(DatabaseInfo { name });
                    }
                }
            }
        }
        databases.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(databases)
    }

    async fn list_schemas(&self, database: &str) -> Result<Vec<SchemaInfo>> {
        Ok(vec![SchemaInfo {
            name: database.to_string(),
        }])
    }

    async fn list_tables(&self, database: &str, _schema: &str) -> Result<Vec<TableInfo>> {
        let result = self
            .session
            .query_unpaged(
                "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?",
                (database,),
            )
            .await
            .map_err(|e| anyhow!("{}", e))?
            .into_rows_result()
            .map_err(|e| anyhow!("{}", e))?;

        let mut tables = Vec::new();
        if let Ok(rows) = result.rows::<(String,)>() {
            for row in rows {
                if let Ok((name,)) = row {
                    tables.push(TableInfo {
                        name: name.clone(),
                        schema: database.to_string(),
                        table_type: "TABLE".to_string(),
                        row_count_estimate: None,
                    });
                }
            }
        }
        tables.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(tables)
    }

    async fn list_columns(
        &self,
        database: &str,
        _schema: &str,
        table: &str,
    ) -> Result<Vec<ColumnInfo>> {
        let result = self
            .session
            .query_unpaged(
                "SELECT column_name, type, kind, position FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?",
                (database, table),
            )
            .await
            .map_err(|e| anyhow!("{}", e))?
            .into_rows_result()
            .map_err(|e| anyhow!("{}", e))?;

        let mut columns = Vec::new();
        if let Ok(rows) = result.rows::<(String, String, String, i32)>() {
            for row in rows {
                if let Ok((col_name, col_type, kind, position)) = row {
                    let is_pk = kind == "partition_key" || kind == "clustering";
                    columns.push(ColumnInfo {
                        name: col_name,
                        data_type: col_type,
                        is_nullable: !is_pk,
                        is_primary_key: is_pk,
                        default_value: None,
                        ordinal_position: position,
                        is_auto_generated: false,
                    });
                }
            }
        }
        columns.sort_by_key(|c| c.ordinal_position);
        Ok(columns)
    }

    async fn list_indexes(
        &self,
        database: &str,
        _schema: &str,
        table: &str,
    ) -> Result<Vec<IndexInfo>> {
        let result = self
            .session
            .query_unpaged(
                "SELECT index_name, options FROM system_schema.indexes WHERE keyspace_name = ? AND table_name = ?",
                (database, table),
            )
            .await
            .map_err(|e| anyhow!("{}", e))?
            .into_rows_result()
            .map_err(|e| anyhow!("{}", e))?;

        let mut indexes = Vec::new();
        if let Ok(rows) = result.rows::<(String, std::collections::HashMap<String, String>)>() {
            for row in rows {
                if let Ok((name, options)) = row {
                    let target = options.get("target").cloned().unwrap_or_default();
                    indexes.push(IndexInfo {
                        name,
                        columns: vec![target],
                        is_unique: false,
                        index_type: "SECONDARY".to_string(),
                    });
                }
            }
        }
        Ok(indexes)
    }

    async fn list_foreign_keys(
        &self,
        _database: &str,
        _schema: &str,
        _table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        Ok(vec![])
    }

    async fn list_functions(&self, _database: &str, _schema: &str) -> Result<Vec<FunctionInfo>> {
        Ok(vec![])
    }

    async fn list_triggers(
        &self,
        _database: &str,
        _schema: &str,
        _table: &str,
    ) -> Result<Vec<TriggerInfo>> {
        Ok(vec![])
    }

    async fn get_table_stats(
        &self,
        database: &str,
        _schema: &str,
        table: &str,
    ) -> Result<TableStats> {
        Ok(TableStats {
            table_name: format!("{}.{}", database, table),
            row_count: -1,
            total_size: "N/A".to_string(),
            index_size: "N/A".to_string(),
            data_size: "N/A".to_string(),
            last_vacuum: None,
            last_analyze: None,
            dead_tuples: None,
            live_tuples: None,
        })
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

        let qualified = format!("{}.{}", quote_ident(database), quote_ident(table));
        let mut cql = format!("SELECT * FROM {}", qualified);

        if let Some(ref f) = filter {
            let f = f.trim();
            if !f.is_empty() {
                cql.push_str(&format!(" WHERE {}", f));
            }
        }

        if let Some(ref s) = sort {
            cql.push_str(&format!(
                " ORDER BY {} {}",
                quote_ident(&s.column),
                match s.direction {
                    SortDirection::Asc => "ASC",
                    SortDirection::Desc => "DESC",
                }
            ));
        }

        let fetch_limit = offset + limit;
        cql.push_str(&format!(" LIMIT {}", fetch_limit));

        let result = self
            .session
            .query_unpaged(cql.as_str(), &[])
            .await
            .map_err(|e| anyhow!("{}", e))?
            .into_rows_result()
            .map_err(|e| anyhow!("{}", e))?;

        let (_, all_rows) = rows_from_result(&result);

        let rows: Vec<Vec<serde_json::Value>> = all_rows
            .into_iter()
            .skip(offset as usize)
            .take(limit as usize)
            .collect();

        let total = rows.len() as i64 + offset as i64;

        Ok(TableData {
            columns,
            rows,
            total_rows: total,
            offset,
            limit,
        })
    }

    async fn execute_query(&self, _database: &str, sql: &str) -> Result<QueryResult> {
        let start = Instant::now();
        let trimmed = sql.trim().to_uppercase();

        let is_select = trimmed.starts_with("SELECT")
            || trimmed.starts_with("DESCRIBE")
            || trimmed.starts_with("LIST");

        if is_select {
            let result = self
                .session
                .query_unpaged(sql, &[])
                .await
                .map_err(|e| anyhow!("{}", e))?
                .into_rows_result()
                .map_err(|e| anyhow!("{}", e))?;

            let (columns, rows) = rows_from_result(&result);
            let elapsed = start.elapsed().as_millis() as u64;

            Ok(QueryResult {
                columns,
                rows_affected: rows.len() as u64,
                rows,
                execution_time_ms: elapsed,
                is_select: true,
            })
        } else {
            self.session
                .query_unpaged(sql, &[])
                .await
                .map_err(|e| anyhow!("{}", e))?;
            let elapsed = start.elapsed().as_millis() as u64;

            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: 0,
                execution_time_ms: elapsed,
                is_select: false,
            })
        }
    }

    async fn explain_query(&self, _database: &str, sql: &str) -> Result<ExplainResult> {
        let start = Instant::now();
        let raw_text = format!(
            "CQL does not support EXPLAIN. The query was: {}",
            sql
        );
        let elapsed = start.elapsed().as_millis() as u64;

        Ok(ExplainResult {
            plan: ExplainNode {
                node_type: "CQL Query".to_string(),
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
            raw_text,
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
        match object_type.to_uppercase().as_str() {
            "TABLE" => {
                let cols = self.list_columns(database, database, object_name).await?;
                let pk_cols: Vec<&str> = cols
                    .iter()
                    .filter(|c| c.is_primary_key)
                    .map(|c| c.name.as_str())
                    .collect();
                let col_defs: Vec<String> = cols
                    .iter()
                    .map(|c| format!("  {} {}", c.name, c.data_type))
                    .collect();

                let pk = if pk_cols.is_empty() {
                    String::new()
                } else {
                    format!(",\n  PRIMARY KEY ({})", pk_cols.join(", "))
                };

                Ok(format!(
                    "CREATE TABLE {}.{} (\n{}{}\n);",
                    database,
                    object_name,
                    col_defs.join(",\n"),
                    pk
                ))
            }
            _ => Err(anyhow!("DDL generation not supported for {}", object_type)),
        }
    }

    async fn apply_changes(&self, changes: &DataChanges) -> Result<()> {
        let ks = &changes.database;
        let tbl = &changes.table;
        let qualified = format!("{}.{}", quote_ident(ks), quote_ident(tbl));

        for update in &changes.updates {
            let pk_clause: Vec<String> = update
                .primary_key_values
                .iter()
                .map(|(col, val)| format!("{} = {}", quote_ident(col), json_to_cql_literal(val)))
                .collect();
            let cql = format!(
                "UPDATE {} SET {} = {} WHERE {}",
                qualified,
                quote_ident(&update.column_name),
                json_to_cql_literal(&update.new_value),
                pk_clause.join(" AND ")
            );
            self.session
                .query_unpaged(cql.as_str(), &[])
                .await
                .map_err(|e| anyhow!("{}", e))?;
        }

        for insert in &changes.inserts {
            let col_names: Vec<String> = insert.values.iter().map(|(c, _)| quote_ident(c)).collect();
            let vals: Vec<String> = insert.values.iter().map(|(_, v)| json_to_cql_literal(v)).collect();
            let cql = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                qualified,
                col_names.join(", "),
                vals.join(", ")
            );
            self.session
                .query_unpaged(cql.as_str(), &[])
                .await
                .map_err(|e| anyhow!("{}", e))?;
        }

        for delete in &changes.deletes {
            let pk_clause: Vec<String> = delete
                .primary_key_values
                .iter()
                .map(|(col, val)| format!("{} = {}", quote_ident(col), json_to_cql_literal(val)))
                .collect();
            let cql = format!("DELETE FROM {} WHERE {}", qualified, pk_clause.join(" AND "));
            self.session
                .query_unpaged(cql.as_str(), &[])
                .await
                .map_err(|e| anyhow!("{}", e))?;
        }

        Ok(())
    }

    async fn create_table(
        &self,
        database: &str,
        _schema: &str,
        table_name: &str,
        columns: &[ColumnDefinition],
    ) -> Result<()> {
        let col_defs: Vec<String> = columns
            .iter()
            .map(|c| format!("{} {}", quote_ident(&c.name), c.data_type))
            .collect();
        let pk_cols: Vec<String> = columns
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| quote_ident(&c.name))
            .collect();

        if pk_cols.is_empty() {
            return Err(anyhow!("Cassandra tables require at least one primary key column"));
        }

        let cql = format!(
            "CREATE TABLE {}.{} ({}, PRIMARY KEY ({}))",
            quote_ident(database),
            quote_ident(table_name),
            col_defs.join(", "),
            pk_cols.join(", ")
        );
        self.session
            .query_unpaged(cql.as_str(), &[])
            .await
            .map_err(|e| anyhow!("{}", e))?;
        Ok(())
    }

    async fn alter_table(
        &self,
        database: &str,
        _schema: &str,
        table_name: &str,
        operations: &[AlterTableOperation],
    ) -> Result<()> {
        let qualified = format!("{}.{}", quote_ident(database), quote_ident(table_name));
        for op in operations {
            let cql = match op {
                AlterTableOperation::AddColumn { column } => {
                    format!(
                        "ALTER TABLE {} ADD {} {}",
                        qualified,
                        quote_ident(&column.name),
                        column.data_type
                    )
                }
                AlterTableOperation::DropColumn { column_name } => {
                    format!("ALTER TABLE {} DROP {}", qualified, quote_ident(column_name))
                }
                AlterTableOperation::RenameColumn { old_name, new_name } => {
                    format!(
                        "ALTER TABLE {} RENAME {} TO {}",
                        qualified,
                        quote_ident(old_name),
                        quote_ident(new_name)
                    )
                }
                AlterTableOperation::ChangeColumnType {
                    column_name,
                    new_type,
                } => {
                    format!(
                        "ALTER TABLE {} ALTER {} TYPE {}",
                        qualified,
                        quote_ident(column_name),
                        new_type
                    )
                }
                AlterTableOperation::RenameTable { new_name } => {
                    return Err(anyhow!(
                        "Cassandra does not support renaming tables (attempted rename to '{}')",
                        new_name
                    ));
                }
                AlterTableOperation::SetNullable { .. }
                | AlterTableOperation::SetDefault { .. } => {
                    continue;
                }
            };
            self.session
                .query_unpaged(cql.as_str(), &[])
                .await
                .map_err(|e| anyhow!("{}", e))?;
        }
        Ok(())
    }

    async fn drop_object(
        &self,
        database: &str,
        _schema: &str,
        object_name: &str,
        object_type: &str,
    ) -> Result<()> {
        let cql = match object_type.to_uppercase().as_str() {
            "TABLE" => format!(
                "DROP TABLE {}.{}",
                quote_ident(database),
                quote_ident(object_name)
            ),
            "INDEX" => format!(
                "DROP INDEX {}.{}",
                quote_ident(database),
                quote_ident(object_name)
            ),
            _ => return Err(anyhow!("Cannot drop object of type '{}'", object_type)),
        };
        self.session
            .query_unpaged(cql.as_str(), &[])
            .await
            .map_err(|e| anyhow!("{}", e))?;
        Ok(())
    }

    async fn truncate_table(&self, database: &str, _schema: &str, table_name: &str) -> Result<()> {
        let cql = format!(
            "TRUNCATE {}.{}",
            quote_ident(database),
            quote_ident(table_name)
        );
        self.session
            .query_unpaged(cql.as_str(), &[])
            .await
            .map_err(|e| anyhow!("{}", e))?;
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
        let qualified = format!("{}.{}", quote_ident(database), quote_ident(table));
        let col_list: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
        let col_str = col_list.join(", ");
        let mut count = 0u64;

        for row in rows {
            let vals: Vec<String> = row.iter().map(json_to_cql_literal).collect();
            let cql = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                qualified,
                col_str,
                vals.join(", ")
            );
            self.session
                .query_unpaged(cql.as_str(), &[])
                .await
                .map_err(|e| anyhow!("{}", e))?;
            count += 1;
        }
        Ok(count)
    }

    async fn get_server_activity(&self) -> Result<Vec<ServerActivity>> {
        Ok(vec![])
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
            message: Some("Query statistics are not available for Cassandra/ScyllaDB".to_string()),
            entries: vec![],
        })
    }

    async fn cancel_query(&self, _pid: &str) -> Result<()> {
        Err(anyhow!("Query cancellation is not supported for Cassandra/ScyllaDB"))
    }

    async fn list_roles(&self) -> Result<Vec<RoleInfo>> {
        let result = self
            .session
            .query_unpaged("SELECT role, is_superuser, can_login FROM system_auth.roles", &[])
            .await;

        match result {
            Ok(qr) => {
                let qr = qr.into_rows_result().map_err(|e| anyhow!("{}", e))?;
                let mut roles = Vec::new();
                if let Ok(rows) = qr.rows::<(String, bool, bool)>() {
                    for row in rows {
                        if let Ok((name, is_superuser, can_login)) = row {
                            roles.push(RoleInfo {
                                name,
                                is_superuser,
                                can_login,
                                can_create_db: false,
                                can_create_role: is_superuser,
                                is_replication: false,
                                connection_limit: -1,
                                valid_until: None,
                                member_of: vec![],
                            });
                        }
                    }
                }
                Ok(roles)
            }
            Err(_) => Ok(vec![]),
        }
    }

    async fn create_role(&self, req: &CreateRoleRequest) -> Result<()> {
        let mut cql = format!("CREATE ROLE '{}'", req.name.replace('\'', "''"));
        let mut with_parts = Vec::new();

        if let Some(ref pw) = req.password {
            with_parts.push(format!("PASSWORD = '{}'", pw.replace('\'', "''")));
        }
        with_parts.push(format!("LOGIN = {}", req.can_login));
        with_parts.push(format!("SUPERUSER = {}", req.is_superuser));

        if !with_parts.is_empty() {
            cql.push_str(&format!(" WITH {}", with_parts.join(" AND ")));
        }

        self.session
            .query_unpaged(cql.as_str(), &[])
            .await
            .map_err(|e| anyhow!("{}", e))?;
        Ok(())
    }

    async fn drop_role(&self, name: &str) -> Result<()> {
        let cql = format!("DROP ROLE '{}'", name.replace('\'', "''"));
        self.session
            .query_unpaged(cql.as_str(), &[])
            .await
            .map_err(|e| anyhow!("{}", e))?;
        Ok(())
    }

    async fn alter_role(&self, req: &AlterRoleRequest) -> Result<()> {
        let mut with_parts = Vec::new();

        if let Some(ref pw) = req.password {
            with_parts.push(format!("PASSWORD = '{}'", pw.replace('\'', "''")));
        }
        if let Some(login) = req.can_login {
            with_parts.push(format!("LOGIN = {}", login));
        }
        if let Some(su) = req.is_superuser {
            with_parts.push(format!("SUPERUSER = {}", su));
        }

        if with_parts.is_empty() {
            return Ok(());
        }

        let cql = format!(
            "ALTER ROLE '{}' WITH {}",
            req.name.replace('\'', "''"),
            with_parts.join(" AND ")
        );
        self.session
            .query_unpaged(cql.as_str(), &[])
            .await
            .map_err(|e| anyhow!("{}", e))?;
        Ok(())
    }

    async fn test_connection(&self) -> Result<bool> {
        self.session
            .query_unpaged("SELECT now() FROM system.local", &[])
            .await
            .map_err(|e| anyhow!("{}", e))?;
        Ok(true)
    }
}
