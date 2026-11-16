pub mod cancel;
pub mod cassandra;
pub mod cockroachdb;
pub mod console;
pub mod db_pools;
pub mod mariadb;
pub mod mssql;
pub mod mysql;
pub mod mysql_common;
pub mod pg_common;
pub mod pool;
pub mod postgres;
pub mod sqlite;
pub mod ssh_tunnel;
pub mod stream;
pub mod tidb;

use crate::models::*;
use anyhow::Result;
use async_trait::async_trait;
use futures_util::{StreamExt, TryStreamExt};
use std::collections::HashMap;

#[async_trait]
#[allow(clippy::too_many_arguments)]
pub trait DatabaseDriver: Send + Sync {
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>>;
    async fn list_schemas(&self, database: &str) -> Result<Vec<SchemaInfo>>;
    async fn list_tables(&self, database: &str, schema: &str) -> Result<Vec<TableInfo>>;
    async fn list_columns(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ColumnInfo>>;
    async fn list_indexes(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<IndexInfo>>;
    async fn list_foreign_keys(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>>;
    /// Returns the FKs from other tables that point at this one. Default
    /// returns an empty Vec so backends without a meaningful FK catalog
    /// (or those that haven't implemented it yet) opt in incrementally.
    async fn list_referenced_by(
        &self,
        _database: &str,
        _schema: &str,
        _table: &str,
    ) -> Result<Vec<ReferencingTableInfo>> {
        Ok(Vec::new())
    }
    /// Every base table of `schema` with its columns and foreign keys,
    /// for the ERD. Each entry's `columns` / `foreign_keys` equal what
    /// [`list_columns`](Self::list_columns) /
    /// [`list_foreign_keys`](Self::list_foreign_keys) return for that
    /// table, in [`list_tables`](Self::list_tables) order.
    ///
    /// The default asks per table, a bounded number at a time; drivers
    /// with a catalog that can answer for a whole schema in one query
    /// override it.
    async fn get_schema_graph(&self, database: &str, schema: &str) -> Result<Vec<TableGraph>> {
        let tables = self.list_tables(database, schema).await?;
        schema_graph_per_table(self, database, schema, tables).await
    }
    async fn list_functions(&self, database: &str, schema: &str) -> Result<Vec<FunctionInfo>>;
    async fn list_triggers(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<TriggerInfo>>;
    async fn get_table_stats(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<TableStats>;
    /// Fetch a window of rows from `table`, ordered by the given
    /// list of `sort` specs (empty list = driver's default
    /// ordering, typically the primary-key columns).
    ///
    /// `sort` is a `Vec` rather than `Option<SortSpec>` so the
    /// data grid's Shift+click multi-column sort actually reaches
    /// the database — see issue #57. Drivers compose the SQL
    /// `ORDER BY` clause by walking the vec in order and
    /// joining with `, `; if the engine can't honour every entry
    /// (e.g. Cassandra's clustering-key constraint), it falls
    /// back to whatever it can and the server surfaces the
    /// limitation as an execution error.
    async fn fetch_rows(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        offset: u64,
        limit: u64,
        sort: Vec<SortSpec>,
        filter: Option<String>,
    ) -> Result<TableData>;
    /// One page of `table` for an export. Like [`fetch_rows`](Self::fetch_rows),
    /// but `columns` (the table's [`list_columns`](Self::list_columns),
    /// read once by the caller) is not read again for every page, and the
    /// matching rows are not counted: `total_rows` is `-1`. The default
    /// falls back to `fetch_rows`.
    async fn fetch_export_page(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        columns: &[ColumnInfo],
        offset: u64,
        limit: u64,
        sort: Vec<SortSpec>,
        filter: Option<String>,
    ) -> Result<TableData> {
        let _ = columns;
        self.fetch_rows(database, schema, table, offset, limit, sort, filter)
            .await
    }
    async fn execute_query(&self, database: &str, sql: &str) -> Result<QueryResult>;
    /// Run a single console statement again and push every row it
    /// returns to `sink`, without the console's row cap, so a result the
    /// console truncated can be exported in full. The statement runs
    /// read-only where the engine allows it (it is being run a second
    /// time), and scripts of several statements are rejected. Rows are
    /// decoded exactly as the console decodes them.
    async fn stream_query(
        &self,
        database: &str,
        sql: &str,
        sink: &mut stream::RowSink,
    ) -> Result<()> {
        let _ = (database, sql, sink);
        anyhow::bail!("Exporting every row of a query isn't supported for this database.")
    }
    /// Like [`execute_query`](Self::execute_query), but stops early when
    /// `cancel` fires and then resolves to
    /// [`cancel::QueryCancelled`]. The default drops the in-flight
    /// future, which is only safe for drivers whose connections survive
    /// that; drivers with a server-side cancel (or connection state that
    /// must be repaired) override it. See `db::cancel`.
    async fn execute_query_cancellable(
        &self,
        database: &str,
        sql: &str,
        cancel: cancel::CancellationToken,
    ) -> Result<QueryResult> {
        cancel::run_abortable(self.execute_query(database, sql), &cancel).await
    }
    async fn explain_query(&self, database: &str, sql: &str) -> Result<ExplainResult>;
    async fn validate_query(&self, database: &str, sql: &str) -> Result<Option<ValidationError>>;
    async fn get_ddl(
        &self,
        database: &str,
        schema: &str,
        object_name: &str,
        object_type: &str,
    ) -> Result<String>;
    async fn apply_changes(&self, changes: &DataChanges) -> Result<()>;
    async fn create_table(
        &self,
        database: &str,
        schema: &str,
        table_name: &str,
        columns: &[ColumnDefinition],
    ) -> Result<()>;
    async fn alter_table(
        &self,
        database: &str,
        schema: &str,
        table_name: &str,
        operations: &[AlterTableOperation],
    ) -> Result<()>;
    async fn drop_object(
        &self,
        database: &str,
        schema: &str,
        object_name: &str,
        object_type: &str,
    ) -> Result<()>;
    async fn truncate_table(&self, database: &str, schema: &str, table_name: &str) -> Result<()>;
    async fn import_data(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        columns: &[String],
        rows: &[Vec<serde_json::Value>],
    ) -> Result<u64>;
    async fn get_server_activity(&self) -> Result<Vec<ServerActivity>>;
    async fn get_database_stats(&self) -> Result<DatabaseStats>;
    async fn get_locks(&self) -> Result<Vec<LockInfo>>;
    async fn get_server_config(&self) -> Result<Vec<ServerConfigEntry>>;
    async fn get_query_stats(&self) -> Result<QueryStatsResponse>;
    async fn cancel_query(&self, pid: &str) -> Result<()>;
    async fn list_roles(&self) -> Result<Vec<RoleInfo>>;
    async fn create_role(&self, req: &CreateRoleRequest) -> Result<()>;
    async fn drop_role(&self, name: &str) -> Result<()>;
    async fn alter_role(&self, req: &AlterRoleRequest) -> Result<()>;
    async fn test_connection(&self) -> Result<bool>;

    /// Close the driver's connections and wait until they are released.
    /// Used before an operation that must not race with open handles
    /// (replacing a SQLite database file). Drivers without such a need
    /// rely on `Drop`.
    async fn close(&self) {}
}

/// How many tables [`schema_graph_per_table`] loads at once. Each one
/// issues two catalog queries, so this keeps a pool of a few connections
/// busy without queueing thousands of requests behind it (which ran into
/// the pool's acquire timeout on large schemas).
pub const SCHEMA_GRAPH_CONCURRENCY: usize = 8;

/// The tables the ERD draws: base tables, not views or foreign tables.
pub fn is_schema_graph_table(t: &TableInfo) -> bool {
    t.table_type == "BASE TABLE" || t.table_type == "TABLE"
}

/// [`DatabaseDriver::get_schema_graph`] from per-table
/// `list_columns` / `list_foreign_keys` calls, at most
/// [`SCHEMA_GRAPH_CONCURRENCY`] tables at a time, keeping table order.
pub async fn schema_graph_per_table<D: DatabaseDriver + ?Sized>(
    driver: &D,
    database: &str,
    schema: &str,
    tables: Vec<TableInfo>,
) -> Result<Vec<TableGraph>> {
    futures_util::stream::iter(tables.into_iter().filter(is_schema_graph_table))
        .map(|table| async move {
            let (columns, foreign_keys) = tokio::try_join!(
                driver.list_columns(database, schema, &table.name),
                driver.list_foreign_keys(database, schema, &table.name),
            )?;
            Ok::<_, anyhow::Error>(TableGraph {
                table,
                columns,
                foreign_keys,
            })
        })
        .buffered(SCHEMA_GRAPH_CONCURRENCY)
        .try_collect()
        .await
}

/// [`DatabaseDriver::get_schema_graph`] from schema-wide column and
/// foreign-key listings keyed by table name. Tables missing from a map
/// get an empty list, as a per-table query would return.
pub fn assemble_schema_graph(
    tables: Vec<TableInfo>,
    mut columns: HashMap<String, Vec<ColumnInfo>>,
    mut foreign_keys: HashMap<String, Vec<ForeignKeyInfo>>,
) -> Vec<TableGraph> {
    tables
        .into_iter()
        .filter(is_schema_graph_table)
        .map(|table| TableGraph {
            columns: columns.remove(&table.name).unwrap_or_default(),
            foreign_keys: foreign_keys.remove(&table.name).unwrap_or_default(),
            table,
        })
        .collect()
}

/// Format a host for the authority part of a connection URL.
///
/// IPv6 literals must be wrapped in brackets (`[::1]`) or the URL
/// parser reads the first `:` as the port separator. A zone id
/// (`fe80::1%eth0`) has its `%` percent-encoded as RFC 6874 requires.
/// Hostnames, IPv4 literals and already-bracketed hosts pass through.
pub fn url_host(host: &str) -> String {
    let h = host.trim();
    if h.starts_with('[') || !h.contains(':') {
        return h.to_string();
    }
    format!("[{}]", h.replace('%', "%25"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(name: &str, table_type: &str) -> TableInfo {
        TableInfo {
            name: name.into(),
            schema: "s".into(),
            table_type: table_type.into(),
            ..Default::default()
        }
    }

    fn column(name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.into(),
            data_type: "int".into(),
            is_nullable: true,
            is_primary_key: false,
            default_value: None,
            ordinal_position: 1,
            is_auto_generated: false,
        }
    }

    #[test]
    fn assemble_schema_graph_keeps_base_tables_in_order() {
        let tables = vec![
            table("b", "BASE TABLE"),
            table("v", "VIEW"),
            table("a", "TABLE"),
            table("empty", "BASE TABLE"),
        ];
        let columns = HashMap::from([
            ("a".to_string(), vec![column("x")]),
            ("b".to_string(), vec![column("y"), column("z")]),
            ("v".to_string(), vec![column("x")]),
        ]);
        let fks = HashMap::from([(
            "b".to_string(),
            vec![ForeignKeyInfo {
                name: "fk".into(),
                column: "y".into(),
                referenced_table: "a".into(),
                referenced_column: "x".into(),
                on_delete: "NO ACTION".into(),
                on_update: "NO ACTION".into(),
            }],
        )]);
        let graph = assemble_schema_graph(tables, columns, fks);
        let names: Vec<&str> = graph.iter().map(|g| g.table.name.as_str()).collect();
        assert_eq!(names, ["b", "a", "empty"]);
        assert_eq!(graph[0].columns.len(), 2);
        assert_eq!(graph[0].foreign_keys.len(), 1);
        assert_eq!(graph[1].columns, vec![column("x")]);
        assert!(graph[1].foreign_keys.is_empty());
        assert!(graph[2].columns.is_empty());
    }

    #[test]
    fn url_host_brackets_ipv6_only() {
        assert_eq!(url_host("localhost"), "localhost");
        assert_eq!(url_host("10.0.0.1"), "10.0.0.1");
        assert_eq!(url_host("::1"), "[::1]");
        assert_eq!(url_host("2001:db8::5"), "[2001:db8::5]");
        assert_eq!(url_host("[::1]"), "[::1]");
        assert_eq!(url_host("fe80::1%eth0"), "[fe80::1%25eth0]");
    }
}
