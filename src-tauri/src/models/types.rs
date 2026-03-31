use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DbType {
    Postgres,
    Mysql,
    Sqlite,
    Mariadb,
    Cockroachdb,
    Tidb,
    Cassandra,
    Mssql,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    pub id: String,
    pub name: String,
    pub db_type: DbType,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub color: String,
    pub ssl: bool,
    #[serde(default)]
    pub trust_server_cert: bool,
    #[serde(default)]
    pub group: Option<String>,
    #[serde(default)]
    pub ssh_enabled: bool,
    #[serde(default)]
    pub ssh_host: String,
    #[serde(default = "default_ssh_port")]
    pub ssh_port: u16,
    #[serde(default)]
    pub ssh_user: String,
    #[serde(default)]
    pub ssh_password: String,
    #[serde(default)]
    pub ssh_key_path: String,
}

fn default_ssh_port() -> u16 {
    22
}

impl ConnectionConfig {
    pub fn new_id() -> String {
        Uuid::new_v4().to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub name: String,
    pub schema: String,
    pub table_type: String,
    pub row_count_estimate: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub is_primary_key: bool,
    pub default_value: Option<String>,
    pub ordinal_position: i32,
    pub is_auto_generated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableData {
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub total_rows: i64,
    pub offset: u64,
    pub limit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub rows_affected: u64,
    pub execution_time_ms: u64,
    pub is_select: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortSpec {
    pub column: String,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellChange {
    pub row_index: usize,
    pub column_name: String,
    pub old_value: serde_json::Value,
    pub new_value: serde_json::Value,
    pub primary_key_values: Vec<(String, serde_json::Value)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewRow {
    pub values: Vec<(String, serde_json::Value)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRow {
    pub primary_key_values: Vec<(String, serde_json::Value)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataChanges {
    pub connection_id: String,
    pub database: String,
    pub schema: String,
    pub table: String,
    pub updates: Vec<CellChange>,
    pub inserts: Vec<NewRow>,
    pub deletes: Vec<DeleteRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaInfo {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseInfo {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchRowsRequest {
    pub connection_id: String,
    pub database: String,
    pub schema: String,
    pub table: String,
    pub offset: u64,
    pub limit: u64,
    pub sort: Option<SortSpec>,
    pub filter: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteQueryRequest {
    pub connection_id: String,
    pub database: String,
    pub sql: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationError {
    pub message: String,
    pub position: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainNode {
    pub node_type: String,
    pub relation: Option<String>,
    pub startup_cost: f64,
    pub total_cost: f64,
    pub actual_time_ms: Option<f64>,
    pub rows_estimated: u64,
    pub rows_actual: Option<u64>,
    pub width: u64,
    pub filter: Option<String>,
    pub children: Vec<ExplainNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainResult {
    pub plan: ExplainNode,
    pub raw_text: String,
    pub execution_time_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetDdlRequest {
    pub connection_id: String,
    pub database: String,
    pub schema: String,
    pub object_name: String,
    pub object_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub is_primary_key: bool,
    pub default_value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTableRequest {
    pub connection_id: String,
    pub database: String,
    pub schema: String,
    pub table_name: String,
    pub columns: Vec<ColumnDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub is_unique: bool,
    pub index_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyInfo {
    pub name: String,
    pub column: String,
    pub referenced_table: String,
    pub referenced_column: String,
    pub on_delete: String,
    pub on_update: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerActivity {
    pub pid: String,
    pub user: String,
    pub database: String,
    pub state: String,
    pub query: String,
    pub duration_ms: Option<f64>,
    pub client_addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseStats {
    pub active_connections: i64,
    pub idle_connections: i64,
    pub idle_in_transaction: i64,
    pub total_connections: i64,
    pub xact_commit: i64,
    pub xact_rollback: i64,
    pub tup_inserted: i64,
    pub tup_updated: i64,
    pub tup_deleted: i64,
    pub tup_fetched: i64,
    pub blks_read: i64,
    pub blks_hit: i64,
    pub timestamp_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockInfo {
    pub pid: i32,
    pub locktype: String,
    pub database: String,
    pub relation: String,
    pub mode: String,
    pub granted: bool,
    pub query: String,
    pub user: String,
    pub state: String,
    pub duration_ms: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfigEntry {
    pub name: String,
    pub setting: String,
    pub unit: Option<String>,
    pub category: String,
    pub description: String,
    pub context: String,
    pub source: String,
    pub pending_restart: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportRequest {
    pub connection_id: String,
    pub database: String,
    pub schema: String,
    pub table: String,
    pub format: String,
    pub filter: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportResultRequest {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub format: String,
    pub table_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropObjectRequest {
    pub connection_id: String,
    pub database: String,
    pub schema: String,
    pub object_name: String,
    pub object_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TruncateTableRequest {
    pub connection_id: String,
    pub database: String,
    pub schema: String,
    pub table_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerActivityRequest {
    pub connection_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelQueryRequest {
    pub connection_id: String,
    pub pid: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStatsRequest {
    pub connection_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStatEntry {
    pub query: String,
    pub queryid: Option<i64>,
    pub user: String,
    pub calls: i64,
    pub total_exec_time_ms: f64,
    pub mean_exec_time_ms: f64,
    pub min_exec_time_ms: f64,
    pub max_exec_time_ms: f64,
    pub rows: i64,
    pub shared_blks_hit: i64,
    pub shared_blks_read: i64,
    pub cache_hit_ratio: f64,
    pub total_plan_time_ms: Option<f64>,
    pub mean_plan_time_ms: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStatsResponse {
    pub available: bool,
    pub message: Option<String>,
    pub entries: Vec<QueryStatEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum AlterTableOperation {
    #[serde(rename = "add_column")]
    AddColumn { column: ColumnDefinition },
    #[serde(rename = "drop_column")]
    DropColumn { column_name: String },
    #[serde(rename = "rename_column")]
    RenameColumn { old_name: String, new_name: String },
    #[serde(rename = "change_type")]
    ChangeColumnType {
        column_name: String,
        new_type: String,
    },
    #[serde(rename = "set_nullable")]
    SetNullable { column_name: String, nullable: bool },
    #[serde(rename = "set_default")]
    SetDefault {
        column_name: String,
        default_value: Option<String>,
    },
    #[serde(rename = "rename_table")]
    RenameTable { new_name: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlterTableRequest {
    pub connection_id: String,
    pub database: String,
    pub schema: String,
    pub table_name: String,
    pub operations: Vec<AlterTableOperation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionInfo {
    pub name: String,
    pub schema: String,
    pub return_type: String,
    pub language: String,
    pub kind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerInfo {
    pub name: String,
    pub table_name: String,
    pub event: String,
    pub timing: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStats {
    pub table_name: String,
    pub row_count: i64,
    pub total_size: String,
    pub index_size: String,
    pub data_size: String,
    pub last_vacuum: Option<String>,
    pub last_analyze: Option<String>,
    pub dead_tuples: Option<i64>,
    pub live_tuples: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportDataRequest {
    pub connection_id: String,
    pub database: String,
    pub schema: String,
    pub table: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedQuery {
    pub id: String,
    pub name: String,
    pub sql: String,
    pub connection_id: Option<String>,
    pub database: Option<String>,
    pub created_at: u64,
    pub updated_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleInfo {
    pub name: String,
    pub is_superuser: bool,
    pub can_login: bool,
    pub can_create_db: bool,
    pub can_create_role: bool,
    pub is_replication: bool,
    pub connection_limit: i32,
    pub valid_until: Option<String>,
    pub member_of: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateRoleRequest {
    pub connection_id: String,
    pub name: String,
    pub password: Option<String>,
    pub is_superuser: bool,
    pub can_login: bool,
    pub can_create_db: bool,
    pub can_create_role: bool,
    pub connection_limit: i32,
    pub valid_until: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropRoleRequest {
    pub connection_id: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlterRoleRequest {
    pub connection_id: String,
    pub name: String,
    pub password: Option<String>,
    pub is_superuser: Option<bool>,
    pub can_login: Option<bool>,
    pub can_create_db: Option<bool>,
    pub can_create_role: Option<bool>,
    pub connection_limit: Option<i32>,
    pub valid_until: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupRequest {
    pub connection_id: String,
    pub database: String,
    pub output_path: String,
    pub format: String,
    pub schema_only: bool,
    pub data_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreRequest {
    pub connection_id: String,
    pub database: String,
    pub input_path: String,
    pub format: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DumpRestoreRequest {
    pub source_connection_id: String,
    pub source_database: String,
    pub target_connection_id: String,
    pub target_database: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn db_type_serializes_lowercase() {
        assert_eq!(
            serde_json::to_string(&DbType::Postgres).unwrap(),
            r#""postgres""#
        );
        assert_eq!(serde_json::to_string(&DbType::Mysql).unwrap(), r#""mysql""#);
        assert_eq!(
            serde_json::to_string(&DbType::Sqlite).unwrap(),
            r#""sqlite""#
        );
        assert_eq!(
            serde_json::to_string(&DbType::Mariadb).unwrap(),
            r#""mariadb""#
        );
        assert_eq!(
            serde_json::to_string(&DbType::Cockroachdb).unwrap(),
            r#""cockroachdb""#
        );
        assert_eq!(serde_json::to_string(&DbType::Tidb).unwrap(), r#""tidb""#);
        assert_eq!(
            serde_json::to_string(&DbType::Cassandra).unwrap(),
            r#""cassandra""#
        );
        assert_eq!(serde_json::to_string(&DbType::Mssql).unwrap(), r#""mssql""#);
    }

    #[test]
    fn db_type_deserializes_lowercase() {
        let p: DbType = serde_json::from_str(r#""postgres""#).unwrap();
        matches!(p, DbType::Postgres);
        let m: DbType = serde_json::from_str(r#""mysql""#).unwrap();
        matches!(m, DbType::Mysql);
        let mb: DbType = serde_json::from_str(r#""mariadb""#).unwrap();
        matches!(mb, DbType::Mariadb);
        let cr: DbType = serde_json::from_str(r#""cockroachdb""#).unwrap();
        matches!(cr, DbType::Cockroachdb);
        let ti: DbType = serde_json::from_str(r#""tidb""#).unwrap();
        matches!(ti, DbType::Tidb);
        let ca: DbType = serde_json::from_str(r#""cassandra""#).unwrap();
        matches!(ca, DbType::Cassandra);
        let ms: DbType = serde_json::from_str(r#""mssql""#).unwrap();
        matches!(ms, DbType::Mssql);
    }

    #[test]
    fn db_type_rejects_unknown() {
        let r: Result<DbType, _> = serde_json::from_str(r#""oracle""#);
        assert!(r.is_err());
    }

    #[test]
    fn sort_direction_serializes() {
        assert_eq!(
            serde_json::to_string(&SortDirection::Asc).unwrap(),
            r#""asc""#
        );
        assert_eq!(
            serde_json::to_string(&SortDirection::Desc).unwrap(),
            r#""desc""#
        );
    }

    #[test]
    fn connection_config_defaults() {
        let json = r##"{
            "id": "1", "name": "test", "db_type": "postgres",
            "host": "localhost", "port": 5432, "user": "u",
            "password": "p", "database": "db", "color": "#fff",
            "ssl": false
        }"##;
        let config: ConnectionConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.ssh_port, 22);
        assert!(!config.ssh_enabled);
        assert_eq!(config.ssh_host, "");
        assert!(config.group.is_none());
    }

    #[test]
    fn connection_config_with_ssh() {
        let json = r##"{
            "id": "1", "name": "test", "db_type": "postgres",
            "host": "localhost", "port": 5432, "user": "u",
            "password": "p", "database": "db", "color": "#fff",
            "ssl": false, "ssh_enabled": true, "ssh_host": "bastion",
            "ssh_port": 2222, "ssh_user": "admin"
        }"##;
        let config: ConnectionConfig = serde_json::from_str(json).unwrap();
        assert!(config.ssh_enabled);
        assert_eq!(config.ssh_host, "bastion");
        assert_eq!(config.ssh_port, 2222);
        assert_eq!(config.ssh_user, "admin");
    }

    #[test]
    fn connection_config_new_id_is_uuid() {
        let id = ConnectionConfig::new_id();
        assert_eq!(id.len(), 36);
        assert!(id.contains('-'));
    }

    #[test]
    fn alter_table_op_tagged_enum() {
        let op = AlterTableOperation::AddColumn {
            column: ColumnDefinition {
                name: "col".into(),
                data_type: "text".into(),
                is_nullable: true,
                is_primary_key: false,
                default_value: None,
            },
        };
        let json = serde_json::to_string(&op).unwrap();
        assert!(json.contains(r#""op":"add_column""#));
    }

    #[test]
    fn alter_table_op_rename() {
        let json = r#"{"op": "rename_column", "old_name": "a", "new_name": "b"}"#;
        let op: AlterTableOperation = serde_json::from_str(json).unwrap();
        matches!(op, AlterTableOperation::RenameColumn { .. });
    }

    #[test]
    fn alter_table_op_set_default_null() {
        let json = r#"{"op": "set_default", "column_name": "c", "default_value": null}"#;
        let op: AlterTableOperation = serde_json::from_str(json).unwrap();
        if let AlterTableOperation::SetDefault { default_value, .. } = op {
            assert!(default_value.is_none());
        } else {
            panic!("wrong variant");
        }
    }

    #[test]
    fn query_stats_response_unavailable() {
        let json = r#"{"available": false, "message": "Extension not installed", "entries": []}"#;
        let resp: QueryStatsResponse = serde_json::from_str(json).unwrap();
        assert!(!resp.available);
        assert_eq!(resp.message.unwrap(), "Extension not installed");
        assert!(resp.entries.is_empty());
    }

    #[test]
    fn database_stats_round_trip() {
        let stats = DatabaseStats {
            active_connections: 5,
            idle_connections: 10,
            idle_in_transaction: 1,
            total_connections: 16,
            xact_commit: 1000,
            xact_rollback: 5,
            tup_inserted: 100,
            tup_updated: 50,
            tup_deleted: 10,
            tup_fetched: 5000,
            blks_read: 200,
            blks_hit: 9800,
            timestamp_ms: 1234567890.0,
        };
        let json = serde_json::to_string(&stats).unwrap();
        let back: DatabaseStats = serde_json::from_str(&json).unwrap();
        assert_eq!(back.active_connections, 5);
        assert_eq!(back.timestamp_ms, 1234567890.0);
    }

    #[test]
    fn saved_query_round_trip() {
        let q = SavedQuery {
            id: "q1".into(),
            name: "My Query".into(),
            sql: "SELECT 1".into(),
            connection_id: Some("conn1".into()),
            database: Some("mydb".into()),
            created_at: 100,
            updated_at: 200,
        };
        let json = serde_json::to_string(&q).unwrap();
        let back: SavedQuery = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, "q1");
        assert_eq!(back.connection_id, Some("conn1".into()));
    }

    #[test]
    fn saved_query_optional_fields() {
        let json = r#"{"id":"1","name":"q","sql":"SELECT 1","connection_id":null,"database":null,"created_at":0,"updated_at":0}"#;
        let q: SavedQuery = serde_json::from_str(json).unwrap();
        assert!(q.connection_id.is_none());
        assert!(q.database.is_none());
    }

    #[test]
    fn lock_info_round_trip() {
        let lock = LockInfo {
            pid: 123,
            locktype: "relation".into(),
            database: "mydb".into(),
            relation: "users".into(),
            mode: "AccessShareLock".into(),
            granted: true,
            query: "SELECT * FROM users".into(),
            user: "admin".into(),
            state: "active".into(),
            duration_ms: Some(150.5),
        };
        let json = serde_json::to_string(&lock).unwrap();
        let back: LockInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(back.pid, 123);
        assert!(back.granted);
        assert_eq!(back.duration_ms, Some(150.5));
    }

    #[test]
    fn server_config_entry_round_trip() {
        let entry = ServerConfigEntry {
            name: "max_connections".into(),
            setting: "100".into(),
            unit: Some("connections".into()),
            category: "Resource Usage".into(),
            description: "Max connections".into(),
            context: "postmaster".into(),
            source: "configuration file".into(),
            pending_restart: false,
        };
        let json = serde_json::to_string(&entry).unwrap();
        let back: ServerConfigEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back.name, "max_connections");
        assert!(!back.pending_restart);
    }

    #[test]
    fn column_info_with_auto_generated() {
        let col = ColumnInfo {
            name: "id".into(),
            data_type: "integer".into(),
            is_nullable: false,
            is_primary_key: true,
            default_value: Some("nextval('seq')".into()),
            ordinal_position: 1,
            is_auto_generated: true,
        };
        let json = serde_json::to_string(&col).unwrap();
        let back: ColumnInfo = serde_json::from_str(&json).unwrap();
        assert!(back.is_auto_generated);
        assert!(back.is_primary_key);
        assert_eq!(back.ordinal_position, 1);
    }

    #[test]
    fn column_info_not_auto_generated() {
        let json = r#"{"name":"email","data_type":"varchar","is_nullable":true,"is_primary_key":false,"default_value":null,"ordinal_position":3,"is_auto_generated":false}"#;
        let col: ColumnInfo = serde_json::from_str(json).unwrap();
        assert!(!col.is_auto_generated);
        assert!(col.is_nullable);
        assert!(col.default_value.is_none());
    }

    #[test]
    fn query_stats_request_round_trip() {
        let req = QueryStatsRequest {
            connection_id: "conn-123".into(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let back: QueryStatsRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.connection_id, "conn-123");
    }

    #[test]
    fn query_stat_entry_round_trip() {
        let entry = QueryStatEntry {
            query: "SELECT * FROM users".into(),
            queryid: Some(12345),
            user: "admin".into(),
            calls: 100,
            total_exec_time_ms: 5000.0,
            mean_exec_time_ms: 50.0,
            min_exec_time_ms: 1.0,
            max_exec_time_ms: 500.0,
            rows: 10000,
            shared_blks_hit: 900,
            shared_blks_read: 100,
            cache_hit_ratio: 0.9,
            total_plan_time_ms: Some(200.0),
            mean_plan_time_ms: Some(2.0),
        };
        let json = serde_json::to_string(&entry).unwrap();
        let back: QueryStatEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back.query, "SELECT * FROM users");
        assert_eq!(back.queryid, Some(12345));
        assert_eq!(back.calls, 100);
        assert_eq!(back.total_plan_time_ms, Some(200.0));
        assert_eq!(back.mean_plan_time_ms, Some(2.0));
        assert_eq!(back.cache_hit_ratio, 0.9);
    }

    #[test]
    fn query_stat_entry_optional_plan_times() {
        let json = r#"{
            "query": "SELECT 1", "queryid": null, "user": "u",
            "calls": 1, "total_exec_time_ms": 0.1, "mean_exec_time_ms": 0.1,
            "min_exec_time_ms": 0.1, "max_exec_time_ms": 0.1, "rows": 1,
            "shared_blks_hit": 0, "shared_blks_read": 0, "cache_hit_ratio": 0.0,
            "total_plan_time_ms": null, "mean_plan_time_ms": null
        }"#;
        let entry: QueryStatEntry = serde_json::from_str(json).unwrap();
        assert!(entry.queryid.is_none());
        assert!(entry.total_plan_time_ms.is_none());
        assert!(entry.mean_plan_time_ms.is_none());
    }

    #[test]
    fn query_stats_response_with_entries() {
        let resp = QueryStatsResponse {
            available: true,
            message: None,
            entries: vec![QueryStatEntry {
                query: "SELECT 1".into(),
                queryid: Some(1),
                user: "u".into(),
                calls: 10,
                total_exec_time_ms: 100.0,
                mean_exec_time_ms: 10.0,
                min_exec_time_ms: 1.0,
                max_exec_time_ms: 50.0,
                rows: 10,
                shared_blks_hit: 5,
                shared_blks_read: 5,
                cache_hit_ratio: 0.5,
                total_plan_time_ms: None,
                mean_plan_time_ms: None,
            }],
        };
        let json = serde_json::to_string(&resp).unwrap();
        let back: QueryStatsResponse = serde_json::from_str(&json).unwrap();
        assert!(back.available);
        assert!(back.message.is_none());
        assert_eq!(back.entries.len(), 1);
        assert_eq!(back.entries[0].calls, 10);
    }

    #[test]
    fn dump_restore_request_round_trip() {
        let req = DumpRestoreRequest {
            source_connection_id: "src-conn".into(),
            source_database: "src_db".into(),
            target_connection_id: "tgt-conn".into(),
            target_database: "tgt_db".into(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let back: DumpRestoreRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.source_connection_id, "src-conn");
        assert_eq!(back.source_database, "src_db");
        assert_eq!(back.target_connection_id, "tgt-conn");
        assert_eq!(back.target_database, "tgt_db");
    }

    #[test]
    fn backup_request_round_trip() {
        let req = BackupRequest {
            connection_id: "c1".into(),
            database: "mydb".into(),
            output_path: "/tmp/backup.sql".into(),
            format: "plain".into(),
            schema_only: true,
            data_only: false,
        };
        let json = serde_json::to_string(&req).unwrap();
        let back: BackupRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.connection_id, "c1");
        assert!(back.schema_only);
        assert!(!back.data_only);
        assert_eq!(back.format, "plain");
    }

    #[test]
    fn restore_request_round_trip() {
        let req = RestoreRequest {
            connection_id: "c1".into(),
            database: "mydb".into(),
            input_path: "/tmp/backup.sql".into(),
            format: "plain".into(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let back: RestoreRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.input_path, "/tmp/backup.sql");
        assert_eq!(back.format, "plain");
    }

    #[test]
    fn lock_info_optional_duration() {
        let json = r#"{
            "pid": 42, "locktype": "relation", "database": "db",
            "relation": "t", "mode": "ShareLock", "granted": false,
            "query": "SELECT 1", "user": "u", "state": "idle",
            "duration_ms": null
        }"#;
        let lock: LockInfo = serde_json::from_str(json).unwrap();
        assert!(!lock.granted);
        assert!(lock.duration_ms.is_none());
        assert_eq!(lock.pid, 42);
    }

    #[test]
    fn server_config_entry_optional_unit() {
        let json = r#"{
            "name": "log_statement", "setting": "none", "unit": null,
            "category": "Reporting", "description": "Sets logging",
            "context": "superuser", "source": "default",
            "pending_restart": false
        }"#;
        let entry: ServerConfigEntry = serde_json::from_str(json).unwrap();
        assert!(entry.unit.is_none());
        assert_eq!(entry.setting, "none");
    }

    #[test]
    fn server_config_entry_pending_restart() {
        let entry = ServerConfigEntry {
            name: "shared_buffers".into(),
            setting: "128MB".into(),
            unit: Some("8kB".into()),
            category: "Resource Usage".into(),
            description: "Sets shared memory buffers".into(),
            context: "postmaster".into(),
            source: "configuration file".into(),
            pending_restart: true,
        };
        let json = serde_json::to_string(&entry).unwrap();
        let back: ServerConfigEntry = serde_json::from_str(&json).unwrap();
        assert!(back.pending_restart);
    }
}
