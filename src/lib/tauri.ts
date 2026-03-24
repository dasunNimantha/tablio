import { invoke as tauriInvoke } from "@tauri-apps/api/core";
import * as mock from "./mockData";

const isTauri = !!(window as any).__TAURI_INTERNALS__;

async function invoke<T>(cmd: string, args?: Record<string, unknown>): Promise<T> {
  if (isTauri) return tauriInvoke<T>(cmd, args);
  return mockInvoke<T>(cmd, args);
}

async function mockInvoke<T>(cmd: string, args?: Record<string, unknown>): Promise<T> {
  await new Promise((r) => setTimeout(r, Math.random() * 200 + 50));

  switch (cmd) {
    case "load_connections":
      return mock.mockConnections as T;
    case "save_connection":
      return undefined as T;
    case "delete_connection":
      return undefined as T;
    case "test_connection":
      return true as T;
    case "connect":
      return "connected" as T;
    case "disconnect":
      return undefined as T;
    case "list_databases":
      return mock.mockDatabases as T;
    case "list_schemas":
      return mock.mockSchemas as T;
    case "list_tables":
      return mock.mockTables as T;
    case "list_columns": {
      const table = (args?.table as string) || "users";
      return mock.getTableColumns(table) as T;
    }
    case "fetch_rows": {
      const req = args?.request as any;
      const table = req?.table || "users";
      const offset = req?.offset || 0;
      const limit = req?.limit || 100;
      const cols = mock.getTableColumns(table);
      const { rows, total } = mock.getTableRows(table, offset, limit);
      return { columns: cols, rows, total_rows: total, offset, limit } as T;
    }
    case "execute_query": {
      const req = args?.request as any;
      return mock.generateQueryResult(req?.sql || "SELECT 1") as T;
    }
    case "explain_query":
      return mock.generateExplainResult() as T;
    case "get_ddl":
      return mock.mockDdl as T;
    case "apply_changes":
      return undefined as T;
    case "create_table":
      return undefined as T;
    case "list_indexes":
      return mock.mockIndexes as T;
    case "list_foreign_keys": {
      const table = (args?.table as string) || "";
      return mock.getTableForeignKeys(table) as T;
    }
    case "drop_object":
      return undefined as T;
    case "truncate_table":
      return undefined as T;
    case "get_server_activity":
      return mock.mockActivity as T;
    case "cancel_query":
      return undefined as T;
    case "export_table_data":
      return "id,email,username\n1,alice@example.com,alice\n2,bob@example.com,bob" as T;
    case "export_table_to_file":
      return undefined as T;
    case "export_query_result":
      return "id,name,value\n1,Item 1,42.50\n2,Item 2,99.99" as T;
    case "export_query_result_to_file":
      return undefined as T;
    case "alter_table":
      return undefined as T;
    case "list_functions":
      return mock.mockFunctions as T;
    case "list_triggers":
      return [] as T;
    case "get_table_stats":
      return mock.mockTableStats as T;
    case "import_data":
      return 10 as T;
    case "load_saved_queries":
      return mock.mockSavedQueries as T;
    case "save_query":
      return undefined as T;
    case "delete_saved_query":
      return undefined as T;
    case "list_roles":
      return mock.mockRoles as T;
    case "create_role":
      return undefined as T;
    case "drop_role":
      return undefined as T;
    case "alter_role":
      return undefined as T;
    case "backup_database":
      return "Backup completed successfully to /tmp/backup.sql" as T;
    case "restore_database":
      return "Restore completed successfully" as T;
    case "dump_and_restore":
      return "Successfully dumped source and restored to target" as T;
    case "get_database_stats":
      return {
        active_connections: 2, idle_connections: 5, idle_in_transaction: 0,
        total_connections: 7, xact_commit: 1000, xact_rollback: 5,
        tup_inserted: 500, tup_updated: 200, tup_deleted: 10,
        tup_fetched: 5000, blks_read: 100, blks_hit: 1000,
        timestamp_ms: Date.now(),
      } as T;
    case "get_locks":
      return [] as T;
    case "get_server_config":
      return [] as T;
    case "get_query_stats":
      return { available: false, message: "Mock mode", entries: [] } as T;
    case "get_app_resource_usage":
      return { memory_mb: 64.5, cpu_percent: 2.3 } as T;
    default:
      throw new Error(`Unknown mock command: ${cmd}`);
  }
}

export interface ConnectionConfig {
  id: string;
  name: string;
  db_type: "postgres" | "mysql" | "sqlite";
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
  color: string;
  ssl: boolean;
  group?: string | null;
  ssh_enabled?: boolean;
  ssh_host?: string;
  ssh_port?: number;
  ssh_user?: string;
  ssh_password?: string;
  ssh_key_path?: string;
}

export interface DatabaseInfo {
  name: string;
}

export interface SchemaInfo {
  name: string;
}

export interface TableInfo {
  name: string;
  schema: string;
  table_type: string;
  row_count_estimate: number | null;
}

export interface ColumnInfo {
  name: string;
  data_type: string;
  is_nullable: boolean;
  is_primary_key: boolean;
  default_value: string | null;
  ordinal_position: number;
  is_auto_generated: boolean;
}

export interface TableData {
  columns: ColumnInfo[];
  rows: unknown[][];
  total_rows: number;
  offset: number;
  limit: number;
}

export interface QueryResult {
  columns: string[];
  rows: unknown[][];
  rows_affected: number;
  execution_time_ms: number;
  is_select: boolean;
}

export interface SortSpec {
  column: string;
  direction: "asc" | "desc";
}

export interface CellChange {
  row_index: number;
  column_name: string;
  old_value: unknown;
  new_value: unknown;
  primary_key_values: [string, unknown][];
}

export interface NewRow {
  values: [string, unknown][];
}

export interface DeleteRow {
  primary_key_values: [string, unknown][];
}

export interface DataChanges {
  connection_id: string;
  database: string;
  schema: string;
  table: string;
  updates: CellChange[];
  inserts: NewRow[];
  deletes: DeleteRow[];
}

export interface FetchRowsRequest {
  connection_id: string;
  database: string;
  schema: string;
  table: string;
  offset: number;
  limit: number;
  sort: SortSpec | null;
  filter: string | null;
}

export interface ExecuteQueryRequest {
  connection_id: string;
  database: string;
  sql: string;
}

export interface ExplainNode {
  node_type: string;
  relation: string | null;
  startup_cost: number;
  total_cost: number;
  actual_time_ms: number | null;
  rows_estimated: number;
  rows_actual: number | null;
  width: number;
  filter: string | null;
  children: ExplainNode[];
}

export interface ExplainResult {
  plan: ExplainNode;
  raw_text: string;
  execution_time_ms: number;
}

export interface GetDdlRequest {
  connection_id: string;
  database: string;
  schema: string;
  object_name: string;
  object_type: string;
}

export interface ColumnDefinition {
  name: string;
  data_type: string;
  is_nullable: boolean;
  is_primary_key: boolean;
  default_value: string | null;
}

export interface CreateTableRequest {
  connection_id: string;
  database: string;
  schema: string;
  table_name: string;
  columns: ColumnDefinition[];
}

export interface IndexInfo {
  name: string;
  columns: string[];
  is_unique: boolean;
  index_type: string;
}

export interface ForeignKeyInfo {
  name: string;
  column: string;
  referenced_table: string;
  referenced_column: string;
  on_delete: string;
  on_update: string;
}

export interface ServerActivity {
  pid: string;
  user: string;
  database: string;
  state: string;
  query: string;
  duration_ms: number | null;
  client_addr: string;
}

export interface ExportRequest {
  connection_id: string;
  database: string;
  schema: string;
  table: string;
  format: string;
  filter: string | null;
}

export interface ExportResultRequest {
  columns: string[];
  rows: unknown[][];
  format: string;
  table_name: string | null;
}

export interface DropObjectRequest {
  connection_id: string;
  database: string;
  schema: string;
  object_name: string;
  object_type: string;
}

export interface TruncateTableRequest {
  connection_id: string;
  database: string;
  schema: string;
  table_name: string;
}

export interface ServerActivityRequest {
  connection_id: string;
}

export interface CancelQueryRequest {
  connection_id: string;
  pid: string;
}

export interface QueryStatsRequest {
  connection_id: string;
}

export interface QueryStatEntry {
  query: string;
  queryid: number | null;
  user: string;
  calls: number;
  total_exec_time_ms: number;
  mean_exec_time_ms: number;
  min_exec_time_ms: number;
  max_exec_time_ms: number;
  rows: number;
  shared_blks_hit: number;
  shared_blks_read: number;
  cache_hit_ratio: number;
  total_plan_time_ms: number | null;
  mean_plan_time_ms: number | null;
}

export interface QueryStatsResponse {
  available: boolean;
  message: string | null;
  entries: QueryStatEntry[];
}

export interface DatabaseStats {
  active_connections: number;
  idle_connections: number;
  idle_in_transaction: number;
  total_connections: number;
  xact_commit: number;
  xact_rollback: number;
  tup_inserted: number;
  tup_updated: number;
  tup_deleted: number;
  tup_fetched: number;
  blks_read: number;
  blks_hit: number;
  timestamp_ms: number;
}

export interface LockInfo {
  pid: number;
  locktype: string;
  database: string;
  relation: string;
  mode: string;
  granted: boolean;
  query: string;
  user: string;
  state: string;
  duration_ms: number | null;
}

export interface ServerConfigEntry {
  name: string;
  setting: string;
  unit: string | null;
  category: string;
  description: string;
  context: string;
  source: string;
  pending_restart: boolean;
}

export interface AlterTableOperation {
  op: string;
  column?: ColumnDefinition;
  column_name?: string;
  old_name?: string;
  new_name?: string;
  new_type?: string;
  nullable?: boolean;
  default_value?: string | null;
}

export interface AlterTableRequest {
  connection_id: string;
  database: string;
  schema: string;
  table_name: string;
  operations: AlterTableOperation[];
}

export interface FunctionInfo {
  name: string;
  schema: string;
  return_type: string;
  language: string;
  kind: string;
}

export interface TriggerInfo {
  name: string;
  table_name: string;
  event: string;
  timing: string;
}

export interface TableStats {
  table_name: string;
  row_count: number;
  total_size: string;
  index_size: string;
  data_size: string;
  last_vacuum: string | null;
  last_analyze: string | null;
  dead_tuples: number | null;
  live_tuples: number | null;
}

export interface ImportDataRequest {
  connection_id: string;
  database: string;
  schema: string;
  table: string;
  columns: string[];
  rows: unknown[][];
}

export interface SavedQuery {
  id: string;
  name: string;
  sql: string;
  connection_id: string | null;
  database: string | null;
  created_at: number;
  updated_at: number;
}

export interface RoleInfo {
  name: string;
  is_superuser: boolean;
  can_login: boolean;
  can_create_db: boolean;
  can_create_role: boolean;
  is_replication: boolean;
  connection_limit: number;
  valid_until: string | null;
  member_of: string[];
}

export interface CreateRoleRequest {
  connection_id: string;
  name: string;
  password: string | null;
  is_superuser: boolean;
  can_login: boolean;
  can_create_db: boolean;
  can_create_role: boolean;
  connection_limit: number;
  valid_until: string | null;
}

export interface DropRoleRequest {
  connection_id: string;
  name: string;
}

export interface AlterRoleRequest {
  connection_id: string;
  name: string;
  password?: string | null;
  is_superuser?: boolean | null;
  can_login?: boolean | null;
  can_create_db?: boolean | null;
  can_create_role?: boolean | null;
  connection_limit?: number | null;
  valid_until?: string | null;
}

export interface BackupRequest {
  connection_id: string;
  database: string;
  output_path: string;
  format: string;
  schema_only: boolean;
  data_only: boolean;
}

export interface RestoreRequest {
  connection_id: string;
  database: string;
  input_path: string;
  format: string;
}

export interface DumpRestoreRequest {
  source_connection_id: string;
  source_database: string;
  target_connection_id: string;
  target_database: string;
}

export const api = {
  testConnection: (config: ConnectionConfig): Promise<boolean> =>
    invoke("test_connection", { config }),

  connect: (config: ConnectionConfig): Promise<string> =>
    invoke("connect", { config }),

  disconnect: (connectionId: string): Promise<void> =>
    invoke("disconnect", { connectionId }),

  saveConnection: (config: ConnectionConfig): Promise<void> =>
    invoke("save_connection", { config }),

  deleteConnection: (connectionId: string): Promise<void> =>
    invoke("delete_connection", { connectionId }),

  loadConnections: (): Promise<ConnectionConfig[]> =>
    invoke("load_connections"),

  listDatabases: (connectionId: string): Promise<DatabaseInfo[]> =>
    invoke("list_databases", { connectionId }),

  listSchemas: (connectionId: string, database: string): Promise<SchemaInfo[]> =>
    invoke("list_schemas", { connectionId, database }),

  listTables: (
    connectionId: string,
    database: string,
    schema: string
  ): Promise<TableInfo[]> =>
    invoke("list_tables", { connectionId, database, schema }),

  listColumns: (
    connectionId: string,
    database: string,
    schema: string,
    table: string
  ): Promise<ColumnInfo[]> =>
    invoke("list_columns", { connectionId, database, schema, table }),

  fetchRows: (request: FetchRowsRequest): Promise<TableData> =>
    invoke("fetch_rows", { request }),

  executeQuery: (request: ExecuteQueryRequest): Promise<QueryResult> =>
    invoke("execute_query", { request }),

  applyChanges: (changes: DataChanges): Promise<void> =>
    invoke("apply_changes", { changes }),

  explainQuery: (request: ExecuteQueryRequest): Promise<ExplainResult> =>
    invoke("explain_query", { request }),

  getDdl: (request: GetDdlRequest): Promise<string> =>
    invoke("get_ddl", { request }),

  createTable: (request: CreateTableRequest): Promise<void> =>
    invoke("create_table", { request }),

  listIndexes: (
    connectionId: string,
    database: string,
    schema: string,
    table: string
  ): Promise<IndexInfo[]> =>
    invoke("list_indexes", { connectionId, database, schema, table }),

  listForeignKeys: (
    connectionId: string,
    database: string,
    schema: string,
    table: string
  ): Promise<ForeignKeyInfo[]> =>
    invoke("list_foreign_keys", { connectionId, database, schema, table }),

  dropObject: (request: DropObjectRequest): Promise<void> =>
    invoke("drop_object", { request }),

  truncateTable: (request: TruncateTableRequest): Promise<void> =>
    invoke("truncate_table", { request }),

  getServerActivity: (request: ServerActivityRequest): Promise<ServerActivity[]> =>
    invoke("get_server_activity", { request }),

  cancelQuery: (request: CancelQueryRequest): Promise<void> =>
    invoke("cancel_query", { request }),

  getDatabaseStats: (request: ServerActivityRequest): Promise<DatabaseStats> =>
    invoke("get_database_stats", { request }),

  getLocks: (request: ServerActivityRequest): Promise<LockInfo[]> =>
    invoke("get_locks", { request }),

  getServerConfig: (request: ServerActivityRequest): Promise<ServerConfigEntry[]> =>
    invoke("get_server_config", { request }),

  getQueryStats: (request: QueryStatsRequest): Promise<QueryStatsResponse> =>
    invoke("get_query_stats", { request }),

  exportTableData: (request: ExportRequest): Promise<string> =>
    invoke("export_table_data", { request }),

  exportTableToFile: (request: ExportRequest, filePath: string): Promise<void> =>
    invoke("export_table_to_file", { request, filePath }),

  exportQueryResult: (request: ExportResultRequest): Promise<string> =>
    invoke("export_query_result", { request }),

  exportQueryResultToFile: (request: ExportResultRequest, filePath: string): Promise<void> =>
    invoke("export_query_result_to_file", { request, filePath }),

  alterTable: (request: AlterTableRequest): Promise<void> =>
    invoke("alter_table", { request }),

  listFunctions: (
    connectionId: string,
    database: string,
    schema: string
  ): Promise<FunctionInfo[]> =>
    invoke("list_functions", { connectionId, database, schema }),

  listTriggers: (
    connectionId: string,
    database: string,
    schema: string,
    table: string
  ): Promise<TriggerInfo[]> =>
    invoke("list_triggers", { connectionId, database, schema, table }),

  getTableStats: (
    connectionId: string,
    database: string,
    schema: string,
    table: string
  ): Promise<TableStats> =>
    invoke("get_table_stats", { connectionId, database, schema, table }),

  importData: (request: ImportDataRequest): Promise<number> =>
    invoke("import_data", { request }),

  loadSavedQueries: (): Promise<SavedQuery[]> =>
    invoke("load_saved_queries"),

  saveQuery: (query: SavedQuery): Promise<void> =>
    invoke("save_query", { query }),

  deleteSavedQuery: (queryId: string): Promise<void> =>
    invoke("delete_saved_query", { queryId }),

  listRoles: (connectionId: string): Promise<RoleInfo[]> =>
    invoke("list_roles", { connectionId }),

  createRole: (request: CreateRoleRequest): Promise<void> =>
    invoke("create_role", { request }),

  dropRole: (request: DropRoleRequest): Promise<void> =>
    invoke("drop_role", { request }),

  alterRole: (request: AlterRoleRequest): Promise<void> =>
    invoke("alter_role", { request }),

  backupDatabase: (request: BackupRequest): Promise<string> =>
    invoke("backup_database", { request }),

  restoreDatabase: (request: RestoreRequest): Promise<string> =>
    invoke("restore_database", { request }),

  dumpAndRestore: (request: DumpRestoreRequest): Promise<string> =>
    invoke("dump_and_restore", { request }),

  getAppResourceUsage: (): Promise<{ memory_mb: number; cpu_percent: number }> =>
    invoke("get_app_resource_usage"),
};
