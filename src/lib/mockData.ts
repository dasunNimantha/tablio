import type {
  ConnectionConfig,
  DatabaseInfo,
  SchemaInfo,
  TableInfo,
  ColumnInfo,
  TableData,
  QueryResult,
  ExplainResult,
  IndexInfo,
  ForeignKeyInfo,
  ServerActivity,
  FunctionInfo,
  TableStats as TableStatsType,
  RoleInfo,
  SavedQuery,
} from "./tauri";

const CONN_ID = "mock-conn-1";

export const mockConnections: ConnectionConfig[] = [
  {
    id: CONN_ID,
    name: "Local Postgres",
    db_type: "postgres",
    host: "127.0.0.1",
    port: 15432,
    user: "postgres",
    password: "postgres",
    database: "postgres",
    color: "#6d9eff",
    ssl: false,
    group: "",
  },
  {
    id: "mock-conn-2",
    name: "Staging DB",
    db_type: "postgres",
    host: "staging.example.com",
    port: 5432,
    user: "app_user",
    password: "secret",
    database: "app_staging",
    color: "#34d399",
    ssl: true,
    group: "Production",
  },
  {
    id: "mock-conn-3",
    name: "Analytics DB",
    db_type: "postgres",
    host: "analytics.example.com",
    port: 5432,
    user: "readonly",
    password: "readonly",
    database: "analytics",
    color: "#fbbf24",
    ssl: true,
    group: "Production",
  },
];

export const mockDatabases: DatabaseInfo[] = [
  { name: "postgres" },
  { name: "app_development" },
  { name: "template1" },
];

export const mockSchemas: SchemaInfo[] = [
  { name: "public" },
  { name: "auth" },
  { name: "analytics" },
];

export const mockTables: TableInfo[] = [
  { name: "users", schema: "public", table_type: "BASE TABLE", row_count_estimate: 15420 },
  { name: "orders", schema: "public", table_type: "BASE TABLE", row_count_estimate: 184320 },
  { name: "products", schema: "public", table_type: "BASE TABLE", row_count_estimate: 2560 },
  { name: "categories", schema: "public", table_type: "BASE TABLE", row_count_estimate: 48 },
  { name: "order_items", schema: "public", table_type: "BASE TABLE", row_count_estimate: 523100 },
  { name: "reviews", schema: "public", table_type: "BASE TABLE", row_count_estimate: 67800 },
  { name: "active_users_view", schema: "public", table_type: "VIEW", row_count_estimate: null },
  { name: "order_summary_view", schema: "public", table_type: "VIEW", row_count_estimate: null },
];

const usersColumns: ColumnInfo[] = [
  { name: "id", data_type: "integer", is_nullable: false, is_primary_key: true, default_value: "nextval('users_id_seq')", ordinal_position: 1 },
  { name: "email", data_type: "varchar(255)", is_nullable: false, is_primary_key: false, default_value: null, ordinal_position: 2 },
  { name: "username", data_type: "varchar(100)", is_nullable: false, is_primary_key: false, default_value: null, ordinal_position: 3 },
  { name: "full_name", data_type: "varchar(255)", is_nullable: true, is_primary_key: false, default_value: null, ordinal_position: 4 },
  { name: "password_hash", data_type: "varchar(255)", is_nullable: false, is_primary_key: false, default_value: null, ordinal_position: 5 },
  { name: "is_active", data_type: "boolean", is_nullable: false, is_primary_key: false, default_value: "true", ordinal_position: 6 },
  { name: "role", data_type: "varchar(50)", is_nullable: false, is_primary_key: false, default_value: "'user'", ordinal_position: 7 },
  { name: "login_count", data_type: "integer", is_nullable: false, is_primary_key: false, default_value: "0", ordinal_position: 8 },
  { name: "created_at", data_type: "timestamptz", is_nullable: false, is_primary_key: false, default_value: "now()", ordinal_position: 9 },
  { name: "updated_at", data_type: "timestamptz", is_nullable: true, is_primary_key: false, default_value: null, ordinal_position: 10 },
];

const names = ["Alice Johnson", "Bob Smith", "Charlie Brown", "Diana Prince", "Eve Wilson", "Frank Miller", "Grace Lee", "Henry Davis", "Ivy Chen", "Jack Taylor", "Karen White", "Leo Martinez", "Mia Anderson", "Noah Garcia", "Olivia Thomas"];
const roles = ["admin", "user", "moderator", "user", "user", "editor", "user", "user", "admin", "user", "user", "user", "moderator", "user", "editor"];

function generateUserRows(count: number): unknown[][] {
  return Array.from({ length: count }, (_, i) => {
    const name = names[i % names.length];
    const username = name.toLowerCase().replace(" ", "_") + (i > 14 ? i : "");
    const email = `${username}@example.com`;
    const date = new Date(2024, 0, 1 + i).toISOString();
    return [
      i + 1,
      email,
      username,
      name,
      "$2b$12$LJ3m4ys..." + i,
      i % 7 !== 0,
      roles[i % roles.length],
      Math.floor(Math.random() * 200),
      date,
      i % 3 === 0 ? date : null,
    ];
  });
}

const ordersColumns: ColumnInfo[] = [
  { name: "id", data_type: "integer", is_nullable: false, is_primary_key: true, default_value: "nextval('orders_id_seq')", ordinal_position: 1 },
  { name: "user_id", data_type: "integer", is_nullable: false, is_primary_key: false, default_value: null, ordinal_position: 2 },
  { name: "status", data_type: "varchar(50)", is_nullable: false, is_primary_key: false, default_value: "'pending'", ordinal_position: 3 },
  { name: "total_amount", data_type: "numeric(10,2)", is_nullable: false, is_primary_key: false, default_value: "0", ordinal_position: 4 },
  { name: "currency", data_type: "varchar(3)", is_nullable: false, is_primary_key: false, default_value: "'USD'", ordinal_position: 5 },
  { name: "shipping_address", data_type: "text", is_nullable: true, is_primary_key: false, default_value: null, ordinal_position: 6 },
  { name: "notes", data_type: "text", is_nullable: true, is_primary_key: false, default_value: null, ordinal_position: 7 },
  { name: "created_at", data_type: "timestamptz", is_nullable: false, is_primary_key: false, default_value: "now()", ordinal_position: 8 },
];

const statuses = ["pending", "processing", "shipped", "delivered", "cancelled"];

function generateOrderRows(count: number): unknown[][] {
  return Array.from({ length: count }, (_, i) => [
    1001 + i,
    (i % 15) + 1,
    statuses[i % statuses.length],
    (Math.random() * 500 + 10).toFixed(2),
    "USD",
    i % 4 !== 0 ? `${100 + i} Main St, City ${i % 50}, ST ${10000 + i}` : null,
    i % 6 === 0 ? "Rush delivery requested" : null,
    new Date(2024, Math.floor(i / 30), (i % 28) + 1).toISOString(),
  ]);
}

const productsColumns: ColumnInfo[] = [
  { name: "id", data_type: "integer", is_nullable: false, is_primary_key: true, default_value: null, ordinal_position: 1 },
  { name: "name", data_type: "varchar(255)", is_nullable: false, is_primary_key: false, default_value: null, ordinal_position: 2 },
  { name: "category_id", data_type: "integer", is_nullable: true, is_primary_key: false, default_value: null, ordinal_position: 3 },
];
const categoriesColumns: ColumnInfo[] = [
  { name: "id", data_type: "integer", is_nullable: false, is_primary_key: true, default_value: null, ordinal_position: 1 },
  { name: "name", data_type: "varchar(100)", is_nullable: false, is_primary_key: false, default_value: null, ordinal_position: 2 },
];
const orderItemsColumns: ColumnInfo[] = [
  { name: "id", data_type: "integer", is_nullable: false, is_primary_key: true, default_value: null, ordinal_position: 1 },
  { name: "order_id", data_type: "integer", is_nullable: false, is_primary_key: false, default_value: null, ordinal_position: 2 },
  { name: "product_id", data_type: "integer", is_nullable: false, is_primary_key: false, default_value: null, ordinal_position: 3 },
];
const reviewsColumns: ColumnInfo[] = [
  { name: "id", data_type: "integer", is_nullable: false, is_primary_key: true, default_value: null, ordinal_position: 1 },
  { name: "user_id", data_type: "integer", is_nullable: false, is_primary_key: false, default_value: null, ordinal_position: 2 },
  { name: "product_id", data_type: "integer", is_nullable: false, is_primary_key: false, default_value: null, ordinal_position: 3 },
];

const columnsMap: Record<string, ColumnInfo[]> = {
  users: usersColumns,
  orders: ordersColumns,
  products: productsColumns,
  categories: categoriesColumns,
  order_items: orderItemsColumns,
  reviews: reviewsColumns,
};

const rowsMap: Record<string, (count: number) => unknown[][]> = {
  users: generateUserRows,
  orders: generateOrderRows,
};

export const mockIndexes: IndexInfo[] = [
  { name: "users_pkey", columns: ["id"], is_unique: true, index_type: "btree" },
  { name: "users_email_idx", columns: ["email"], is_unique: true, index_type: "btree" },
  { name: "users_username_idx", columns: ["username"], is_unique: true, index_type: "btree" },
  { name: "users_created_at_idx", columns: ["created_at"], is_unique: false, index_type: "btree" },
];

export const mockForeignKeys: ForeignKeyInfo[] = [
  { name: "orders_user_id_fkey", column: "user_id", referenced_table: "users", referenced_column: "id", on_delete: "CASCADE", on_update: "NO ACTION" },
];

/** Returns FKs for the given table (table that owns the FK column). */
export function getTableForeignKeys(table: string): ForeignKeyInfo[] {
  if (table === "orders") return mockForeignKeys;
  return [];
}

export const mockFunctions: FunctionInfo[] = [
  { name: "get_user_orders", schema: "public", return_type: "SETOF orders", language: "sql", kind: "function" },
  { name: "calculate_total", schema: "public", return_type: "numeric", language: "plpgsql", kind: "function" },
  { name: "update_timestamp", schema: "public", return_type: "trigger", language: "plpgsql", kind: "function" },
];

export const mockActivity: ServerActivity[] = [
  { pid: "12345", user: "postgres", database: "postgres", state: "active", query: "SELECT * FROM users WHERE id = 42", duration_ms: 15, client_addr: "127.0.0.1:54321" },
  { pid: "12346", user: "app_user", database: "postgres", state: "idle", query: "COMMIT", duration_ms: null, client_addr: "127.0.0.1:54322" },
  { pid: "12347", user: "postgres", database: "postgres", state: "active", query: "UPDATE orders SET status = 'shipped' WHERE id IN (SELECT id FROM orders WHERE status = 'processing' AND created_at < now() - interval '2 days')", duration_ms: 2340, client_addr: "192.168.1.50:43210" },
  { pid: "12348", user: "readonly", database: "analytics", state: "idle in transaction", query: "SELECT count(*) FROM events GROUP BY date_trunc('day', created_at)", duration_ms: 890, client_addr: "10.0.0.5:55123" },
];

export const mockRoles: RoleInfo[] = [
  { name: "postgres", is_superuser: true, can_login: true, can_create_db: true, can_create_role: true, is_replication: true, connection_limit: -1, valid_until: null, member_of: [] },
  { name: "app_user", is_superuser: false, can_login: true, can_create_db: false, can_create_role: false, is_replication: false, connection_limit: 20, valid_until: null, member_of: ["app_readers"] },
  { name: "readonly", is_superuser: false, can_login: true, can_create_db: false, can_create_role: false, is_replication: false, connection_limit: 5, valid_until: "2026-12-31", member_of: [] },
  { name: "app_readers", is_superuser: false, can_login: false, can_create_db: false, can_create_role: false, is_replication: false, connection_limit: -1, valid_until: null, member_of: [] },
  { name: "backup_user", is_superuser: false, can_login: true, can_create_db: false, can_create_role: false, is_replication: true, connection_limit: 2, valid_until: null, member_of: [] },
];

export const mockSavedQueries: SavedQuery[] = [
  { id: "sq-1", name: "Active users this month", sql: "SELECT * FROM users WHERE is_active = true AND created_at > now() - interval '30 days' ORDER BY created_at DESC;", connection_id: CONN_ID, database: "postgres", created_at: Date.now() - 86400000 * 3, updated_at: Date.now() - 86400000 * 3 },
  { id: "sq-2", name: "Revenue by month", sql: "SELECT date_trunc('month', created_at) as month, SUM(total_amount) as revenue, COUNT(*) as order_count FROM orders WHERE status != 'cancelled' GROUP BY 1 ORDER BY 1 DESC;", connection_id: CONN_ID, database: "postgres", created_at: Date.now() - 86400000 * 7, updated_at: Date.now() - 86400000 },
];

export function getTableColumns(table: string): ColumnInfo[] {
  return columnsMap[table] || usersColumns;
}

export function getTableRows(table: string, offset: number, limit: number): { rows: unknown[][]; total: number } {
  const generator = rowsMap[table] || generateUserRows;
  const total = table === "orders" ? 184320 : table === "users" ? 15420 : 100;
  const allRows = generator(Math.min(total, 200));
  const sliced = allRows.slice(offset, offset + limit);
  return { rows: sliced, total };
}

export function generateQueryResult(sql: string): QueryResult {
  const lowerSql = sql.toLowerCase().trim();
  if (lowerSql.startsWith("select")) {
    const columns = ["id", "name", "value", "category", "created_at"];
    const categories = ["Electronics", "Books", "Clothing", "Food", "Sports"];
    const rows: unknown[][] = Array.from({ length: 25 }, (_, i) => [
      i + 1,
      `Item ${i + 1}`,
      (Math.random() * 1000).toFixed(2),
      categories[i % categories.length],
      new Date(2024, i % 12, (i % 28) + 1).toISOString(),
    ]);
    return {
      columns,
      rows,
      rows_affected: 0,
      execution_time_ms: Math.floor(Math.random() * 50 + 5),
      is_select: true,
    };
  }
  return {
    columns: [],
    rows: [],
    rows_affected: Math.floor(Math.random() * 10 + 1),
    execution_time_ms: Math.floor(Math.random() * 20 + 2),
    is_select: false,
  };
}

export function generateExplainResult(): ExplainResult {
  return {
    plan: {
      node_type: "Seq Scan",
      relation: "users",
      startup_cost: 0,
      total_cost: 245.2,
      actual_time_ms: 12.5,
      rows_estimated: 15420,
      rows_actual: 15420,
      width: 128,
      filter: "(is_active = true)",
      children: [],
    },
    raw_text: "Seq Scan on users  (cost=0.00..245.20 rows=15420 width=128) (actual time=0.015..12.500 rows=15420 loops=1)\n  Filter: (is_active = true)\n  Rows Removed by Filter: 0\nPlanning Time: 0.150 ms\nExecution Time: 14.200 ms",
    execution_time_ms: 14.2,
  };
}

export const mockTableStats: TableStatsType = {
  table_name: "users",
  row_count: 15420,
  total_size: "4128 kB",
  index_size: "1024 kB",
  data_size: "3104 kB",
  last_vacuum: "2026-03-08 14:30:00",
  last_analyze: "2026-03-09 09:15:00",
  dead_tuples: 42,
  live_tuples: 15420,
};

export const mockDdl = `CREATE TABLE public.users (
    id integer NOT NULL DEFAULT nextval('users_id_seq'::regclass),
    email character varying(255) NOT NULL,
    username character varying(100) NOT NULL,
    full_name character varying(255),
    password_hash character varying(255) NOT NULL,
    is_active boolean NOT NULL DEFAULT true,
    role character varying(50) NOT NULL DEFAULT 'user'::character varying,
    login_count integer NOT NULL DEFAULT 0,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    updated_at timestamp with time zone,
    CONSTRAINT users_pkey PRIMARY KEY (id),
    CONSTRAINT users_email_key UNIQUE (email),
    CONSTRAINT users_username_key UNIQUE (username)
);

CREATE INDEX users_created_at_idx ON public.users USING btree (created_at);
`;
