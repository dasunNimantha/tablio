import { quoteIdent, quoteQualified, type DbType } from "./sqlDialect";

/**
 * Per-dialect column-type catalogs and defaults for the Create Table
 * dialog and the Alter Table "add column" rows.
 *
 * The lists are curated, not exhaustive: the common types for each
 * engine, spelled the way that engine expects them. The default id
 * column is the idiomatic auto-generated surrogate key per engine.
 */

const PG_TYPES = [
  "integer",
  "bigint",
  "smallint",
  "serial",
  "bigserial",
  "text",
  "varchar(255)",
  "char(1)",
  "boolean",
  "timestamp",
  "timestamptz",
  "date",
  "time",
  "numeric",
  "real",
  "double precision",
  "uuid",
  "json",
  "jsonb",
  "bytea",
];

const CRDB_TYPES = [
  "INT8",
  "INT4",
  "INT2",
  "SERIAL",
  "STRING",
  "VARCHAR(255)",
  "BOOL",
  "TIMESTAMP",
  "TIMESTAMPTZ",
  "DATE",
  "TIME",
  "DECIMAL",
  "FLOAT4",
  "FLOAT8",
  "UUID",
  "JSONB",
  "BYTES",
];

const MYSQL_TYPES = [
  "INT",
  "INT AUTO_INCREMENT",
  "BIGINT",
  "BIGINT AUTO_INCREMENT",
  "SMALLINT",
  "TINYINT",
  "VARCHAR(255)",
  "CHAR(1)",
  "TEXT",
  "LONGTEXT",
  "BOOLEAN",
  "DATETIME",
  "TIMESTAMP",
  "DATE",
  "TIME",
  "DECIMAL(10,2)",
  "FLOAT",
  "DOUBLE",
  "JSON",
  "BLOB",
];

const MSSQL_TYPES = [
  "INT",
  "INT IDENTITY(1,1)",
  "BIGINT",
  "BIGINT IDENTITY(1,1)",
  "SMALLINT",
  "TINYINT",
  "NVARCHAR(255)",
  "NVARCHAR(MAX)",
  "VARCHAR(255)",
  "NCHAR(1)",
  "BIT",
  "DATETIME2",
  "DATETIMEOFFSET",
  "DATE",
  "TIME",
  "DECIMAL(18,2)",
  "REAL",
  "FLOAT",
  "UNIQUEIDENTIFIER",
  "VARBINARY(MAX)",
];

const SQLITE_TYPES = ["INTEGER", "TEXT", "REAL", "NUMERIC", "BLOB", "BOOLEAN", "DATETIME"];

const CASSANDRA_TYPES = [
  "uuid",
  "timeuuid",
  "text",
  "int",
  "bigint",
  "smallint",
  "boolean",
  "timestamp",
  "date",
  "time",
  "decimal",
  "float",
  "double",
  "blob",
  "inet",
  "list<text>",
  "set<text>",
  "map<text, text>",
];

export function columnTypesFor(dbType: DbType | undefined): string[] {
  switch (dbType) {
    case "cockroachdb":
      return CRDB_TYPES;
    case "mysql":
    case "mariadb":
    case "tidb":
      return MYSQL_TYPES;
    case "mssql":
      return MSSQL_TYPES;
    case "sqlite":
      return SQLITE_TYPES;
    case "cassandra":
      return CASSANDRA_TYPES;
    case "postgres":
    default:
      return PG_TYPES;
  }
}

/** Type preselected for a freshly added (non-key) column. */
export function defaultColumnType(dbType: DbType | undefined): string {
  switch (dbType) {
    case "cockroachdb":
      return "STRING";
    case "mysql":
    case "mariadb":
    case "tidb":
      return "VARCHAR(255)";
    case "mssql":
      return "NVARCHAR(255)";
    case "sqlite":
      return "TEXT";
    case "cassandra":
      return "text";
    default:
      return "text";
  }
}

/** Type of the auto-generated `id` primary key the dialog starts with.
 *  SQLite uses plain INTEGER: with the table-level PRIMARY KEY the
 *  backend emits, an INTEGER key becomes the auto-assigned rowid alias. */
export function defaultIdType(dbType: DbType | undefined): string {
  switch (dbType) {
    case "cockroachdb":
      return "SERIAL";
    case "mysql":
    case "mariadb":
    case "tidb":
      return "INT AUTO_INCREMENT";
    case "mssql":
      return "INT IDENTITY(1,1)";
    case "sqlite":
      return "INTEGER";
    case "cassandra":
      return "uuid";
    default:
      return "serial";
  }
}

export interface DdlColumn {
  name: string;
  data_type: string;
  is_nullable: boolean;
  is_primary_key: boolean;
  default_value: string;
}

/** Whether `NOT NULL` / `DEFAULT` are meaningful for the dialect
 *  (CQL has neither; the backend ignores them for Cassandra). */
export function supportsNullability(dbType: DbType | undefined): boolean {
  return dbType !== "cassandra";
}

/**
 * Preview of the CREATE TABLE the backend will run, quoted for the
 * dialect. Mirrors the shape of the per-driver `create_table` code in
 * src-tauri/src/db/*.rs (table-level PRIMARY KEY, schema dropped on
 * SQLite, keyspace-qualified on Cassandra).
 */
export function buildCreateTableSql(
  dbType: DbType | undefined,
  database: string,
  schema: string,
  tableName: string,
  columns: DdlColumn[],
): string {
  const cols = columns.filter((c) => c.name.trim() !== "");
  const q = (n: string) => quoteIdent(dbType, n);
  const nullability = supportsNullability(dbType);
  const defs = cols.map((c) => {
    let def = `    ${q(c.name)} ${c.data_type}`;
    if (nullability && c.default_value) def += ` DEFAULT ${c.default_value}`;
    if (nullability && !c.is_nullable) def += " NOT NULL";
    return def;
  });
  const pk = cols.filter((c) => c.is_primary_key);
  if (pk.length > 0) defs.push(`    PRIMARY KEY (${pk.map((c) => q(c.name)).join(", ")})`);
  let target: string;
  if (dbType === "cassandra") target = `${q(database)}.${q(tableName)}`;
  else if (dbType === "mysql" || dbType === "mariadb" || dbType === "tidb")
    target = quoteQualified(dbType, database, tableName);
  else target = quoteQualified(dbType, schema, tableName);
  return `CREATE TABLE ${target} (\n${defs.join(",\n")}\n);`;
}

/** Inline validation for the column list: empty names on rows that
 *  have other input, and case-insensitive duplicates. */
export function validateColumnNames(columns: { name: string }[]): {
  duplicates: Set<number>;
  message: string | null;
} {
  const seen = new Map<string, number>();
  const duplicates = new Set<number>();
  columns.forEach((c, i) => {
    const key = c.name.trim().toLowerCase();
    if (!key) return;
    const first = seen.get(key);
    if (first !== undefined) {
      duplicates.add(first);
      duplicates.add(i);
    } else {
      seen.set(key, i);
    }
  });
  if (duplicates.size > 0) {
    const names = [...new Set([...duplicates].map((i) => columns[i].name.trim()))];
    return { duplicates, message: `Duplicate column name: ${names.join(", ")}` };
  }
  return { duplicates, message: null };
}
