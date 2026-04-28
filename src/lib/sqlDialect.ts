import type { ConnectionConfig } from "./tauri";

export type DbType = ConnectionConfig["db_type"];

/**
 * Quote a SQL identifier (column / table / schema) using the right
 * delimiter for the target dialect.
 *
 * Behaviour by dialect:
 *  - PostgreSQL / CockroachDB / SQLite / Cassandra+ScyllaDB → `"name"`
 *    (escape `"` by doubling)
 *  - MySQL / MariaDB / TiDB → `` `name` `` (escape `` ` `` by doubling)
 *  - SQL Server → `[name]` (escape `]` by doubling)
 *
 * The frontend used to hardcode PG-style `"name"` everywhere, which
 * silently broke filter / explain / copy-as-INSERT against default
 * MySQL (where `"name"` only works under `ANSI_QUOTES`) and produced
 * fragile SQL on MSSQL.
 */
export function quoteIdent(dbType: DbType | undefined, name: string): string {
  switch (dbType) {
    case "mysql":
    case "mariadb":
    case "tidb":
      return "`" + name.replace(/`/g, "``") + "`";
    case "mssql":
      return "[" + name.replace(/]/g, "]]") + "]";
    case "postgres":
    case "cockroachdb":
    case "sqlite":
    case "cassandra":
    default:
      return '"' + name.replace(/"/g, '""') + '"';
  }
}

/**
 * Render a schema-qualified identifier. Schema is dropped on dialects
 * that don't have one (SQLite, Cassandra — schema == keyspace and is
 * already part of the connection context). Empty / undefined schemas
 * are dropped on every dialect so the caller doesn't have to special
 * case it.
 */
export function quoteQualified(
  dbType: DbType | undefined,
  schema: string | null | undefined,
  table: string,
): string {
  const t = quoteIdent(dbType, table);
  if (!schema) return t;
  // SQLite has no schema concept beyond `main` / `temp` / attached DBs;
  // emitting `"main"."t"` works but is just noise. Cassandra qualifies
  // with the keyspace, but the `connection.database` already carries
  // the keyspace so the explicit schema usually duplicates it.
  if (dbType === "sqlite") return t;
  return `${quoteIdent(dbType, schema)}.${t}`;
}

/**
 * Whether the dialect supports the `ILIKE` operator natively. Only
 * Postgres-family engines do; MySQL/SQLite/MSSQL/Cassandra need a
 * `LOWER(col) LIKE LOWER('value')` rewrite (see `caseInsensitiveLike`).
 */
export function supportsIlike(dbType: DbType | undefined): boolean {
  return dbType === "postgres" || dbType === "cockroachdb";
}

/**
 * Render a case-insensitive LIKE comparison correctly for the dialect.
 * On PG/CRDB this is just `col ILIKE 'value'`; everywhere else we
 * fall back to `LOWER(col) LIKE LOWER('value')` which is portable
 * (slow on MySQL without a functional index, but correct).
 */
export function caseInsensitiveLike(
  dbType: DbType | undefined,
  quotedCol: string,
  quotedValue: string,
): string {
  if (supportsIlike(dbType)) {
    return `${quotedCol} ILIKE ${quotedValue}`;
  }
  return `LOWER(${quotedCol}) LIKE LOWER(${quotedValue})`;
}

/**
 * Format a top-N LIMIT/OFFSET clause for the dialect. PG/MySQL/SQLite
 * accept `LIMIT N OFFSET M`; MSSQL needs the `OFFSET ... ROWS FETCH
 * NEXT ... ROWS ONLY` shape and requires an `ORDER BY` to be present.
 *
 * Caller-supplied `limit` / `offset` are coerced to non-negative
 * integers — passing fractional or negative values silently rounds
 * them to a safe representation rather than emitting bad SQL.
 */
export function paginationClause(
  dbType: DbType | undefined,
  limit: number,
  offset: number,
): string {
  // `Number.isFinite` rejects NaN AND ±Infinity, which `Math.max(0, ...)`
  // alone does not (Math.max(0, Infinity) === Infinity, which would
  // ship `LIMIT Infinity` to the database). Clamp to 0 then floor.
  const safe = (n: number): number => {
    const v = Number(n);
    if (!Number.isFinite(v)) return 0;
    return Math.max(0, Math.floor(v));
  };
  const l = safe(limit);
  const o = safe(offset);
  if (dbType === "mssql") {
    // MSSQL requires ORDER BY before OFFSET FETCH; the caller is
    // responsible for ensuring one is present (handleExplain prepends
    // `ORDER BY (SELECT NULL)` when no sort is set).
    return `OFFSET ${o} ROWS FETCH NEXT ${l} ROWS ONLY`;
  }
  return `LIMIT ${l} OFFSET ${o}`;
}

/**
 * Format a literal boolean for INSERT/UPDATE. PG/SQLite/MSSQL accept
 * uppercase TRUE/FALSE keywords; MySQL family accepts them too but
 * canonical literals are `1` / `0`. Cassandra wants lowercase
 * `true` / `false`. We pick TRUE/FALSE everywhere because every
 * supported engine accepts them — keeping the surface uniform — and
 * fall back to `1`/`0` only for MySQL family where some legacy modes
 * trip on the keyword form.
 */
export function boolLiteral(dbType: DbType | undefined, b: boolean): string {
  switch (dbType) {
    case "cassandra":
      return b ? "true" : "false";
    case "mysql":
    case "mariadb":
    case "tidb":
      return b ? "1" : "0";
    default:
      return b ? "TRUE" : "FALSE";
  }
}
