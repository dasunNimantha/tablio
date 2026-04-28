import type { ColumnInfo } from "./tauri";
import { caseInsensitiveLike, quoteIdent, supportsIlike, type DbType } from "./sqlDialect";

export type JoinType = "AND" | "OR";

export interface FilterCondition {
  id: string;
  column: string;
  operator: string;
  value: string;
  join: JoinType;
}

export const NO_VALUE_OPS = ["IS NULL", "IS NOT NULL"];

// Matches a finite decimal literal: optional sign, digits, optional fractional
// part, optional exponent. Deliberately rejects hex (`0x10`), octal (`0o17`),
// binary (`0b101`), `Infinity`, `NaN`, and stray whitespace -- those are all
// things `Number(...)` would happily coerce, but most database engines either
// reject them outright or interpret them differently across dialects (e.g.
// MySQL accepts `0x10` as 16, Postgres treats it as a syntax error).
//
// Match groups follow the SQL numeric-literal grammar so the unmodified
// trimmed string can be embedded verbatim, preserving exact precision for
// NUMERIC/DECIMAL columns.
const DECIMAL_LITERAL = /^-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?$/;

const NUMERIC_TYPE_RE = /int|float|double|decimal|numeric|real|serial/i;

/**
 * Builds a SQL WHERE clause from conditions and column metadata.
 *
 * Conditions are validated before being emitted:
 *  - The column name must be present.
 *  - For value-bearing operators the value must be non-empty after trimming
 *    (a whitespace-only "value" is treated as no input, not as the literal
 *    string of spaces).
 *  - For numeric columns the value must look like a SQL decimal literal
 *    (`-?\d+(\.\d+)?(eN)?`); anything else falls through to the quoted
 *    string branch so type coercion is the database's job, not ours.
 *
 * Identifier quoting is dialect-aware (PG `"col"`, MySQL `` `col` ``,
 * MSSQL `[col]`) — see `sqlDialect.quoteIdent`. The default of `"col"`
 * applies when `dbType` isn't supplied so legacy callers keep working.
 *
 * `ILIKE` is rewritten to `LOWER(col) LIKE LOWER('value')` on dialects
 * that don't support it natively (everything except Postgres and
 * CockroachDB), so the operator stays meaningful in the UI regardless
 * of the connected engine.
 *
 * Conditions are grouped by AND/OR with parentheses when the join type
 * changes so SQL precedence (AND binds tighter than OR) doesn't surprise
 * users who chained mixed operators.
 */
export function buildWhereClause(
  conditions: FilterCondition[],
  columns: ColumnInfo[],
  dbType?: DbType,
): string | null {
  const valid = conditions.filter((c) => {
    if (!c.column) return false;
    if (NO_VALUE_OPS.includes(c.operator)) return true;
    return c.value.trim().length > 0;
  });

  if (valid.length === 0) return null;

  const toClause = (c: FilterCondition): string => {
    const col = quoteIdent(dbType, c.column);
    if (NO_VALUE_OPS.includes(c.operator)) {
      return `${col} ${c.operator}`;
    }
    const value = c.value.trim();
    if (c.operator === "LIKE") {
      return `${col} LIKE '${value.replace(/'/g, "''")}'`;
    }
    if (c.operator === "ILIKE") {
      // PG / CRDB get the native operator. Everywhere else falls back
      // to a portable LOWER(...) LIKE LOWER(...) rewrite so the user's
      // case-insensitive intent isn't silently dropped on engines that
      // don't have ILIKE.
      const literal = `'${value.replace(/'/g, "''")}'`;
      return supportsIlike(dbType)
        ? `${col} ILIKE ${literal}`
        : caseInsensitiveLike(dbType, col, literal);
    }
    const colInfo = columns.find((ci) => ci.name === c.column);
    const isNum = !!colInfo && NUMERIC_TYPE_RE.test(colInfo.data_type);
    // Only embed unquoted when the trimmed value matches a SQL decimal
    // literal exactly. Falls through to the quoted-string branch for
    // hex / Infinity / NaN / whitespace-padded inputs so the database
    // can decide how to coerce them (or reject them with a clear error)
    // instead of us silently shipping non-portable SQL.
    if (isNum && DECIMAL_LITERAL.test(value)) {
      return `${col} ${c.operator} ${value}`;
    }
    return `${col} ${c.operator} '${value.replace(/'/g, "''")}'`;
  };

  if (valid.length === 1) return toClause(valid[0]);

  const groups: { join: JoinType; clauses: string[] }[] = [];
  let currentGroup: { join: JoinType; clauses: string[] } = {
    join: valid[0].join,
    clauses: [toClause(valid[0])],
  };

  for (let i = 1; i < valid.length; i++) {
    if (valid[i].join === currentGroup.join) {
      currentGroup.clauses.push(toClause(valid[i]));
    } else {
      groups.push(currentGroup);
      currentGroup = { join: valid[i].join, clauses: [toClause(valid[i])] };
    }
  }
  groups.push(currentGroup);

  if (groups.length === 1) {
    return groups[0].clauses.join(` ${groups[0].join} `);
  }

  const wrap = (g: { join: JoinType; clauses: string[] }) =>
    g.clauses.length > 1
      ? `(${g.clauses.join(` ${g.join} `)})`
      : g.clauses[0];

  let result = wrap(groups[0]);
  for (let i = 1; i < groups.length; i++) {
    result = `${result} ${groups[i].join} ${wrap(groups[i])}`;
  }
  return result;
}
