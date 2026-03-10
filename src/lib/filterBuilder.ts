import type { ColumnInfo } from "./tauri";

export type JoinType = "AND" | "OR";

export interface FilterCondition {
  id: string;
  column: string;
  operator: string;
  value: string;
  join: JoinType;
}

export const NO_VALUE_OPS = ["IS NULL", "IS NOT NULL"];

/**
 * Builds a SQL WHERE clause from conditions and column metadata.
 * Escapes identifiers and string literals; groups by AND/OR with parentheses when mixed.
 */
export function buildWhereClause(
  conditions: FilterCondition[],
  columns: ColumnInfo[]
): string | null {
  const valid = conditions.filter((c) => {
    if (NO_VALUE_OPS.includes(c.operator)) return c.column;
    return c.column && c.value;
  });

  if (valid.length === 0) return null;

  const toClause = (c: FilterCondition): string => {
    const col = `"${c.column.replace(/"/g, '""')}"`;
    if (NO_VALUE_OPS.includes(c.operator)) {
      return `${col} ${c.operator}`;
    }
    if (c.operator === "LIKE" || c.operator === "ILIKE") {
      return `${col} ${c.operator} '${c.value.replace(/'/g, "''")}'`;
    }
    const colInfo = columns.find((ci) => ci.name === c.column);
    const isNum =
      colInfo && /int|float|double|decimal|numeric|real|serial/i.test(colInfo.data_type);
    if (isNum && !Number.isNaN(Number(c.value))) {
      return `${col} ${c.operator} ${c.value}`;
    }
    return `${col} ${c.operator} '${c.value.replace(/'/g, "''")}'`;
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
