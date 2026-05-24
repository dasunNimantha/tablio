/**
 * Pure helpers shared by the Alter Table modal and the in-tab
 * editor on the Schema view. These were originally inline at the
 * top of `AlterTableDialog.tsx`; extracting them here lets both
 * mount sites import the same logic AND gives them direct unit-test
 * coverage (the component previously had zero).
 */

import { AlterTableOperation, ColumnInfo } from "../../lib/tauri";

/**
 * Catalog of canonical PostgreSQL data types offered in the
 * column-type select. Tablio's other engines map onto these by
 * convention — there's deliberately no per-engine version of the
 * list yet (tracked in a separate follow-up).
 */
export const PG_TYPES = [
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

/**
 * A column the user has added to the table but not yet saved.
 * Mirrors the `AddColumn` op's shape but kept loose (string `default_value`
 * rather than `string | null`) because the user is mid-typing — we
 * normalise on commit, not on every keystroke.
 */
export interface PendingNewColumn {
  name: string;
  data_type: string;
  is_nullable: boolean;
  default_value: string;
}

/**
 * Snapshot of a column after the pending `operations` are applied.
 * Drives the "effective" view of each row in the editor so the user
 * sees what the table will look like post-save, not the raw catalog
 * values.
 */
export interface EffectiveColumn {
  name: string;
  type: string;
  nullable: boolean;
  default: string | null;
}

/**
 * For each original column, fold every queued operation that targets
 * it (rename / change_type / set_nullable / set_default) into a single
 * "this is what the column looks like after Apply" row. Drops are
 * tracked separately via {@link isColumnDropped} — they stay in the
 * result map so the row still renders (with a "Dropped" badge), they
 * just get flagged.
 *
 * Key invariants:
 *
 * - Ordering of operations doesn't matter for the *effective* state
 *   here because we look up the last matching op for each
 *   column/field. We do still reorder at apply-time (see
 *   {@link reorderOperations}) so the DB receives renames before
 *   field-targeted ops.
 * - `default_value: undefined` and `default_value: null` are treated
 *   identically — `null` means "drop the default".
 */
export function applyOperations(
  columns: ColumnInfo[],
  operations: AlterTableOperation[],
): Map<string, EffectiveColumn> {
  const result = new Map<string, EffectiveColumn>();

  for (const col of columns) {
    let effectiveName = col.name;
    let effectiveType = col.data_type;
    let effectiveNullable = col.is_nullable;
    let effectiveDefault = col.default_value;

    const renameOp = operations.find(
      (o) => o.op === "rename_column" && o.old_name === col.name,
    );
    if (renameOp?.new_name) effectiveName = renameOp.new_name;

    const typeOp = operations.find(
      (o) => o.op === "change_type" && o.column_name === col.name,
    );
    if (typeOp?.new_type) effectiveType = typeOp.new_type;

    const nullableOp = operations.find(
      (o) => o.op === "set_nullable" && o.column_name === col.name,
    );
    if (nullableOp?.nullable !== undefined) effectiveNullable = nullableOp.nullable;

    const defaultOp = operations.find(
      (o) => o.op === "set_default" && o.column_name === col.name,
    );
    if (defaultOp !== undefined) effectiveDefault = defaultOp.default_value ?? null;

    result.set(col.name, {
      name: effectiveName,
      type: effectiveType,
      nullable: effectiveNullable,
      default: effectiveDefault,
    });
  }

  return result;
}

/**
 * Render a human-readable preview of the SQL the editor would emit on
 * Apply. Quotes identifiers Postgres-style (`"name"`) — the actual
 * server-side execution goes through the engine-specific code path
 * in the Rust backend, so this preview is informational only.
 */
export function generatePreviewSql(
  schema: string,
  tableName: string,
  tableNameNew: string | null,
  operations: AlterTableOperation[],
  pendingNewColumns: PendingNewColumn[],
): string {
  const parts: string[] = [];
  let currentQual = `"${schema}"."${tableName}"`;

  if (tableNameNew && tableNameNew !== tableName) {
    parts.push(`ALTER TABLE ${currentQual} RENAME TO "${tableNameNew}";`);
    currentQual = `"${schema}"."${tableNameNew}"`;
  }

  for (const op of operations) {
    switch (op.op) {
      case "add_column":
        if (op.column) {
          let def = `"${op.column.name}" ${op.column.data_type}`;
          if (!op.column.is_nullable) def += " NOT NULL";
          if (op.column.default_value) def += ` DEFAULT ${op.column.default_value}`;
          parts.push(`ALTER TABLE ${currentQual} ADD COLUMN ${def};`);
        }
        break;
      case "drop_column":
        if (op.column_name) {
          parts.push(`ALTER TABLE ${currentQual} DROP COLUMN "${op.column_name}";`);
        }
        break;
      case "rename_column":
        if (op.old_name && op.new_name) {
          parts.push(
            `ALTER TABLE ${currentQual} RENAME COLUMN "${op.old_name}" TO "${op.new_name}";`,
          );
        }
        break;
      case "change_type":
        if (op.column_name && op.new_type) {
          parts.push(
            `ALTER TABLE ${currentQual} ALTER COLUMN "${op.column_name}" TYPE ${op.new_type};`,
          );
        }
        break;
      case "set_nullable":
        if (op.column_name && op.nullable !== undefined) {
          const action = op.nullable ? "DROP NOT NULL" : "SET NOT NULL";
          parts.push(
            `ALTER TABLE ${currentQual} ALTER COLUMN "${op.column_name}" ${action};`,
          );
        }
        break;
      case "set_default":
        if (op.column_name) {
          const def =
            op.default_value !== null && op.default_value !== undefined
              ? `SET DEFAULT ${op.default_value}`
              : "DROP DEFAULT";
          parts.push(
            `ALTER TABLE ${currentQual} ALTER COLUMN "${op.column_name}" ${def};`,
          );
        }
        break;
    }
  }

  for (const col of pendingNewColumns) {
    if (!col.name.trim()) continue;
    let def = `"${col.name}" ${col.data_type}`;
    if (!col.is_nullable) def += " NOT NULL";
    if (col.default_value) def += ` DEFAULT ${col.default_value}`;
    parts.push(`ALTER TABLE ${currentQual} ADD COLUMN ${def};`);
  }

  return parts.length > 0 ? parts.join("\n") : "-- No changes";
}

/**
 * Deterministic ordering for execution. `RENAME` operations must run
 * before any field-targeted op so column_name lookups inside the
 * server resolve to the new identifier. The DB-level errors you get
 * for an unordered rename + change_type are obscure ("column X
 * doesn't exist"); reordering here is a much better UX than
 * surfacing those.
 *
 * Categories (in execution order):
 *   1. `rename_table` — has to win because every subsequent op
 *      qualifies against the new table name.
 *   2. `rename_column` — same reasoning for columns.
 *   3. everything else (add / drop / change_type / set_nullable /
 *      set_default) — order within this bucket doesn't matter
 *      because each targets a unique (column, field) pair.
 */
export function reorderOperations(
  ops: AlterTableOperation[],
): AlterTableOperation[] {
  const renameTable = ops.filter((o) => o.op === "rename_table");
  const renameColumn = ops.filter((o) => o.op === "rename_column");
  const rest = ops.filter(
    (o) => o.op !== "rename_table" && o.op !== "rename_column",
  );
  return [...renameTable, ...renameColumn, ...rest];
}
