import { useState, useEffect, useRef } from "react";
import { api, AlterTableOperation, ColumnInfo } from "../../lib/tauri";
import { X, Plus, Loader2, Eye, EyeOff } from "lucide-react";
import "./AlterTableDialog.css";

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

interface PendingNewColumn {
  name: string;
  data_type: string;
  is_nullable: boolean;
  default_value: string;
}

interface Props {
  connectionId: string;
  database: string;
  schema: string;
  tableName: string;
  onClose: () => void;
  onAltered: () => void;
}

function applyOperations(
  columns: ColumnInfo[],
  operations: AlterTableOperation[]
): Map<string, { name: string; type: string; nullable: boolean; default: string | null }> {
  const result = new Map<
    string,
    { name: string; type: string; nullable: boolean; default: string | null }
  >();
  const dropped = new Set<string>();

  for (const op of operations) {
    if (op.op === "drop_column" && op.column_name) {
      dropped.add(op.column_name);
    }
  }

  for (const col of columns) {
    let effectiveName = col.name;
    let effectiveType = col.data_type;
    let effectiveNullable = col.is_nullable;
    let effectiveDefault = col.default_value;

    const renameOp = operations.find(
      (o) => o.op === "rename_column" && o.old_name === col.name
    );
    if (renameOp?.new_name) effectiveName = renameOp.new_name;

    const typeOp = operations.find(
      (o) => o.op === "change_type" && o.column_name === col.name
    );
    if (typeOp?.new_type) effectiveType = typeOp.new_type;

    const nullableOp = operations.find(
      (o) => o.op === "set_nullable" && o.column_name === col.name
    );
    if (nullableOp?.nullable !== undefined) effectiveNullable = nullableOp.nullable;

    const defaultOp = operations.find(
      (o) => o.op === "set_default" && o.column_name === col.name
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

function generatePreviewSql(
  schema: string,
  tableName: string,
  tableNameNew: string | null,
  operations: AlterTableOperation[],
  pendingNewColumns: PendingNewColumn[]
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
            `ALTER TABLE ${currentQual} RENAME COLUMN "${op.old_name}" TO "${op.new_name}";`
          );
        }
        break;
      case "change_type":
        if (op.column_name && op.new_type) {
          parts.push(
            `ALTER TABLE ${currentQual} ALTER COLUMN "${op.column_name}" TYPE ${op.new_type};`
          );
        }
        break;
      case "set_nullable":
        if (op.column_name && op.nullable !== undefined) {
          const action = op.nullable ? "DROP NOT NULL" : "SET NOT NULL";
          parts.push(
            `ALTER TABLE ${currentQual} ALTER COLUMN "${op.column_name}" ${action};`
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
            `ALTER TABLE ${currentQual} ALTER COLUMN "${op.column_name}" ${def};`
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

function reorderOperations(ops: AlterTableOperation[]): AlterTableOperation[] {
  const renameTable = ops.filter((o) => o.op === "rename_table");
  const renameColumn = ops.filter((o) => o.op === "rename_column");
  const rest = ops.filter(
    (o) => o.op !== "rename_table" && o.op !== "rename_column"
  );
  return [...renameTable, ...renameColumn, ...rest];
}

export function AlterTableDialog({
  connectionId,
  database,
  schema,
  tableName,
  onClose,
  onAltered,
}: Props) {
  const [columns, setColumns] = useState<ColumnInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [applying, setApplying] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [tableNameLocal, setTableNameLocal] = useState(tableName);
  const [operations, setOperations] = useState<AlterTableOperation[]>([]);
  const [pendingNewColumns, setPendingNewColumns] = useState<PendingNewColumn[]>(
    []
  );
  const [showPreview, setShowPreview] = useState(false);
  const dialogRef = useRef<HTMLDivElement>(null);
  const previewRef = useRef<HTMLDivElement>(null);
  const [editingCell, setEditingCell] = useState<{
    type: "existing";
    colName: string;
    field: "name" | "type" | "default";
  } | null>(null);

  useEffect(() => {
    if (showPreview) {
      const frame = window.requestAnimationFrame(() => {
        const dialog = dialogRef.current;
        const preview = previewRef.current;
        if (!dialog || !preview) return;
        dialog.scrollTo({
          top: Math.max(0, preview.offsetTop - 16),
          behavior: "smooth",
        });
      });
      return () => window.cancelAnimationFrame(frame);
    }
  }, [showPreview]);

  useEffect(() => {
    setTableNameLocal(tableName);
  }, [tableName]);

  useEffect(() => {
    let cancelled = false;
    const loadColumns = async () => {
      setLoading(true);
      setError(null);
      try {
        const cols = await api.listColumns(
          connectionId,
          database,
          schema,
          tableName
        );
        if (cancelled) return;
        setColumns(cols);
      } catch (e) {
        if (cancelled) return;
        setError(String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    loadColumns();
    return () => { cancelled = true; };
  }, [connectionId, database, schema, tableName]);

  const effectiveState = applyOperations(columns, operations);
  const droppedColumnNames = new Set(
    operations.filter((o) => o.op === "drop_column" && o.column_name).map((o) => o.column_name!)
  );

  const isColumnDropped = (colName: string) => {
    const eff = effectiveState.get(colName);
    return eff ? droppedColumnNames.has(eff.name) : false;
  };

  const addOperation = (op: AlterTableOperation) => {
    setOperations((prev) => [...prev, op]);
  };

  const addOrUpdateOp = (
    predicate: (o: AlterTableOperation) => boolean,
    newOp: AlterTableOperation
  ) => {
    setOperations((prev) => {
      const idx = prev.findIndex(predicate);
      if (idx >= 0) {
        const next = [...prev];
        next[idx] = newOp;
        return next;
      }
      return [...prev, newOp];
    });
  };

  const addColumn = () => {
    setPendingNewColumns((prev) => [
      ...prev,
      {
        name: "",
        data_type: "text",
        is_nullable: true,
        default_value: "",
      },
    ]);
  };

  const updatePendingColumn = (idx: number, field: keyof PendingNewColumn, value: string | boolean) => {
    setPendingNewColumns((prev) => {
      const next = [...prev];
      next[idx] = { ...next[idx], [field]: value };
      return next;
    });
  };

  const removePendingColumn = (idx: number) => {
    setPendingNewColumns((prev) => prev.filter((_, i) => i !== idx));
  };

  const markDropColumn = (colName: string) => {
    const effectiveName = getEffectiveName(colName);
    if (droppedColumnNames.has(effectiveName)) {
      setOperations((prev) =>
        prev.filter(
          (o) =>
            !(o.op === "drop_column" && o.column_name === effectiveName)
        )
      );
    } else {
      addOperation({ op: "drop_column", column_name: effectiveName });
    }
  };

  const getEffectiveName = (colName: string): string => {
    const renameOp = operations.find(
      (o) => o.op === "rename_column" && o.old_name === colName
    );
    return renameOp?.new_name ?? colName;
  };

  const handleRenameColumn = (oldName: string, newName: string) => {
    if (newName.trim() === oldName) return;
    addOrUpdateOp(
      (o) => o.op === "rename_column" && o.old_name === oldName,
      { op: "rename_column", old_name: oldName, new_name: newName.trim() }
    );
  };

  const handleChangeType = (colName: string, newType: string) => {
    const effectiveName = getEffectiveName(colName);
    addOrUpdateOp(
      (o) => o.op === "change_type" && o.column_name === effectiveName,
      { op: "change_type", column_name: effectiveName, new_type: newType }
    );
  };

  const handleSetNullable = (colName: string, nullable: boolean) => {
    const effectiveName = getEffectiveName(colName);
    addOrUpdateOp(
      (o) => o.op === "set_nullable" && o.column_name === effectiveName,
      { op: "set_nullable", column_name: effectiveName, nullable }
    );
  };

  const handleSetDefault = (colName: string, value: string | null) => {
    const effectiveName = getEffectiveName(colName);
    addOrUpdateOp(
      (o) => o.op === "set_default" && o.column_name === effectiveName,
      { op: "set_default", column_name: effectiveName, default_value: value || null }
    );
  };

  const commitPendingNewColumns = (): AlterTableOperation[] => {
    const addOps: AlterTableOperation[] = pendingNewColumns
      .filter((c) => c.name.trim())
      .map((c) => ({
        op: "add_column" as const,
        column: {
          name: c.name.trim(),
          data_type: c.data_type,
          is_nullable: c.is_nullable,
          is_primary_key: false,
          default_value: c.default_value || null,
        },
      }));
    return addOps;
  };

  const handleApply = async () => {
    setApplying(true);
    setError(null);

    let allOps: AlterTableOperation[] = [...operations, ...commitPendingNewColumns()];

    if (tableNameLocal.trim() !== tableName) {
      allOps.unshift({
        op: "rename_table",
        new_name: tableNameLocal.trim(),
      });
    }

    allOps = reorderOperations(allOps);

    if (allOps.length === 0) {
      setError("No changes to apply");
      setApplying(false);
      return;
    }

    try {
      await api.alterTable({
        connection_id: connectionId,
        database,
        schema,
        table_name: tableName,
        operations: allOps,
      });

      if (tableNameLocal.trim() !== tableName) {
        // Table was renamed - caller may need to refresh with new name
      }
      onAltered();
      onClose();
    } catch (e) {
      setError(String(e));
    } finally {
      setApplying(false);
    }
  };

  const previewSql = generatePreviewSql(
    schema,
    tableName,
    tableNameLocal !== tableName ? tableNameLocal : null,
    operations,
    pendingNewColumns
  );

  const hasChanges =
    operations.length > 0 ||
    pendingNewColumns.some((c) => c.name.trim()) ||
    tableNameLocal.trim() !== tableName;

  const pendingAddCount = pendingNewColumns.filter((c) => c.name.trim()).length;
  const pendingChangeCount =
    operations.length + pendingAddCount + (tableNameLocal.trim() !== tableName ? 1 : 0);
  const modifiedExistingCount = columns.filter((col) => {
    const eff = effectiveState.get(col.name);
    if (!eff) return false;
    return (
      isColumnDropped(col.name) ||
      eff.name !== col.name ||
      eff.type !== col.data_type ||
      eff.nullable !== col.is_nullable ||
      (eff.default ?? null) !== (col.default_value ?? null)
    );
  }).length;

  if (loading) {
    return (
      <div className="dialog-overlay" onClick={onClose}>
        <div
          className="dialog alter-table-dialog"
          ref={dialogRef}
          onClick={(e) => e.stopPropagation()}
        >
          <div className="dialog-header">
            <h2>Alter Table</h2>
            <button className="btn-icon" onClick={onClose}>
              <X size={18} />
            </button>
          </div>
          <div className="dialog-body dialog-loading">
            <Loader2 size={24} className="spin" />
            <span>Loading columns…</span>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="dialog-overlay" onClick={onClose}>
      <div
        className="dialog alter-table-dialog"
        ref={dialogRef}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="dialog-header">
          <h2>Alter Table</h2>
          <button className="btn-icon" onClick={onClose}>
            <X size={18} />
          </button>
        </div>

        <div className="dialog-body">
          <div className="alter-table-summary">
            <div className="alter-table-summary-main">
              <div className="alter-table-summary-title">Review changes before applying</div>
              <div className="alter-table-summary-path">
                {database}.{schema}.{tableName}
              </div>
              <div className="alter-table-summary-note">
                Double-click an existing column name, type, or default to edit it inline.
              </div>
            </div>
            <div className="alter-table-summary-badges">
              <span className="alter-table-badge">{columns.length} existing columns</span>
              <span className="alter-table-badge">
                {modifiedExistingCount} modified
              </span>
              <span
                className={`alter-table-badge ${
                  pendingChangeCount > 0 ? "alter-table-badge--pending" : ""
                }`}
              >
                {pendingChangeCount} pending {pendingChangeCount === 1 ? "change" : "changes"}
              </span>
            </div>
          </div>

          <div className="form-row">
            <div className="form-group flex-1">
              <label>Schema</label>
              <input value={`${database}.${schema}`} disabled />
            </div>
            <div className="form-group flex-1">
              <label>Table Name</label>
              <input
                value={tableNameLocal}
                onChange={(e) => setTableNameLocal(e.target.value)}
                placeholder="table_name"
              />
            </div>
          </div>

          <div className="form-group">
            <div className="alter-table-section-header">
              <label>Columns</label>
              <button className="btn-ghost alter-table-add-btn" onClick={addColumn}>
                <Plus size={14} /> Add Column
              </button>
            </div>
            <div className="alter-table-columns">
              <div className="alter-table-columns-header">
                <span style={{ flex: 2 }}>Name</span>
                <span style={{ flex: 2 }}>Type</span>
                <span style={{ width: 60, textAlign: "center" }}>Nullable</span>
                <span style={{ width: 40, textAlign: "center" }}>PK</span>
                <span style={{ flex: 1 }}>Default</span>
                <span style={{ width: 32 }}></span>
              </div>

              {pendingNewColumns.map((col, idx) => (
                <div key={`new-${idx}`} className="alter-table-column-row new-column">
                  <div style={{ flex: 2 }} className="alter-table-cell alter-table-cell--name">
                    <div className="alter-table-name-stack">
                      <input
                        value={col.name}
                        onChange={(e) =>
                          updatePendingColumn(idx, "name", e.target.value)
                        }
                        placeholder="column_name"
                      />
                    </div>
                  </div>
                  <select
                    style={{ flex: 2 }}
                    value={col.data_type}
                    onChange={(e) =>
                      updatePendingColumn(idx, "data_type", e.target.value)
                    }
                  >
                    {PG_TYPES.map((t) => (
                      <option key={t} value={t}>
                        {t}
                      </option>
                    ))}
                  </select>
                  <div
                    style={{
                      width: 60,
                      display: "flex",
                      justifyContent: "center",
                    }}
                  >
                    <input
                      className="alter-table-checkbox"
                      type="checkbox"
                      checked={col.is_nullable}
                      onChange={(e) =>
                        updatePendingColumn(idx, "is_nullable", e.target.checked)
                      }
                    />
                  </div>
                  <div style={{ width: 40 }} />
                  <input
                    style={{ flex: 1 }}
                    value={col.default_value}
                    onChange={(e) =>
                      updatePendingColumn(idx, "default_value", e.target.value)
                    }
                    placeholder="default"
                  />
                  <button
                    className="btn-icon"
                    onClick={() => removePendingColumn(idx)}
                  >
                    <X size={12} />
                  </button>
                </div>
              ))}

              {columns.map((col) => {
                const dropped = isColumnDropped(col.name);
                const eff = effectiveState.get(col.name);
                if (!eff) return null;
                const rowChanged =
                  dropped ||
                  eff.name !== col.name ||
                  eff.type !== col.data_type ||
                  eff.nullable !== col.is_nullable ||
                  (eff.default ?? null) !== (col.default_value ?? null);

                const isEditingName =
                  editingCell?.type === "existing" &&
                  editingCell.colName === col.name &&
                  editingCell.field === "name";
                const isEditingType =
                  editingCell?.type === "existing" &&
                  editingCell.colName === col.name &&
                  editingCell.field === "type";
                const isEditingDefault =
                  editingCell?.type === "existing" &&
                  editingCell.colName === col.name &&
                  editingCell.field === "default";

                return (
                  <div
                    key={col.name}
                    className={`alter-table-column-row ${dropped ? "dropped" : ""} ${
                      rowChanged && !dropped ? "has-changes" : ""
                    }`}
                  >
                    <div style={{ flex: 2 }} className="alter-table-cell alter-table-cell--name">
                      {isEditingName ? (
                        <input
                          autoFocus
                          defaultValue={eff.name}
                          onBlur={(e) => {
                            handleRenameColumn(col.name, e.target.value);
                            setEditingCell(null);
                          }}
                          onKeyDown={(e) => {
                            if (e.key === "Enter") {
                              (e.target as HTMLInputElement).blur();
                            } else if (e.key === "Escape") {
                              setEditingCell(null);
                            }
                          }}
                        />
                      ) : (
                        <div className="alter-table-name-stack">
                          <span
                            className="editable-cell"
                            title="Double-click to rename column"
                            onDoubleClick={() =>
                              setEditingCell({
                                type: "existing",
                                colName: col.name,
                                field: "name",
                              })
                            }
                          >
                            {eff.name}
                          </span>
                          {dropped ? (
                            <span className="alter-table-row-badge alter-table-row-badge--dropped">
                              Dropped
                            </span>
                          ) : rowChanged ? (
                            <span className="alter-table-row-badge alter-table-row-badge--changed">
                              Modified
                            </span>
                          ) : null}
                        </div>
                      )}
                    </div>
                    <div style={{ flex: 2 }} className="alter-table-cell">
                      {isEditingType ? (
                        <select
                          autoFocus
                          defaultValue={eff.type}
                          onBlur={(e) => {
                            handleChangeType(col.name, e.target.value);
                            setEditingCell(null);
                          }}
                          onKeyDown={(e) => {
                            if (e.key === "Escape") setEditingCell(null);
                          }}
                        >
                          {PG_TYPES.map((t) => (
                            <option key={t} value={t}>
                              {t}
                            </option>
                          ))}
                        </select>
                      ) : (
                        <span
                          className="editable-cell"
                          title="Double-click to change type"
                          onDoubleClick={() =>
                            setEditingCell({
                              type: "existing",
                              colName: col.name,
                              field: "type",
                            })
                          }
                        >
                          {eff.type}
                        </span>
                      )}
                    </div>
                    <div
                      style={{ width: 60, display: "flex", justifyContent: "center" }}
                    >
                      <input
                        className="alter-table-checkbox"
                        type="checkbox"
                        checked={eff.nullable}
                        onChange={(e) =>
                          handleSetNullable(col.name, e.target.checked)
                        }
                        disabled={dropped}
                      />
                    </div>
                    <div
                      style={{
                        width: 40,
                        display: "flex",
                        justifyContent: "center",
                      }}
                    >
                      {col.is_primary_key ? (
                        <span className="alter-table-pk-badge">PK</span>
                      ) : null}
                    </div>
                    <div style={{ flex: 1 }} className="alter-table-cell">
                      {isEditingDefault ? (
                        <input
                          autoFocus
                          defaultValue={eff.default ?? ""}
                          placeholder="null"
                          onBlur={(e) => {
                            const v = e.target.value.trim();
                            handleSetDefault(col.name, v || null);
                            setEditingCell(null);
                          }}
                          onKeyDown={(e) => {
                            if (e.key === "Enter") (e.target as HTMLInputElement).blur();
                            else if (e.key === "Escape") setEditingCell(null);
                          }}
                        />
                      ) : (
                        <span
                          className="editable-cell"
                          title="Double-click to edit default value"
                          onDoubleClick={() =>
                            setEditingCell({
                              type: "existing",
                              colName: col.name,
                              field: "default",
                            })
                          }
                        >
                          {eff.default ?? "—"}
                        </span>
                      )}
                    </div>
                    <button
                      className="btn-icon drop-column-btn"
                      onClick={() => markDropColumn(col.name)}
                      title={dropped ? "Undo drop" : "Drop column"}
                    >
                      <X size={12} />
                    </button>
                  </div>
                );
              })}

            </div>
          </div>

          {showPreview && (
            <div className="ddl-preview" ref={previewRef}>
              <div className="ddl-preview-label">Generated SQL</div>
              <pre>{previewSql}</pre>
            </div>
          )}

          {error && (
            <div className="test-result error">{error}</div>
          )}
        </div>

        <div className="dialog-footer">
          <button
            className={`btn-ghost ${showPreview ? "active-filter" : ""}`}
            onClick={() => setShowPreview(!showPreview)}
          >
            {showPreview ? <EyeOff size={14} /> : <Eye size={14} />}
            {showPreview ? "Hide SQL" : "Preview SQL"}
          </button>
          <div className="dialog-footer-right">
            <button className="btn-secondary" onClick={onClose}>
              Cancel
            </button>
            <button
              className="btn-primary"
              onClick={handleApply}
              disabled={applying || !hasChanges}
            >
              {applying && <Loader2 size={14} className="spin" />}
              Apply Changes
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
