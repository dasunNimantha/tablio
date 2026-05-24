import { useState, useEffect, useMemo, useRef, useCallback } from "react";
import { api, AlterTableOperation, ColumnInfo } from "../../lib/tauri";
import { X, Plus, Loader2, Eye, EyeOff, Search } from "lucide-react";
import {
  applyOperations,
  generatePreviewSql,
  reorderOperations,
  PG_TYPES,
  type PendingNewColumn,
} from "./operations";
import {
  useAlterTableDraftStore,
  draftKey as makeDraftKey,
} from "../../stores/alterTableDraftStore";

/**
 * Variant-controlled rendering for the editor.
 *
 * The component body (column list, filter, summary, SQL preview, etc.)
 * is identical across both surfaces — only the chrome surrounding it
 * and the footer/toolbar shape differ:
 *
 * - **modal**: classic dialog footer (`.dialog-footer` with Preview /
 *   Cancel / Apply). All existing E2E selectors live in this branch.
 * - **inline**: in-tab Schema view. The Preview button becomes a
 *   pill at the top, the Cancel button becomes "Discard", and the
 *   whole thing has a sticky bottom toolbar so the Apply CTA stays
 *   visible while the user scrolls a long column list.
 *
 * No keyboard / focus differences between variants.
 */
export type AlterTableEditorVariant = "modal" | "inline";

interface Props {
  connectionId: string;
  database: string;
  schema: string;
  tableName: string;
  /**
   * Columns provided by the parent. If supplied, the editor uses
   * them directly and skips the `api.listColumns` fetch — useful
   * for the in-tab variant where `TableView` has already loaded
   * the metadata. If omitted, the editor fetches its own copy
   * (this is how the modal entry works today).
   */
  initialColumns?: ColumnInfo[];
  /** Fired after a successful `api.alterTable` save. */
  onSaved: () => void;
  /** Fired on user-initiated Discard / Cancel. */
  onDiscard: () => void;
  variant: AlterTableEditorVariant;
}

export function AlterTableEditor({
  connectionId,
  database,
  schema,
  tableName,
  initialColumns,
  onSaved,
  onDiscard,
  variant,
}: Props) {
  // ---------------------------------------------------------------
  // Per-table draft persistence (issue #59).
  //
  // The "session-of-truth" for the editor's pending state lives in
  // the Zustand store keyed by (connection, db, schema, table). The
  // local `useState` hooks below hydrate FROM the store on mount and
  // write back through `setDraft` on every change, so closing the
  // modal, switching tabs, or remounting via the in-tab variant all
  // pick up where the user left off.
  // ---------------------------------------------------------------
  const draftKey = makeDraftKey(connectionId, database, schema, tableName);
  const draftFromStore = useAlterTableDraftStore((s) => s.drafts[draftKey]);
  const setDraft = useAlterTableDraftStore((s) => s.setDraft);
  const clearDraft = useAlterTableDraftStore((s) => s.clearDraft);

  // ---------------------------------------------------------------
  // Local state. Hydrated from the persisted draft on first mount
  // (falling back to defaults when there's no draft yet) and pushed
  // back via small effects further down so the store always
  // reflects the latest user input.
  // ---------------------------------------------------------------
  const [columns, setColumns] = useState<ColumnInfo[]>(initialColumns ?? []);
  // Skip the loading spinner when the parent supplied the columns;
  // we have nothing to wait on.
  const [loading, setLoading] = useState(!initialColumns);
  const [applying, setApplying] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [tableNameLocal, setTableNameLocal] = useState(
    draftFromStore?.tableNameLocal || tableName,
  );
  const [operations, setOperations] = useState<AlterTableOperation[]>(
    draftFromStore?.operations ?? [],
  );
  const [pendingNewColumns, setPendingNewColumns] = useState<PendingNewColumn[]>(
    draftFromStore?.pendingNewColumns ?? [],
  );
  const [showPreview, setShowPreview] = useState(
    draftFromStore?.showPreview ?? false,
  );
  const [columnFilter, setColumnFilter] = useState(
    draftFromStore?.columnFilter ?? "",
  );

  // Transient — never persisted. Which inline cell is in edit mode.
  // The type column doesn't appear here because it's always rendered
  // as a select (no double-click toggle).
  const [editingCell, setEditingCell] = useState<{
    type: "existing";
    colName: string;
    field: "name" | "default";
  } | null>(null);

  const containerRef = useRef<HTMLDivElement>(null);
  const previewRef = useRef<HTMLDivElement>(null);

  // ---------------------------------------------------------------
  // Effects
  // ---------------------------------------------------------------

  // Initial column fetch — only when the parent didn't pre-supply.
  useEffect(() => {
    if (initialColumns) {
      setColumns(initialColumns);
      setLoading(false);
      return;
    }
    let cancelled = false;
    const load = async () => {
      setLoading(true);
      setError(null);
      try {
        const cols = await api.listColumns(
          connectionId,
          database,
          schema,
          tableName,
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
    load();
    return () => {
      cancelled = true;
    };
  }, [connectionId, database, schema, tableName, initialColumns]);

  // Keep `tableNameLocal` in sync when the prop changes (e.g. the
  // table was renamed via another flow). Only does so when there's
  // no in-flight rename queued, otherwise we'd clobber the user's
  // typed value.
  useEffect(() => {
    if (!draftFromStore?.tableNameLocal) {
      setTableNameLocal(tableName);
    }
  }, [tableName, draftFromStore?.tableNameLocal]);

  // Push every state change back into the persistent draft store
  // so closing + reopening picks up exactly where the user left off.
  // We deliberately push the *current* tableNameLocal / operations /
  // etc. rather than chasing the store via subscription — the store
  // is the persistence side of a one-way pipe.
  useEffect(() => {
    setDraft(draftKey, {
      tableNameLocal,
      operations,
      pendingNewColumns,
      showPreview,
      columnFilter,
    });
  }, [
    draftKey,
    tableNameLocal,
    operations,
    pendingNewColumns,
    showPreview,
    columnFilter,
    setDraft,
  ]);

  // SQL-preview-scroll-into-view, same as the original dialog.
  useEffect(() => {
    if (showPreview) {
      const frame = window.requestAnimationFrame(() => {
        const container = containerRef.current;
        const preview = previewRef.current;
        if (!container || !preview) return;
        container.scrollTo({
          top: Math.max(0, preview.offsetTop - 16),
          behavior: "smooth",
        });
      });
      return () => window.cancelAnimationFrame(frame);
    }
  }, [showPreview]);

  // ---------------------------------------------------------------
  // Derived state
  // ---------------------------------------------------------------

  // Column-name filter (#60). Pass through identity when the filter
  // is empty so we don't re-key every row on every keystroke.
  const filteredExistingColumns = useMemo(() => {
    const q = columnFilter.trim().toLowerCase();
    if (!q) return columns;
    return columns.filter((c) => c.name.toLowerCase().includes(q));
  }, [columns, columnFilter]);

  const effectiveState = applyOperations(columns, operations);
  const droppedColumnNames = new Set(
    operations
      .filter((o) => o.op === "drop_column" && o.column_name)
      .map((o) => o.column_name!),
  );

  const isColumnDropped = useCallback(
    (colName: string) => {
      const eff = effectiveState.get(colName);
      return eff ? droppedColumnNames.has(eff.name) : false;
    },
    [effectiveState, droppedColumnNames],
  );

  const previewSql = generatePreviewSql(
    schema,
    tableName,
    tableNameLocal !== tableName ? tableNameLocal : null,
    operations,
    pendingNewColumns,
  );

  const hasChanges =
    operations.length > 0 ||
    pendingNewColumns.some((c) => c.name.trim()) ||
    tableNameLocal.trim() !== tableName;

  const pendingAddCount = pendingNewColumns.filter((c) => c.name.trim()).length;
  const pendingChangeCount =
    operations.length +
    pendingAddCount +
    (tableNameLocal.trim() !== tableName ? 1 : 0);
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

  // ---------------------------------------------------------------
  // Mutators
  // ---------------------------------------------------------------

  const addOperation = (op: AlterTableOperation) => {
    setOperations((prev) => [...prev, op]);
  };

  const addOrUpdateOp = (
    predicate: (o: AlterTableOperation) => boolean,
    newOp: AlterTableOperation,
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
      { name: "", data_type: "text", is_nullable: true, default_value: "" },
    ]);
  };

  const updatePendingColumn = (
    idx: number,
    field: keyof PendingNewColumn,
    value: string | boolean,
  ) => {
    setPendingNewColumns((prev) => {
      const next = [...prev];
      next[idx] = { ...next[idx], [field]: value };
      return next;
    });
  };

  const removePendingColumn = (idx: number) => {
    setPendingNewColumns((prev) => prev.filter((_, i) => i !== idx));
  };

  const getEffectiveName = (colName: string): string => {
    const renameOp = operations.find(
      (o) => o.op === "rename_column" && o.old_name === colName,
    );
    return renameOp?.new_name ?? colName;
  };

  const markDropColumn = (colName: string) => {
    const effectiveName = getEffectiveName(colName);
    if (droppedColumnNames.has(effectiveName)) {
      setOperations((prev) =>
        prev.filter(
          (o) => !(o.op === "drop_column" && o.column_name === effectiveName),
        ),
      );
    } else {
      addOperation({ op: "drop_column", column_name: effectiveName });
    }
  };

  const handleRenameColumn = (oldName: string, newName: string) => {
    if (newName.trim() === oldName) return;
    addOrUpdateOp(
      (o) => o.op === "rename_column" && o.old_name === oldName,
      { op: "rename_column", old_name: oldName, new_name: newName.trim() },
    );
  };

  const handleChangeType = (colName: string, newType: string) => {
    const effectiveName = getEffectiveName(colName);
    addOrUpdateOp(
      (o) => o.op === "change_type" && o.column_name === effectiveName,
      { op: "change_type", column_name: effectiveName, new_type: newType },
    );
  };

  const handleSetNullable = (colName: string, nullable: boolean) => {
    const effectiveName = getEffectiveName(colName);
    addOrUpdateOp(
      (o) => o.op === "set_nullable" && o.column_name === effectiveName,
      { op: "set_nullable", column_name: effectiveName, nullable },
    );
  };

  const handleSetDefault = (colName: string, value: string | null) => {
    const effectiveName = getEffectiveName(colName);
    addOrUpdateOp(
      (o) => o.op === "set_default" && o.column_name === effectiveName,
      {
        op: "set_default",
        column_name: effectiveName,
        default_value: value || null,
      },
    );
  };

  const commitPendingNewColumns = (): AlterTableOperation[] =>
    pendingNewColumns
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

  const handleApply = async () => {
    setApplying(true);
    setError(null);

    let allOps: AlterTableOperation[] = [
      ...operations,
      ...commitPendingNewColumns(),
    ];

    if (tableNameLocal.trim() !== tableName) {
      allOps.unshift({ op: "rename_table", new_name: tableNameLocal.trim() });
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
      // Wipe the persisted draft now that the changes are on the
      // server; otherwise the next open of the same table would
      // show stale "pending" rows.
      clearDraft(draftKey);
      onSaved();
    } catch (e) {
      setError(String(e));
    } finally {
      setApplying(false);
    }
  };

  const handleDiscard = () => {
    // Discard wipes the persisted draft too — the user is
    // explicitly saying "throw away my pending edits".
    clearDraft(draftKey);
    onDiscard();
  };

  // ---------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------

  if (loading) {
    return (
      <div className="alter-table-editor alter-table-editor--loading" data-variant={variant}>
        <Loader2 size={24} className="spin" />
        <span>Loading columns…</span>
      </div>
    );
  }

  // The body block is identical for both variants — only the
  // surrounding chrome (caller's responsibility for "modal", or our
  // own toolbar for "inline") differs.
  const body = (
    <>
      <div className="alter-table-summary">
        <div className="alter-table-summary-main">
          <div className="alter-table-summary-title">
            Review changes before applying
          </div>
          <div className="alter-table-summary-path">
            {database}.{schema}.{tableName}
          </div>
          <div className="alter-table-summary-note">
            Click a column type to change it, or double-click a name
            or default value to edit it inline.
          </div>
        </div>
        <div className="alter-table-summary-badges">
          <span className="alter-table-badge">
            {columns.length} existing columns
          </span>
          <span className="alter-table-badge">
            {modifiedExistingCount} modified
          </span>
          <span
            className={`alter-table-badge ${
              pendingChangeCount > 0 ? "alter-table-badge--pending" : ""
            }`}
          >
            {pendingChangeCount} pending{" "}
            {pendingChangeCount === 1 ? "change" : "changes"}
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

        {/* Column-name filter (#60). */}
        <div className="alter-table-columns-filter">
          <Search size={13} className="alter-table-columns-filter-icon" />
          <input
            type="text"
            className="alter-table-columns-filter-input"
            value={columnFilter}
            onChange={(e) => setColumnFilter(e.target.value)}
            placeholder="Search columns..."
            aria-label="Filter columns by name"
          />
          {columnFilter && (
            <button
              type="button"
              className="btn-icon alter-table-columns-filter-clear"
              onClick={() => setColumnFilter("")}
              aria-label="Clear column filter"
            >
              <X size={13} />
            </button>
          )}
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
            <div
              key={`new-${idx}`}
              className="alter-table-column-row new-column"
            >
              <div
                style={{ flex: 2 }}
                className="alter-table-cell alter-table-cell--name"
              >
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

          {columns.length > 0 && filteredExistingColumns.length === 0 && (
            <div className="alter-table-columns-empty">
              No columns match &ldquo;{columnFilter}&rdquo;
            </div>
          )}

          {filteredExistingColumns.map((col) => {
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
            // The type column is always rendered as a select (no
            // double-click-to-edit dance). The PG_TYPES list is
            // curated but doesn't cover every possible value the
            // server might return (e.g. `varchar(50)`,
            // `numeric(10,2)`, custom domain types). If the current
            // effective type isn't in the catalog we prepend it so
            // the select has a valid `value` to display.
            const typeOptions =
              PG_TYPES.includes(eff.type) || !eff.type
                ? PG_TYPES
                : [eff.type, ...PG_TYPES];
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
                <div
                  style={{ flex: 2 }}
                  className="alter-table-cell alter-table-cell--name"
                >
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
                  {/* Type cell: always a select. A single click opens
                   *  the native dropdown — no double-click required.
                   *  Disabled while the row is queued for drop so the
                   *  user can't queue a contradictory type change. */}
                  <select
                    className="alter-table-type-select"
                    value={eff.type}
                    onChange={(e) => handleChangeType(col.name, e.target.value)}
                    disabled={dropped}
                  >
                    {typeOptions.map((t) => (
                      <option key={t} value={t}>
                        {t}
                      </option>
                    ))}
                  </select>
                </div>
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
                        if (e.key === "Enter")
                          (e.target as HTMLInputElement).blur();
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

      {error && <div className="test-result error">{error}</div>}
    </>
  );

  if (variant === "modal") {
    // The modal wrapper renders the dialog-overlay and -header; this
    // component only owns the body + footer so the dialog's chrome
    // stays under AlterTableDialog's control.
    return (
      <>
        <div className="dialog-body" ref={containerRef}>
          {body}
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
            <button className="btn-secondary" onClick={handleDiscard}>
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
      </>
    );
  }

  // variant === "inline" — in-tab Schema view. Sticky toolbar so the
  // Apply CTA stays visible on long column lists.
  return (
    <div className="alter-table-editor-inline" ref={containerRef}>
      <div className="alter-table-inline-body">{body}</div>
      <div className="alter-table-inline-toolbar">
        <button
          className={`btn-ghost ${showPreview ? "active-filter" : ""}`}
          onClick={() => setShowPreview(!showPreview)}
        >
          {showPreview ? <EyeOff size={14} /> : <Eye size={14} />}
          {showPreview ? "Hide SQL" : "Preview SQL"}
        </button>
        <div className="alter-table-inline-toolbar-right">
          <button
            className="btn-secondary alter-table-inline-discard"
            onClick={handleDiscard}
          >
            Discard
          </button>
          <button
            className="btn-primary alter-table-inline-apply"
            onClick={handleApply}
            disabled={applying || !hasChanges}
          >
            {applying && <Loader2 size={14} className="spin" />}
            Apply Changes
          </button>
        </div>
      </div>
    </div>
  );
}
