import { useEffect, useState, useCallback, useRef } from "react";
import {
  api,
  ColumnInfo,
  SortSpec,
  TableData,
  DataChanges,
  CellChange,
  NewRow,
  DeleteRow,
} from "../../lib/tauri";
import { EditableCell } from "./EditableCell";
import { FilterBar } from "./FilterBar";
import { RowDetailView } from "./RowDetailView";
import { ExportMenu } from "../ExportMenu";
import {
  Save,
  Undo2,
  Plus,
  Trash2,
  ChevronLeft,
  ChevronRight,
  ChevronsLeft,
  ChevronsRight,
  ArrowUp,
  ArrowDown,
  Loader2,
  Filter,
  Copy,
} from "lucide-react";
import "./DataGrid.css";

interface Props {
  connectionId: string;
  database: string;
  schema: string;
  table: string;
}

interface PendingChanges {
  updates: Map<string, CellChange>;
  inserts: NewRow[];
  deletedKeys: Set<string>;
}

const PAGE_SIZE = 100;

export function DataGrid({ connectionId, database, schema, table }: Props) {
  const [data, setData] = useState<TableData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState(0);
  const [sort, setSort] = useState<SortSpec | null>(null);
  const [saving, setSaving] = useState(false);
  const [showFilter, setShowFilter] = useState(false);
  const [activeFilter, setActiveFilter] = useState<string | null>(null);

  const [changes, setChanges] = useState<PendingChanges>({
    updates: new Map(),
    inserts: [],
    deletedKeys: new Set(),
  });

  const [editingRows, setEditingRows] = useState<unknown[][]>([]);
  const [columnWidths, setColumnWidths] = useState<Record<string, number>>({});
  const [detailRowIdx, setDetailRowIdx] = useState<number | null>(null);
  const tableRef = useRef<HTMLDivElement>(null);
  const resizingRef = useRef<{ col: string; startX: number; startWidth: number } | null>(null);

  const hasChanges =
    changes.updates.size > 0 ||
    changes.inserts.length > 0 ||
    changes.deletedKeys.size > 0;

  const fetchData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const result = await api.fetchRows({
        connection_id: connectionId,
        database,
        schema,
        table,
        offset: page * PAGE_SIZE,
        limit: PAGE_SIZE,
        sort,
        filter: activeFilter,
      });
      setData(result);
      setEditingRows(result.rows.map((r) => [...r]));
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, [connectionId, database, schema, table, page, sort, activeFilter]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const totalPages = data ? Math.ceil(data.total_rows / PAGE_SIZE) : 0;

  const handleSort = (column: string) => {
    setSort((prev) => {
      if (prev?.column === column) {
        if (prev.direction === "asc") return { column, direction: "desc" };
        return null;
      }
      return { column, direction: "asc" };
    });
    setPage(0);
  };

  const getPkValues = (rowIndex: number): [string, unknown][] => {
    if (!data) return [];
    return data.columns
      .filter((c) => c.is_primary_key)
      .map((c) => {
        const colIdx = data.columns.findIndex((col) => col.name === c.name);
        return [c.name, data.rows[rowIndex][colIdx]] as [string, unknown];
      });
  };

  const getPkKey = (rowIndex: number): string => {
    return getPkValues(rowIndex)
      .map(([, v]) => String(v))
      .join(":");
  };

  const handleCellChange = (
    rowIndex: number,
    colIndex: number,
    newValue: unknown
  ) => {
    if (!data) return;
    const col = data.columns[colIndex];
    const oldValue = data.rows[rowIndex][colIndex];
    const pkValues = getPkValues(rowIndex);
    const changeKey = `${getPkKey(rowIndex)}:${col.name}`;

    setChanges((prev) => {
      const next = {
        ...prev,
        updates: new Map(prev.updates),
      };
      if (JSON.stringify(oldValue) === JSON.stringify(newValue)) {
        next.updates.delete(changeKey);
      } else {
        next.updates.set(changeKey, {
          row_index: rowIndex,
          column_name: col.name,
          old_value: oldValue,
          new_value: newValue,
          primary_key_values: pkValues,
        });
      }
      return next;
    });

    setEditingRows((prev) => {
      const next = prev.map((r) => [...r]);
      next[rowIndex][colIndex] = newValue;
      return next;
    });
  };

  const handleAddRow = () => {
    if (!data) return;
    const emptyRow: unknown[] = data.columns.map(() => null);
    setEditingRows((prev) => [...prev, emptyRow]);
    const values: [string, unknown][] = data.columns.map((c) => [c.name, null]);
    setChanges((prev) => ({
      ...prev,
      inserts: [...prev.inserts, { values }],
    }));
  };

  const handleDeleteRow = (rowIndex: number) => {
    const pkKey = getPkKey(rowIndex);
    setChanges((prev) => {
      const next = { ...prev, deletedKeys: new Set(prev.deletedKeys) };
      if (next.deletedKeys.has(pkKey)) {
        next.deletedKeys.delete(pkKey);
      } else {
        next.deletedKeys.add(pkKey);
      }
      return next;
    });
  };

  const handleSave = async () => {
    if (!data || !hasChanges) return;
    setSaving(true);
    try {
      const allChanges: DataChanges = {
        connection_id: connectionId,
        database,
        schema,
        table,
        updates: Array.from(changes.updates.values()),
        inserts: changes.inserts.map((ins) => {
          const lastRowIdx = editingRows.length - changes.inserts.length;
          const insertIdx = changes.inserts.indexOf(ins);
          const row = editingRows[lastRowIdx + insertIdx];
          return {
            values: data.columns
              .map((c, ci): [string, unknown] => [c.name, row[ci]])
              .filter(([, v]) => v !== null),
          };
        }),
        deletes: Array.from(changes.deletedKeys).map((key) => {
          const parts = key.split(":");
          const pkCols = data.columns.filter((c) => c.is_primary_key);
          const pkValues: [string, unknown][] = pkCols.map((c, i) => [
            c.name,
            parts[i],
          ]);
          return { primary_key_values: pkValues } as DeleteRow;
        }),
      };
      await api.applyChanges(allChanges);
      setChanges({ updates: new Map(), inserts: [], deletedKeys: new Set() });
      await fetchData();
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  const handleDiscard = () => {
    setChanges({ updates: new Map(), inserts: [], deletedKeys: new Set() });
    if (data) {
      setEditingRows(data.rows.map((r) => [...r]));
    }
  };

  const handleFilterApply = (filter: string | null) => {
    setActiveFilter(filter);
    setPage(0);
  };

  const handleExport = async (format: "csv" | "json" | "sql") => {
    try {
      const content = await api.exportTableData({
        connection_id: connectionId,
        database,
        schema,
        table,
        format,
        filter: activeFilter,
      });
      const ext = format === "sql" ? "sql" : format;
      const blob = new Blob([content], { type: "text/plain" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `${table}.${ext}`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (e) {
      setError(String(e));
    }
  };

  const handleCopyAsInsert = (rowIdx: number) => {
    if (!data) return;
    const row = editingRows[rowIdx];
    const cols = data.columns.map((c) => `"${c.name}"`).join(", ");
    const vals = row
      .map((v) => {
        if (v === null) return "NULL";
        if (typeof v === "number") return String(v);
        if (typeof v === "boolean") return v ? "TRUE" : "FALSE";
        return `'${String(v).replace(/'/g, "''")}'`;
      })
      .join(", ");
    const sql = `INSERT INTO "${schema}"."${table}" (${cols}) VALUES (${vals});`;
    navigator.clipboard.writeText(sql);
  };

  const handleCopyAllAsInsert = () => {
    if (!data) return;
    const cols = data.columns.map((c) => `"${c.name}"`).join(", ");
    const lines = editingRows.map((row) => {
      const vals = row
        .map((v) => {
          if (v === null) return "NULL";
          if (typeof v === "number") return String(v);
          if (typeof v === "boolean") return v ? "TRUE" : "FALSE";
          return `'${String(v).replace(/'/g, "''")}'`;
        })
        .join(", ");
      return `INSERT INTO "${schema}"."${table}" (${cols}) VALUES (${vals});`;
    });
    navigator.clipboard.writeText(lines.join("\n"));
  };

  const handleResizeStart = useCallback(
    (e: React.MouseEvent, colName: string, thEl: HTMLElement) => {
      e.preventDefault();
      e.stopPropagation();
      const startWidth = thEl.offsetWidth;
      resizingRef.current = { col: colName, startX: e.clientX, startWidth };

      const onMove = (ev: MouseEvent) => {
        if (!resizingRef.current) return;
        const diff = ev.clientX - resizingRef.current.startX;
        const newWidth = Math.max(60, resizingRef.current.startWidth + diff);
        setColumnWidths((prev) => ({ ...prev, [resizingRef.current!.col]: newWidth }));
      };

      const onUp = () => {
        resizingRef.current = null;
        document.removeEventListener("mousemove", onMove);
        document.removeEventListener("mouseup", onUp);
        document.body.style.cursor = "";
        document.body.style.userSelect = "";
      };

      document.body.style.cursor = "col-resize";
      document.body.style.userSelect = "none";
      document.addEventListener("mousemove", onMove);
      document.addEventListener("mouseup", onUp);
    },
    []
  );

  if (loading && !data) {
    return (
      <div className="grid-loading">
        <Loader2 size={24} className="spin" />
        <span>Loading data...</span>
      </div>
    );
  }

  if (error && !data) {
    return (
      <div className="grid-error">
        <p>{error}</p>
        <button className="btn-secondary" onClick={fetchData}>
          Retry
        </button>
      </div>
    );
  }

  if (!data) return null;

  const originalRowCount = data.rows.length;

  return (
    <div className="data-grid-container">
      <div className="grid-toolbar">
        <div className="grid-toolbar-left">
          <span className="grid-table-name">
            {schema}.{table}
          </span>
          <span className="grid-row-count">
            {data.total_rows.toLocaleString()} rows
          </span>
        </div>
        <div className="grid-toolbar-right">
          <button
            className={`btn-ghost ${showFilter || activeFilter ? "active-filter" : ""}`}
            onClick={() => setShowFilter(!showFilter)}
            title="Toggle Filter"
          >
            <Filter size={14} /> Filter{activeFilter ? " (active)" : ""}
          </button>
          <ExportMenu onExport={handleExport} />
          <button
            className="btn-ghost"
            onClick={handleCopyAllAsInsert}
            title="Copy all rows as INSERT statements"
          >
            <Copy size={14} /> Copy as SQL
          </button>
          <button
            className="btn-ghost"
            onClick={handleAddRow}
            title="Add Row"
          >
            <Plus size={14} /> Add Row
          </button>
          {hasChanges && (
            <>
              <button className="btn-ghost" onClick={handleDiscard}>
                <Undo2 size={14} /> Discard
              </button>
              <button
                className="btn-primary"
                onClick={handleSave}
                disabled={saving}
                style={{ display: "flex", alignItems: "center", gap: 6 }}
              >
                {saving ? <Loader2 size={14} className="spin" /> : <Save size={14} />}
                Save {changes.updates.size + changes.inserts.length + changes.deletedKeys.size} changes
              </button>
            </>
          )}
        </div>
      </div>

      {showFilter && data && (
        <FilterBar
          columns={data.columns}
          onApply={handleFilterApply}
          onClose={() => setShowFilter(false)}
        />
      )}

      {error && (
        <div className="grid-error-banner">{error}</div>
      )}

      <div className="grid-table-wrapper" ref={tableRef}>
        <table className="grid-table">
          <thead>
            <tr>
              <th className="grid-row-number">#</th>
              {data.columns.map((col) => (
                <th
                  key={col.name}
                  className={col.is_primary_key ? "pk" : ""}
                  style={columnWidths[col.name] ? { width: columnWidths[col.name], minWidth: columnWidths[col.name] } : undefined}
                  onClick={() => handleSort(col.name)}
                >
                  <div className="grid-header-content">
                    <span className="grid-header-name">
                      {col.is_primary_key && <span className="pk-badge">PK</span>}
                      {col.name}
                    </span>
                    <span className="grid-header-type">{col.data_type}</span>
                  </div>
                  {sort?.column === col.name && (
                    <span className="grid-sort-icon">
                      {sort.direction === "asc" ? (
                        <ArrowUp size={12} />
                      ) : (
                        <ArrowDown size={12} />
                      )}
                    </span>
                  )}
                  <div
                    className="col-resize-handle"
                    onMouseDown={(e) => {
                      const th = e.currentTarget.parentElement as HTMLElement;
                      handleResizeStart(e, col.name, th);
                    }}
                  />
                </th>
              ))}
              <th className="grid-actions-header">
                <Trash2 size={12} />
              </th>
            </tr>
          </thead>
          <tbody>
            {editingRows.map((row, rowIdx) => {
              const isInserted = rowIdx >= originalRowCount;
              const pkKey = !isInserted ? getPkKey(rowIdx) : "";
              const isDeleted = !isInserted && changes.deletedKeys.has(pkKey);

              return (
                <tr
                  key={rowIdx}
                  className={`
                    ${isDeleted ? "row-deleted" : ""}
                    ${isInserted ? "row-inserted" : ""}
                    ${detailRowIdx === rowIdx ? "row-selected" : ""}
                  `}
                  onClick={() => setDetailRowIdx(rowIdx)}
                >
                  <td className="grid-row-number">
                    {page * PAGE_SIZE + rowIdx + 1}
                  </td>
                  {row.map((value, colIdx) => {
                    const col = data.columns[colIdx];
                    const changeKey = !isInserted
                      ? `${pkKey}:${col.name}`
                      : null;
                    const isModified =
                      changeKey !== null && changes.updates.has(changeKey);

                    return (
                      <EditableCell
                        key={`${rowIdx}-${colIdx}`}
                        value={value}
                        column={col}
                        isModified={isModified}
                        isInserted={isInserted}
                        onChange={(newVal) =>
                          handleCellChange(rowIdx, colIdx, newVal)
                        }
                      />
                    );
                  })}
                  <td className="grid-actions-cell">
                    {!isInserted && (
                      <button
                        className="btn-icon"
                        onClick={(e) => { e.stopPropagation(); handleDeleteRow(rowIdx); }}
                        title={isDeleted ? "Undo Delete" : "Mark for Delete"}
                      >
                        {isDeleted ? (
                          <Undo2 size={12} />
                        ) : (
                          <Trash2 size={12} />
                        )}
                      </button>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <div className="grid-pagination">
        <div className="grid-pagination-info">
          Showing {page * PAGE_SIZE + 1} -{" "}
          {Math.min((page + 1) * PAGE_SIZE, data.total_rows)} of{" "}
          {data.total_rows.toLocaleString()}
        </div>
        <div className="grid-pagination-controls">
          <button
            className="btn-icon"
            disabled={page === 0}
            onClick={() => setPage(0)}
          >
            <ChevronsLeft size={16} />
          </button>
          <button
            className="btn-icon"
            disabled={page === 0}
            onClick={() => setPage((p) => p - 1)}
          >
            <ChevronLeft size={16} />
          </button>
          <span className="grid-page-info">
            Page {page + 1} of {totalPages || 1}
          </span>
          <button
            className="btn-icon"
            disabled={page >= totalPages - 1}
            onClick={() => setPage((p) => p + 1)}
          >
            <ChevronRight size={16} />
          </button>
          <button
            className="btn-icon"
            disabled={page >= totalPages - 1}
            onClick={() => setPage(totalPages - 1)}
          >
            <ChevronsRight size={16} />
          </button>
        </div>
      </div>

      {detailRowIdx !== null && data && editingRows[detailRowIdx] && (
        <RowDetailView
          columns={data.columns}
          row={editingRows[detailRowIdx]}
          rowIndex={detailRowIdx}
          onClose={() => setDetailRowIdx(null)}
          onCellChange={(colIndex, newValue) =>
            handleCellChange(detailRowIdx, colIndex, newValue)
          }
        />
      )}
    </div>
  );
}
