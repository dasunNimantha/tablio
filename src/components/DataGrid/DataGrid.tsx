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
  Timer,
  Search,
  X,
} from "lucide-react";
import { save } from "@tauri-apps/plugin-dialog";
import { writeTextFile } from "@tauri-apps/plugin-fs";
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
const REFRESH_OPTIONS = [
  { label: "Off", value: 0 },
  { label: "5s", value: 5 },
  { label: "10s", value: 10 },
  { label: "30s", value: 30 },
  { label: "1m", value: 60 },
  { label: "5m", value: 300 },
];

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
  const [rowContextMenu, setRowContextMenu] = useState<{ x: number; y: number; rowIdx: number } | null>(null);
  const [refreshInterval, setRefreshInterval] = useState(10);
  const [showRefreshMenu, setShowRefreshMenu] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [showSearch, setShowSearch] = useState(false);
  const refreshBtnRef = useRef<HTMLButtonElement>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);
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

  useEffect(() => {
    if (!rowContextMenu) return;
    const close = () => setRowContextMenu(null);
    document.addEventListener("click", close);
    return () => document.removeEventListener("click", close);
  }, [rowContextMenu]);

  useEffect(() => {
    if (refreshInterval <= 0 || hasChanges) return;
    const id = setInterval(() => {
      fetchData();
    }, refreshInterval * 1000);
    return () => clearInterval(id);
  }, [refreshInterval, fetchData, hasChanges]);

  useEffect(() => {
    if (!showRefreshMenu) return;
    const close = () => setShowRefreshMenu(false);
    document.addEventListener("click", close);
    return () => document.removeEventListener("click", close);
  }, [showRefreshMenu]);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key === "f") {
        e.preventDefault();
        setShowSearch(true);
        setTimeout(() => searchInputRef.current?.focus(), 0);
      }
      if (e.key === "Escape" && showSearch) {
        setShowSearch(false);
        setSearchQuery("");
      }
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [showSearch]);

  const searchLower = searchQuery.toLowerCase();

  const cellMatches = (value: unknown): boolean => {
    if (!searchQuery) return false;
    if (value === null || value === undefined) return "null".includes(searchLower);
    const str = typeof value === "object" ? JSON.stringify(value) : String(value);
    return str.toLowerCase().includes(searchLower);
  };

  const searchMatchCount = (() => {
    if (!searchQuery || !data) return 0;
    let count = 0;
    editingRows.forEach((row) => {
      row.forEach((val) => { if (cellMatches(val)) count++; });
    });
    return count;
  })();

  useEffect(() => {
    if (!searchQuery || !tableRef.current) return;
    const firstMatch = tableRef.current.querySelector(".cell-search-match");
    if (firstMatch) {
      firstMatch.scrollIntoView({ behavior: "smooth", block: "center", inline: "nearest" });
    }
  }, [searchQuery]);

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
      const ext = format === "sql" ? "sql" : format;
      const filePath = await save({
        defaultPath: `${table}.${ext}`,
        filters: [{
          name: format.toUpperCase(),
          extensions: [ext],
        }],
      });
      if (!filePath) return;

      const content = await api.exportTableData({
        connection_id: connectionId,
        database,
        schema,
        table,
        format,
        filter: activeFilter,
      });

      await writeTextFile(filePath, content);
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
          <div className="refresh-interval-wrapper">
            <button
              ref={refreshBtnRef}
              className={`btn-ghost ${refreshInterval > 0 ? "active-filter" : ""}`}
              onClick={(e) => { e.stopPropagation(); setShowRefreshMenu((v) => !v); }}
              title="Auto-refresh interval"
            >
              <Timer size={14} /> {refreshInterval > 0 ? (refreshInterval >= 60 ? `${refreshInterval / 60}m` : `${refreshInterval}s`) : "Off"}
            </button>
            {showRefreshMenu && (
              <div className="refresh-menu" onClick={(e) => e.stopPropagation()}>
                {REFRESH_OPTIONS.map((opt) => (
                  <button
                    key={opt.value}
                    className={`refresh-menu-item ${refreshInterval === opt.value ? "refresh-menu-active" : ""}`}
                    onClick={() => { setRefreshInterval(opt.value); setShowRefreshMenu(false); }}
                  >
                    {opt.label}
                  </button>
                ))}
              </div>
            )}
          </div>
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

      {showSearch && (
        <div className="grid-search-bar">
          <Search size={14} className="grid-search-icon" />
          <input
            ref={searchInputRef}
            className="grid-search-input"
            placeholder="Search columns and values..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Escape") { setShowSearch(false); setSearchQuery(""); }
            }}
          />
          {searchQuery && (
            <span className="grid-search-count">
              {searchMatchCount} match{searchMatchCount !== 1 ? "es" : ""}
            </span>
          )}
          <button
            className="btn-icon grid-search-close"
            onClick={() => { setShowSearch(false); setSearchQuery(""); }}
          >
            <X size={14} />
          </button>
        </div>
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
                  onContextMenu={(e) => {
                    e.preventDefault();
                    setRowContextMenu({ x: e.clientX, y: e.clientY, rowIdx });
                  }}
                >
                  <td
                    className="grid-row-number"
                    onClick={() => setDetailRowIdx(rowIdx)}
                    title="Click to open row detail"
                  >
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
                        searchQuery={searchQuery}
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

      {rowContextMenu && data && editingRows[rowContextMenu.rowIdx] && (
        <div
          className="context-menu"
          style={{ left: rowContextMenu.x, top: rowContextMenu.y }}
        >
          <button
            className="context-menu-item"
            onClick={() => {
              setDetailRowIdx(rowContextMenu.rowIdx);
              setRowContextMenu(null);
            }}
          >
            View as JSON
          </button>
          <button
            className="context-menu-item"
            onClick={() => {
              const obj: Record<string, unknown> = {};
              data.columns.forEach((col, i) => {
                obj[col.name] = editingRows[rowContextMenu.rowIdx][i];
              });
              navigator.clipboard.writeText(JSON.stringify(obj, null, 2));
              setRowContextMenu(null);
            }}
          >
            Copy row as JSON
          </button>
          <button
            className="context-menu-item"
            onClick={() => {
              const rIdx = rowContextMenu.rowIdx;
              const isInserted = rIdx >= originalRowCount;
              if (!isInserted) handleDeleteRow(rIdx);
              setRowContextMenu(null);
            }}
          >
            {changes.deletedKeys.has(getPkKey(rowContextMenu.rowIdx)) ? "Undo delete" : "Delete row"}
          </button>
        </div>
      )}

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
