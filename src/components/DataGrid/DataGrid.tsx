import { useEffect, useState, useCallback, useRef, useMemo } from "react";
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
import { FilterBar } from "./FilterBar";
import { RowDetailView } from "./RowDetailView";
import { ExportMenu } from "../ExportMenu";
import { ColumnOrganizer, ColumnSettings, loadColumnSettings, saveColumnSettings, applyColumnSettings } from "./ColumnOrganizer";
import { useToastStore } from "../../stores/toastStore";
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

const PAGE_SIZE = 50;
const REFRESH_OPTIONS = [
  { label: "Off", value: 0 },
  { label: "5s", value: 5 },
  { label: "10s", value: 10 },
  { label: "30s", value: 30 },
  { label: "1m", value: 60 },
  { label: "5m", value: 300 },
];

export function DataGrid({ connectionId, database, schema, table }: Props) {
  const addToast = useToastStore((s) => s.addToast);
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
  const [editingCell, setEditingCell] = useState<{ row: number; col: number } | null>(null);
  const [editValue, setEditValue] = useState("");
  const editInputRef = useRef<HTMLInputElement>(null);
  const editCommittedRef = useRef(false);
  const [scrollTop, setScrollTop] = useState(0);
  const [viewportHeight, setViewportHeight] = useState(600);

  const [colSettings, setColSettings] = useState<ColumnSettings>(() => {
    return loadColumnSettings(connectionId, database, schema, table) || { order: [], hidden: new Set() };
  });

  const handleColSettingsChange = useCallback((next: ColumnSettings) => {
    setColSettings(next);
    saveColumnSettings(connectionId, database, schema, table, next);
  }, [connectionId, database, schema, table]);

  const visibleIndices = useMemo(() => {
    if (!data) return [];
    return applyColumnSettings(data.columns, colSettings).visibleIndices;
  }, [data, colSettings]);

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
      const pkIndices = result.columns
        .map((c, i) => ({ col: c, idx: i }))
        .filter((x) => x.col.is_primary_key)
        .map((x) => x.idx);
      const nonPkIndices = result.columns
        .map((_, i) => i)
        .filter((i) => !result.columns[i].is_primary_key);
      const reorder = [...pkIndices, ...nonPkIndices];

      const sortedColumns = reorder.map((i) => result.columns[i]);
      const sortedRows = result.rows.map((row) => reorder.map((i) => row[i]));

      const sorted = { ...result, columns: sortedColumns, rows: sortedRows };
      setData(sorted);
      setEditingRows(sorted.rows.map((r) => [...r]));
      setDetailRowIdx((prev) => {
        if (prev == null) return null;
        if (prev >= sorted.rows.length) return null;
        return prev;
      });
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
    setColSettings(loadColumnSettings(connectionId, database, schema, table) || { order: [], hidden: new Set() });
  }, [connectionId, database, schema, table]);

  useEffect(() => {
    if (error && data) {
      const timer = setTimeout(() => setError(null), 5000);
      return () => clearTimeout(timer);
    }
  }, [error, data]);

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
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "f") {
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

  const searchTokens = searchQuery.toLowerCase().split(/\s+/).filter(Boolean);

  const cellMatches = (value: unknown): boolean => {
    if (!searchTokens.length) return false;
    const str = (value === null || value === undefined)
      ? "null"
      : typeof value === "object" ? JSON.stringify(value) : String(value);
    const lower = str.toLowerCase();
    return searchTokens.every((t) => lower.includes(t));
  };

  const searchMatchCount = useMemo(() => {
    if (!searchQuery || !data) return 0;
    let count = 0;
    for (const row of editingRows) {
      for (const val of row) {
        if (cellMatches(val)) count++;
      }
    }
    return count;
  }, [searchQuery, editingRows, data]);

  useEffect(() => {
    if (!searchQuery || !tableRef.current) return;
    const firstMatch = tableRef.current.querySelector(".cell-search-match");
    if (firstMatch) {
      firstMatch.scrollIntoView({ behavior: "smooth", block: "center", inline: "nearest" });
    }
  }, [searchQuery]);

  const ROW_HEIGHT = 36;
  const HEADER_HEIGHT = 38;
  const OVERSCAN = 5;

  const viewportHeightRef = useRef(600);
  useEffect(() => {
    const el = tableRef.current;
    if (!el) return;
    let rafId = 0;
    const onScroll = () => {
      if (rafId) return;
      rafId = requestAnimationFrame(() => {
        setScrollTop(el.scrollTop);
        rafId = 0;
      });
    };
    el.addEventListener("scroll", onScroll, { passive: true });
    const ro = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const h = Math.round(entry.contentRect.height);
        if (Math.abs(h - viewportHeightRef.current) > 2) {
          viewportHeightRef.current = h;
          setViewportHeight(h);
        }
      }
    });
    ro.observe(el);
    const h = Math.round(el.clientHeight);
    viewportHeightRef.current = h;
    setViewportHeight(h);
    return () => {
      el.removeEventListener("scroll", onScroll);
      if (rafId) cancelAnimationFrame(rafId);
      ro.disconnect();
    };
  }, [data]);

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
    if (!data || rowIndex < 0 || rowIndex >= data.rows.length) return [];
    const row = data.rows[rowIndex];
    return data.columns
      .filter((c) => c.is_primary_key)
      .map((c) => {
        const colIdx = data.columns.findIndex((col) => col.name === c.name);
        return [c.name, colIdx >= 0 ? row[colIdx] : undefined] as [string, unknown];
      });
  };

  const getPkKey = (rowIndex: number): string => {
    const values = getPkValues(rowIndex).map(([, v]) => v);
    return JSON.stringify(values);
  };

  const handleCellChange = useCallback((
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
  }, [data, getPkValues, getPkKey]);

  const startCellEdit = useCallback((rowIdx: number, colIdx: number, value: unknown) => {
    editCommittedRef.current = false;
    setEditingCell({ row: rowIdx, col: colIdx });
    setEditValue(value === null || value === undefined ? "" : String(value));
  }, []);

  const commitCellEdit = useCallback(() => {
    if (editCommittedRef.current || !editingCell || !data) return;
    editCommittedRef.current = true;
    const { row, col } = editingCell;
    const column = data.columns[col];
    setEditingCell(null);
    if (editValue === "" && column.is_nullable) {
      handleCellChange(row, col, null);
      return;
    }
    const parsed = parseCellValue(editValue, column.data_type);
    handleCellChange(row, col, parsed);
  }, [editingCell, editValue, data, handleCellChange]);

  const cancelCellEdit = useCallback(() => {
    editCommittedRef.current = true;
    setEditingCell(null);
  }, []);

  useEffect(() => {
    if (editingCell && editInputRef.current) {
      editInputRef.current.focus();
      editInputRef.current.select();
    }
  }, [editingCell]);

  const handleAddRow = () => {
    if (!data) return;
    const emptyRow: unknown[] = data.columns.map(() => null);
    setEditingRows((prev) => [...prev, emptyRow]);
    const values: [string, unknown][] = data.columns.map((c) => [c.name, null]);
    setChanges((prev) => ({
      ...prev,
      inserts: [...prev.inserts, { values }],
    }));
    requestAnimationFrame(() => {
      if (tableRef.current) {
        tableRef.current.scrollTop = 0;
      }
    });
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
        inserts: changes.inserts.map((_ins, insertIdx) => {
          const lastRowIdx = editingRows.length - changes.inserts.length;
          const row = editingRows[lastRowIdx + insertIdx];
          if (!row) {
            return { values: data.columns.map((c): [string, unknown] => [c.name, null]) };
          }
          return {
            values: data.columns
              .map((c, ci): [string, unknown] => [c.name, row[ci]])
              .filter(([, v]) => v !== null),
          };
        }),
        deletes: Array.from(changes.deletedKeys).flatMap((key) => {
          let pkValues: unknown[];
          try {
            pkValues = JSON.parse(key) as unknown[];
          } catch {
            return [];
          }
          const pkCols = data.columns.filter((c) => c.is_primary_key);
          if (pkCols.length === 0 || pkValues.length !== pkCols.length) return [];
          const primary_key_values: [string, unknown][] = pkCols.map((c, i) => [
            c.name,
            pkValues[i],
          ]);
          return [{ primary_key_values } as DeleteRow];
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

      await api.exportTableToFile({
        connection_id: connectionId,
        database,
        schema,
        table,
        format,
        filter: activeFilter,
      }, filePath);
      addToast(`Exported ${table} as ${format.toUpperCase()}`);
    } catch (e) {
      addToast(String(e), "error");
    }
  };

  const formatRowAsInsert = useCallback((row: unknown[]): string => {
    if (!data) return "";
    const cols = data.columns.map((c) => `"${c.name}"`).join(", ");
    const vals = row
      .map((v) => {
        if (v === null) return "NULL";
        if (typeof v === "number") return String(v);
        if (typeof v === "boolean") return v ? "TRUE" : "FALSE";
        return `'${String(v).replace(/'/g, "''")}'`;
      })
      .join(", ");
    return `INSERT INTO "${schema}"."${table}" (${cols}) VALUES (${vals});`;
  }, [data, schema, table]);

  const handleCopyAsInsert = (rowIdx: number) => {
    navigator.clipboard.writeText(formatRowAsInsert(editingRows[rowIdx]));
  };

  const handleCopyAllAsInsert = () => {
    navigator.clipboard.writeText(editingRows.map(formatRowAsInsert).join("\n"));
  };

  const handleResizeStart = useCallback(
    (e: React.MouseEvent, colName: string, thEl: HTMLElement) => {
      e.preventDefault();
      e.stopPropagation();
      const startWidth = thEl.offsetWidth;
      resizingRef.current = { col: colName, startX: e.clientX, startWidth };
      let rafId = 0;

      const onMove = (ev: MouseEvent) => {
        if (!resizingRef.current) return;
        if (rafId) return;
        rafId = requestAnimationFrame(() => {
          if (!resizingRef.current) return;
          const diff = ev.clientX - resizingRef.current.startX;
          const newWidth = Math.max(60, resizingRef.current.startWidth + diff);
          setColumnWidths((prev) => ({ ...prev, [resizingRef.current!.col]: newWidth }));
          rafId = 0;
        });
      };

      const onUp = () => {
        resizingRef.current = null;
        if (rafId) cancelAnimationFrame(rafId);
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

  const originalRowCount = data?.rows.length ?? 0;

  const orderedRowIndices = useMemo(() => {
    if (!editingRows.length) return [];
    const inserted: number[] = [];
    const existing: number[] = [];
    for (let i = 0; i < editingRows.length; i++) {
      if (i >= originalRowCount) inserted.push(i);
      else existing.push(i);
    }
    return [...inserted, ...existing];
  }, [editingRows.length, originalRowCount]);

  const virtualRange = useMemo(() => {
    const totalRows = orderedRowIndices.length;
    if (totalRows === 0) return { start: 0, end: 0, topPad: 0, bottomPad: 0 };
    const visibleStart = Math.floor(Math.max(0, scrollTop - HEADER_HEIGHT) / ROW_HEIGHT);
    const visibleCount = Math.ceil(viewportHeight / ROW_HEIGHT);
    const start = Math.max(0, visibleStart - OVERSCAN);
    const end = Math.min(totalRows, visibleStart + visibleCount + OVERSCAN);
    return {
      start,
      end,
      topPad: start * ROW_HEIGHT,
      bottomPad: Math.max(0, (totalRows - end) * ROW_HEIGHT),
    };
  }, [orderedRowIndices.length, scrollTop, viewportHeight]);

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

  return (
    <div className="data-grid-container">
      <div className="grid-toolbar">
        <div className="grid-toolbar-left">
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
          {data && (
            <ColumnOrganizer
              columns={data.columns}
              settings={colSettings}
              onChange={handleColSettingsChange}
            />
          )}
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
              {visibleIndices.map((colIdx) => {
                const col = data.columns[colIdx];
                return (
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
                );
              })}
              <th className="grid-actions-header">
                <Trash2 size={12} />
              </th>
            </tr>
          </thead>
          <tbody>
            {virtualRange.topPad > 0 && (
              <tr style={{ height: virtualRange.topPad }} aria-hidden="true"><td colSpan={visibleIndices.length + 2} /></tr>
            )}
            {orderedRowIndices.slice(virtualRange.start, virtualRange.end).map((rowIdx) => {
              const row = editingRows[rowIdx];
              const isInserted = rowIdx >= originalRowCount;
              const pkKey = !isInserted ? getPkKey(rowIdx) : "";
              const isDeleted = !isInserted && changes.deletedKeys.has(pkKey);

              return (
                <tr
                  key={rowIdx}
                  className={[
                    isDeleted && "row-deleted",
                    isInserted && "row-inserted",
                    detailRowIdx === rowIdx && "row-selected",
                  ].filter(Boolean).join(" ")}
                  onContextMenu={(e) => {
                    e.preventDefault();
                    const z = parseFloat(document.documentElement.style.zoom || "100") / 100;
                    setRowContextMenu({ x: e.clientX / z, y: e.clientY / z, rowIdx });
                  }}
                >
                  <td
                    className="grid-row-number"
                    onClick={() => setDetailRowIdx(rowIdx)}
                    title="Click to open row detail"
                  >
                    {isInserted ? "+" : page * PAGE_SIZE + rowIdx + 1}
                  </td>
                  {visibleIndices.map((colIdx) => {
                    const col = data.columns[colIdx];
                    const value = row[colIdx];
                    const isNull = value === null || value === undefined;
                    const changeKey = !isInserted ? `${pkKey}:${col.name}` : null;
                    const isModified = changeKey !== null && changes.updates.has(changeKey);
                    const isAutoOnInsert = isInserted && col.is_auto_generated && isNull;
                    const isEditing = editingCell?.row === rowIdx && editingCell?.col === colIdx;

                    let cls = "grid-cell";
                    if (isAutoOnInsert) cls += " cell-auto";
                    else if (isNull) cls += " cell-null";
                    if (isModified) cls += " cell-modified";
                    if (isInserted) cls += " cell-inserted";
                    if (isEditing) cls += " cell-editing";

                    if (isAutoOnInsert) {
                      return <td key={colIdx} className={cls} title="Auto-generated by database"><span className="cell-value cell-auto-label">auto</span></td>;
                    }

                    if (isEditing) {
                      return (
                        <td key={colIdx} className={cls}>
                          <input
                            ref={editInputRef}
                            className="cell-input"
                            value={editValue}
                            onChange={(e) => setEditValue(e.target.value)}
                            onBlur={commitCellEdit}
                            onKeyDown={(e) => {
                              if (e.key === "Enter") commitCellEdit();
                              else if (e.key === "Escape") cancelCellEdit();
                              else if (e.key === "Tab") { e.preventDefault(); commitCellEdit(); }
                            }}
                          />
                        </td>
                      );
                    }

                    const displayValue = isNull ? "NULL" : String(value);
                    let content: React.ReactNode = displayValue;
                    if (searchQuery) {
                      const q = searchQuery.toLowerCase();
                      const str = isNull ? "null" : (typeof value === "object" ? JSON.stringify(value) : String(value));
                      if (str.toLowerCase().includes(q)) {
                        cls += " cell-search-match";
                        const idx = displayValue.toLowerCase().indexOf(q);
                        if (idx !== -1) {
                          content = <>{displayValue.slice(0, idx)}<mark className="cell-search-highlight">{displayValue.slice(idx, idx + searchQuery.length)}</mark>{displayValue.slice(idx + searchQuery.length)}</>;
                        }
                      }
                    }

                    return (
                      <td key={colIdx} className={cls} onDoubleClick={() => startCellEdit(rowIdx, colIdx, value)}>
                        <span className="cell-value">{content}</span>
                      </td>
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
            {virtualRange.bottomPad > 0 && (
              <tr style={{ height: virtualRange.bottomPad }} aria-hidden="true"><td colSpan={visibleIndices.length + 2} /></tr>
            )}
          </tbody>
        </table>
      </div>

      <div className="grid-pagination">
        <div className="grid-pagination-info">
          {data.total_rows.toLocaleString()} rows
          {totalPages > 1 && (
            <span className="grid-pagination-range">
              {" · "}Showing {page * PAGE_SIZE + 1} -{" "}
              {Math.min((page + 1) * PAGE_SIZE, data.total_rows)}
            </span>
          )}
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
          ref={(el) => {
            if (!el) return;
            const z = parseFloat(document.documentElement.style.zoom || "100") / 100;
            const rect = el.getBoundingClientRect();
            const vh = window.innerHeight / z;
            const vw = window.innerWidth / z;
            const menuH = rect.height / z;
            const menuW = rect.width / z;
            if (rowContextMenu.y + menuH > vh) {
              el.style.top = `${Math.max(4, rowContextMenu.y - menuH)}px`;
            }
            if (rowContextMenu.x + menuW > vw) {
              el.style.left = `${Math.max(4, rowContextMenu.x - menuW)}px`;
            }
          }}
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

function parseCellValue(raw: string, dataType: string): unknown {
  const t = dataType.toLowerCase();
  if (t.includes("int") || t === "serial" || t === "bigserial" || t === "smallserial") {
    const n = parseInt(raw, 10);
    return isNaN(n) ? raw : n;
  }
  if (t.includes("float") || t.includes("double") || t.includes("decimal") || t.includes("numeric") || t === "real") {
    const n = parseFloat(raw);
    return isNaN(n) ? raw : n;
  }
  if (t === "boolean" || t === "bool") {
    return raw.toLowerCase() === "true" || raw === "1";
  }
  return raw;
}
