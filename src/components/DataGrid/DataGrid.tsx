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
  ForeignKeyInfo,
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
  ChevronLeft,
  ChevronRight,
  ChevronsLeft,
  ChevronsRight,
  Loader2,
  Filter,
  Copy,
  Timer,
  Search,
  X,
} from "lucide-react";
import { save } from "@tauri-apps/plugin-dialog";
import { AllCommunityModule, themeQuartz, type ColDef, type CellContextMenuEvent, type GridApi, type GridReadyEvent, type IHeaderParams } from "ag-grid-community";
import { AgGridReact } from "ag-grid-react";
import "./DataGrid.css";
import "./ag-grid-theme.css";

function DataTypeHeader(props: IHeaderParams & { dataType?: string; isPk?: boolean; fk?: ForeignKeyInfo }) {
  const [sortState, setSortState] = useState<"asc" | "desc" | null>(null);

  useEffect(() => {
    const listener = () => {
      if (props.column.isSortAscending()) setSortState("asc");
      else if (props.column.isSortDescending()) setSortState("desc");
      else setSortState(null);
    };
    props.column.addEventListener("sortChanged", listener);
    listener();
    return () => {
      props.column.removeEventListener("sortChanged", listener);
    };
  }, [props.column]);

  const onSortRequested = (e: React.MouseEvent) => {
    props.progressSort(e.shiftKey);
  };

  return (
    <div className="ag-custom-header" onClick={onSortRequested}>
      <div className="ag-custom-header-labels">
        <span className="ag-custom-header-name">
          {props.isPk && <span className="ag-custom-pk-badge">PK</span>}
          {props.fk && <span className="ag-custom-fk-badge" title={`→ ${props.fk.referenced_table}.${props.fk.referenced_column}`}>FK</span>}
          {props.displayName}
          {sortState && (
            <svg className="ag-custom-sort-icon" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              {sortState === "asc"
                ? <><line x1="12" y1="19" x2="12" y2="5" /><polyline points="5 12 12 5 19 12" /></>
                : <><line x1="12" y1="5" x2="12" y2="19" /><polyline points="5 12 12 19 19 12" /></>}
            </svg>
          )}
        </span>
        {props.dataType && (
          <span className="ag-custom-header-type">
            {props.dataType}
            {props.fk && <span className="ag-custom-fk-ref"> → {props.fk.referenced_table}</span>}
          </span>
        )}
      </div>
    </div>
  );
}

const gridTheme = themeQuartz.withParams({
  backgroundColor: "var(--bg-secondary)",
  foregroundColor: "var(--text-primary)",
  headerBackgroundColor: "var(--bg-header)",
  accentColor: "var(--accent)",
  borderColor: "var(--border)",
  columnBorder: true,
  oddRowBackgroundColor: "var(--bg-primary)",
  rowHoverColor: "var(--bg-hover)",
  selectedRowBackgroundColor: "var(--bg-selected)",
  headerTextColor: "var(--text-secondary)",
  cellTextColor: "var(--text-primary)",
  fontFamily: "var(--font-mono)",
  fontSize: 14,
  headerFontSize: 12,
  rowHeight: 36,
  headerHeight: 38,
  cellHorizontalPadding: 10,
  wrapperBorderRadius: 0,
  borderRadius: 0,
  headerColumnResizeHandleColor: "transparent",
});

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

interface RowObj {
  __rowIdx: number;
  __isInserted: boolean;
  [key: string]: unknown;
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
  const [detailRowIdx, setDetailRowIdx] = useState<number | null>(null);
  const [rowContextMenu, setRowContextMenu] = useState<{ x: number; y: number; rowIdx: number } | null>(null);
  const [refreshInterval, setRefreshInterval] = useState(10);
  const [showRefreshMenu, setShowRefreshMenu] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [showSearch, setShowSearch] = useState(false);
  const refreshBtnRef = useRef<HTMLButtonElement>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);
  const searchQueryRef = useRef("");
  const gridApiRef = useRef<GridApi | null>(null);
  const [fkMap, setFkMap] = useState<Map<string, ForeignKeyInfo>>(new Map());

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

  const fetchGenRef = useRef(0);

  const fetchData = useCallback(async () => {
    const gen = ++fetchGenRef.current;
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
      if (gen !== fetchGenRef.current) return;
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
      if (gen !== fetchGenRef.current) return;
      setError(String(e));
    } finally {
      if (gen === fetchGenRef.current) setLoading(false);
    }
  }, [connectionId, database, schema, table, page, sort, activeFilter]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  useEffect(() => {
    let cancelled = false;
    setColSettings(loadColumnSettings(connectionId, database, schema, table) || { order: [], hidden: new Set() });
    api.listForeignKeys(connectionId, database, schema, table)
      .then((fks) => {
        if (cancelled) return;
        const map = new Map<string, ForeignKeyInfo>();
        fks.forEach((fk) => map.set(fk.column, fk));
        setFkMap(map);
      })
      .catch(() => { if (!cancelled) setFkMap(new Map()); });
    return () => { cancelled = true; };
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
    let focusTimer: ReturnType<typeof setTimeout>;
    const handler = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "f") {
        e.preventDefault();
        setShowSearch(true);
        focusTimer = setTimeout(() => searchInputRef.current?.focus(), 0);
      }
      if (e.key === "Escape" && showSearch) {
        setShowSearch(false);
        setSearchQuery("");
      }
    };
    document.addEventListener("keydown", handler);
    return () => {
      document.removeEventListener("keydown", handler);
      clearTimeout(focusTimer);
    };
  }, [showSearch]);

  const searchMatchCount = useMemo(() => {
    if (!searchQuery || !data) return 0;
    const q = searchQuery.toLowerCase();
    let count = 0;
    for (const row of editingRows) {
      for (const val of row) {
        const str = (val === null || val === undefined) ? "null" : typeof val === "object" ? JSON.stringify(val) : String(val);
        if (str.toLowerCase().includes(q)) count++;
      }
    }
    return count;
  }, [searchQuery, editingRows, data]);

  useEffect(() => {
    searchQueryRef.current = searchQuery;
    if (!gridApiRef.current) return;
    gridApiRef.current.refreshCells({ force: true });
  }, [searchQuery]);

  const totalPages = data ? Math.ceil(data.total_rows / PAGE_SIZE) : 0;

  const originalRowCount = data?.rows.length ?? 0;

  const getPkValues = useCallback((rowIndex: number): [string, unknown][] => {
    if (!data || rowIndex < 0 || rowIndex >= data.rows.length) return [];
    const row = data.rows[rowIndex];
    return data.columns
      .filter((c) => c.is_primary_key)
      .map((c) => {
        const colIdx = data.columns.findIndex((col) => col.name === c.name);
        return [c.name, colIdx >= 0 ? row[colIdx] : undefined] as [string, unknown];
      });
  }, [data]);

  const getPkKey = useCallback((rowIndex: number): string => {
    const values = getPkValues(rowIndex).map(([, v]) => v);
    return JSON.stringify(values);
  }, [getPkValues]);

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

  // Convert array-of-arrays to array-of-objects for AG Grid
  const agRowData = useMemo((): RowObj[] => {
    if (!data) return [];
    const inserted: RowObj[] = [];
    const existing: RowObj[] = [];
    for (let i = 0; i < editingRows.length; i++) {
      const row = editingRows[i];
      const obj: RowObj = { __rowIdx: i, __isInserted: i >= originalRowCount };
      data.columns.forEach((col, colIdx) => { obj[col.name] = row[colIdx]; });
      if (i >= originalRowCount) inserted.push(obj);
      else existing.push(obj);
    }
    return [...inserted, ...existing];
  }, [editingRows, data, originalRowCount]);

  // Build AG Grid column definitions
  const columnDefs = useMemo((): ColDef[] => {
    if (!data) return [];

    const rowNumCol: ColDef = {
      headerName: "#",
      width: 60,
      minWidth: 60,
      maxWidth: 80,
      pinned: "left",
      editable: false,
      sortable: false,
      resizable: false,
      suppressMovable: true,
      cellClass: "grid-row-number",
      headerClass: "grid-row-number",
      valueGetter: (params) => {
        if (!params.data) return "";
        if (params.data.__isInserted) return "+";
        return page * PAGE_SIZE + params.data.__rowIdx + 1;
      },
      onCellClicked: (params) => {
        if (params.data) setDetailRowIdx(params.data.__rowIdx);
      },
    };

    const dataCols: ColDef[] = visibleIndices.map((colIdx) => {
      const col = data.columns[colIdx];
      const isAutoGen = col.is_auto_generated;

      return {
        field: col.name,
        headerName: col.name,
        headerTooltip: `${col.name} (${col.data_type})`,
        headerComponent: DataTypeHeader,
        headerComponentParams: {
          dataType: col.data_type,
          isPk: col.is_primary_key,
          fk: fkMap.get(col.name),
        },
        editable: (params) => {
          if (!params.data) return false;
          if (params.data.__isInserted && isAutoGen && (params.data[col.name] === null || params.data[col.name] === undefined)) return false;
          return true;
        },
        cellClassRules: {
          "cell-null": (params) => params.value === null || params.value === undefined,
          "cell-modified": (params) => {
            if (!params.data || params.data.__isInserted) return false;
            const pkKey = getPkKey(params.data.__rowIdx);
            return changes.updates.has(`${pkKey}:${col.name}`);
          },
          "cell-inserted": (params) => params.data?.__isInserted === true,
          "cell-auto": (params) => {
            return params.data?.__isInserted && isAutoGen && (params.value === null || params.value === undefined);
          },
          "cell-search-match": (params) => {
            const q = searchQueryRef.current;
            if (!q) return false;
            const v = params.value;
            const str = (v === null || v === undefined) ? "null" : typeof v === "object" ? JSON.stringify(v) : String(v);
            return str.toLowerCase().includes(q.toLowerCase());
          },
        },
        valueFormatter: (params) => {
          if (params.data?.__isInserted && isAutoGen && (params.value === null || params.value === undefined)) return "auto";
          if (params.value === null || params.value === undefined) return "NULL";
          return String(params.value);
        },
        valueSetter: (params) => {
          const rowIdx = params.data.__rowIdx;
          const raw = params.newValue;
          let parsed: unknown;
          if ((raw === "" || raw === null || raw === undefined) && col.is_nullable) {
            parsed = null;
          } else if (raw === "" || raw === null || raw === undefined) {
            parsed = raw;
          } else {
            parsed = parseCellValue(String(raw), col.data_type);
          }
          handleCellChange(rowIdx, colIdx, parsed);
          return true;
        },
        minWidth: 80,
      };
    });

    return [rowNumCol, ...dataCols];
  }, [data, visibleIndices, page, changes, getPkKey, handleCellChange, fkMap]);

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

  const handleCopyAllAsInsert = () => {
    navigator.clipboard.writeText(editingRows.map(formatRowAsInsert).join("\n"));
  };

  const onGridReady = useCallback((params: GridReadyEvent) => {
    gridApiRef.current = params.api;
  }, []);

  const onCellContextMenu = useCallback((event: CellContextMenuEvent) => {
    if (!event.data) return;
    const e = event.event as MouseEvent;
    if (!e) return;
    e.preventDefault();
    const z = parseFloat(document.documentElement.style.zoom || "100") / 100;
    setRowContextMenu({ x: e.clientX / z, y: e.clientY / z, rowIdx: event.data.__rowIdx });
  }, []);

  const getRowClass = useCallback((params: { data: RowObj }) => {
    if (!params.data) return "";
    const classes: string[] = [];
    if (params.data.__isInserted) classes.push("row-inserted");
    else {
      const pkKey = getPkKey(params.data.__rowIdx);
      if (changes.deletedKeys.has(pkKey)) classes.push("row-deleted");
    }
    if (detailRowIdx === params.data.__rowIdx) classes.push("row-selected");
    return classes.join(" ");
  }, [changes.deletedKeys, detailRowIdx, getPkKey]);

  const onSortChanged = useCallback(() => {
    if (!gridApiRef.current) return;
    const sortModel = gridApiRef.current.getColumnState()
      .filter((c) => c.sort)
      .sort((a, b) => (a.sortIndex ?? 0) - (b.sortIndex ?? 0));
    if (sortModel.length === 0) {
      setSort(null);
    } else {
      const first = sortModel[0];
      setSort({ column: first.colId, direction: first.sort as "asc" | "desc" });
    }
    setPage(0);
  }, []);

  const defaultColDef = useMemo((): ColDef => ({
    resizable: true,
    sortable: true,
    suppressHeaderMenuButton: true,
    suppressKeyboardEvent: (params) => {
      if (!params.editing) {
        const key = params.event.key;
        if (key === "Delete" || key === "Backspace" || key === "Enter" || key === "F2" ||
          (key.length === 1 && !params.event.ctrlKey && !params.event.metaKey)) {
          return true;
        }
      }
      return false;
    },
  }), []);

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

      <div className="grid-table-wrapper ag-grid-wrapper">
        <AgGridReact
          theme={gridTheme}
          modules={[AllCommunityModule]}
          rowData={agRowData}
          columnDefs={columnDefs}
          defaultColDef={defaultColDef}
          getRowId={(params) => String(params.data.__rowIdx)}
          getRowClass={getRowClass as any}
          onGridReady={onGridReady}
          onCellContextMenu={onCellContextMenu}
          onSortChanged={onSortChanged}
          preventDefaultOnContextMenu={true}
          animateRows={false}
          suppressCellFocus={false}
          stopEditingWhenCellsLoseFocus={true}
          singleClickEdit={false}
          enableCellTextSelection={true}
          suppressRowClickSelection={true}
        />
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
              navigator.clipboard.writeText(formatRowAsInsert(editingRows[rowContextMenu.rowIdx]));
              setRowContextMenu(null);
            }}
          >
            Copy as INSERT
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
