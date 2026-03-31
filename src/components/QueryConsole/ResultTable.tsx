import { useMemo, useRef, useState, useEffect, useCallback } from "react";
import { api, QueryResult, ColumnInfo, DataChanges, CellChange, NewRow, DeleteRow, ForeignKeyInfo } from "../../lib/tauri";
import { AllCommunityModule, themeQuartz, type ColDef, type GridApi, type GridReadyEvent, type CellContextMenuEvent, type CellClickedEvent, type IHeaderParams } from "ag-grid-community";
import { AgGridReact } from "ag-grid-react";
import { Search, X, Save, Undo2, ChevronUp, ChevronDown as ChevronDownIcon, Plus, Shuffle, Trash2, BarChart3, Database, Lock, Loader2, ExternalLink } from "lucide-react";
import { RowDetailView } from "../DataGrid/RowDetailView";
import { ExportMenu } from "../ExportMenu";
import { useToastStore } from "../../stores/toastStore";
import { useTabStore, TabInfo } from "../../stores/tabStore";
import { useConnectionStore } from "../../stores/connectionStore";
import "../DataGrid/ag-grid-theme.css";
import "./QueryConsole.css";

function ResultDataTypeHeader(props: IHeaderParams & { dataType?: string; isPk?: boolean; fk?: ForeignKeyInfo }) {
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
  rowHeight: 32,
  headerHeight: 34,
  cellHorizontalPadding: 10,
  wrapperBorderRadius: 0,
  borderRadius: 0,
  headerColumnResizeHandleColor: "transparent",
});

export interface SourceTable {
  schema: string;
  table: string;
}

/**
 * Best-effort extraction of schema.table from a simple SELECT query.
 * Returns null for JOINs, UNIONs, subqueries, CTEs, or non-SELECT statements.
 */
export function parseSimpleSelect(sql: string): SourceTable | null {
  const trimmed = sql.trim();
  const upper = trimmed.toUpperCase();

  if (!upper.startsWith("SELECT") && !upper.startsWith("WITH")) return null;
  if (upper.startsWith("WITH")) return null;
  if (/\bJOIN\b/i.test(trimmed)) return null;
  if (/\bUNION\b/i.test(trimmed)) return null;
  if (/\bINTERSECT\b/i.test(trimmed)) return null;
  if (/\bEXCEPT\b/i.test(trimmed)) return null;

  // Match FROM clause — support: FROM "schema"."table", FROM schema.table, FROM "table", FROM table
  const fromMatch = trimmed.match(
    /\bFROM\s+(?:"([^"]+)"\s*\.\s*"([^"]+)"|"([^"]+)"\s*\.\s*(\w+)|(\w+)\s*\.\s*"([^"]+)"|(\w+)\s*\.\s*(\w+)|"([^"]+)"|(\w+))/i
  );
  if (!fromMatch) return null;

  let schema: string;
  let table: string;

  if (fromMatch[1] && fromMatch[2]) {
    schema = fromMatch[1]; table = fromMatch[2]; // "schema"."table"
  } else if (fromMatch[3] && fromMatch[4]) {
    schema = fromMatch[3]; table = fromMatch[4]; // "schema".table
  } else if (fromMatch[5] && fromMatch[6]) {
    schema = fromMatch[5]; table = fromMatch[6]; // schema."table"
  } else if (fromMatch[7] && fromMatch[8]) {
    schema = fromMatch[7]; table = fromMatch[8]; // schema.table
  } else if (fromMatch[9]) {
    schema = ""; table = fromMatch[9]; // "table" — schema unknown, caller resolves
  } else if (fromMatch[10]) {
    schema = ""; table = fromMatch[10]; // table — schema unknown, caller resolves
  } else {
    return null;
  }

  // Reject if there's a comma after the table name (multiple tables)
  const afterFrom = trimmed.slice((fromMatch.index ?? 0) + fromMatch[0].length).trimStart();
  if (afterFrom.startsWith(",")) return null;
  // Reject subqueries in FROM
  if (afterFrom.startsWith("(")) return null;

  return { schema, table };
}

interface Props {
  result: QueryResult;
  resultMode: "results" | "explain" | "chart";
  onToggleChart: () => void;
  onExport: (format: "csv" | "json" | "sql") => void;
  connectionId: string;
  database: string;
  sourceTable: SourceTable | null;
  onReExecute: () => void;
}

interface RowObj {
  __rowIdx: number;
  __isInserted?: boolean;
  __isDeleted?: boolean;
  [key: string]: unknown;
}

function generateTestValue(colName: string): unknown {
  const name = colName.toLowerCase();
  if (name.includes("id")) return Math.floor(Math.random() * 100000);
  if (name.includes("name") || name.includes("title")) {
    const words = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Theta"];
    return words[Math.floor(Math.random() * words.length)];
  }
  if (name.includes("email")) return `test${Math.floor(Math.random() * 1000)}@example.com`;
  if (name.includes("date") || name.includes("time") || name.includes("created") || name.includes("updated")) {
    return new Date(Date.now() - Math.random() * 365 * 86400000).toISOString().split("T")[0];
  }
  if (name.includes("price") || name.includes("amount") || name.includes("cost") || name.includes("total")) {
    return +(Math.random() * 1000).toFixed(2);
  }
  if (name.includes("count") || name.includes("quantity") || name.includes("num") || name.includes("age")) {
    return Math.floor(Math.random() * 100);
  }
  if (name.includes("active") || name.includes("enabled") || name.includes("is_")) {
    return Math.random() > 0.5;
  }
  if (name.includes("description") || name.includes("comment") || name.includes("note")) {
    return "Sample text " + Math.floor(Math.random() * 1000);
  }
  if (name.includes("url") || name.includes("link")) {
    return `https://example.com/${Math.floor(Math.random() * 1000)}`;
  }
  return "test_" + Math.floor(Math.random() * 10000);
}

function parseCellValue(raw: string, dataType: string): unknown {
  const t = dataType.toLowerCase();
  if (t.includes("int") || t === "serial" || t === "bigserial" || t === "smallserial") {
    const n = parseInt(raw.trim(), 10);
    return isNaN(n) ? raw : n;
  }
  if (t.includes("float") || t.includes("double") || t.includes("decimal") || t.includes("numeric") || t === "real") {
    const n = parseFloat(raw.trim());
    return isNaN(n) ? raw : n;
  }
  if (t === "boolean" || t === "bool") {
    return raw.trim().toLowerCase() === "true" || raw.trim() === "1";
  }
  return raw;
}

export function ResultTable({ result, resultMode, onToggleChart, onExport, connectionId, database, sourceTable, onReExecute }: Props) {
  const addToast = useToastStore((s) => s.addToast);
  const openTab = useTabStore((s) => s.openTab);
  const connections = useConnectionStore((s) => s.connections);
  const gridApiRef = useRef<GridApi | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [showSearch, setShowSearch] = useState(false);
  const searchInputRef = useRef<HTMLInputElement>(null);
  const searchQueryRef = useRef("");
  const searchCurrentRef = useRef<{ rowIndex: number; colId: string } | null>(null);
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; rowIdx: number } | null>(null);
  const [detailRowIdx, setDetailRowIdx] = useState<number | null>(null);
  const [editingRows, setEditingRows] = useState<unknown[][]>(() =>
    result.rows.map((r) => [...r])
  );
  const [selectedIndices, setSelectedIndices] = useState<Set<number>>(new Set());
  const [deletedIndices, setDeletedIndices] = useState<Set<number>>(new Set());
  const [saving, setSaving] = useState(false);
  const [tableColumns, setTableColumns] = useState<ColumnInfo[] | null>(null);
  const [fkMap, setFkMap] = useState<Map<string, ForeignKeyInfo>>(new Map());

  useEffect(() => {
    setEditingRows(result.rows.map((r) => [...r]));
    setDetailRowIdx(null);
    setSelectedIndices(new Set());
    setDeletedIndices(new Set());
  }, [result]);

  // Fetch real column metadata (with PK info) and foreign keys when sourceTable is set
  useEffect(() => {
    if (!sourceTable) {
      setTableColumns(null);
      setFkMap(new Map());
      return;
    }
    let cancelled = false;
    (async () => {
      try {
        const [cols, fks] = await Promise.all([
          api.listColumns(connectionId, database, sourceTable.schema, sourceTable.table),
          api.listForeignKeys(connectionId, database, sourceTable.schema, sourceTable.table),
        ]);
        if (!cancelled) {
          setTableColumns(cols);
          const map = new Map<string, ForeignKeyInfo>();
          for (const fk of fks) map.set(fk.column, fk);
          setFkMap(map);
        }
      } catch {
        if (!cancelled) {
          setTableColumns(null);
          setFkMap(new Map());
        }
      }
    })();
    return () => { cancelled = true; };
  }, [connectionId, database, sourceTable]);

  const pkColumns = useMemo(() => {
    if (!tableColumns) return [];
    return tableColumns.filter((c) => c.is_primary_key);
  }, [tableColumns]);

  const isUpdatable = sourceTable !== null && pkColumns.length > 0 &&
    pkColumns.every((pk) => result.columns.includes(pk.name));

  const columnInfos = useMemo((): ColumnInfo[] => {
    if (tableColumns && sourceTable) {
      return result.columns.map((name, i) => {
        const real = tableColumns.find((c) => c.name === name);
        return real ?? {
          name,
          data_type: "text",
          is_nullable: true,
          is_primary_key: false,
          default_value: null,
          ordinal_position: i,
          is_auto_generated: false,
        };
      });
    }
    return result.columns.map((name, i) => ({
      name,
      data_type: "text",
      is_nullable: true,
      is_primary_key: false,
      default_value: null,
      ordinal_position: i,
      is_auto_generated: false,
    }));
  }, [result.columns, tableColumns, sourceTable]);

  const rowData = useMemo((): RowObj[] => {
    return editingRows.map((row, i) => {
      const obj: RowObj = {
        __rowIdx: i,
        __isInserted: i >= result.rows.length,
        __isDeleted: deletedIndices.has(i),
      };
      result.columns.forEach((col, colIdx) => {
        obj[col] = row[colIdx];
      });
      return obj;
    });
  }, [editingRows, result.columns, result.rows.length, deletedIndices]);

  const editCount = useMemo(() => {
    let count = deletedIndices.size;
    const insertedCount = Math.max(0, editingRows.length - result.rows.length);
    count += insertedCount;
    for (let r = 0; r < Math.min(editingRows.length, result.rows.length); r++) {
      if (deletedIndices.has(r)) continue;
      const orig = result.rows[r];
      if (!orig) continue;
      for (let c = 0; c < editingRows[r].length; c++) {
        const ov = orig[c], nv = editingRows[r][c];
        if (ov !== nv && !(ov === null && nv === null)) count++;
      }
    }
    return count;
  }, [editingRows, result.rows, deletedIndices]);

  const handleCellChange = useCallback((rowIndex: number, colIndex: number, newValue: unknown) => {
    setEditingRows((prev) => {
      const next = prev.map((r) => [...r]);
      next[rowIndex][colIndex] = newValue;
      return next;
    });
  }, []);

  const handleAddRow = useCallback(() => {
    setEditingRows((prev) => [...prev, result.columns.map(() => null)]);
  }, [result.columns]);

  const handleGenerateTestData = useCallback(() => {
    setEditingRows((prev) => [
      ...prev,
      result.columns.map((col) => generateTestValue(col)),
    ]);
  }, [result.columns]);

  const handleDeleteSelected = useCallback(() => {
    if (selectedIndices.size === 0) return;
    setDeletedIndices((prev) => {
      const next = new Set(prev);
      selectedIndices.forEach((i) => next.add(i));
      return next;
    });
    setSelectedIndices(new Set());
    gridApiRef.current?.deselectAll();
  }, [selectedIndices]);

  const handleDiscard = useCallback(() => {
    setEditingRows(result.rows.map((r) => [...r]));
    setDeletedIndices(new Set());
    setSelectedIndices(new Set());
    gridApiRef.current?.deselectAll();
  }, [result.rows]);

  const handleSaveToDb = useCallback(async () => {
    if (!isUpdatable || !sourceTable) return;
    setSaving(true);
    try {
      const pkColNames = pkColumns.map((c) => c.name);
      const pkColIndices = pkColNames.map((name) => result.columns.indexOf(name));

      const getPkValues = (row: unknown[]): [string, unknown][] =>
        pkColNames.map((name, i) => [name, row[pkColIndices[i]]]);

      // Build updates
      const updates: CellChange[] = [];
      for (let r = 0; r < Math.min(editingRows.length, result.rows.length); r++) {
        if (deletedIndices.has(r)) continue;
        const orig = result.rows[r];
        if (!orig) continue;
        for (let c = 0; c < editingRows[r].length; c++) {
          const ov = orig[c], nv = editingRows[r][c];
          if (ov !== nv && !(ov === null && nv === null)) {
            updates.push({
              row_index: r,
              column_name: result.columns[c],
              old_value: ov,
              new_value: nv,
              primary_key_values: getPkValues(orig),
            });
          }
        }
      }

      // Build inserts
      const inserts: NewRow[] = [];
      for (let r = result.rows.length; r < editingRows.length; r++) {
        if (deletedIndices.has(r)) continue;
        const values: [string, unknown][] = result.columns.map((col, ci) => [col, editingRows[r][ci]]);
        inserts.push({ values });
      }

      // Build deletes
      const deletes: DeleteRow[] = [];
      deletedIndices.forEach((idx) => {
        if (idx >= result.rows.length) return;
        const orig = result.rows[idx];
        if (!orig) return;
        deletes.push({ primary_key_values: getPkValues(orig) });
      });

      const changes: DataChanges = {
        connection_id: connectionId,
        database,
        schema: sourceTable.schema,
        table: sourceTable.table,
        updates,
        inserts,
        deletes,
      };

      await api.applyChanges(changes);
      addToast(`Saved ${updates.length + inserts.length + deletes.length} changes`);
      setDeletedIndices(new Set());
      setSelectedIndices(new Set());
      onReExecute();
    } catch (e) {
      addToast(String(e), "error");
    } finally {
      setSaving(false);
    }
  }, [isUpdatable, sourceTable, pkColumns, result, editingRows, deletedIndices, connectionId, database, addToast, onReExecute]);

  const lastClickedIdxRef = useRef<number | null>(null);

  const onCellClicked = useCallback((event: CellClickedEvent) => {
    if (!event.data) return;
    const e = event.event as MouseEvent;
    if (!e) return;
    const idx = event.data.__rowIdx as number;

    if (e.ctrlKey || e.metaKey) {
      e.preventDefault();
      setSelectedIndices((prev) => {
        const next = new Set(prev);
        next.has(idx) ? next.delete(idx) : next.add(idx);
        return next;
      });
      lastClickedIdxRef.current = idx;
    } else if (e.shiftKey && lastClickedIdxRef.current !== null) {
      e.preventDefault();
      const from = Math.min(lastClickedIdxRef.current, idx);
      const to = Math.max(lastClickedIdxRef.current, idx);
      setSelectedIndices((prev) => {
        const next = new Set(prev);
        for (let i = from; i <= to; i++) next.add(i);
        return next;
      });
    } else {
      lastClickedIdxRef.current = idx;
    }
  }, []);

  const columnDefs = useMemo((): ColDef[] => {
    const rowNumCol: ColDef = {
      headerName: "#",
      width: 55,
      minWidth: 55,
      maxWidth: 70,
      pinned: "left",
      editable: false,
      sortable: false,
      resizable: false,
      suppressMovable: true,
      cellClass: "grid-row-number",
      headerClass: "grid-row-number",
      valueGetter: (params) => {
        if (!params.data) return "";
        return params.data.__rowIdx + 1;
      },
      onCellClicked: (params) => {
        const e = params.event as MouseEvent;
        if (e && (e.ctrlKey || e.metaKey)) return;
        if (params.data) setDetailRowIdx(params.data.__rowIdx);
      },
    };

    const dataCols: ColDef[] = result.columns.map((col, colIdx) => {
      const colMeta = columnInfos[colIdx];
      const fk = fkMap.get(col);
      const isAutoGen = colMeta?.is_auto_generated || false;
      const def: ColDef = {
        field: col,
        headerName: col,
        headerTooltip: colMeta ? `${col} (${colMeta.data_type})` : col,
        ...(tableColumns ? {
          headerComponent: ResultDataTypeHeader,
          headerComponentParams: {
            dataType: colMeta?.data_type,
            isPk: colMeta?.is_primary_key,
            fk,
          },
        } : {}),
        sortable: true,
        resizable: true,
        editable: (params) => {
          if (!isUpdatable) return false;
          if (!params.data) return false;
          if (params.data.__isDeleted) return false;
          if (params.data.__isInserted && isAutoGen && (params.data[col] === null || params.data[col] === undefined)) return false;
          return true;
        },
        minWidth: 80,
        cellClassRules: {
          "cell-null": (params) => params.value === null || params.value === undefined,
          "cell-modified": (params) => {
            if (!params.data || params.data.__isInserted || params.data.__isDeleted) return false;
            const orig = result.rows[params.data.__rowIdx];
            if (!orig) return false;
            const ov = orig[colIdx], nv = params.value;
            return ov !== nv && !(ov === null && nv === null);
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
          "cell-search-current": (params) => {
            const cur = searchCurrentRef.current;
            if (!cur) return false;
            return params.rowIndex === cur.rowIndex && params.colDef.field === cur.colId;
          },
        },
        valueFormatter: (params) => {
          if (params.data?.__isInserted && isAutoGen && (params.value === null || params.value === undefined)) return "auto";
          if (params.value === null || params.value === undefined) return "NULL";
          if (typeof params.value === "object") return JSON.stringify(params.value);
          return String(params.value);
        },
        valueSetter: (params) => {
          const raw = params.newValue;
          let parsed: unknown;
          if ((raw === "" || raw === null || raw === undefined) && colMeta?.is_nullable) {
            parsed = null;
          } else if (raw === "" || raw === null || raw === undefined) {
            parsed = raw;
          } else if (colMeta) {
            parsed = parseCellValue(String(raw), colMeta.data_type);
          } else {
            parsed = raw;
          }
          handleCellChange(params.data.__rowIdx, colIdx, parsed);
          return true;
        },
      };
      return def;
    });

    return [rowNumCol, ...dataCols];
  }, [result.columns, result.rows, handleCellChange, columnInfos, fkMap, tableColumns, isUpdatable]);

  const selectedIndicesRef = useRef(selectedIndices);
  selectedIndicesRef.current = selectedIndices;

  const getRowClass = useCallback((params: { data?: RowObj }) => {
    if (!params.data) return "";
    const classes: string[] = [];
    if (selectedIndicesRef.current.has(params.data.__rowIdx)) classes.push("row-multi-selected");
    if (params.data.__isInserted) classes.push("row-inserted");
    if (params.data.__isDeleted) classes.push("row-deleted");
    return classes.join(" ");
  }, []);

  useEffect(() => {
    gridApiRef.current?.redrawRows();
  }, [selectedIndices, deletedIndices]);

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

  const [searchMatchIdx, setSearchMatchIdx] = useState(-1);

  const searchMatches = useMemo(() => {
    if (!searchQuery) return [];
    const q = searchQuery.toLowerCase();
    const matches: { rowIndex: number; colId: string }[] = [];
    for (let r = 0; r < editingRows.length; r++) {
      if (deletedIndices.has(r)) continue;
      for (let c = 0; c < editingRows[r].length; c++) {
        const val = editingRows[r][c];
        const str = (val === null || val === undefined) ? "null" : typeof val === "object" ? JSON.stringify(val) : String(val);
        if (str.toLowerCase().includes(q) && result.columns[c]) {
          matches.push({ rowIndex: r, colId: result.columns[c] });
        }
      }
    }
    return matches;
  }, [searchQuery, editingRows, result.columns, deletedIndices]);

  const searchMatchCount = searchMatches.length;

  const navigateToMatch = useCallback((idx: number) => {
    if (idx < 0 || idx >= searchMatches.length) return;
    setSearchMatchIdx(idx);
    const match = searchMatches[idx];
    searchCurrentRef.current = match;
    const api = gridApiRef.current;
    if (!api) return;
    api.ensureIndexVisible(match.rowIndex, "middle");
    api.ensureColumnVisible(match.colId);
    api.refreshCells({ force: true });
  }, [searchMatches]);

  useEffect(() => {
    if (searchMatches.length > 0) {
      navigateToMatch(0);
    } else {
      setSearchMatchIdx(-1);
      searchCurrentRef.current = null;
    }
  }, [searchMatches, navigateToMatch]);

  const searchNext = useCallback(() => {
    if (searchMatches.length === 0) return;
    const next = searchMatchIdx + 1 >= searchMatches.length ? 0 : searchMatchIdx + 1;
    navigateToMatch(next);
  }, [searchMatchIdx, searchMatches, navigateToMatch]);

  const searchPrev = useCallback(() => {
    if (searchMatches.length === 0) return;
    const prev = searchMatchIdx - 1 < 0 ? searchMatches.length - 1 : searchMatchIdx - 1;
    navigateToMatch(prev);
  }, [searchMatchIdx, searchMatches, navigateToMatch]);

  useEffect(() => {
    searchQueryRef.current = searchQuery;
    const api = gridApiRef.current;
    if (!api) return;
    api.refreshCells({ force: true });
  }, [searchQuery]);

  useEffect(() => {
    let focusTimer: ReturnType<typeof setTimeout>;
    const handler = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "f") {
        e.preventDefault();
        setShowSearch(true);
        focusTimer = setTimeout(() => searchInputRef.current?.focus(), 0);
      }
      if (e.key === "Escape") {
        if (showSearch) {
          setShowSearch(false);
          setSearchQuery("");
          searchCurrentRef.current = null;
        }
        if (selectedIndices.size > 0) {
          setSelectedIndices(new Set());
          gridApiRef.current?.deselectAll();
        }
      }
      if (isUpdatable && (e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "a") {
        const target = e.target as HTMLElement;
        if (target?.tagName === "INPUT" || target?.tagName === "TEXTAREA") return;
        if (target?.getAttribute("role") === "textbox") return;
        e.preventDefault();
        setSelectedIndices(new Set(editingRows.map((_, i) => i)));
      }
      if (isUpdatable && (e.key === "Delete" || e.key === "Backspace") && selectedIndices.size > 0) {
        const target = e.target as HTMLElement;
        if (target?.tagName === "INPUT" || target?.tagName === "TEXTAREA") return;
        if (target?.getAttribute("role") === "textbox") return;
        e.preventDefault();
        handleDeleteSelected();
      }
    };
    document.addEventListener("keydown", handler);
    return () => {
      document.removeEventListener("keydown", handler);
      clearTimeout(focusTimer);
    };
  }, [showSearch, selectedIndices, handleDeleteSelected, editingRows, isUpdatable]);

  useEffect(() => {
    if (!contextMenu) return;
    const close = () => setContextMenu(null);
    document.addEventListener("click", close);
    return () => document.removeEventListener("click", close);
  }, [contextMenu]);

  const onGridReady = useCallback((params: GridReadyEvent) => {
    gridApiRef.current = params.api;
  }, []);

  const onCellContextMenu = useCallback((event: CellContextMenuEvent) => {
    if (!event.data) return;
    const e = event.event as MouseEvent;
    if (!e) return;
    e.preventDefault();
    const z = parseFloat(document.documentElement.style.zoom || "100") / 100;
    setContextMenu({ x: e.clientX / z, y: e.clientY / z, rowIdx: event.data.__rowIdx });
  }, []);

  const handleCopyRowAsJson = useCallback(() => {
    if (contextMenu === null) return;
    const obj: Record<string, unknown> = {};
    result.columns.forEach((col, i) => {
      obj[col] = editingRows[contextMenu.rowIdx]?.[i];
    });
    navigator.clipboard.writeText(JSON.stringify(obj, null, 2));
    setContextMenu(null);
  }, [contextMenu, result.columns, editingRows]);

  const handleCopyCell = useCallback(() => {
    if (contextMenu === null) return;
    const focusedCell = gridApiRef.current?.getFocusedCell();
    if (focusedCell) {
      const colId = focusedCell.column.getColId();
      const colIdx = result.columns.indexOf(colId);
      if (colIdx >= 0) {
        const val = editingRows[contextMenu.rowIdx]?.[colIdx];
        const str = val === null || val === undefined ? "NULL" : typeof val === "object" ? JSON.stringify(val) : String(val);
        navigator.clipboard.writeText(str);
      }
    }
    setContextMenu(null);
  }, [contextMenu, result.columns, editingRows]);

  const formatRowAsInsert = useCallback((row: unknown[]): string => {
    if (!sourceTable) return "";
    const cols = result.columns.map((c) => `"${c}"`).join(", ");
    const vals = row
      .map((v) => {
        if (v === null) return "NULL";
        if (typeof v === "number") return String(v);
        if (typeof v === "boolean") return v ? "TRUE" : "FALSE";
        return `'${String(v).replace(/'/g, "''")}'`;
      })
      .join(", ");
    const tbl = sourceTable.schema ? `"${sourceTable.schema}"."${sourceTable.table}"` : `"${sourceTable.table}"`;
    return `INSERT INTO ${tbl} (${cols}) VALUES (${vals});`;
  }, [sourceTable, result.columns]);

  const handleCopyAsInsert = useCallback(() => {
    if (contextMenu === null) return;
    const row = editingRows[contextMenu.rowIdx];
    if (row) navigator.clipboard.writeText(formatRowAsInsert(row));
    setContextMenu(null);
  }, [contextMenu, editingRows, formatRowAsInsert]);

  const handleToggleDeleteRow = useCallback((rowIdx: number) => {
    setDeletedIndices((prev) => {
      const next = new Set(prev);
      next.has(rowIdx) ? next.delete(rowIdx) : next.add(rowIdx);
      return next;
    });
  }, []);

  const handleJumpToFk = useCallback((fk: ForeignKeyInfo, cellValue: unknown) => {
    if (cellValue === null || cellValue === undefined || !sourceTable) return;
    const conn = connections.find((c) => c.id === connectionId);
    const tabId = `${connectionId}:${database}:${sourceTable.schema}:${fk.referenced_table}`;
    const tab: TabInfo = {
      id: tabId,
      type: "table",
      title: `${sourceTable.schema}.${fk.referenced_table}`,
      connectionId,
      connectionColor: conn?.color || "#6398ff",
      database,
      schema: sourceTable.schema,
      table: fk.referenced_table,
    };
    openTab(tab);
    addToast(`Opened ${fk.referenced_table} — filter by ${fk.referenced_column} = ${JSON.stringify(cellValue)}`);
  }, [connectionId, database, sourceTable, connections, openTab, addToast]);

  const handleDownloadCsv = useCallback(() => {
    const escape = (v: unknown) => {
      if (v === null || v === undefined) return "";
      const s = typeof v === "object" ? JSON.stringify(v) : String(v);
      if (s.includes(",") || s.includes('"') || s.includes("\n")) {
        return `"${s.replace(/"/g, '""')}"`;
      }
      return s;
    };
    const header = result.columns.map((c) => escape(c)).join(",");
    const activeRows = editingRows.filter((_, i) => !deletedIndices.has(i));
    const rows = activeRows.map((row) => row.map((v) => escape(v)).join(","));
    const csv = [header, ...rows].join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "query_result.csv";
    a.click();
    URL.revokeObjectURL(url);
  }, [result.columns, editingRows, deletedIndices]);

  return (
    <div className="result-table-ag-wrapper">
      <div className="result-table-toolbar">
        {isUpdatable && (
          <>
            <button className="btn-ghost" onClick={handleAddRow} title="Add empty row">
              <Plus size={14} /> Add Row
            </button>
            <button className="btn-ghost" onClick={handleGenerateTestData} title="Generate a row with random test data">
              <Shuffle size={14} /> Test Data
            </button>
          </>
        )}
        {isUpdatable && selectedIndices.size > 0 && (
          <button className="btn-delete-selected" onClick={handleDeleteSelected} title={`Delete ${selectedIndices.size} selected row${selectedIndices.size > 1 ? "s" : ""}`}>
            <Trash2 size={14} /> Delete {selectedIndices.size} row{selectedIndices.size > 1 ? "s" : ""}
          </button>
        )}
        <span className={`result-updatable-badge ${isUpdatable ? "updatable" : "readonly"}`} title={
          isUpdatable
            ? `Editable: ${sourceTable!.schema}.${sourceTable!.table} (PK: ${pkColumns.map(c => c.name).join(", ")})`
            : sourceTable && pkColumns.length > 0
              ? `Read-only: primary key column${pkColumns.length > 1 ? "s" : ""} (${pkColumns.map(c => c.name).join(", ")}) not in result`
              : sourceTable
                ? "Read-only: table has no primary key"
                : "Read-only: complex query or non-SELECT"
        }>
          {isUpdatable ? <Database size={12} /> : <Lock size={12} />}
          {isUpdatable ? "Editable" : "Read-only"}
        </span>
        <div className="flex-spacer" />
        {isUpdatable && editCount > 0 && (
          <>
            <button className="btn-discard" onClick={handleDiscard}>
              <Undo2 size={14} /> Discard
            </button>
            <button className="result-save-btn" onClick={handleSaveToDb} disabled={saving} title="Save changes to database">
              {saving ? <Loader2 size={14} className="spin" /> : <Save size={14} />}
              {saving ? "Saving..." : `Save ${editCount} change${editCount !== 1 ? "s" : ""}`}
            </button>
          </>
        )}
        <button
          className={`btn-ghost ${resultMode === "chart" ? "active-filter" : ""}`}
          onClick={onToggleChart}
          title="Toggle Chart View"
        >
          <BarChart3 size={14} /> Chart
        </button>
        <ExportMenu onExport={onExport} />
      </div>
      {showSearch && (
        <div className="grid-search-bar result-search-bar">
          <Search size={14} className="grid-search-icon" />
          <input
            ref={searchInputRef}
            className="grid-search-input"
            placeholder="Search results..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Escape") { setShowSearch(false); setSearchQuery(""); searchCurrentRef.current = null; }
              if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); searchNext(); }
              if (e.key === "Enter" && e.shiftKey) { e.preventDefault(); searchPrev(); }
            }}
          />
          {searchQuery && (
            <div className="grid-search-nav">
              <span className="grid-search-count">
                {searchMatchCount > 0 ? `${searchMatchIdx + 1} / ${searchMatchCount}` : "No results"}
              </span>
              <button
                className="btn-icon grid-search-nav-btn"
                onClick={searchPrev}
                disabled={searchMatchCount === 0}
                title="Previous match (Shift+Enter)"
              >
                <ChevronUp size={14} />
              </button>
              <button
                className="btn-icon grid-search-nav-btn"
                onClick={searchNext}
                disabled={searchMatchCount === 0}
                title="Next match (Enter)"
              >
                <ChevronDownIcon size={14} />
              </button>
            </div>
          )}
          <button
            className="btn-icon grid-search-close"
            onClick={() => { setShowSearch(false); setSearchQuery(""); searchCurrentRef.current = null; }}
          >
            <X size={14} />
          </button>
        </div>
      )}
      <div className="ag-grid-wrapper" style={{ flex: 1 }}>
        <AgGridReact
          theme={gridTheme}
          modules={[AllCommunityModule]}
          rowData={rowData}
          columnDefs={columnDefs}
          defaultColDef={defaultColDef}
          getRowId={(params) => String(params.data.__rowIdx)}
          getRowClass={getRowClass as any}
          onGridReady={onGridReady}
          onCellClicked={onCellClicked}
          onCellContextMenu={onCellContextMenu}
          preventDefaultOnContextMenu={true}
          animateRows={false}
          suppressCellFocus={false}
          stopEditingWhenCellsLoseFocus={true}
          singleClickEdit={false}
          enableCellTextSelection={true}
          suppressRowClickSelection={true}
        />
      </div>

      {contextMenu !== null && editingRows[contextMenu.rowIdx] && (
        <div
          className="context-menu"
          style={{ left: contextMenu.x, top: contextMenu.y }}
          ref={(el) => {
            if (!el) return;
            const z = parseFloat(document.documentElement.style.zoom || "100") / 100;
            const cssVh = window.innerHeight / z;
            const cssVw = window.innerWidth / z;
            if (contextMenu.y + el.offsetHeight > cssVh) {
              el.style.top = `${Math.max(4, cssVh - el.offsetHeight)}px`;
            }
            if (contextMenu.x + el.offsetWidth > cssVw) {
              el.style.left = `${Math.max(4, cssVw - el.offsetWidth)}px`;
            }
          }}
        >
          <button
            className="context-menu-item"
            onClick={() => { setDetailRowIdx(contextMenu.rowIdx); setContextMenu(null); }}
          >
            View as JSON
          </button>
          <button className="context-menu-item" onClick={handleCopyCell}>
            Copy cell value
          </button>
          <button className="context-menu-item" onClick={handleCopyRowAsJson}>
            Copy row as JSON
          </button>
          {sourceTable && (
            <button className="context-menu-item" onClick={handleCopyAsInsert}>
              Copy as INSERT
            </button>
          )}
          {isUpdatable && (
            <>
              <div className="context-menu-divider" />
              {selectedIndices.size > 1 && selectedIndices.has(contextMenu.rowIdx) ? (
                <button className="context-menu-item context-menu-danger" onClick={() => { handleDeleteSelected(); setContextMenu(null); }}>
                  <Trash2 size={14} /> Delete {selectedIndices.size} selected rows
                </button>
              ) : (
                <button
                  className="context-menu-item"
                  onClick={() => { handleToggleDeleteRow(contextMenu.rowIdx); setContextMenu(null); }}
                >
                  {deletedIndices.has(contextMenu.rowIdx) ? "Undo delete" : "Delete row"}
                </button>
              )}
            </>
          )}
          {columnInfos.map((colMeta, colIdx) => {
            const fk = fkMap.get(colMeta.name);
            if (!fk) return null;
            const cellVal = editingRows[contextMenu.rowIdx]?.[colIdx];
            if (cellVal === null || cellVal === undefined) return null;
            return (
              <button
                key={colMeta.name}
                className="context-menu-item context-menu-fk"
                onClick={() => { handleJumpToFk(fk, cellVal); setContextMenu(null); }}
              >
                <ExternalLink size={12} />
                Jump to {fk.referenced_table}.{fk.referenced_column} = {JSON.stringify(cellVal)}
              </button>
            );
          })}
        </div>
      )}

      {detailRowIdx !== null && editingRows[detailRowIdx] && (
        <RowDetailView
          columns={columnInfos}
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
