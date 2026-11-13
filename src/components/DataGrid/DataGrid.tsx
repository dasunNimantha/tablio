import { useEffect, useState, useCallback, useRef, useMemo } from "react";
import { usePolling } from "../../hooks/usePolling";
import { useReportTabDirty } from "../../hooks/useTabDirty";
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
  ExplainResult,
} from "../../lib/tauri";
import { readZoomFactor } from "../../lib/zoom";
import { FilterBar, defaultFilterConditions } from "./FilterBar";
import { buildWhereClause, type FilterCondition } from "../../lib/filterBuilder";
import {
  LARGE_TEXT_EDITOR_PARAMS,
  cellEditText,
  colField,
  colIndexFromId,
  restoreLineEndings,
  rowToObject,
  shouldSuppressGridKey,
  usesLargeTextEditor,
} from "../../lib/gridData";
import { RowDetailView } from "./RowDetailView";
import { ConfirmDialog } from "../ConfirmDialog";
import { editorInputToValue } from "../../lib/cellValues";
import { ExplainView } from "../QueryConsole/ExplainView";
import { ExportMenu } from "../ExportMenu";
import { ColumnOrganizer, ColumnSettings, loadColumnSettings, saveColumnSettings, applyColumnSettings } from "./ColumnOrganizer";
import { useToastStore } from "../../stores/toastStore";
import { useTabStore, TabInfo } from "../../stores/tabStore";
import { useUserSettingsStore } from "../../stores/userSettingsStore";
import { useConnectionStore } from "../../stores/connectionStore";
import { boolLiteral, paginationClause, quoteIdent, quoteQualified, stringLiteral } from "../../lib/sqlDialect";
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
  ChevronUp,
  ChevronDown as ChevronDownIcon,
  X,
  RefreshCw,
  Zap,
  ExternalLink,
  Shuffle,
  Trash2,
  Lock,
} from "lucide-react";
import { save } from "@tauri-apps/plugin-dialog";
import { AllCommunityModule, themeQuartz, type ColDef, type CellClickedEvent, type CellContextMenuEvent, type CellKeyDownEvent, type GridApi, type GridReadyEvent, type IHeaderParams, type SelectionChangedEvent } from "ag-grid-community";
import { AgGridReact } from "ag-grid-react";
import { pendingInsertToNewRow, type PendingInsert } from "./pendingInsert";
import { nullCellSelector } from "./NullCell";
import "./DataGrid.css";
import "./ag-grid-theme.css";

function DataTypeHeader(props: IHeaderParams & { dataType?: string; isPk?: boolean; fk?: ForeignKeyInfo }) {
  const [sortState, setSortState] = useState<"asc" | "desc" | null>(null);
  // Priority position of this column inside ag-grid's multi-sort
  // model: 0 = primary, 1 = secondary, etc. `null` when this
  // column isn't sorted. We render a small numeric badge next to
  // the direction arrow when sortIndex >= 1 so users can tell
  // which column drives the primary sort — before issue #57 the
  // server only honoured the first sort silently. (When only one
  // column is sorted, `sortIndex` is 0 and we hide the badge to
  // keep the header clean for the common single-sort case.)
  const [sortIndex, setSortIndex] = useState<number | null>(null);

  useEffect(() => {
    const listener = () => {
      if (props.column.isSortAscending()) setSortState("asc");
      else if (props.column.isSortDescending()) setSortState("desc");
      else setSortState(null);
      // ag-grid types `getSortIndex` as `number | null | undefined`;
      // normalise to `number | null` so the render side has one
      // shape to check.
      const idx = props.column.getSortIndex?.();
      setSortIndex(idx == null ? null : idx);
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
          {sortState && sortIndex !== null && sortIndex > 0 && (
            <span
              className="ag-custom-sort-priority"
              title={`Sort priority ${sortIndex + 1}`}
            >
              {sortIndex + 1}
            </span>
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

/**
 * AG Grid theme that respects the user's editor-font preference
 * (issue #62). `themeQuartz.withParams` emits CSS variables (e.g.
 * `--ag-font-size`) that AG Grid's own runtime stylesheet reads
 * with higher specificity than any static rule we could write, so
 * we have to go through this API to make the font size flow into
 * cells + headers.
 */
function buildGridTheme(editorFontSize: number) {
  return themeQuartz.withParams({
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
    fontSize: editorFontSize,
    // Header keeps its smaller fixed size — header text isn't the
    // primary "read your data" surface, so we don't tie it to the
    // user's pref. Cells are what matter for the editor-font
    // mental model.
    headerFontSize: 12,
    rowHeight: 36,
    headerHeight: 38,
    cellHorizontalPadding: 10,
    wrapperBorderRadius: 0,
    borderRadius: 0,
    headerColumnResizeHandleColor: "transparent",
  });
}

interface Props {
  /** Owning tab, used to report unsaved changes before the tab closes. */
  tabId?: string;
  connectionId: string;
  database: string;
  schema: string;
  table: string;
  hideTitle?: boolean;
  /** True only while this grid is actually visible: its tab is the
   *  active tab and the Data panel is showing. When false the periodic
   *  auto-refresh is suspended and the grid's keyboard shortcuts are
   *  ignored (all tabs stay mounted, so hidden grids must not react to
   *  Ctrl+A / Delete meant for the visible one). Defaults to `true`. */
  isActive?: boolean;
  /** Changes when the table's columns or foreign keys were altered
   *  outside the grid; rows and FK links are re-read in response. */
  structureVersion?: number;
}

interface PendingChanges {
  updates: Map<string, CellChange>;
  inserts: PendingInsert[];
  deletedKeys: Set<string>;
}

let nextInsertTempId = 1;

function sameSort(a: SortSpec[], b: SortSpec[]): boolean {
  return a.length === b.length &&
    a.every((p, i) => p.column === b[i].column && p.direction === b[i].direction);
}

/** Grid columns are addressed by position (see `colField`), sorts by name. */
function applySortToGrid(api: GridApi | null, spec: SortSpec[], columns: ColumnInfo[]) {
  api?.applyColumnState({
    state: spec.flatMap((s, i) => {
      const idx = columns.findIndex((c) => c.name === s.column);
      return idx < 0 ? [] : [{ colId: colField(idx), sort: s.direction, sortIndex: i }];
    }),
    defaultState: { sort: null },
  });
}

/** Row object handed to ag-grid; column values live under `colField(i)`. */
interface RowObj {
  __rowIdx: number;
  __pkKey: string;
  __isInserted: boolean;
  __isDeleted: boolean;
  /** Pending inserts: which columns the user has set. */
  __touched?: boolean[];
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

export function DataGrid({
  tabId,
  connectionId,
  database,
  schema,
  table,
  hideTitle = false,
  isActive = true,
  structureVersion = 0,
}: Props) {
  const addToast = useToastStore((s) => s.addToast);
  const openTab = useTabStore((s) => s.openTab);
  // Editor font preference flows into the grid's cell font-size via
  // the AG Grid theme API. Rebuilding the theme each time the user
  // adjusts the preference is cheap (it's a pure factory) and AG
  // Grid handles the theme swap without remount.
  const editorFontSize = useUserSettingsStore(
    (s) => s.settings.editorFontSize,
  );
  const gridTheme = useMemo(
    () => buildGridTheme(editorFontSize),
    [editorFontSize],
  );
  const connections = useConnectionStore((s) => s.connections);
  const [data, setData] = useState<TableData | null>(null);
  const dataRef = useRef<TableData | null>(null);
  dataRef.current = data;
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [page, setPage] = useState(0);
  // Multi-column sort (issue #57). The array is in priority order:
  // index 0 is the primary sort, index 1 the secondary, etc.
  // Empty = no user-picked sort (driver falls back to PK ordering).
  const [sort, setSort] = useState<SortSpec[]>([]);
  const sortRef = useRef<SortSpec[]>([]);
  const [saving, setSaving] = useState(false);
  const [showFilter, setShowFilter] = useState(false);
  const [activeFilter, setActiveFilter] = useState<string | null>(null);
  const [filterConditions, setFilterConditions] = useState<FilterCondition[] | null>(null);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [filterLoading, setFilterLoading] = useState(false);

  const [changes, setChanges] = useState<PendingChanges>({
    updates: new Map(),
    inserts: [],
    deletedKeys: new Set(),
  });

  // Working copy of the fetched page's rows (edits applied). Pending
  // inserts are kept in `changes.inserts`; `editingRows` below is the
  // combined view used for rendering: existing rows first, then inserts.
  const [existingRows, setExistingRows] = useState<unknown[][]>([]);
  const editingRows = useMemo(
    () => (changes.inserts.length === 0
      ? existingRows
      : [...existingRows, ...changes.inserts.map((ins) => ins.values)]),
    [existingRows, changes.inserts],
  );
  const [pendingNav, setPendingNav] = useState<null | (() => void)>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const isConnected = useConnectionStore((s) => s.activeConnections.has(connectionId));
  const [detailRowIdx, setDetailRowIdx] = useState<number | null>(null);
  const [rowContextMenu, setRowContextMenu] = useState<{ x: number; y: number; rowIdx: number; colIdx: number } | null>(null);
  const [selectedPkKeys, setSelectedPkKeys] = useState<Set<string>>(new Set());
  const [refreshInterval, setRefreshInterval] = useState(10);
  const [showRefreshMenu, setShowRefreshMenu] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [showSearch, setShowSearch] = useState(false);
  const refreshBtnRef = useRef<HTMLButtonElement>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);
  const searchQueryRef = useRef("");
  const searchCurrentRef = useRef<{ rowIndex: number; colId: string } | null>(null);
  const gridApiRef = useRef<GridApi | null>(null);
  const [fkMap, setFkMap] = useState<Map<string, ForeignKeyInfo>>(new Map());
  const [queryRunning, setQueryRunning] = useState(false);
  const fetchStartRef = useRef<number | null>(null);
  const [showExplain, setShowExplain] = useState(false);
  const [explainResult, setExplainResult] = useState<ExplainResult | null>(null);
  const [explainLoading, setExplainLoading] = useState(false);
  const [explainHeight, setExplainHeight] = useState(220);
  const explainDragging = useRef(false);

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
  const changeCount = changes.updates.size + changes.inserts.length + changes.deletedKeys.size;
  const hasChangesLatestRef = useRef(hasChanges);
  hasChangesLatestRef.current = hasChanges;
  useReportTabDirty(tabId, "grid", hasChanges);

  const fetchGenRef = useRef(0);

  const fetchData = useCallback(async (opts?: { silent?: boolean }) => {
    const gen = ++fetchGenRef.current;
    // Periodic auto-refresh passes { silent: true } so the loading
    // bar / data dimming don't flicker on every interval tick. The
    // newly fetched rows still swap in atomically when the request
    // resolves.
    if (!opts?.silent) setLoading(true);
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
      // A background refresh that lands after the user started editing
      // must not replace the rows the pending edits were made against
      // (it would wipe the visible edit while Save/Discard still show).
      if (opts?.silent && hasChangesLatestRef.current) return;
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
      setExistingRows(sorted.rows.map((r) => [...r]));
      setDetailRowIdx((prev) => {
        if (prev == null) return null;
        if (prev >= sorted.rows.length) return null;
        return prev;
      });
    } catch (e) {
      if (gen !== fetchGenRef.current) return;
      setError(String(e));
    } finally {
      if (gen === fetchGenRef.current) {
        setLoading(false);
        setFilterLoading(false);
        setQueryRunning(false);
        fetchStartRef.current = null;
      }
    }
  }, [connectionId, database, schema, table, page, sort, activeFilter]);

  useEffect(() => {
    setSelectedPkKeys(new Set());
    gridApiRef.current?.deselectAll();
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
  }, [connectionId, database, schema, table, structureVersion]);

  // Unsaved edits are keyed to the current column set, so leave them
  // alone; the post-save fetch picks up the new structure.
  const seenStructureVersionRef = useRef(structureVersion);
  useEffect(() => {
    if (seenStructureVersionRef.current === structureVersion) return;
    seenStructureVersionRef.current = structureVersion;
    if (!hasChangesLatestRef.current) fetchData({ silent: true });
  }, [structureVersion, fetchData]);

  useEffect(() => {
    if (!rowContextMenu) return;
    const close = () => setRowContextMenu(null);
    document.addEventListener("click", close);
    return () => document.removeEventListener("click", close);
  }, [rowContextMenu]);

  useEffect(() => {
    gridApiRef.current?.redrawRows();
  }, [changes, detailRowIdx]);

  // Skip the timer entirely when:
  //  - the user disabled auto-refresh (refreshInterval = 0)
  //  - there are unsaved changes (would clobber pending edits)
  //  - the grid is mounted but currently hidden (TableView keeps the
  //    Data tab in the DOM with display:none when Schema is shown;
  //    polling a hidden grid is wasted work and can confuse users
  //    who think "I'm not on this tab, why is it making requests?")
  //
  // usePolling additionally pauses on document.hidden and prevents
  // overlapping fetches — important for slow queries on large
  // tables, where a 5s refresh interval used to let two or three
  // fetchData calls stack up and freeze the grid for the duration
  // of all of them.
  //  - the connection is closed (nothing to poll; also stops tabs of a
  //    disconnected connection from erroring every interval)
  //
  // `isActive` is false for grids in background tabs, so only the
  // visible grid polls even though every tab stays mounted.
  const refreshEnabled = refreshInterval > 0 && !hasChanges && isActive && isConnected;
  const silentFetch = useCallback(() => {
    fetchData({ silent: true });
  }, [fetchData]);
  usePolling(silentFetch, refreshInterval * 1000, refreshEnabled);

  useEffect(() => {
    if (!showRefreshMenu) return;
    const close = () => setShowRefreshMenu(false);
    document.addEventListener("click", close);
    return () => document.removeEventListener("click", close);
  }, [showRefreshMenu]);


  const deleteSelectedRef = useRef<() => void>(() => {});

  useEffect(() => {
    // Every open tab keeps its grid mounted, so this document-level
    // listener exists once per tab. Only the visible grid may react,
    // and destructive / selection shortcuts additionally require the
    // keystroke to originate inside this grid.
    if (!isActive) return;
    const handler = (e: KeyboardEvent) => {
      const target = e.target as Node | null;
      const inGrid = !!target && !!containerRef.current?.contains(target);
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "f") {
        e.preventDefault();
        setShowSearch(true);
        requestAnimationFrame(() => searchInputRef.current?.focus());
      }
      if (!inGrid) return;
      if (e.key === "Escape" && showSearch) {
        setShowSearch(false);
        setSearchQuery("");
        searchCurrentRef.current = null;
      }
      if (e.key === "Escape" && selectedPkKeys.size > 0) {
        setSelectedPkKeys(new Set());
        gridApiRef.current?.deselectAll();
      }
      if ((e.key === "Delete" || e.key === "Backspace") && selectedPkKeys.size > 0 && !readOnlyRef.current) {
        const target = e.target as HTMLElement;
        if (target.tagName === "INPUT" || target.tagName === "TEXTAREA" || target.isContentEditable) return;
        e.preventDefault();
        deleteSelectedRef.current();
      }
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === "a") {
        const target = e.target as HTMLElement;
        if (target.tagName === "INPUT" || target.tagName === "TEXTAREA" || target.isContentEditable) return;
        if (!gridApiRef.current) return;
        e.preventDefault();
        gridApiRef.current.selectAll();
      }
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [showSearch, selectedPkKeys, isActive]);

  const [searchMatchIdx, setSearchMatchIdx] = useState(-1);

  const searchMatches = useMemo(() => {
    if (!searchQuery || !data) return [];
    const q = searchQuery.toLowerCase();
    const matches: { rowIndex: number; colId: string }[] = [];
    // Only visible columns: a match in a hidden column can't be shown.
    for (let r = 0; r < editingRows.length; r++) {
      for (const c of visibleIndices) {
        const str = cellEditText(editingRows[r][c]) ?? "null";
        if (str.toLowerCase().includes(q)) {
          matches.push({ rowIndex: r, colId: colField(c) });
        }
      }
    }
    return matches;
  }, [searchQuery, editingRows, data, visibleIndices]);

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
    if (!gridApiRef.current) return;
    gridApiRef.current.refreshCells({ force: true });
  }, [searchQuery]);

  const totalPages = data ? Math.ceil(data.total_rows / PAGE_SIZE) : 0;

  const originalRowCount = data?.rows.length ?? 0;

  const getPkValues = useCallback((rowIndex: number): [string, unknown][] => {
    if (!data || rowIndex < 0 || rowIndex >= data.rows.length) return [];
    const row = data.rows[rowIndex];
    return data.columns.flatMap((c, i) =>
      c.is_primary_key ? [[c.name, row[i]] as [string, unknown]] : []);
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
    // Mark dirty immediately so a refresh resolving before the next
    // render can't overwrite this edit.
    hasChangesLatestRef.current = true;
    if (rowIndex >= data.rows.length) {
      const insertIdx = rowIndex - data.rows.length;
      setChanges((prev) => {
        const target = prev.inserts[insertIdx];
        if (!target) return prev;
        const values = [...target.values];
        const touched = [...target.touched];
        values[colIndex] = newValue;
        touched[colIndex] = true;
        const inserts = [...prev.inserts];
        inserts[insertIdx] = { ...target, values, touched };
        return { ...prev, inserts };
      });
      return;
    }
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

    setExistingRows((prev) => {
      if (!prev[rowIndex]) return prev;
      const next = [...prev];
      next[rowIndex] = [...prev[rowIndex]];
      next[rowIndex][colIndex] = newValue;
      return next;
    });
  }, [data, getPkValues, getPkKey]);

  const pkIndices = useMemo(() => {
    return data?.columns.flatMap((c, i) => (c.is_primary_key ? [i] : [])) ?? [];
  }, [data]);
  // Rows can only be addressed for UPDATE / DELETE through a primary
  // key. Without one every row would share the same key, so edits would
  // collide and deletes would hit every row: the grid is read-only.
  const readOnly = !!data && pkIndices.length === 0;
  const readOnlyRef = useRef(readOnly);
  readOnlyRef.current = readOnly;
  const savingRef = useRef(false);
  const selectedCountRef = useRef(0);
  selectedCountRef.current = selectedPkKeys.size;

  const { agRowData, pinnedTopRows } = useMemo(() => {
    if (!data) return { agRowData: [] as RowObj[], pinnedTopRows: [] as RowObj[] };
    const existing: RowObj[] = [];
    const inserted: RowObj[] = [];
    for (let i = 0; i < editingRows.length; i++) {
      const row = editingRows[i];
      const isInserted = i >= originalRowCount;
      const obj: RowObj = { __rowIdx: i, __pkKey: "", __isInserted: isInserted, __isDeleted: false };
      data.columns.forEach((_, colIdx) => { obj[colField(colIdx)] = row[colIdx]; });
      if (isInserted) {
        const ins = changes.inserts[i - originalRowCount];
        obj.__pkKey = `__ins_${ins ? ins.tempId : i - originalRowCount}`;
        obj.__touched = ins?.touched;
        inserted.push(obj);
      } else if (pkIndices.length > 0) {
        // Keyed by the *original* PK values so an edit to a PK cell
        // doesn't change the row's identity.
        obj.__pkKey = getPkKey(i);
        obj.__isDeleted = changes.deletedKeys.has(obj.__pkKey);
        existing.push(obj);
      } else {
        obj.__pkKey = `__nopk_${i}`;
        existing.push(obj);
      }
    }
    return { agRowData: existing, pinnedTopRows: inserted };
  }, [editingRows, data, originalRowCount, pkIndices, getPkKey, changes.deletedKeys, changes.inserts]);

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
        // Marked for deletion: a clear marker on top of the
        // strikethrough so it reads at a glance.
        if (params.data.__isDeleted) return "−";
        return page * PAGE_SIZE + params.data.__rowIdx + 1;
      },
      onCellClicked: (params) => {
        if (!params.data) return;
        const e = params.event as MouseEvent;
        if (e && (e.ctrlKey || e.metaKey || e.shiftKey)) return;
        setDetailRowIdx((prev) => prev === params.data.__rowIdx ? null : params.data.__rowIdx);
      },
    };

    const dataCols: ColDef[] = visibleIndices.map((colIdx) => {
      const col = data.columns[colIdx];
      const isAutoGen = col.is_auto_generated;
      const field = colField(colIdx);
      // Untouched columns of a pending insert are left out of the
      // INSERT, so the database fills them in: show that instead of
      // a NULL that will never be written.
      const placeholderFor = (row: RowObj | undefined, value: unknown): string | null => {
        if (!row?.__isInserted || row.__touched?.[colIdx]) return null;
        if (value !== null && value !== undefined) return null;
        if (isAutoGen) return "auto";
        if (col.default_value !== null && col.default_value !== undefined) return "default";
        return null;
      };

      return {
        colId: field,
        headerName: col.name,
        headerTooltip: `${col.name} (${col.data_type})`,
        headerComponent: DataTypeHeader,
        headerComponentParams: {
          dataType: col.data_type,
          isPk: col.is_primary_key,
          fk: fkMap.get(col.name),
        },
        // Read by position, never by name: a "." in a column name would
        // otherwise be taken as a nested path. Objects (Cassandra maps,
        // UDTs) become their JSON text so display, search and the editor
        // all see the same string.
        valueGetter: (params) => (params.data ? cellEditText(params.data[field]) : null),
        editable: (params) => {
          if (readOnlyRef.current || savingRef.current) return false;
          if (!params.data || params.data.__isDeleted) return false;
          if (params.data.__isInserted && isAutoGen && (params.data[field] === null || params.data[field] === undefined)) return false;
          return true;
        },
        // ag-grid's default editor is a single-line <input> that drops
        // newlines; long, multi-line, text and JSON values get a
        // textarea (Shift+Enter for a new line, Enter to commit).
        cellRendererSelector: nullCellSelector,
        cellEditorSelector: (params) =>
          usesLargeTextEditor(col.data_type, params.data?.[field])
            ? { component: "agLargeTextCellEditor", popup: true, params: LARGE_TEXT_EDITOR_PARAMS }
            : { component: "agTextCellEditor" },
        cellClassRules: {
          "cell-null": (params) => (params.value === null || params.value === undefined) && !placeholderFor(params.data, params.value),
          "cell-modified": (params) => {
            if (!params.data || params.data.__isInserted) return false;
            return changes.updates.has(`${params.data.__pkKey}:${col.name}`);
          },
          "cell-inserted": (params) => params.data?.__isInserted === true,
          "cell-auto": (params) => placeholderFor(params.data, params.value) !== null,
          "cell-search-match": (params) => {
            const q = searchQueryRef.current;
            if (!q) return false;
            const str = params.value === null || params.value === undefined ? "null" : String(params.value);
            return str.toLowerCase().includes(q.toLowerCase());
          },
          "cell-search-current": (params) => {
            const cur = searchCurrentRef.current;
            if (!cur) return false;
            return params.rowIndex === cur.rowIndex && params.column.getColId() === cur.colId;
          },
        },
        valueFormatter: (params) => {
          const placeholder = placeholderFor(params.data, params.value);
          if (placeholder) return placeholder;
          if (params.value === null || params.value === undefined) return "NULL";
          return String(params.value);
        },
        valueSetter: (params) => {
          const rowIdx = params.data.__rowIdx;
          // Opening and closing the editor without typing must not
          // record an edit (e.g. a NULL cell would otherwise become '').
          if (params.newValue === params.oldValue) return false;
          if ((params.oldValue === null || params.oldValue === undefined) &&
            (params.newValue === "" || params.newValue === null || params.newValue === undefined)) return false;
          // The Delete key clears a cell with `null`: that empties a text
          // value (NULL stays an explicit action) and means NULL for
          // other types, exactly like clearing the editor.
          const typed = params.newValue === null || params.newValue === undefined
            ? ""
            : restoreLineEndings(params.data[field], String(params.newValue));
          const parsed = editorInputToValue(typed, col);
          if (!parsed.ok) {
            addToast(`${col.name}: ${parsed.error}`, "error");
            return false;
          }
          handleCellChange(rowIdx, colIdx, parsed.value);
          return true;
        },
        minWidth: 80,
      };
    });

    return [rowNumCol, ...dataCols];
  }, [data, visibleIndices, page, changes, handleCellChange, fkMap, addToast]);

  const handleAddRow = () => {
    if (!data || readOnly) return;
    hasChangesLatestRef.current = true;
    const insert: PendingInsert = {
      tempId: nextInsertTempId++,
      values: data.columns.map(() => null),
      touched: data.columns.map(() => false),
    };
    setChanges((prev) => ({
      ...prev,
      inserts: [...prev.inserts, insert],
    }));
    setTimeout(() => gridApiRef.current?.ensureIndexVisible(0, "top"), 0);
  };

  // Spreadsheet-style "Down on last row → add new pending row"
  // (feature #64). The newly-added row goes into the pinned-top
  // area, same as the toolbar "Add Row" button, so users can save
  // a batch of inserts in one transaction. We move focus straight
  // to the first editable cell of the new row so the user can
  // type immediately without reaching for the mouse.
  const handleCellKeyDown = useCallback((event: CellKeyDownEvent) => {
    const ev = event.event as KeyboardEvent | null;
    if (!ev || ev.key !== "ArrowDown") return;
    // Modified Down is reserved for selection extension / page nav;
    // don't hijack those.
    if (ev.shiftKey || ev.ctrlKey || ev.metaKey || ev.altKey) return;

    // Only react when focus is on an existing data row at the very
    // bottom of the main grid. The pinned-top area holds in-progress
    // inserts; chaining Down there would create rows indefinitely.
    if (event.rowPinned) return;
    const lastIdx = agRowData.length - 1;
    if (lastIdx < 0 || event.rowIndex !== lastIdx) return;

    // ag-grid forwards keydown to the cell editor while a cell is
    // being edited; we don't want to mutate the row set out from
    // under the editor.
    const api = event.api;
    if (api.getEditingCells().length > 0) return;

    if (!data || readOnly) return;
    // First non-auto-generated column is the first cell the user
    // can actually type into. If every column is auto-generated
    // (a degenerate case but possible for tables with only a
    // serial PK), there's nothing to focus, so we don't trigger
    // the shortcut.
    const firstEditableIdx = data.columns.findIndex((c, i) => !c.is_auto_generated && visibleIndices.includes(i));
    if (firstEditableIdx < 0) return;

    ev.preventDefault();

    // The new pinned row's index = current count of pinned rows
    // (we're appending). We capture this *before* `handleAddRow`
    // mutates state so we can target it from a setTimeout that
    // runs after the next render pass.
    const newPinnedIdx = pinnedTopRows.length;
    handleAddRow();
    setTimeout(() => {
      gridApiRef.current?.setFocusedCell(newPinnedIdx, colField(firstEditableIdx), "top");
    }, 0);
  }, [agRowData.length, pinnedTopRows.length, data, handleAddRow, readOnly, visibleIndices]);

  const handleDeleteRow = (rowIndex: number) => {
    if (readOnly) return;
    hasChangesLatestRef.current = true;
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

  const handleDeleteSelected = useCallback(() => {
    if (selectedPkKeys.size === 0 || readOnlyRef.current) return;
    hasChangesLatestRef.current = true;
    setChanges((prev) => {
      const next = { ...prev, deletedKeys: new Set(prev.deletedKeys) };
      for (const pkKey of selectedPkKeys) {
        if (pkKey.startsWith("__")) continue;
        next.deletedKeys.add(pkKey);
      }
      return next;
    });
    setSelectedPkKeys(new Set());
    gridApiRef.current?.deselectAll();
  }, [selectedPkKeys]);

  useEffect(() => {
    deleteSelectedRef.current = handleDeleteSelected;
  }, [handleDeleteSelected]);

  const onSelectionChanged = useCallback((event: SelectionChangedEvent) => {
    const selected = event.api.getSelectedRows() as RowObj[];
    setSelectedPkKeys(new Set(selected.map((r) => r.__pkKey)));
  }, []);

  const handleSave = async () => {
    // The ref blocks a second click / Enter before `saving` re-renders,
    // which would otherwise apply the same inserts twice.
    if (!data || !hasChanges || readOnly || savingRef.current) return;
    savingRef.current = true;
    gridApiRef.current?.stopEditing();
    setSaving(true);
    setSaveError(null);
    setFilterLoading(true);
    try {
      const allChanges: DataChanges = {
        connection_id: connectionId,
        database,
        schema,
        table,
        updates: Array.from(changes.updates.values()),
        inserts: changes.inserts.map((ins) =>
          pendingInsertToNewRow(ins, data.columns.map((c) => c.name)),
        ),
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
      hasChangesLatestRef.current = false;
      await fetchData();
    } catch (e) {
      // Kept until dismissed (or the next successful save): the pending
      // edits are still there to fix and retry.
      setSaveError(String(e));
      setFilterLoading(false);
    } finally {
      savingRef.current = false;
      setSaving(false);
    }
  };

  const handleDiscard = () => {
    setChanges({ updates: new Map(), inserts: [], deletedKeys: new Set() });
    hasChangesLatestRef.current = false;
    setSaveError(null);
    if (data) {
      setExistingRows(data.rows.map((r) => [...r]));
    }
  };

  /**
   * Run an action that replaces the current page (paging, sorting,
   * filtering, refresh). Pending edits are tied to the rows on screen,
   * so with unsaved changes we ask first and discard them on confirm.
   */
  const guardNav = useCallback((action: () => void) => {
    if (hasChangesLatestRef.current) {
      setPendingNav(() => action);
    } else {
      action();
    }
  }, []);

  const startManualTimer = useCallback(() => {
    fetchStartRef.current = performance.now();
    setQueryRunning(true);
  }, []);

  const handleRefresh = useCallback(() => {
    guardNav(() => {
      startManualTimer();
      setFilterLoading(true);
      fetchData();
    });
  }, [guardNav, startManualTimer, fetchData]);

  const dbType = connections.find((c) => c.id === connectionId)?.db_type;

  const handleFilterApply = (filter: string | null) => {
    guardNav(() => {
      startManualTimer();
      setFilterLoading(true);
      if (filter === activeFilter) {
        fetchData();
      } else {
        setActiveFilter(filter);
        setPage(0);
      }
    });
  };

  const handleExport = async (format: "csv" | "json" | "sql" | "xlsx") => {
    try {
      const ext = format === "sql" ? "sql" : format;
      const label = format === "xlsx" ? "Excel" : format.toUpperCase();
      const filePath = await save({
        defaultPath: `${table}.${ext}`,
        filters: [{
          name: label,
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
      addToast(`Exported ${table} as ${label}`);
    } catch (e) {
      addToast(String(e), "error");
    }
  };

  /** INSERT for a row; `include` limits the columns (pending inserts
   *  leave untouched columns to their DEFAULT, as Save does). */
  const formatRowAsInsert = useCallback((row: unknown[], include?: boolean[]): string => {
    if (!data) return "";
    // Dialect-aware: previously hardcoded `"col"` / `"schema"."table"`
    // / `TRUE` regardless of connection type, producing snippets that
    // didn't run as-is on default-mode MySQL or MSSQL.
    const idx = data.columns.map((_, i) => i).filter((i) => !include || include[i]);
    if (idx.length === 0) {
      return dbType === "mysql" || dbType === "mariadb" || dbType === "tidb"
        ? `INSERT INTO ${quoteQualified(dbType, schema, table)} () VALUES ();`
        : `INSERT INTO ${quoteQualified(dbType, schema, table)} DEFAULT VALUES;`;
    }
    const cols = idx.map((i) => quoteIdent(dbType, data.columns[i].name)).join(", ");
    const vals = idx
      .map((i) => row[i])
      .map((v) => {
        if (v === null) return "NULL";
        if (typeof v === "number") return String(v);
        if (typeof v === "boolean") return boolLiteral(dbType, v);
        return stringLiteral(dbType, typeof v === "object" ? JSON.stringify(v) : String(v));
      })
      .join(", ");
    return `INSERT INTO ${quoteQualified(dbType, schema, table)} (${cols}) VALUES (${vals});`;
  }, [data, schema, table, dbType]);

  /** INSERT for the row at `rowIdx` of `editingRows` as it would be saved. */
  const insertForRow = useCallback((rowIdx: number): string => {
    const row = editingRows[rowIdx];
    if (!row) return "";
    const ins = rowIdx >= originalRowCount ? changes.inserts[rowIdx - originalRowCount] : undefined;
    return formatRowAsInsert(row, ins?.touched);
  }, [editingRows, originalRowCount, changes.inserts, formatRowAsInsert]);

  const handleCopyAllAsInsert = async () => {
    // Rows marked for deletion are left out: they won't exist after Save.
    const statements: string[] = [];
    for (let i = 0; i < editingRows.length; i++) {
      if (i < originalRowCount && changes.deletedKeys.has(getPkKey(i))) continue;
      statements.push(insertForRow(i));
    }
    try {
      await navigator.clipboard.writeText(statements.join("\n"));
      addToast(`Copied ${statements.length} INSERT statement${statements.length === 1 ? "" : "s"}`);
    } catch (e) {
      addToast(`Copy failed: ${String(e)}`, "error");
    }
  };

  const onGridReady = useCallback((params: GridReadyEvent) => {
    gridApiRef.current = params.api;
  }, []);

  const onCellContextMenu = useCallback((event: CellContextMenuEvent) => {
    if (!event.data) return;
    const e = event.event as MouseEvent;
    if (!e) return;
    e.preventDefault();
    const z = readZoomFactor();
    setRowContextMenu({ x: e.clientX / z, y: e.clientY / z, rowIdx: event.data.__rowIdx, colIdx: colIndexFromId(event.column?.getColId()) });
  }, []);

  const onCellClicked = useCallback((params: CellClickedEvent) => {
    if (!params.data) return;
    const e = params.event as MouseEvent;
    if (!e) return;
    if (e.ctrlKey || e.metaKey) {
      params.node.setSelected(!params.node.isSelected());
    } else if (e.shiftKey) {
      params.node.setSelected(true, false);
    }
  }, []);

  const getRowClass = useCallback((params: { data: RowObj }) => {
    if (!params.data) return "";
    const classes: string[] = [];
    if (params.data.__isInserted) classes.push("row-inserted");
    if (params.data.__isDeleted) classes.push("row-deleted");
    if (detailRowIdx === params.data.__rowIdx) classes.push("row-selected");
    if (selectedPkKeys.has(params.data.__pkKey)) classes.push("row-multi-selected");
    return classes.join(" ");
  }, [detailRowIdx, selectedPkKeys]);

  const onSortChanged = useCallback(() => {
    if (!gridApiRef.current) return;
    // ag-grid's multi-column sort model (Shift+click to add) is
    // exposed via `getColumnState()`; we sort by `sortIndex` to
    // preserve priority order. Before issue #57's fix this handler
    // truncated `sortModel` to its first entry and emitted a
    // single-column `SortSpec` — the grid lit up multiple sort
    // arrows, but the backend ORDER BY only used the primary.
    const sortModel = gridApiRef.current.getColumnState()
      .filter((c) => c.sort)
      .sort((a, b) => (a.sortIndex ?? 0) - (b.sortIndex ?? 0));
    const cols = dataRef.current?.columns ?? [];
    const next: SortSpec[] = sortModel.flatMap((c) => {
      const col = cols[colIndexFromId(c.colId)];
      return col ? [{ column: col.name, direction: c.sort as "asc" | "desc" }] : [];
    });
    const prev = sortRef.current;
    // Cheap structural equality check — avoids spuriously re-fetching
    // (and resetting the page) when ag-grid emits a sortChanged event
    // with the same effective model (e.g. on a no-op click, after a
    // tab regains focus, or when we revert the header state below).
    if (sameSort(prev, next)) return;
    const apply = (restoreHeaders = false) => {
      sortRef.current = next;
      if (restoreHeaders) applySortToGrid(gridApiRef.current, next, cols);
      setFilterLoading(true);
      setSort(next);
      setPage(0);
    };
    if (hasChangesLatestRef.current) {
      // Keep the headers showing the sort the rows are actually in
      // until the user decides what to do with the pending edits.
      applySortToGrid(gridApiRef.current, prev, cols);
      setPendingNav(() => () => apply(true));
      return;
    }
    apply();
  }, []);

  const handleExplain = useCallback(async () => {
    if (showExplain) {
      setShowExplain(false);
      setExplainResult(null);
      return;
    }
    setExplainLoading(true);
    setShowExplain(true);
    try {
      const dbType = connections.find((c) => c.id === connectionId)?.db_type;
      let sql = `SELECT * FROM ${quoteQualified(dbType, schema, table)}`;
      if (activeFilter) sql += ` WHERE ${activeFilter}`;
      if (sort.length > 0) {
        // Multi-column sort (issue #57): mirror what the driver
        // emits server-side so the EXPLAIN plan reflects the same
        // ORDER BY the row fetch is about to issue.
        const orderBy = sort
          .map((s) => `${quoteIdent(dbType, s.column)} ${s.direction}`)
          .join(", ");
        sql += ` ORDER BY ${orderBy}`;
      } else if (dbType === "mssql") {
        // MSSQL's `OFFSET ... ROWS FETCH NEXT ... ROWS ONLY` syntax
        // requires an `ORDER BY`. Use a NULL-sort sentinel so the
        // pagination clause is always valid even without a user sort.
        sql += ` ORDER BY (SELECT NULL)`;
      }
      sql += ` ${paginationClause(dbType, PAGE_SIZE, page * PAGE_SIZE)}`;
      const result = await api.explainQuery({
        connection_id: connectionId,
        database,
        sql,
      });
      setExplainResult(result);
    } catch (e) {
      addToast(String(e), "error");
      setShowExplain(false);
    } finally {
      setExplainLoading(false);
    }
  }, [connectionId, database, schema, table, activeFilter, sort, page, addToast, showExplain, connections]);

  const handleGenerateTestData = useCallback(() => {
    if (!data) return;
    const generateValue = (col: ColumnInfo): unknown => {
      // `data_type` should always be a non-null string from the
      // backend, but Cassandra's collection types and a handful of
      // synthetic columns (e.g. computed views) have produced
      // undefined here in the wild. Fall back to a typeless string
      // bucket so we never crash the dialog with `cannot read
      // properties of undefined (reading 'toLowerCase')`.
      const t = (col.data_type ?? "").toLowerCase();
      if (col.is_auto_generated || col.is_primary_key) return null;
      if (t.includes("smallint") || t === "smallserial")
        return Math.floor(Math.random() * 30000);
      if (t.includes("int") || t === "serial" || t === "bigserial")
        return Math.floor(Math.random() * 10000);
      if (t.includes("float") || t.includes("double") || t.includes("numeric") || t.includes("decimal") || t === "real")
        return Math.round(Math.random() * 10000) / 100;
      if (t === "boolean" || t === "bool")
        return Math.random() > 0.5;
      if (t.includes("timestamp") || t === "timestamptz") {
        const d = new Date(Date.now() - Math.floor(Math.random() * 365 * 24 * 60 * 60 * 1000));
        return d.toISOString();
      }
      if (t === "date") {
        const d = new Date(Date.now() - Math.floor(Math.random() * 365 * 24 * 60 * 60 * 1000));
        return d.toISOString().split("T")[0];
      }
      if (t === "time") {
        const h = String(Math.floor(Math.random() * 24)).padStart(2, "0");
        const m = String(Math.floor(Math.random() * 60)).padStart(2, "0");
        const s = String(Math.floor(Math.random() * 60)).padStart(2, "0");
        return `${h}:${m}:${s}`;
      }
      if (t === "uuid") {
        return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
          const r = (Math.random() * 16) | 0;
          return (c === "x" ? r : (r & 0x3) | 0x8).toString(16);
        });
      }
      if (t === "json" || t === "jsonb")
        return JSON.stringify({ key: `value_${Math.floor(Math.random() * 100)}` });
      if (t.includes("char") || t === "text" || t.includes("varchar")) {
        const lenMatch = t.match(/\((\d+)\)/);
        const maxLen = lenMatch ? parseInt(lenMatch[1], 10) : 0;
        const words = ["ab", "cd", "hi", "go", "up", "ok", "ax", "do", "be", "my",
                        "alpha", "beta", "gamma", "delta", "test", "data", "item", "node", "key", "val"];
        if (maxLen > 0) {
          const candidates = words.filter((w) => w.length <= maxLen);
          if (candidates.length > 0) {
            const word = candidates[Math.floor(Math.random() * candidates.length)];
            const remaining = maxLen - word.length;
            if (remaining >= 2) {
              const numDigits = Math.min(remaining - 1, 4);
              const num = Math.floor(Math.random() * Math.pow(10, numDigits));
              return (word + "_" + num).slice(0, maxLen);
            }
            return word;
          }
          const chars = "abcdefghijklmnopqrstuvwxyz";
          let s = "";
          for (let i = 0; i < maxLen; i++) s += chars[Math.floor(Math.random() * chars.length)];
          return s;
        }
        return words[Math.floor(Math.random() * words.length)] + "_" + Math.floor(Math.random() * 1000);
      }
      if (col.is_nullable) return null;
      return `test_${Math.floor(Math.random() * 1000)}`;
    };

    const newRow: unknown[] = data.columns.map(generateValue);
    // Generated NULLs mean "let the database decide" (auto/PK/nullable
    // columns), so they stay untouched and the DEFAULT applies.
    const insert: PendingInsert = {
      tempId: nextInsertTempId++,
      values: newRow,
      touched: newRow.map((v) => v !== null),
    };
    setChanges((prev) => ({
      ...prev,
      inserts: [...prev.inserts, insert],
    }));
    addToast("Generated test row with random data");
    setTimeout(() => gridApiRef.current?.ensureIndexVisible(0, "top"), 0);
  }, [data, addToast]);

  const handleJumpToFk = useCallback((fk: ForeignKeyInfo, cellValue: unknown) => {
    if (cellValue === null || cellValue === undefined) return;
    const conn = connections.find((c) => c.id === connectionId);
    const tabId = `${connectionId}:${database}:${schema}:${fk.referenced_table}`;
    const tab: TabInfo = {
      id: tabId,
      type: "table",
      title: `${schema}.${fk.referenced_table}`,
      connectionId,
      connectionColor: conn?.color || "#6398ff",
      database,
      schema,
      table: fk.referenced_table,
      subTab: "data",
      initialFilter: { column: fk.referenced_column, value: cellValue, nonce: Date.now() },
    };
    openTab(tab);
  }, [connectionId, database, schema, connections, openTab]);

  // `handleOpenQuery` used to live here; it now lives on TableView's
  // header so the "Query" button is reachable from both Data and Schema
  // modes (the DataGrid toolbar isn't rendered in Schema mode).

  const handleExplainResizeStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    explainDragging.current = true;
    const startY = e.clientY;
    const startH = explainHeight;
    const onMove = (ev: MouseEvent) => {
      const delta = startY - ev.clientY;
      setExplainHeight(Math.max(100, Math.min(600, startH + delta)));
    };
    const onUp = () => {
      explainDragging.current = false;
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
    };
    document.body.style.cursor = "row-resize";
    document.body.style.userSelect = "none";
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }, [explainHeight]);

  const defaultColDef = useMemo((): ColDef => ({
    resizable: true,
    sortable: true,
    comparator: () => 0,
    suppressHeaderMenuButton: true,
    cellDataType: false,
    suppressKeyboardEvent: (params) => shouldSuppressGridKey({
      editing: params.editing,
      key: params.event.key,
      hasRowSelection: selectedCountRef.current > 0,
    }),
  }), []);

  // "Jump to FK" (from another grid) opens this tab with a one-shot
  // filter on the referenced column; apply it once per request.
  const initialFilter = useTabStore((s) =>
    tabId ? s.tabs.find((t) => t.id === tabId)?.initialFilter : undefined);
  const appliedFilterNonceRef = useRef<number | null>(null);
  const handleFilterApplyRef = useRef(handleFilterApply);
  handleFilterApplyRef.current = handleFilterApply;
  useEffect(() => {
    if (!initialFilter || !data || appliedFilterNonceRef.current === initialFilter.nonce) return;
    appliedFilterNonceRef.current = initialFilter.nonce;
    const conds: FilterCondition[] = [{
      id: `fk-${initialFilter.nonce}`,
      column: initialFilter.column,
      operator: initialFilter.value === null || initialFilter.value === undefined ? "IS NULL" : "=",
      value: cellEditText(initialFilter.value) ?? "",
      join: "AND",
    }];
    setFilterConditions(conds);
    setShowFilter(true);
    handleFilterApplyRef.current(buildWhereClause(conds, data.columns, dbType));
  }, [initialFilter, data, dbType]);

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
        <button className="btn-secondary" onClick={() => fetchData()}>
          Retry
        </button>
      </div>
    );
  }

  if (!data) return null;

  return (
    <div className="data-grid-container" ref={containerRef}>
      <div className={`grid-toolbar ${hideTitle ? "grid-toolbar-no-title" : ""}`}>
        {!hideTitle && (
          <div className="grid-toolbar-left">
            <span
              className="grid-table-name"
              title={`${database}.${schema}.${table}`}
            >
              {schema}.{table}
            </span>
          </div>
        )}
        <div className="grid-toolbar-right">
          {readOnly && (
            <span
              className="result-updatable-badge readonly grid-readonly-badge"
              title="Read-only: table has no primary key, so rows can't be addressed for updates or deletes"
            >
              <Lock size={12} /> Read-only<span className="btn-label">: no primary key</span>
            </span>
          )}
          <button
            className={`btn-ghost ${showFilter || activeFilter ? "active-filter" : ""}`}
            onClick={() => setShowFilter(!showFilter)}
            title={activeFilter ? "Toggle Filter (a filter is applied)" : "Toggle Filter"}
            aria-label={activeFilter ? "Filter (active)" : "Filter"}
          >
            <Filter size={14} /> <span className="btn-label">Filter{activeFilter ? " (active)" : ""}</span>
          </button>
          <button
            className="btn-ghost"
            onClick={handleRefresh}
            title="Refresh now"
            aria-label="Refresh now"
          >
            <RefreshCw size={14} />
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
              foreignKeyColumns={new Set(fkMap.keys())}
            />
          )}
          <button
            className={`btn-ghost ${showExplain ? "active-filter" : ""}`}
            onClick={handleExplain}
            title="Explain current query"
            aria-label="Explain"
          >
            <Zap size={14} /> <span className="btn-label">Explain</span>
          </button>
          <ExportMenu onExport={handleExport} />
          <button
            className="btn-ghost"
            onClick={handleCopyAllAsInsert}
            title="Copy all rows as INSERT statements"
            aria-label="Copy as SQL"
          >
            <Copy size={14} /> <span className="btn-label">Copy as SQL</span>
          </button>
          {!readOnly && (
            <>
              <button
                className="btn-ghost"
                onClick={handleAddRow}
                title="Add Row"
                aria-label="Add Row"
              >
                <Plus size={14} /> <span className="btn-label">Add Row</span>
              </button>
              <button
                className="btn-ghost"
                onClick={handleGenerateTestData}
                title="Generate a row with random test data"
                aria-label="Test Data"
              >
                <Shuffle size={14} /> <span className="btn-label">Test Data</span>
              </button>
            </>
          )}
          {!readOnly && selectedPkKeys.size > 0 && (
            <button
              className="btn-delete-selected"
              onClick={handleDeleteSelected}
              title={`Delete ${selectedPkKeys.size} selected row${selectedPkKeys.size > 1 ? "s" : ""} (Delete key)`}
            >
              <Trash2 size={14} />
              Delete {selectedPkKeys.size} row{selectedPkKeys.size > 1 ? "s" : ""}
            </button>
          )}
        </div>
        {hasChanges && (
          <div className="grid-toolbar-changes">
            <button className="btn-discard" onClick={handleDiscard}>
              <Undo2 size={14} /> Discard
            </button>
            <button
              className="btn-primary"
              onClick={handleSave}
              disabled={saving}
              style={{ display: "flex", alignItems: "center", gap: 6 }}
              title="Save changes to the database"
            >
              {saving ? <Loader2 size={14} className="spin" /> : <Save size={14} />}
              {saving ? "Saving..." : `Save ${changeCount} change${changeCount === 1 ? "" : "s"}`}
            </button>
          </div>
        )}
      </div>

      {showFilter && data && (
        <FilterBar
          columns={data.columns}
          conditions={filterConditions ?? defaultFilterConditions(data.columns)}
          onConditionsChange={setFilterConditions}
          onApply={handleFilterApply}
          onClose={() => setShowFilter(false)}
          dbType={dbType}
        />
      )}

      {(saveError || error) && (
        <div className="grid-error-banner" role="alert">
          <span className="grid-error-banner-text">{saveError ?? error}</span>
          <button
            className="btn-icon grid-error-banner-close"
            onClick={() => { setSaveError(null); setError(null); }}
            title="Dismiss"
            aria-label="Dismiss error"
          >
            <X size={14} />
          </button>
        </div>
      )}

      {showSearch && (
        <div className="grid-search-bar">
          <Search size={14} className="grid-search-icon" />
          <input
            ref={searchInputRef}
            className="grid-search-input"
            placeholder="Search values in visible columns..."
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

      <div className="grid-body">
      <div
        className={`grid-table-wrapper ag-grid-wrapper${
          loading && data ? " grid-table-wrapper-stale" : ""
        }`}
      >
        {(filterLoading || (loading && data)) && (
          <div className="grid-filter-loading-bar" />
        )}
        <AgGridReact
          theme={gridTheme}
          modules={[AllCommunityModule]}
          rowData={agRowData}
          pinnedTopRowData={pinnedTopRows}
          columnDefs={columnDefs}
          defaultColDef={defaultColDef}
          getRowId={(params) => params.data.__pkKey}
          getRowClass={getRowClass as any}
          onGridReady={onGridReady}
          onCellClicked={onCellClicked}
          onCellContextMenu={onCellContextMenu}
          onCellKeyDown={handleCellKeyDown}
          onSortChanged={onSortChanged}
          onSelectionChanged={onSelectionChanged}
          rowSelection={{ mode: "multiRow", checkboxes: false, headerCheckbox: false, enableClickSelection: false }}
          preventDefaultOnContextMenu={true}
          animateRows={false}
          suppressCellFocus={false}
          stopEditingWhenCellsLoseFocus={true}
          singleClickEdit={false}
          enableCellTextSelection={true}
          suppressRowClickSelection={true}
        />
      </div>
      {/* Inside the grid body so the panel never covers the toolbar
          (Save / Discard stay reachable while it is open). */}
      {detailRowIdx !== null && data && editingRows[detailRowIdx] && (
        <RowDetailView
          columns={data.columns}
          row={editingRows[detailRowIdx]}
          rowIndex={detailRowIdx}
          onClose={() => setDetailRowIdx(null)}
          readOnly={readOnly || (detailRowIdx < originalRowCount && changes.deletedKeys.has(getPkKey(detailRowIdx)))}
          onCellChange={(colIndex, newValue) =>
            handleCellChange(detailRowIdx, colIndex, newValue)
          }
        />
      )}
      </div>

      <div className="grid-pagination">
        <div className="grid-pagination-info">
          {data.total_rows.toLocaleString()} rows
          {totalPages > 1 && (
            <span
              className={`grid-pagination-range${
                loading && data ? " grid-pagination-range-stale" : ""
              }`}
            >
              {" · "}Showing {page * PAGE_SIZE + 1} -{" "}
              {Math.min((page + 1) * PAGE_SIZE, data.total_rows)}
            </span>
          )}
        </div>
        <div className="grid-pagination-controls">
          <button
            className="btn-icon"
            disabled={page === 0}
            onClick={() => guardNav(() => setPage(0))}
          >
            <ChevronsLeft size={16} />
          </button>
          <button
            className="btn-icon"
            disabled={page === 0}
            onClick={() => guardNav(() => setPage((p) => p - 1))}
          >
            <ChevronLeft size={16} />
          </button>
          <span className="grid-page-info">
            Page {page + 1} of {totalPages || 1}
          </span>
          <button
            className="btn-icon"
            disabled={page >= totalPages - 1}
            onClick={() => guardNav(() => setPage((p) => p + 1))}
          >
            <ChevronRight size={16} />
          </button>
          <button
            className="btn-icon"
            disabled={page >= totalPages - 1}
            onClick={() => guardNav(() => setPage(totalPages - 1))}
          >
            <ChevronsRight size={16} />
          </button>
        </div>
      </div>

      {showExplain && (
        <div className="grid-explain-panel" style={{ height: explainHeight }}>
          <div className="grid-explain-resize" onMouseDown={handleExplainResizeStart} />
          <div className="grid-explain-header">
            <span className="grid-explain-title">
              <Zap size={14} /> Execution Plan
            </span>
            <button className="btn-icon" onClick={() => { setShowExplain(false); setExplainResult(null); }}>
              <X size={14} />
            </button>
          </div>
          <div className="grid-explain-body">
            {explainLoading ? (
              <div className="grid-explain-loading">
                <Loader2 size={16} className="spin" /> Analyzing...
              </div>
            ) : explainResult ? (
              <ExplainView result={explainResult} />
            ) : null}
          </div>
        </div>
      )}

      {rowContextMenu && data && editingRows[rowContextMenu.rowIdx] && (
        <div
          className="context-menu"
          style={{ left: rowContextMenu.x, top: rowContextMenu.y }}
          ref={(el) => {
            if (!el) return;
            const z = readZoomFactor();
            const cssVh = window.innerHeight / z;
            const cssVw = window.innerWidth / z;
            if (rowContextMenu.y + el.offsetHeight > cssVh) {
              el.style.top = `${Math.max(4, cssVh - el.offsetHeight)}px`;
            }
            if (rowContextMenu.x + el.offsetWidth > cssVw) {
              el.style.left = `${Math.max(4, cssVw - el.offsetWidth)}px`;
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
              const obj = rowToObject(data.columns.map((c) => c.name), editingRows[rowContextMenu.rowIdx]);
              navigator.clipboard.writeText(JSON.stringify(obj, null, 2));
              setRowContextMenu(null);
            }}
          >
            Copy row as JSON
          </button>
          <button
            className="context-menu-item"
            onClick={() => {
              navigator.clipboard.writeText(insertForRow(rowContextMenu.rowIdx));
              setRowContextMenu(null);
            }}
          >
            Copy as INSERT
          </button>
          {(() => {
            // Explicit NULL: clearing a text cell stores an empty
            // string, so NULL needs its own action.
            const colIdx = rowContextMenu.colIdx;
            const col = colIdx >= 0 ? data.columns[colIdx] : null;
            if (readOnly || !col || !col.is_nullable) return null;
            const cur = editingRows[rowContextMenu.rowIdx][colIdx];
            const isInsertedRow = rowContextMenu.rowIdx >= originalRowCount;
            if (!isInsertedRow && cur === null) return null;
            if (!isInsertedRow && changes.deletedKeys.has(getPkKey(rowContextMenu.rowIdx))) return null;
            return (
              <button
                className="context-menu-item"
                onClick={() => {
                  handleCellChange(rowContextMenu.rowIdx, colIdx, null);
                  setRowContextMenu(null);
                }}
              >
                Set {col.name} to NULL
              </button>
            );
          })()}
          {readOnly ? null : selectedPkKeys.size > 1 && selectedPkKeys.has(getPkKey(rowContextMenu.rowIdx)) ? (
            <button
              className="context-menu-item context-menu-danger"
              onClick={() => {
                handleDeleteSelected();
                setRowContextMenu(null);
              }}
            >
              <Trash2 size={12} />
              Delete {selectedPkKeys.size} selected rows
            </button>
          ) : (
            <button
              className="context-menu-item"
              onClick={() => {
                const rIdx = rowContextMenu.rowIdx;
                const isInserted = rIdx >= originalRowCount;
                if (isInserted) {
                  const insertIdx = rIdx - originalRowCount;
                  setChanges((prev) => ({
                    ...prev,
                    inserts: prev.inserts.filter((_, i) => i !== insertIdx),
                  }));
                  setDetailRowIdx(null);
                } else {
                  handleDeleteRow(rIdx);
                }
                setRowContextMenu(null);
              }}
            >
              {rowContextMenu.rowIdx >= originalRowCount
                ? "Remove new row"
                : changes.deletedKeys.has(getPkKey(rowContextMenu.rowIdx)) ? "Undo delete" : "Delete row"}
            </button>
          )}
          {data.columns.map((col, colIdx) => {
            const fk = fkMap.get(col.name);
            if (!fk) return null;
            const cellVal = editingRows[rowContextMenu.rowIdx][colIdx];
            if (cellVal === null || cellVal === undefined) return null;
            return (
              <button
                key={col.name}
                className="context-menu-item context-menu-fk"
                onClick={() => {
                  handleJumpToFk(fk, cellVal);
                  setRowContextMenu(null);
                }}
              >
                <ExternalLink size={12} />
                Jump to {fk.referenced_table}.{fk.referenced_column} = {JSON.stringify(cellVal)}
              </button>
            );
          })}
        </div>
      )}

      {pendingNav && (
        <ConfirmDialog
          title="Discard unsaved changes?"
          message="This page has unsaved edits. Changing page, sort or filter, or refreshing, will discard them."
          confirmLabel="Discard changes"
          danger
          onConfirm={() => {
            const action = pendingNav;
            setPendingNav(null);
            handleDiscard();
            action();
          }}
          onCancel={() => setPendingNav(null)}
        />
      )}

    </div>
  );
}
