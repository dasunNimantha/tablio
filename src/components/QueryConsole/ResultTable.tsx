import { useMemo, useRef, useState, useEffect, useCallback } from "react";
import { QueryResult, ColumnInfo } from "../../lib/tauri";
import { AllCommunityModule, themeQuartz, type ColDef, type GridApi, type GridReadyEvent, type CellContextMenuEvent } from "ag-grid-community";
import { AgGridReact } from "ag-grid-react";
import { Search, X, Copy, Save, Undo2 } from "lucide-react";
import { RowDetailView } from "../DataGrid/RowDetailView";
import "../DataGrid/ag-grid-theme.css";
import "./QueryConsole.css";

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

interface Props {
  result: QueryResult;
}

interface RowObj {
  __rowIdx: number;
  [key: string]: unknown;
}

export function ResultTable({ result }: Props) {
  const gridApiRef = useRef<GridApi | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [showSearch, setShowSearch] = useState(false);
  const searchInputRef = useRef<HTMLInputElement>(null);
  const searchQueryRef = useRef("");
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; rowIdx: number } | null>(null);
  const [detailRowIdx, setDetailRowIdx] = useState<number | null>(null);
  const [editingRows, setEditingRows] = useState<unknown[][]>(() =>
    result.rows.map((r) => [...r])
  );

  useEffect(() => {
    setEditingRows(result.rows.map((r) => [...r]));
    setDetailRowIdx(null);
  }, [result]);

  const columnInfos = useMemo((): ColumnInfo[] => {
    return result.columns.map((name, i) => ({
      name,
      data_type: "text",
      is_nullable: true,
      is_primary_key: false,
      default_value: null,
      ordinal_position: i,
      is_auto_generated: false,
    }));
  }, [result.columns]);

  const rowData = useMemo((): RowObj[] => {
    return editingRows.map((row, i) => {
      const obj: RowObj = { __rowIdx: i };
      result.columns.forEach((col, colIdx) => {
        obj[col] = row[colIdx];
      });
      return obj;
    });
  }, [editingRows, result.columns]);

  const editCount = useMemo(() => {
    let count = 0;
    for (let r = 0; r < editingRows.length; r++) {
      const orig = result.rows[r];
      if (!orig) continue;
      for (let c = 0; c < editingRows[r].length; c++) {
        const ov = orig[c], nv = editingRows[r][c];
        if (ov !== nv && !(ov === null && nv === null)) count++;
      }
    }
    return count;
  }, [editingRows, result.rows]);

  const handleCellChange = useCallback((rowIndex: number, colIndex: number, newValue: unknown) => {
    setEditingRows((prev) => {
      const next = prev.map((r) => [...r]);
      next[rowIndex][colIndex] = newValue;
      return next;
    });
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
        if (params.data) setDetailRowIdx(params.data.__rowIdx);
      },
    };

    const dataCols: ColDef[] = result.columns.map((col, colIdx) => ({
      field: col,
      headerName: col,
      sortable: true,
      resizable: true,
      editable: true,
      minWidth: 80,
      cellClassRules: {
        "cell-null": (params) => params.value === null || params.value === undefined,
        "cell-search-match": (params) => {
          const q = searchQueryRef.current;
          if (!q) return false;
          const v = params.value;
          const str = (v === null || v === undefined) ? "null" : typeof v === "object" ? JSON.stringify(v) : String(v);
          return str.toLowerCase().includes(q.toLowerCase());
        },
      },
      valueFormatter: (params) => {
        if (params.value === null || params.value === undefined) return "NULL";
        if (typeof params.value === "object") return JSON.stringify(params.value);
        return String(params.value);
      },
      valueSetter: (params) => {
        const raw = params.newValue;
        let parsed: unknown;
        if (raw === "" || raw === null || raw === undefined) {
          parsed = null;
        } else {
          parsed = raw;
        }
        handleCellChange(params.data.__rowIdx, colIdx, parsed);
        return true;
      },
    }));

    return [rowNumCol, ...dataCols];
  }, [result.columns, handleCellChange]);

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

  const findFirstMatch = useCallback((q: string): { rowIdx: number; colIdx: number } | null => {
    if (!q) return null;
    const ql = q.toLowerCase();
    for (let r = 0; r < editingRows.length; r++) {
      for (let c = 0; c < editingRows[r].length; c++) {
        const val = editingRows[r][c];
        const str = (val === null || val === undefined) ? "null" : typeof val === "object" ? JSON.stringify(val) : String(val);
        if (str.toLowerCase().includes(ql)) return { rowIdx: r, colIdx: c };
      }
    }
    return null;
  }, [editingRows]);

  const searchMatchCount = useMemo(() => {
    if (!searchQuery) return 0;
    const q = searchQuery.toLowerCase();
    let count = 0;
    for (const row of editingRows) {
      for (const val of row) {
        const str = (val === null || val === undefined) ? "null" : typeof val === "object" ? JSON.stringify(val) : String(val);
        if (str.toLowerCase().includes(q)) count++;
      }
    }
    return count;
  }, [searchQuery, editingRows]);

  useEffect(() => {
    searchQueryRef.current = searchQuery;
    const api = gridApiRef.current;
    if (!api) return;
    api.refreshCells({ force: true });

    if (searchQuery) {
      const match = findFirstMatch(searchQuery);
      if (match) {
        const colId = result.columns[match.colIdx];
        api.ensureIndexVisible(match.rowIdx, "middle");
        api.ensureColumnVisible(colId);
        api.setFocusedCell(match.rowIdx, colId);
      }
    }
  }, [searchQuery, findFirstMatch, result.columns]);

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
    const rows = editingRows.map((row) => row.map((v) => escape(v)).join(","));
    const csv = [header, ...rows].join("\n");
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "query_result.csv";
    a.click();
    URL.revokeObjectURL(url);
  }, [result.columns, editingRows]);

  const handleDiscard = useCallback(() => {
    setEditingRows(result.rows.map((r) => [...r]));
  }, [result.rows]);

  return (
    <div className="result-table-ag-wrapper">
      <div className="result-table-toolbar">
        {editCount > 0 && (
          <button className="btn-ghost result-discard-btn" onClick={handleDiscard} title="Discard all changes">
            <Undo2 size={14} /> Discard
          </button>
        )}
        <button
          className={`result-save-btn ${editCount > 0 ? "result-save-active" : ""}`}
          onClick={handleDownloadCsv}
          disabled={editCount === 0}
          title={editCount > 0 ? `Save ${editCount} change${editCount !== 1 ? "s" : ""} as CSV` : "No changes to save"}
        >
          <Save size={14} />
          {editCount > 0 ? `Save ${editCount} change${editCount !== 1 ? "s" : ""}` : "Save"}
        </button>
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
      <div className="ag-grid-wrapper" style={{ flex: 1 }}>
        <AgGridReact
          theme={gridTheme}
          modules={[AllCommunityModule]}
          rowData={rowData}
          columnDefs={columnDefs}
          defaultColDef={defaultColDef}
          getRowId={(params) => String(params.data.__rowIdx)}
          onGridReady={onGridReady}
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
            const rect = el.getBoundingClientRect();
            const vh = window.innerHeight / z;
            const vw = window.innerWidth / z;
            const menuH = rect.height / z;
            const menuW = rect.width / z;
            if (contextMenu.y + menuH > vh) el.style.top = `${Math.max(4, contextMenu.y - menuH)}px`;
            if (contextMenu.x + menuW > vw) el.style.left = `${Math.max(4, contextMenu.x - menuW)}px`;
          }}
        >
          <button className="context-menu-item" onClick={handleCopyCell}>
            Copy cell value
          </button>
          <button className="context-menu-item" onClick={handleCopyRowAsJson}>
            Copy row as JSON
          </button>
          <button
            className="context-menu-item"
            onClick={() => { setDetailRowIdx(contextMenu.rowIdx); setContextMenu(null); }}
          >
            View as JSON
          </button>
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
