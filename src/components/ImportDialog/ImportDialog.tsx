import { useEffect, useId, useMemo, useRef, useState } from "react";
import {
  api,
  ColumnInfo,
  XlsxWorkbookPreview,
  XlsxColumnType,
} from "../../lib/tauri";
import { X, FileSpreadsheet, Loader2, CheckCircle, AlertTriangle } from "lucide-react";
import { Modal } from "../Modal/Modal";
import { normalizeHeaders, parseCsv } from "../../lib/csv";
import "./ImportDialog.css";

interface Props {
  connectionId: string;
  database: string;
  schema: string;
  tableName: string;
  onClose: () => void;
  /** Called after rows were written (the dialog stays open on its success
   *  state until the user clicks Done). */
  onImported: (rowCount: number) => void;
}

const MB = 1024 * 1024;
/**
 * CSV is read and parsed in the renderer, so its cap only guards memory
 * in this process.
 */
const MAX_CSV_BYTES = 100 * MB;
/**
 * Excel files are parsed by the backend: the bytes cross the IPC bridge
 * as a JSON number array (~4x the file size), once for the preview and
 * again for the chosen sheet, and an .xlsx is a zip whose XML usually
 * inflates 5-10x while parsing. 25 MB of xlsx is already well over a
 * million cells, so anything larger is rejected before it is read.
 * Must match `MAX_BYTES` in `src-tauri/src/commands/xlsx_import.rs`.
 */
export const MAX_XLSX_BYTES = 25 * MB;
/** Example error-cell references shown in the warning. */
const ERROR_CELL_EXAMPLES = 5;
const ACCEPTED_EXTS = [".csv", ".xlsx", ".xls"] as const;

type SourceKind = "csv" | "xlsx";

interface ParsedSheet {
  headers: string[];
  rows: unknown[][];
  inferredTypes?: XlsxColumnType[];
  totalRows?: number;
  /** Excel error cells (`"B5: #N/A"`), imported as NULL. */
  errorCells?: string[];
  errorCellCount?: number;
}

function formatMb(bytes: number): string {
  return `${(bytes / MB).toFixed(1)} MB`;
}

function detectKind(name: string): SourceKind | null {
  const lower = name.toLowerCase();
  if (lower.endsWith(".csv")) return "csv";
  if (lower.endsWith(".xlsx") || lower.endsWith(".xls")) return "xlsx";
  return null;
}

type DelimiterChoice = "auto" | "," | ";" | "\t" | "|";

const DELIMITER_OPTIONS: { value: DelimiterChoice; label: string }[] = [
  { value: "auto", label: "Detect automatically" },
  { value: ",", label: "Comma (,)" },
  { value: ";", label: "Semicolon (;)" },
  { value: "\t", label: "Tab" },
  { value: "|", label: "Pipe (|)" },
];

/** Table columns that more than one source column is mapped to. */
export function duplicateTargets(columnMap: Record<string, string>): string[] {
  const seen = new Map<string, number>();
  for (const target of Object.values(columnMap)) {
    if (!target) continue;
    seen.set(target, (seen.get(target) ?? 0) + 1);
  }
  return [...seen.entries()].filter(([, n]) => n > 1).map(([t]) => t);
}

export function ImportDialog({
  connectionId,
  database,
  schema,
  tableName,
  onClose,
  onImported,
}: Props) {
  const [tableColumns, setTableColumns] = useState<ColumnInfo[]>([]);
  const [columnsLoading, setColumnsLoading] = useState(true);
  const [sourceFile, setSourceFile] = useState<File | null>(null);
  const [sourceKind, setSourceKind] = useState<SourceKind | null>(null);
  const [sourceData, setSourceData] = useState<ParsedSheet | null>(null);
  // Cache the workbook bytes so the user can flip between sheets
  // without re-reading the file from disk.
  const [workbookBytes, setWorkbookBytes] = useState<Uint8Array | null>(null);
  const [workbook, setWorkbook] = useState<XlsxWorkbookPreview | null>(null);
  const [activeSheet, setActiveSheet] = useState<string | null>(null);
  const [parsing, setParsing] = useState(false);
  const [parseError, setParseError] = useState<string | null>(null);
  const [columnMap, setColumnMap] = useState<Record<string, string>>({});
  const [importing, setImporting] = useState(false);
  const [importResult, setImportResult] = useState<number | null>(null);
  const [importError, setImportError] = useState<string | null>(null);
  const [delimiter, setDelimiter] = useState<DelimiterChoice>("auto");
  const [csvText, setCsvText] = useState<string | null>(null);
  const ids = useId();
  const titleId = `${ids}-title`;
  const doneRef = useRef<HTMLButtonElement>(null);
  // The Import button disappears on success; move focus to Done.
  useEffect(() => {
    if (importResult != null) doneRef.current?.focus();
  }, [importResult]);
  const dupTargets = useMemo(() => duplicateTargets(columnMap), [columnMap]);
  const busy = importing;
  // Work in progress (a parsed file / mapping) shouldn't vanish on a stray
  // backdrop click; after a successful import there's nothing to lose.
  const dirty = !!sourceData && importResult == null;

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      setColumnsLoading(true);
      try {
        const cols = await api.listColumns(
          connectionId,
          database,
          schema,
          tableName
        );
        if (cancelled) return;
        setTableColumns(cols);
      } catch (e) {
        if (cancelled) return;
        setImportError(String(e));
      } finally {
        if (!cancelled) setColumnsLoading(false);
      }
    };
    load();
    return () => { cancelled = true; };
  }, [connectionId, database, schema, tableName]);

  // Match source headers to table columns by case-insensitive name.
  // Centralized so the CSV path, the initial XLSX parse, and re-parses
  // after a sheet swap all initialize the mapping the same way.
  const seedColumnMap = (headers: string[]) => {
    const initialMap: Record<string, string> = {};
    headers.forEach((h) => {
      const match = tableColumns.find(
        (c) => c.name.toLowerCase() === h.toLowerCase()
      );
      initialMap[h] = match ? match.name : "";
    });
    setColumnMap(initialMap);
  };

  // If table columns finish loading after the file was parsed, seed
  // the mapping retroactively rather than leaving everything as
  // "— Skip —".
  useEffect(() => {
    if (sourceData && tableColumns.length > 0) {
      const hasMappings = Object.values(columnMap).some((v) => v);
      if (!hasMappings) {
        seedColumnMap(sourceData.headers);
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sourceData, tableColumns]);

  const resetSourceState = () => {
    setSourceData(null);
    setWorkbookBytes(null);
    setWorkbook(null);
    setActiveSheet(null);
    setParseError(null);
    setImportResult(null);
    setImportError(null);
    setColumnMap({});
    setCsvText(null);
  };

  const applyCsv = (text: string, choice: DelimiterChoice) => {
    const { headers, rows } = parseCsv(text, choice === "auto" ? undefined : choice);
    setSourceData({ headers, rows });
    seedColumnMap(headers);
  };

  const handleDelimiterChange = (choice: DelimiterChoice) => {
    setDelimiter(choice);
    if (csvText == null) return;
    setParseError(null);
    setImportResult(null);
    try {
      applyCsv(csvText, choice);
    } catch (err) {
      setSourceData(null);
      setParseError(err instanceof Error ? err.message : String(err));
    }
  };

  const handleFileChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    resetSourceState();

    const kind = detectKind(file.name);
    setSourceFile(file);

    if (kind === null) {
      setSourceKind(null);
      setParseError(
        `Unsupported file type. Choose a ${ACCEPTED_EXTS.join(", ")} file.`
      );
      return;
    }

    // Checked before the file is read into memory or sent to the backend.
    const limit = kind === "xlsx" ? MAX_XLSX_BYTES : MAX_CSV_BYTES;
    if (file.size > limit) {
      setSourceKind(null);
      setParseError(
        kind === "xlsx"
          ? `File is too large (${formatMb(file.size)}). Excel imports are limited to ${limit / MB} MB; save the sheet as CSV (up to ${MAX_CSV_BYTES / MB} MB) or split the workbook.`
          : `File is too large (${formatMb(file.size)}). CSV imports are limited to ${limit / MB} MB.`
      );
      return;
    }
    setSourceKind(kind);

    setParsing(true);
    try {
      if (kind === "csv") {
        const text = await file.text();
        setCsvText(text);
        applyCsv(text, delimiter);
      } else {
        const buf = new Uint8Array(await file.arrayBuffer());
        setWorkbookBytes(buf);
        const preview = await api.parseXlsxWorkbook(buf);
        setWorkbook(preview);
        setActiveSheet(preview.default_sheet);
        await loadXlsxSheet(buf, preview.default_sheet);
      }
    } catch (err) {
      setParseError(err instanceof Error ? err.message : String(err));
    } finally {
      setParsing(false);
    }
  };

  // Fetch the full payload for a sheet (headers + rows + inferred
  // types) and seed the column mapping. Used both for the initial
  // load and any sheet-picker change.
  const loadXlsxSheet = async (buf: Uint8Array, sheetName: string) => {
    const payload = await api.parseXlsxSheet(buf, sheetName);
    // Blank / repeated header cells would collide as mapping keys.
    const headers = normalizeHeaders(payload.headers);
    setSourceData({
      headers,
      rows: payload.rows,
      inferredTypes: payload.inferred_types,
      totalRows: payload.rows.length,
      errorCells: payload.error_cells ?? [],
      errorCellCount: payload.error_cell_count ?? 0,
    });
    seedColumnMap(headers);
  };

  const handleSheetChange = async (sheetName: string) => {
    if (!workbookBytes) return;
    setActiveSheet(sheetName);
    setParsing(true);
    setParseError(null);
    try {
      await loadXlsxSheet(workbookBytes, sheetName);
    } catch (err) {
      setParseError(String(err));
    } finally {
      setParsing(false);
    }
  };

  const handleImport = async () => {
    if (!sourceData) return;

    // Pair each mapped source column (by position) with its target.
    const mapped = sourceData.headers
      .map((h, idx) => ({ idx, target: columnMap[h] }))
      .filter((m): m is { idx: number; target: string } => !!m.target);

    if (mapped.length === 0) {
      setImportError("Map at least one source column to a table column");
      return;
    }
    if (dupTargets.length > 0) {
      setImportError(
        `Each table column can only be filled from one source column: ${dupTargets.join(", ")}.`,
      );
      return;
    }

    const mappedCols = mapped.map((m) => m.target);
    const colIndices = mapped.map((m) => m.idx);

    // CSV rows always arrive as strings; XLSX rows already carry typed
    // values from the backend. Preserve those types here so numeric /
    // boolean / null fidelity flows all the way to `import_data`.
    const rows: unknown[][] = sourceData.rows.map((row) =>
      colIndices.map((idx) => normalizeCell(row[idx]))
    );

    setImporting(true);
    setImportError(null);
    try {
      const count = await api.importData({
        connection_id: connectionId,
        database,
        schema,
        table: tableName,
        columns: mappedCols,
        rows,
      });
      setImportResult(count);
      onImported(count);
    } catch (e) {
      setImportError(e instanceof Error ? e.message : String(e));
    } finally {
      setImporting(false);
    }
  };

  const previewRows = sourceData?.rows.slice(0, 10) ?? [];
  const inferredFor = (header: string): XlsxColumnType | null => {
    if (!sourceData?.inferredTypes) return null;
    const idx = sourceData.headers.indexOf(header);
    return idx >= 0 ? sourceData.inferredTypes[idx] ?? null : null;
  };

  return (
    <Modal
      onClose={onClose}
      busy={busy}
      closeOnOverlayClick={!dirty}
      className="dialog import-dialog"
      labelledBy={titleId}
    >
        <div className="dialog-header">
          <h2 id={titleId}>
            <FileSpreadsheet size={18} aria-hidden="true" />
            Import Data
          </h2>
          <button className="btn-icon" onClick={onClose} aria-label="Close" title="Close" disabled={busy}>
            <X size={18} />
          </button>
        </div>

        <div className="dialog-body">
          <div className="form-group">
            <label htmlFor={`${ids}-target`}>Target Table</label>
            <input
              id={`${ids}-target`}
              value={`${schema}.${tableName}`}
              readOnly
              aria-readonly="true"
              className="import-target-input"
            />
          </div>

          <div className="form-group">
            <span className="form-label" id={`${ids}-source-label`}>Source File</span>
            <div className="import-file-input">
              <input
                type="file"
                accept={ACCEPTED_EXTS.join(",")}
                onChange={handleFileChange}
                id="csv-file-input"
                aria-describedby={`${ids}-file-hint`}
              />
              <label htmlFor="csv-file-input" className="import-file-label">
                {sourceFile?.name ?? "Choose a .csv, .xlsx, or .xls file..."}
              </label>
            </div>
            <p className="import-file-hint" id={`${ids}-file-hint`}>
              Supported: CSV (max {MAX_CSV_BYTES / MB} MB), Excel .xlsx / .xls (max {MAX_XLSX_BYTES / MB} MB).
            </p>
          </div>

          {sourceKind === "csv" && (
            <div className="form-group">
              <label htmlFor={`${ids}-delimiter`}>Delimiter</label>
              <select
                id={`${ids}-delimiter`}
                className="import-delimiter-select"
                value={delimiter}
                onChange={(e) => handleDelimiterChange(e.target.value as DelimiterChoice)}
                disabled={parsing || importing}
              >
                {DELIMITER_OPTIONS.map((o) => (
                  <option key={o.value} value={o.value}>
                    {o.label}
                  </option>
                ))}
              </select>
            </div>
          )}

          {parsing && (
            <div className="import-loading">
              <Loader2 size={16} className="spin" />
              <span>Parsing file...</span>
            </div>
          )}

          {parseError && (
            <div className="test-result error" role="alert">{parseError}</div>
          )}

          {workbook && workbook.sheet_names.length > 1 && (
            <div className="form-group">
              <label htmlFor={`${ids}-sheet`}>Sheet</label>
              <select
                id={`${ids}-sheet`}
                className="import-sheet-select"
                value={activeSheet ?? workbook.default_sheet}
                onChange={(e) => handleSheetChange(e.target.value)}
                disabled={parsing}
              >
                {workbook.sheet_names.map((name) => (
                  <option key={name} value={name}>
                    {name}
                  </option>
                ))}
              </select>
              {sourceData?.totalRows != null && (
                <p className="import-sheet-meta">
                  {sourceData.totalRows.toLocaleString()} data rows
                </p>
              )}
            </div>
          )}

          {sourceKind === "xlsx" && !parsing && (sourceData?.errorCellCount ?? 0) > 0 && (
            <div className="import-error-cells" role="alert" data-testid="import-error-cells">
              <AlertTriangle size={16} />
              <span>
                {sourceData!.errorCellCount === 1
                  ? "1 cell contains"
                  : `${sourceData!.errorCellCount!.toLocaleString()} cells contain`}{" "}
                an Excel error and will be imported as NULL:{" "}
                {sourceData!.errorCells!.slice(0, ERROR_CELL_EXAMPLES).join(", ")}
                {sourceData!.errorCellCount! > Math.min(sourceData!.errorCells!.length, ERROR_CELL_EXAMPLES) && ", …"}
              </span>
            </div>
          )}

          {columnsLoading && (
            <div className="import-loading">
              <Loader2 size={20} className="spin" />
              <span>Loading table columns...</span>
            </div>
          )}

          {sourceData && !columnsLoading && (
            <>
              <div className="form-group" role="group" aria-labelledby={`${ids}-mapping`}>
                <span className="form-label" id={`${ids}-mapping`}>Column Mapping</span>
                <div className="import-column-mapping">
                  {sourceData.headers.map((sourceCol) => {
                    const inferred = inferredFor(sourceCol);
                    const target = columnMap[sourceCol] ?? "";
                    const clash = !!target && dupTargets.includes(target);
                    return (
                      <div key={sourceCol} className={`import-mapping-row${clash ? " import-mapping-row--clash" : ""}`}>
                        <span className="import-mapping-csv">
                          {sourceCol}
                          {inferred && (
                            <span className="import-mapping-type">
                              {inferred}
                            </span>
                          )}
                        </span>
                        <span className="import-mapping-arrow" aria-hidden="true">→</span>
                        <select
                          aria-label={`Table column for ${sourceCol}`}
                          aria-invalid={clash || undefined}
                          value={target}
                          onChange={(e) =>
                            setColumnMap((prev) => ({
                              ...prev,
                              [sourceCol]: e.target.value,
                            }))
                          }
                        >
                          <option value="">— Skip —</option>
                          {tableColumns.map((col) => (
                            <option key={col.name} value={col.name}>
                              {col.name} ({col.data_type})
                            </option>
                          ))}
                        </select>
                      </div>
                    );
                  })}
                </div>
                {dupTargets.length > 0 && (
                  <div className="import-mapping-warning" role="alert">
                    <AlertTriangle size={14} aria-hidden="true" />
                    <span>
                      More than one source column is mapped to{" "}
                      {dupTargets.map((t) => `"${t}"`).join(", ")}. Map each table column once.
                    </span>
                  </div>
                )}
              </div>

              <div className="form-group">
                <span className="form-label">Preview (first 10 rows)</span>
                <div className="import-preview">
                  <table className="import-preview-table">
                    <thead>
                      <tr>
                        {sourceData.headers.map((h) => (
                          <th key={h}>{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {previewRows.map((row, i) => (
                        <tr key={i}>
                          {sourceData.headers.map((_, j) => (
                            <td key={j}>{formatPreviewCell(row[j])}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </>
          )}

          {importError && (
            <div className="test-result error" role="alert">{importError}</div>
          )}

          {importResult != null && (
            <div className="test-result success" role="status" data-testid="import-success">
              <CheckCircle size={16} aria-hidden="true" />
              {importResult.toLocaleString()} {importResult === 1 ? "row" : "rows"} imported into {schema}.{tableName}
            </div>
          )}
        </div>

        <div className="dialog-footer">
          {importResult != null ? (
            <button className="btn-primary" onClick={onClose} ref={doneRef}>
              Done
            </button>
          ) : (
            <>
              <button className="btn-secondary" onClick={onClose} disabled={importing}>
                Cancel
              </button>
              <button
                className="btn-primary"
                onClick={handleImport}
                disabled={!sourceData || importing || dupTargets.length > 0}
              >
                {importing && <Loader2 size={14} className="spin" aria-hidden="true" />}
                Import
              </button>
            </>
          )}
        </div>
    </Modal>
  );
}

/// CSV-style "" / "null" sentinel handling that the dialog used to do
/// inline. XLSX rows already carry real `null` values from calamine,
/// so the same coercion is a no-op for them. Numeric / boolean values
/// pass through unchanged so the typed fidelity from #69 isn't lost
/// at the last mile.
function normalizeCell(val: unknown): unknown {
  if (val === null || val === undefined) return null;
  if (typeof val === "string") {
    if (val === "") return null;
    if (val.toLowerCase() === "null") return null;
    return val;
  }
  return val;
}

function formatPreviewCell(val: unknown): string {
  if (val === null || val === undefined) return "";
  if (typeof val === "boolean") return val ? "true" : "false";
  if (typeof val === "object") return JSON.stringify(val);
  return String(val);
}
