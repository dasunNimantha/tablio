import { useEffect, useState } from "react";
import {
  api,
  ColumnInfo,
  XlsxWorkbookPreview,
  XlsxColumnType,
} from "../../lib/tauri";
import { X, FileSpreadsheet, Loader2, CheckCircle } from "lucide-react";
import "./ImportDialog.css";

interface Props {
  connectionId: string;
  database: string;
  schema: string;
  tableName: string;
  onClose: () => void;
  onImported: () => void;
}

const MAX_FILE_BYTES = 100 * 1024 * 1024;
const ACCEPTED_EXTS = [".csv", ".xlsx", ".xls"] as const;

type SourceKind = "csv" | "xlsx";

interface ParsedSheet {
  headers: string[];
  rows: unknown[][];
  inferredTypes?: XlsxColumnType[];
  totalRows?: number;
}

function detectKind(name: string): SourceKind | null {
  const lower = name.toLowerCase();
  if (lower.endsWith(".csv")) return "csv";
  if (lower.endsWith(".xlsx") || lower.endsWith(".xls")) return "xlsx";
  return null;
}

function parseCSV(text: string): { headers: string[]; rows: string[][] } {
  const lines = text.split(/\r?\n/).filter((line) => line.trim().length > 0);
  if (lines.length === 0) return { headers: [], rows: [] };

  const parseRow = (line: string): string[] => {
    const result: string[] = [];
    let i = 0;
    while (i < line.length) {
      if (line[i] === '"') {
        let value = "";
        i++;
        while (i < line.length) {
          if (line[i] === '"') {
            if (line[i + 1] === '"') {
              value += '"';
              i += 2;
            } else {
              i++;
              break;
            }
          } else {
            value += line[i];
            i++;
          }
        }
        result.push(value);
      } else {
        const comma = line.indexOf(",", i);
        if (comma === -1) {
          result.push(line.slice(i).trim());
          break;
        }
        result.push(line.slice(i, comma).trim());
        i = comma + 1;
      }
    }
    return result;
  };

  const headers = parseRow(lines[0]);
  const rows = lines.slice(1).map(parseRow);
  return { headers, rows };
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
  };

  const handleFileChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    resetSourceState();

    if (file.size > MAX_FILE_BYTES) {
      setSourceFile(file);
      setSourceKind(null);
      setParseError(
        `File is too large (${(file.size / 1024 / 1024).toFixed(1)} MB). Max ${MAX_FILE_BYTES / 1024 / 1024} MB.`
      );
      return;
    }

    const kind = detectKind(file.name);
    setSourceFile(file);
    setSourceKind(kind);

    if (kind === null) {
      setParseError(
        `Unsupported file type. Choose a ${ACCEPTED_EXTS.join(", ")} file.`
      );
      return;
    }

    setParsing(true);
    try {
      if (kind === "csv") {
        const text = await file.text();
        const { headers, rows } = parseCSV(text);
        setSourceData({ headers, rows });
        seedColumnMap(headers);
      } else {
        const buf = new Uint8Array(await file.arrayBuffer());
        setWorkbookBytes(buf);
        const preview = await api.parseXlsxWorkbook(buf);
        setWorkbook(preview);
        setActiveSheet(preview.default_sheet);
        await loadXlsxSheet(buf, preview.default_sheet);
      }
    } catch (err) {
      setParseError(String(err));
    } finally {
      setParsing(false);
    }
  };

  // Fetch the full payload for a sheet (headers + rows + inferred
  // types) and seed the column mapping. Used both for the initial
  // load and any sheet-picker change.
  const loadXlsxSheet = async (buf: Uint8Array, sheetName: string) => {
    const payload = await api.parseXlsxSheet(buf, sheetName);
    setSourceData({
      headers: payload.headers,
      rows: payload.rows,
      inferredTypes: payload.inferred_types,
      totalRows: payload.rows.length,
    });
    seedColumnMap(payload.headers);
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

    const mappedCols = Object.entries(columnMap)
      .filter(([, tableCol]) => tableCol)
      .map(([, tableCol]) => tableCol);

    if (mappedCols.length === 0) {
      setImportError("Map at least one source column to a table column");
      return;
    }

    const headers = sourceData.headers;
    const colIndices = mappedCols.map((tc) => {
      const sourceCol = Object.entries(columnMap).find(([, v]) => v === tc)?.[0];
      return headers.indexOf(sourceCol!);
    });

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
      onImported();
    } catch (e) {
      setImportError(String(e));
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
    <div className="dialog-overlay" onClick={onClose}>
      <div
        className="dialog import-dialog"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="dialog-header">
          <h2>
            <FileSpreadsheet size={18} />
            Import Data
          </h2>
          <button className="btn-icon" onClick={onClose}>
            <X size={18} />
          </button>
        </div>

        <div className="dialog-body">
          <div className="form-group">
            <label>Target Table</label>
            <input
              value={`${schema}.${tableName}`}
              disabled
              className="import-target-input"
            />
          </div>

          <div className="form-group">
            <label>Source File</label>
            <div className="import-file-input">
              <input
                type="file"
                accept={ACCEPTED_EXTS.join(",")}
                onChange={handleFileChange}
                id="csv-file-input"
              />
              <label htmlFor="csv-file-input" className="import-file-label">
                {sourceFile?.name ?? "Choose a .csv, .xlsx, or .xls file..."}
              </label>
            </div>
            <p className="import-file-hint">
              Supported: CSV, Excel (.xlsx, .xls). Max {MAX_FILE_BYTES / 1024 / 1024} MB.
            </p>
          </div>

          {parsing && (
            <div className="import-loading">
              <Loader2 size={16} className="spin" />
              <span>Parsing file...</span>
            </div>
          )}

          {parseError && (
            <div className="test-result error">{parseError}</div>
          )}

          {workbook && workbook.sheet_names.length > 1 && (
            <div className="form-group">
              <label>Sheet</label>
              <select
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

          {columnsLoading && (
            <div className="import-loading">
              <Loader2 size={20} className="spin" />
              <span>Loading table columns...</span>
            </div>
          )}

          {sourceData && !columnsLoading && (
            <>
              <div className="form-group">
                <label>Column Mapping</label>
                <div className="import-column-mapping">
                  {sourceData.headers.map((sourceCol) => {
                    const inferred = inferredFor(sourceCol);
                    return (
                      <div key={sourceCol} className="import-mapping-row">
                        <span className="import-mapping-csv">
                          {sourceCol}
                          {inferred && (
                            <span className="import-mapping-type">
                              {inferred}
                            </span>
                          )}
                        </span>
                        <span className="import-mapping-arrow">→</span>
                        <select
                          value={columnMap[sourceCol] ?? ""}
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
              </div>

              <div className="form-group">
                <label>Preview (first 10 rows)</label>
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
            <div className="test-result error">{importError}</div>
          )}

          {importResult != null && (
            <div className="test-result success">
              <CheckCircle size={16} />
              {importResult} rows imported successfully
            </div>
          )}
        </div>

        <div className="dialog-footer">
          <button className="btn-secondary" onClick={onClose}>
            {importResult != null ? "Done" : "Cancel"}
          </button>
          <button
            className="btn-primary"
            onClick={handleImport}
            disabled={!sourceData || importing}
          >
            {importing && <Loader2 size={14} className="spin" />}
            Import
          </button>
        </div>
      </div>
    </div>
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
