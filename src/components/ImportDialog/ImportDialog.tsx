import { useEffect, useState } from "react";
import { api, ColumnInfo } from "../../lib/tauri";
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
  const [csvFile, setCsvFile] = useState<File | null>(null);
  const [csvData, setCsvData] = useState<{
    headers: string[];
    rows: string[][];
  } | null>(null);
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

  useEffect(() => {
    if (csvData && tableColumns.length > 0) {
      const hasMappings = Object.values(columnMap).some((v) => v);
      if (!hasMappings) {
        const initialMap: Record<string, string> = {};
        csvData.headers.forEach((h) => {
          const match = tableColumns.find(
            (c) => c.name.toLowerCase() === h.toLowerCase()
          );
          initialMap[h] = match ? match.name : "";
        });
        setColumnMap(initialMap);
      }
    }
  }, [csvData, tableColumns]);

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    setCsvFile(file);
    setCsvData(null);
    setParseError(null);
    setImportResult(null);
    setImportError(null);

    const reader = new FileReader();
    reader.onload = () => {
      try {
        const text = String(reader.result);
        const { headers, rows } = parseCSV(text);
        setCsvData({ headers, rows });
        const initialMap: Record<string, string> = {};
        headers.forEach((h) => {
          const match = tableColumns.find(
            (c) => c.name.toLowerCase() === h.toLowerCase()
          );
          initialMap[h] = match ? match.name : "";
        });
        setColumnMap(initialMap);
      } catch (err) {
        setParseError(String(err));
      }
    };
    reader.readAsText(file);
  };

  const handleImport = async () => {
    if (!csvData) return;

    const mappedCols = Object.entries(columnMap)
      .filter(([, tableCol]) => tableCol)
      .map(([, tableCol]) => tableCol);

    if (mappedCols.length === 0) {
      setImportError("Map at least one CSV column to a table column");
      return;
    }

    const csvHeaders = csvData.headers;
    const colIndices = mappedCols.map((tc) => {
      const csvCol = Object.entries(columnMap).find(([, v]) => v === tc)?.[0];
      return csvHeaders.indexOf(csvCol!);
    });

    const rows: unknown[][] = csvData.rows.map((row) =>
      colIndices.map((idx) => {
        const val = row[idx] ?? "";
        if (val === "" || val.toLowerCase() === "null") return null;
        return val;
      })
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

  const previewRows = csvData?.rows.slice(0, 10) ?? [];

  return (
    <div className="dialog-overlay" onClick={onClose}>
      <div
        className="dialog import-dialog"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="dialog-header">
          <h2>
            <FileSpreadsheet size={18} />
            Import CSV
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
            <label>CSV File</label>
            <div className="import-file-input">
              <input
                type="file"
                accept=".csv"
                onChange={handleFileChange}
                id="csv-file-input"
              />
              <label htmlFor="csv-file-input" className="import-file-label">
                {csvFile?.name ?? "Choose a CSV file..."}
              </label>
            </div>
          </div>

          {parseError && (
            <div className="test-result error">{parseError}</div>
          )}

          {columnsLoading && (
            <div className="import-loading">
              <Loader2 size={20} className="spin" />
              <span>Loading table columns...</span>
            </div>
          )}

          {csvData && !columnsLoading && (
            <>
              <div className="form-group">
                <label>Column Mapping</label>
                <div className="import-column-mapping">
                  {csvData.headers.map((csvCol) => (
                    <div key={csvCol} className="import-mapping-row">
                      <span className="import-mapping-csv">{csvCol}</span>
                      <span className="import-mapping-arrow">→</span>
                      <select
                        value={columnMap[csvCol] ?? ""}
                        onChange={(e) =>
                          setColumnMap((prev) => ({
                            ...prev,
                            [csvCol]: e.target.value,
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
                  ))}
                </div>
              </div>

              <div className="form-group">
                <label>Preview (first 10 rows)</label>
                <div className="import-preview">
                  <table className="import-preview-table">
                    <thead>
                      <tr>
                        {csvData.headers.map((h) => (
                          <th key={h}>{h}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {previewRows.map((row, i) => (
                        <tr key={i}>
                          {csvData.headers.map((h, j) => (
                            <td key={j}>{row[j] ?? ""}</td>
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
            disabled={!csvData || importing}
          >
            {importing && <Loader2 size={14} className="spin" />}
            Import
          </button>
        </div>
      </div>
    </div>
  );
}
