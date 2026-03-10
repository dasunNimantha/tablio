import { useState, useRef, useCallback, useEffect } from "react";
import Editor, { OnMount } from "@monaco-editor/react";
import { api, QueryResult, ExplainResult } from "../../lib/tauri";
import { ResultTable } from "./ResultTable";
import { ExplainView } from "./ExplainView";
import { Play, Search, Clock, Loader2, History, Download, AlignLeft, Bookmark, BookmarkPlus, BarChart3 } from "lucide-react";
import { ExportMenu } from "../ExportMenu";
import { SavedQueries } from "../SavedQueries/SavedQueries";
import { ChartView } from "../ChartView/ChartView";
import { format as formatSQL } from "sql-formatter";
import { save } from "@tauri-apps/plugin-dialog";
import { useToastStore } from "../../stores/toastStore";
import "./QueryConsole.css";

interface Props {
  connectionId: string;
  database: string;
}

interface HistoryEntry {
  sql: string;
  timestamp: number;
  executionTimeMs: number;
  rowCount: number;
  error?: string;
}

type ResultMode = "results" | "explain" | "chart";

export function QueryConsole({ connectionId, database }: Props) {
  const addToast = useToastStore((s) => s.addToast);
  const [sql, setSql] = useState("SELECT 1;");
  const [result, setResult] = useState<QueryResult | null>(null);
  const [explainResult, setExplainResult] = useState<ExplainResult | null>(null);
  const [resultMode, setResultMode] = useState<ResultMode>("results");
  const [executing, setExecuting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [history, setHistory] = useState<HistoryEntry[]>([]);
  const [showHistory, setShowHistory] = useState(false);
  const [showSavedQueries, setShowSavedQueries] = useState(false);
  const editorRef = useRef<any>(null);
  const [monacoTheme, setMonacoTheme] = useState<string>(() => {
    return document.documentElement.getAttribute("data-theme") === "light" ? "light" : "vs-dark";
  });

  useEffect(() => {
    const observer = new MutationObserver(() => {
      const t = document.documentElement.getAttribute("data-theme");
      setMonacoTheme(t === "light" ? "light" : "vs-dark");
    });
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme"] });
    return () => observer.disconnect();
  }, []);

  const handleEditorMount: OnMount = (editor) => {
    editorRef.current = editor;

    editor.addAction({
      id: "execute-query",
      label: "Execute Query",
      keybindings: [2048 | 3],
      run: () => {
        executeCurrentStatement();
      },
    });
  };

  const getQueryText = (): string => {
    let queryToRun = sql.trim();
    if (editorRef.current) {
      const selection = editorRef.current.getSelection();
      const model = editorRef.current.getModel();
      if (selection && !selection.isEmpty() && model) {
        queryToRun = model.getValueInRange(selection);
      }
    }
    return queryToRun;
  };

  const executeCurrentStatement = useCallback(async () => {
    const queryToRun = getQueryText();
    if (!queryToRun) return;

    setExecuting(true);
    setError(null);
    setResultMode("results");
    try {
      const res = await api.executeQuery({
        connection_id: connectionId,
        database,
        sql: queryToRun,
      });
      setResult(res);
      setExplainResult(null);
      setHistory((prev) => [
        {
          sql: queryToRun,
          timestamp: Date.now(),
          executionTimeMs: res.execution_time_ms,
          rowCount: res.rows.length,
        },
        ...prev.slice(0, 99),
      ]);
    } catch (e) {
      const errMsg = String(e);
      setError(errMsg);
      setResult(null);
      setHistory((prev) => [
        {
          sql: queryToRun,
          timestamp: Date.now(),
          executionTimeMs: 0,
          rowCount: 0,
          error: errMsg,
        },
        ...prev.slice(0, 99),
      ]);
    } finally {
      setExecuting(false);
    }
  }, [sql, connectionId, database]);

  const explainCurrentStatement = useCallback(async () => {
    const queryToRun = getQueryText();
    if (!queryToRun) return;

    setExecuting(true);
    setError(null);
    setResultMode("explain");
    try {
      const res = await api.explainQuery({
        connection_id: connectionId,
        database,
        sql: queryToRun,
      });
      setExplainResult(res);
      setResult(null);
    } catch (e) {
      setError(String(e));
      setExplainResult(null);
    } finally {
      setExecuting(false);
    }
  }, [sql, connectionId, database]);

  const handleExportResult = async (format: "csv" | "json" | "sql") => {
    if (!result || !result.is_select) return;
    try {
      const ext = format === "sql" ? "sql" : format;
      const filePath = await save({
        defaultPath: `query_result.${ext}`,
        filters: [{
          name: format.toUpperCase(),
          extensions: [ext],
        }],
      });
      if (!filePath) return;

      await api.exportQueryResultToFile({
        columns: result.columns,
        rows: result.rows as unknown[][],
        format,
        table_name: null,
      }, filePath);
      addToast(`Exported query result as ${format.toUpperCase()}`);
    } catch (e) {
      addToast(String(e), "error");
    }
  };

  const handleFormatSql = () => {
    try {
      const formatted = formatSQL(sql, { language: "postgresql", tabWidth: 2 });
      setSql(formatted);
    } catch {
      // silently fail if formatting fails
    }
  };

  const handleSaveQuery = async () => {
    const name = prompt("Query name:");
    if (!name) return;
    try {
      await api.saveQuery({
        id: crypto.randomUUID(),
        name,
        sql,
        connection_id: connectionId,
        database,
        created_at: Date.now(),
        updated_at: Date.now(),
      });
    } catch (e) {
      setError(String(e));
    }
  };

  const handleSelectSavedQuery = (querySql: string) => {
    setSql(querySql);
    setShowSavedQueries(false);
  };

  const handleHistorySelect = (entry: HistoryEntry) => {
    setSql(entry.sql);
    setShowHistory(false);
  };

  return (
    <div className="query-console">
      <div className="query-editor-section">
        <div className="query-toolbar">
          <button
            className="btn-primary"
            onClick={executeCurrentStatement}
            disabled={executing}
          >
            {executing && resultMode === "results" ? (
              <Loader2 size={14} className="spin" />
            ) : (
              <Play size={14} />
            )}
            Execute
          </button>
          <button
            className="btn-secondary"
            onClick={explainCurrentStatement}
            disabled={executing}
            style={{ height: 24, fontSize: 11, borderRadius: 6 }}
          >
            {executing && resultMode === "explain" ? (
              <Loader2 size={14} className="spin" />
            ) : (
              <Search size={14} />
            )}
            Explain
          </button>
          <button
            className="btn-ghost"
            onClick={handleFormatSql}
            title="Format SQL (Ctrl+Shift+F)"
          >
            <AlignLeft size={14} /> Format
          </button>
          <span className="query-hint">Ctrl+Enter to run</span>
          <div style={{ flex: 1 }} />
          <button
            className="btn-ghost"
            onClick={handleSaveQuery}
            title="Save query"
          >
            <BookmarkPlus size={14} /> Save
          </button>
          <button
            className="btn-ghost"
            onClick={() => setShowSavedQueries(!showSavedQueries)}
          >
            <Bookmark size={14} /> Saved
          </button>
          <button
            className="btn-ghost"
            onClick={() => setShowHistory(!showHistory)}
          >
            <History size={14} /> History
          </button>
        </div>
        <div className="query-editor-wrapper">
          <Editor
            height="100%"
            defaultLanguage="sql"
            value={sql}
            onChange={(v) => setSql(v || "")}
            onMount={handleEditorMount}
            theme={monacoTheme}
            options={{
              minimap: { enabled: false },
              fontSize: 13,
              fontFamily: "var(--font-mono)",
              lineNumbers: "on",
              scrollBeyondLastLine: false,
              wordWrap: "on",
              automaticLayout: true,
              tabSize: 2,
              padding: { top: 8 },
            }}
          />
        </div>
      </div>

      <div className="query-results-section">
        {error && (
          <div className="query-error">
            <span>{error}</span>
          </div>
        )}

        {resultMode === "explain" && explainResult && (
          <ExplainView result={explainResult} />
        )}

        {(resultMode === "results" || resultMode === "chart") && result && (
          <>
            <div className="query-result-info">
              <Clock size={12} />
              <span>{result.execution_time_ms}ms</span>
              {result.is_select ? (
                <span>{result.rows.length} rows returned</span>
              ) : (
                <span>{result.rows_affected} rows affected</span>
              )}
              <div style={{ flex: 1 }} />
              {result.is_select && result.rows.length > 0 && (
                <>
                  <button
                    className={`btn-ghost ${resultMode === "chart" ? "active-filter" : ""}`}
                    onClick={() => setResultMode(resultMode === "chart" ? "results" : "chart")}
                    title="Toggle Chart View"
                  >
                    <BarChart3 size={14} /> Chart
                  </button>
                  <ExportMenu onExport={handleExportResult} />
                </>
              )}
            </div>
            {resultMode === "chart" && result.is_select && result.rows.length > 0 ? (
              <ChartView
                columns={result.columns}
                rows={result.rows as unknown[][]}
              />
            ) : (
              result.is_select && result.rows.length > 0 && (
                <ResultTable result={result} />
              )
            )}
          </>
        )}

        {!result && !explainResult && !error && (
          <div className="query-empty">
            <p>Execute a query to see results here</p>
          </div>
        )}
      </div>

      {showHistory && (
        <div className="query-history-panel">
          <div className="query-history-header">
            <span>Query History</span>
            <button className="btn-icon" onClick={() => setShowHistory(false)}>
              ×
            </button>
          </div>
          <div className="query-history-list">
            {history.length === 0 ? (
              <div className="query-history-empty">No queries yet</div>
            ) : (
              history.map((entry, idx) => (
                <div
                  key={idx}
                  className={`query-history-item ${entry.error ? "error" : ""}`}
                  onClick={() => handleHistorySelect(entry)}
                >
                  <code className="query-history-sql">{entry.sql}</code>
                  <div className="query-history-meta">
                    <span>
                      {new Date(entry.timestamp).toLocaleTimeString()}
                    </span>
                    {entry.error ? (
                      <span className="text-error">Failed</span>
                    ) : (
                      <span>
                        {entry.executionTimeMs}ms · {entry.rowCount} rows
                      </span>
                    )}
                  </div>
                </div>
              ))
            )}
          </div>
        </div>
      )}
      {showSavedQueries && (
        <SavedQueries
          onSelectQuery={handleSelectSavedQuery}
          onClose={() => setShowSavedQueries(false)}
        />
      )}
    </div>
  );
}
