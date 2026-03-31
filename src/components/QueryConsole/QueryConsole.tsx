import { useState, useRef, useCallback, useEffect, useMemo } from "react";
import Editor, { OnMount, BeforeMount, type Monaco } from "@monaco-editor/react";
import { api, QueryResult, ExplainResult } from "../../lib/tauri";
import { syncMonacoTheme, isLightTheme } from "../../lib/monacoTheme";
import { ResultTable, parseSimpleSelect, type SourceTable } from "./ResultTable";
import { ExplainView } from "./ExplainView";
import { Play, Search, Clock, Loader2, History, AlignLeft, Bookmark, BookmarkPlus, Copy, Pin, CheckCircle2, Sparkles } from "lucide-react";
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

const SQL_KEYWORDS = [
  "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "IS", "NULL",
  "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE", "CREATE",
  "TABLE", "ALTER", "DROP", "INDEX", "JOIN", "LEFT", "RIGHT", "INNER",
  "OUTER", "ON", "AS", "ORDER", "BY", "GROUP", "HAVING", "LIMIT",
  "OFFSET", "DISTINCT", "COUNT", "SUM", "AVG", "MIN", "MAX", "CASE",
  "WHEN", "THEN", "ELSE", "END", "EXISTS", "BETWEEN", "LIKE", "ILIKE",
  "UNION", "ALL", "WITH", "RETURNING", "CASCADE", "TRUNCATE", "EXPLAIN",
  "ANALYZE", "COALESCE", "CAST", "EXTRACT", "LATERAL", "CROSS", "FULL",
  "NATURAL", "USING", "EXCEPT", "INTERSECT", "FETCH", "FIRST", "NEXT",
  "ROWS", "ONLY", "ASC", "DESC", "NULLS", "LAST", "SCHEMA", "GRANT",
  "REVOKE", "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "UNIQUE",
  "CHECK", "DEFAULT", "CONSTRAINT", "BEGIN", "COMMIT", "ROLLBACK",
  "BOOLEAN", "INTEGER", "BIGINT", "SMALLINT", "TEXT", "VARCHAR",
  "TIMESTAMP", "DATE", "TIME", "NUMERIC", "DECIMAL", "SERIAL",
  "BIGSERIAL", "UUID", "JSONB", "JSON", "ARRAY", "INTERVAL",
];

function formatValidationMessage(message: string) {
  return message.replace(/[A-Za-z]/, (char) => char.toUpperCase());
}

export function QueryConsole({ connectionId, database }: Props) {
  const addToast = useToastStore((s) => s.addToast);
  const [sql, setSql] = useState("SELECT 1;");
  const [result, setResult] = useState<QueryResult | null>(null);
  const [explainResult, setExplainResult] = useState<ExplainResult | null>(null);
  const [resultMode, setResultMode] = useState<ResultMode>("results");
  const [executing, setExecuting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [sourceTable, setSourceTable] = useState<SourceTable | null>(null);
  const lastQueryRef = useRef("");
  const [history, setHistory] = useState<HistoryEntry[]>([]);
  const [showHistory, setShowHistory] = useState(false);
  const [showSavedQueries, setShowSavedQueries] = useState(false);
  const [showSaveDialog, setShowSaveDialog] = useState(false);
  const [saveQueryName, setSaveQueryName] = useState("");
  const [editorHeight, setEditorHeight] = useState(45);
  const [pinnedQueries, setPinnedQueries] = useState<Set<number>>(() => new Set());
  const [suggestionsEnabled, setSuggestionsEnabled] = useState(true);
  const suggestionsEnabledRef = useRef(true);
  const [monacoTheme, setMonacoTheme] = useState<string>(() =>
    isLightTheme() ? "tablio-light-0" : "tablio-dark-0"
  );

  const saveInputRef = useRef<HTMLInputElement>(null);
  const editorRef = useRef<any>(null);
  const monacoRef = useRef<Monaco | null>(null);
  const consoleRef = useRef<HTMLDivElement>(null);
  const draggingRef = useRef(false);
  const themeVersionRef = useRef(0);
  const executeRef = useRef<() => void>(() => {});
  const connRef = useRef({ connectionId, database });
  const tablesRef = useRef<{ name: string; schema: string }[]>([]);
  const tablesLoadedRef = useRef(false);
  const columnsCache = useRef<Map<string, { name: string; type: string }[]>>(new Map());
  const historyRef = useRef<HistoryEntry[]>([]);

  useEffect(() => { connRef.current = { connectionId, database }; }, [connectionId, database]);
  useEffect(() => { historyRef.current = history; }, [history]);
  useEffect(() => { suggestionsEnabledRef.current = suggestionsEnabled; }, [suggestionsEnabled]);

  // ── Monaco theme sync ──

  const syncTheme = useCallback(() => {
    const monaco = monacoRef.current;
    if (!monaco) return;
    const ver = ++themeVersionRef.current;
    setMonacoTheme(syncMonacoTheme(monaco, ver));
  }, []);

  useEffect(() => {
    const observer = new MutationObserver(() => syncTheme());
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme", "style"] });
    return () => observer.disconnect();
  }, [syncTheme]);

  // ── Autocomplete data loading ──

  const loadTablesIfNeeded = useCallback(async () => {
    if (tablesLoadedRef.current && tablesRef.current.length > 0) return;
    const { connectionId: cid, database: db } = connRef.current;
    try {
      const schemas = await api.listSchemas(cid, db);
      const allTables: { name: string; schema: string }[] = [];
      const tablePromises = schemas
        .filter((s) => !s.name.startsWith("pg_") && s.name !== "information_schema")
        .map(async (s) => {
          try {
            const tables = await api.listTables(cid, db, s.name);
            return tables.map((t) => ({ name: t.name, schema: t.schema || s.name }));
          } catch { return []; }
        });
      const results = await Promise.all(tablePromises);
      for (const batch of results) allTables.push(...batch);
      tablesRef.current = allTables;
      tablesLoadedRef.current = true;
    } catch {}
  }, []);

  const loadColumnsForTable = useCallback(async (tableName: string, schema: string) => {
    const cached = columnsCache.current.get(tableName);
    if (cached) return cached;
    const { connectionId: cid, database: db } = connRef.current;
    try {
      const colInfos = await api.listColumns(cid, db, schema, tableName);
      const cols = colInfos.map((c) => ({ name: c.name, type: c.data_type }));
      columnsCache.current.set(tableName, cols);
      return cols;
    } catch { return []; }
  }, []);

  useEffect(() => {
    tablesLoadedRef.current = false;
    tablesRef.current = [];
    columnsCache.current.clear();
    const timer = window.setTimeout(() => loadTablesIfNeeded(), 250);

    return () => window.clearTimeout(timer);
  }, [connectionId, database, loadTablesIfNeeded]);

  // ── Editor setup ──

  const handleEditorBeforeMount: BeforeMount = (monaco) => {
    monacoRef.current = monaco;
    syncTheme();

    if ((monaco as any).__tablioCompletionDisposable) {
      try { (monaco as any).__tablioCompletionDisposable.dispose(); } catch {}
    }
    if ((monaco as any).__tablioInlineDisposable) {
      try { (monaco as any).__tablioInlineDisposable.dispose(); } catch {}
    }

    (monaco as any).__tablioCompletionDisposable = monaco.languages.registerCompletionItemProvider("sql", {
      triggerCharacters: [".", "("],
      provideCompletionItems: async (model: any, position: any) => {
        await loadTablesIfNeeded();
        const word = model.getWordUntilPosition(position);

        const range = {
          startLineNumber: position.lineNumber, endLineNumber: position.lineNumber,
          startColumn: word.startColumn, endColumn: word.endColumn,
        };
        const textBefore = model.getValueInRange({
          startLineNumber: 1, startColumn: 1,
          endLineNumber: position.lineNumber, endColumn: position.column,
        });

        const dotMatch = textBefore.match(/(\w+)\.\w*$/);
        if (dotMatch) {
          const tableName = dotMatch[1];
          const entry = tablesRef.current.find((t) => t.name === tableName);
          const cols = await loadColumnsForTable(tableName, entry?.schema || "public");
          return {
            suggestions: cols.map((c, i) => ({
              label: { label: c.name, description: c.type },
              kind: monaco.languages.CompletionItemKind.Field,
              insertText: c.name,
              sortText: String(i).padStart(4, "0"),
              range,
            })),
          };
        }

        const referencedTables = new Set<string>();
        for (const m of textBefore.matchAll(/\b(?:FROM|JOIN|UPDATE|INTO)\s+(\w+)/gi)) {
          referencedTables.add(m[1]);
        }

        const colSuggestions: any[] = [];
        const seen = new Set<string>();
        const colPromises = [...referencedTables].map(async (tName) => {
          const entry = tablesRef.current.find((t) => t.name === tName);
          return { tName, cols: await loadColumnsForTable(tName, entry?.schema || "public") };
        });
        for (const { tName, cols } of await Promise.all(colPromises)) {
          for (const c of cols) {
            if (seen.has(c.name)) continue;
            seen.add(c.name);
            colSuggestions.push({
              label: { label: c.name, description: c.type },
              kind: monaco.languages.CompletionItemKind.Field,
              insertText: c.name,
              detail: tName,
              sortText: "0" + c.name,
              range,
            });
          }
        }

        const seenTables = new Set<string>();
        const tableSuggestions = tablesRef.current.filter((t) => {
          if (seenTables.has(t.name)) return false;
          seenTables.add(t.name);
          return true;
        }).map((t) => ({
          label: { label: t.name, description: t.schema },
          kind: monaco.languages.CompletionItemKind.Struct,
          insertText: t.name,
          sortText: "1" + t.name,
          range,
        }));

        const kwSuggestions = SQL_KEYWORDS.map((kw) => ({
          label: kw,
          kind: monaco.languages.CompletionItemKind.Keyword,
          insertText: kw + " ",
          sortText: "2" + kw,
          range,
        }));

        return { suggestions: [...colSuggestions, ...tableSuggestions, ...kwSuggestions] };
      },
    });

    (monaco as any).__tablioInlineDisposable = monaco.languages.registerInlineCompletionsProvider("sql", {
      provideInlineCompletions(model: any, position: any) {
        if (!suggestionsEnabledRef.current) return { items: [] };

        const textUntilCursor = model.getValueInRange({
          startLineNumber: 1, startColumn: 1,
          endLineNumber: position.lineNumber, endColumn: position.column,
        }).trimStart();

        if (textUntilCursor.length < 3) return { items: [] };

        const prefix = textUntilCursor.toLowerCase();
        const matches = historyRef.current
          .filter((h: HistoryEntry) => h.sql.toLowerCase().startsWith(prefix) && h.sql.length > textUntilCursor.length)
          .sort((a: HistoryEntry, b: HistoryEntry) => b.timestamp - a.timestamp);

        if (matches.length === 0) return { items: [] };

        const best = matches[0];
        const insertText = best.sql.slice(textUntilCursor.length);

        return {
          items: [{
            insertText,
            range: {
              startLineNumber: position.lineNumber,
              startColumn: position.column,
              endLineNumber: position.lineNumber,
              endColumn: position.column,
            },
          }],
        };
      },
      freeInlineCompletions() {},
      disposeInlineCompletions() {},
    });
  };

  const handleEditorMount: OnMount = useCallback((editor) => {
    editorRef.current = editor;
    editor.addAction({
      id: "execute-query",
      label: "Execute Query",
      keybindings: [2048 | 3],
      run: () => { executeRef.current(); },
    });
  }, []);

  useEffect(() => {
    const editor = editorRef.current;
    const monaco = monacoRef.current;
    if (!editor || !monaco) return;

    const typeDisposable = editor.onDidType((text: string) => {
      if (text === " ") {
        const pos = editor.getPosition();
        const model = editor.getModel();
        if (pos && model && pos.column > 2) {
          const prevChar = model.getValueInRange({
            startLineNumber: pos.lineNumber, startColumn: pos.column - 2,
            endLineNumber: pos.lineNumber, endColumn: pos.column - 1,
          });
          if (prevChar === " ") {
            editor.trigger("keyboard", "hideSuggestWidget", null);
            return;
          }
        }
        editor.trigger("editor", "editor.action.triggerSuggest", null);
      }
    });

    const contentDisposable = editor.onDidChangeModelContent((e) => {
      if (e.isFlush) return;
      for (const change of e.changes) {
        if (change.text.endsWith(" ") && change.text.length > 1) {
          setTimeout(() => editor.trigger("editor", "editor.action.triggerSuggest", null), 50);
          break;
        }
      }
    });

    return () => {
      typeDisposable.dispose();
      contentDisposable.dispose();
    };
  });

  const handleSqlChange = useCallback((v: string | undefined) => setSql(v || ""), []);

  // ── Real-time SQL validation ──

  const validationTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const validationSeqRef = useRef(0);

  useEffect(() => {
    const monaco = monacoRef.current;
    const editor = editorRef.current;
    if (!monaco || !editor) return;
    const model = editor.getModel();
    if (!model) return;

    if (validationTimerRef.current) clearTimeout(validationTimerRef.current);

    monaco.editor.setModelMarkers(model, "sql-validation", []);

    const trimmed = sql.trim();
    if (!trimmed) return;

    const seq = ++validationSeqRef.current;

    validationTimerRef.current = setTimeout(async () => {
      if (executing) return;
      try {
        const result = await api.validateQuery({
          connection_id: connectionId,
          database,
          sql: trimmed,
        });
        if (seq !== validationSeqRef.current) return;

        const currentModel = editorRef.current?.getModel();
        if (!currentModel) return;

        if (result && result.message) {
          let startLine = 1, startCol = 1, endLine = currentModel.getLineCount(), endCol = currentModel.getLineMaxColumn(endLine);

          if (result.position != null && result.position > 0) {
            const pos = currentModel.getPositionAt(result.position - 1);
            startLine = pos.lineNumber;
            startCol = pos.column;
            const wordAtPos = currentModel.getWordAtPosition(pos);
            if (wordAtPos) {
              endLine = startLine;
              endCol = wordAtPos.endColumn;
            } else {
              endLine = startLine;
              endCol = Math.min(startCol + 10, currentModel.getLineMaxColumn(startLine));
            }
          }

          monaco.editor.setModelMarkers(currentModel, "sql-validation", [{
            severity: monaco.MarkerSeverity.Error,
            message: formatValidationMessage(result.message),
            startLineNumber: startLine,
            startColumn: startCol,
            endLineNumber: endLine,
            endColumn: endCol,
          }]);
        }
      } catch {
        // validation failures are non-critical
      }
    }, 800);

    return () => {
      if (validationTimerRef.current) clearTimeout(validationTimerRef.current);
    };
  }, [sql, connectionId, database, executing]);

  // ── Query execution ──

  const getQueryText = useCallback((): string => {
    const editor = editorRef.current;
    if (editor) {
      const model = editor.getModel();
      const selection = editor.getSelection();
      if (selection && !selection.isEmpty() && model) return model.getValueInRange(selection).trim();
      if (model) return model.getValue().trim();
    }
    return sql.trim();
  }, [sql]);

  const executeCurrentStatement = useCallback(async () => {
    const queryToRun = getQueryText();
    if (!queryToRun) return;
    setExecuting(true);
    setError(null);
    setResultMode("results");
    lastQueryRef.current = queryToRun;
    try {
      const res = await api.executeQuery({ connection_id: connectionId, database, sql: queryToRun });
      setResult(res);
      setExplainResult(null);
      if (res.is_select) {
        const parsed = parseSimpleSelect(queryToRun);
        if (parsed && !parsed.schema) {
          const entry = tablesRef.current.find(
            (t) => t.name.toLowerCase() === parsed.table.toLowerCase()
          );
          if (entry) {
            parsed.schema = entry.schema;
          } else {
            parsed.schema = "public";
          }
        }
        setSourceTable(parsed);
      } else {
        setSourceTable(null);
      }
      setHistory((prev) => {
        const trimmed = queryToRun.trim();
        const filtered = prev.filter((h) => h.sql.trim() !== trimmed);
        return [{
          sql: queryToRun, timestamp: Date.now(),
          executionTimeMs: res.execution_time_ms, rowCount: res.rows.length,
        }, ...filtered.slice(0, 99)];
      });
      setPinnedQueries(new Set());
    } catch (e) {
      const errMsg = String(e);
      setError(errMsg);
      setResult(null);
      setSourceTable(null);
      setHistory((prev) => {
        const trimmed = queryToRun.trim();
        const filtered = prev.filter((h) => h.sql.trim() !== trimmed);
        return [{
          sql: queryToRun, timestamp: Date.now(),
          executionTimeMs: 0, rowCount: 0, error: errMsg,
        }, ...filtered.slice(0, 99)];
      });
      setPinnedQueries(new Set());
    } finally {
      setExecuting(false);
    }
  }, [connectionId, database, getQueryText]);

  const handleReExecute = useCallback(async () => {
    const queryToRun = lastQueryRef.current;
    if (!queryToRun) return;
    setExecuting(true);
    setError(null);
    try {
      const res = await api.executeQuery({ connection_id: connectionId, database, sql: queryToRun });
      setResult(res);
    } catch (e) {
      setError(String(e));
    } finally {
      setExecuting(false);
    }
  }, [connectionId, database]);

  useEffect(() => { executeRef.current = executeCurrentStatement; }, [executeCurrentStatement]);

  useEffect(() => {
    if (editorRef.current) requestAnimationFrame(() => editorRef.current?.layout());
  }, [error, result]);

  const explainCurrentStatement = useCallback(async () => {
    const queryToRun = getQueryText();
    if (!queryToRun) return;
    setExecuting(true);
    setError(null);
    setResultMode("explain");
    try {
      const res = await api.explainQuery({ connection_id: connectionId, database, sql: queryToRun });
      setExplainResult(res);
      setResult(null);
    } catch (e) {
      setError(String(e));
      setExplainResult(null);
    } finally {
      setExecuting(false);
    }
  }, [connectionId, database, getQueryText]);

  // ── Toolbar actions ──

  const handleExportResult = useCallback(async (format: "csv" | "json" | "sql") => {
    if (!result || !result.is_select) return;
    try {
      const ext = format === "sql" ? "sql" : format;
      const filePath = await save({
        defaultPath: `query_result.${ext}`,
        filters: [{ name: format.toUpperCase(), extensions: [ext] }],
      });
      if (!filePath) return;
      await api.exportQueryResultToFile({
        columns: result.columns, rows: result.rows as unknown[][],
        format, table_name: null,
      }, filePath);
      addToast(`Exported query result as ${format.toUpperCase()}`);
    } catch (e) {
      addToast(String(e), "error");
    }
  }, [result, addToast]);

  const handleFormatSql = useCallback(() => {
    try {
      setSql(formatSQL(sql, { language: "postgresql", tabWidth: 2 }));
    } catch {}
  }, [sql]);

  const handleSaveQuery = useCallback(() => {
    setSaveQueryName("");
    setShowSaveDialog(true);
    setTimeout(() => saveInputRef.current?.focus(), 50);
  }, []);

  const handleSaveQueryConfirm = useCallback(async () => {
    const name = saveQueryName.trim();
    if (!name) return;
    try {
      await api.saveQuery({
        id: crypto.randomUUID(), name, sql,
        connection_id: connectionId, database,
        created_at: Date.now(), updated_at: Date.now(),
      });
      addToast(`Query "${name}" saved`);
    } catch (e) {
      setError(String(e));
    }
    setShowSaveDialog(false);
  }, [saveQueryName, sql, connectionId, database, addToast]);

  const handleSelectSavedQuery = useCallback((querySql: string) => {
    setSql(querySql);
    setShowSavedQueries(false);
  }, []);

  // ── Resizable split ──

  const handleSplitDragStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    draggingRef.current = true;
    const startY = e.clientY;
    const startHeight = editorHeight;
    const container = consoleRef.current;
    if (!container) return;
    const totalH = container.getBoundingClientRect().height;
    const zoom = parseFloat(document.documentElement.style.zoom || "100") / 100;

    const onMove = (ev: MouseEvent) => {
      if (!draggingRef.current) return;
      const dy = (ev.clientY - startY) / zoom;
      setEditorHeight(Math.max(15, Math.min(85, startHeight + (dy / totalH) * 100)));
    };
    const onUp = () => {
      draggingRef.current = false;
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
      editorRef.current?.layout();
    };
    document.body.style.cursor = "row-resize";
    document.body.style.userSelect = "none";
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }, [editorHeight]);

  // ── History ──

  const handleTogglePin = useCallback((idx: number) => {
    setPinnedQueries((prev) => {
      const next = new Set(prev);
      next.has(idx) ? next.delete(idx) : next.add(idx);
      return next;
    });
  }, []);

  const handleCopyHistorySql = useCallback((entry: HistoryEntry) => {
    navigator.clipboard.writeText(entry.sql);
    addToast("Query copied to clipboard");
  }, [addToast]);

  const sortedHistory = useMemo(() => {
    const indexed = history.map((entry, idx) => ({ entry, idx }));
    return indexed.sort((a, b) => {
      const ap = pinnedQueries.has(a.idx) ? 0 : 1;
      const bp = pinnedQueries.has(b.idx) ? 0 : 1;
      return ap - bp || a.idx - b.idx;
    });
  }, [history, pinnedQueries]);

  const handleHistorySelect = useCallback((entry: HistoryEntry) => {
    setSql(entry.sql);
    setShowHistory(false);
  }, []);

  const toggleResultMode = useCallback(() => {
    setResultMode((m) => m === "chart" ? "results" : "chart");
  }, []);

  const editorSectionStyle = useMemo(() => ({ height: `${editorHeight}%` }), [editorHeight]);

  // ── Render ──

  return (
    <div className="query-console" ref={consoleRef}>
      <div className="query-editor-section" style={editorSectionStyle}>
        <div className="query-toolbar">
          <button className="btn-primary" onClick={executeCurrentStatement} disabled={executing}>
            {executing && resultMode === "results" ? <Loader2 size={14} className="spin" /> : <Play size={14} />}
            Execute
          </button>
          <button className="btn-secondary" onClick={explainCurrentStatement} disabled={executing}>
            {executing && resultMode === "explain" ? <Loader2 size={14} className="spin" /> : <Search size={14} />}
            Explain
          </button>
          <button className="btn-ghost" onClick={handleFormatSql} title="Format SQL (Ctrl+Shift+F)">
            <AlignLeft size={14} /> Format
          </button>
          <span className="query-hint">Ctrl+Enter to run</span>
          <div className="flex-spacer" />
          <button className="btn-ghost" onClick={handleSaveQuery} title="Save query">
            <BookmarkPlus size={14} /> Save
          </button>
          <button className="btn-ghost" onClick={() => setShowSavedQueries(!showSavedQueries)}>
            <Bookmark size={14} /> Saved
          </button>
          <button className="btn-ghost" onClick={() => setShowHistory(!showHistory)}>
            <History size={14} /> History
          </button>
          <button
            className={`btn-ghost ${suggestionsEnabled ? "active-filter" : ""}`}
            onClick={() => setSuggestionsEnabled((v) => !v)}
            title={suggestionsEnabled ? "Disable inline suggestions from history" : "Enable inline suggestions from history"}
          >
            <Sparkles size={14} /> Suggest
          </button>
        </div>
        <div className="query-editor-wrapper">
          <Editor
            height="100%"
            defaultLanguage="sql"
            value={sql}
            onChange={handleSqlChange}
            beforeMount={handleEditorBeforeMount}
            onMount={handleEditorMount}
            theme={monacoTheme}
            options={{
              minimap: { enabled: false },
              fontSize: 16,
              fontWeight: "350",
              fontFamily: "var(--font-mono)",
              lineNumbers: "on",
              scrollBeyondLastLine: false,
              wordWrap: "on",
              automaticLayout: true,
              tabSize: 2,
              padding: { top: 8 },
              fixedOverflowWidgets: true,
              hover: { enabled: true, delay: 300 },
              quickSuggestions: true,
              wordBasedSuggestions: "off",
              acceptSuggestionOnCommitCharacter: false,
              suggest: { showIcons: true, showStatusBar: false, preview: false, insertMode: "replace" },
              inlineSuggest: { enabled: true, showToolbar: "onHover" },
            }}
          />
        </div>
      </div>

      <div className="query-split-handle" onMouseDown={handleSplitDragStart} />

      <div className="query-results-section">
        {executing && <div className="query-loading-bar" />}
        {error && (
          <div className="query-error"><span>{error}</span></div>
        )}

        {resultMode === "explain" && explainResult && <ExplainView result={explainResult} />}

        {(resultMode === "results" || resultMode === "chart") && result && (
          <>
            <div className="query-result-info">
              <Clock size={12} />
              <span>{result.execution_time_ms}ms</span>
              {result.is_select ? (
                <span>{result.rows.length} rows returned</span>
              ) : (
                <span className="query-success-badge">
                  <CheckCircle2 size={12} />
                  {result.rows_affected} rows affected
                </span>
              )}
            </div>
            {resultMode === "chart" && result.is_select && result.rows.length > 0 ? (
              <ChartView columns={result.columns} rows={result.rows as unknown[][]} />
            ) : (
              result.is_select && result.rows.length > 0 && (
                <ResultTable
                  result={result}
                  resultMode={resultMode}
                  onToggleChart={toggleResultMode}
                  onExport={handleExportResult}
                  connectionId={connectionId}
                  database={database}
                  sourceTable={sourceTable}
                  onReExecute={handleReExecute}
                />
              )
            )}
          </>
        )}

        {!result && !explainResult && !error && (
          <div className="query-empty"><p>Execute a query to see results here</p></div>
        )}
      </div>

      {showHistory && (
        <div className="query-history-panel">
          <div className="query-history-header">
            <span>Query History</span>
            <button className="btn-icon" onClick={() => setShowHistory(false)}>×</button>
          </div>
          <div className="query-history-list">
            {history.length === 0 ? (
              <div className="query-history-empty">No queries yet</div>
            ) : (
              sortedHistory.map(({ entry, idx }) => {
                const isPinned = pinnedQueries.has(idx);
                return (
                  <div
                    key={idx}
                    className={`query-history-item ${entry.error ? "error" : ""} ${isPinned ? "pinned" : ""}`}
                    onClick={() => handleHistorySelect(entry)}
                  >
                    <div className="query-history-top">
                      <code className="query-history-sql">{entry.sql}</code>
                      <div className="query-history-actions">
                        <button
                          className={`btn-icon query-history-action ${isPinned ? "pinned" : ""}`}
                          onClick={(e) => { e.stopPropagation(); handleTogglePin(idx); }}
                          title={isPinned ? "Unpin" : "Pin"}
                        >
                          <Pin size={12} />
                        </button>
                        <button
                          className="btn-icon query-history-action"
                          onClick={(e) => { e.stopPropagation(); handleCopyHistorySql(entry); }}
                          title="Copy SQL"
                        >
                          <Copy size={12} />
                        </button>
                      </div>
                    </div>
                    <div className="query-history-meta">
                      <span>{new Date(entry.timestamp).toLocaleTimeString()}</span>
                      {entry.error ? (
                        <span className="text-error">Failed</span>
                      ) : (
                        <span>{entry.executionTimeMs}ms · {entry.rowCount} rows</span>
                      )}
                    </div>
                  </div>
                );
              })
            )}
          </div>
        </div>
      )}

      {showSavedQueries && (
        <SavedQueries onSelectQuery={handleSelectSavedQuery} onClose={() => setShowSavedQueries(false)} />
      )}

      {showSaveDialog && (
        <div className="dialog-overlay" onClick={() => setShowSaveDialog(false)}>
          <div className="save-query-dialog" onClick={(e) => e.stopPropagation()}>
            <h3>Save Query</h3>
            <input
              ref={saveInputRef}
              className="save-query-input"
              type="text"
              placeholder="Enter query name..."
              value={saveQueryName}
              onChange={(e) => setSaveQueryName(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") handleSaveQueryConfirm();
                if (e.key === "Escape") setShowSaveDialog(false);
              }}
            />
            <div className="save-query-actions">
              <button className="btn-secondary" onClick={() => setShowSaveDialog(false)}>Cancel</button>
              <button className="btn-primary" onClick={handleSaveQueryConfirm} disabled={!saveQueryName.trim()}>Save</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
