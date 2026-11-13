import { useState, useRef, useCallback, useEffect, useMemo } from "react";
import Editor, { OnMount, BeforeMount, type Monaco } from "@monaco-editor/react";
import { api, QueryResult, ExplainResult, SavedQuery, QUERY_CANCELLED_MESSAGE } from "../../lib/tauri";
import { syncMonacoTheme, isLightTheme } from "../../lib/monacoTheme";
import { ResultTable, parseSimpleSelect, type SourceTable } from "./ResultTable";
import { ExplainView } from "./ExplainView";
import { Play, Search, Clock, Loader2, History, AlignLeft, Bookmark, BookmarkPlus, Copy, Pin, CheckCircle2, Sparkles, Square, Ban } from "lucide-react";
import { SavedQueries } from "../SavedQueries/SavedQueries";
import { ChartView } from "../ChartView/ChartView";
import { format as formatSQL } from "sql-formatter";
import { save } from "@tauri-apps/plugin-dialog";
import { useToastStore } from "../../stores/toastStore";
import { useTabStore } from "../../stores/tabStore";
import { useUserSettingsStore } from "../../stores/userSettingsStore";
import { useReportTabDirty } from "../../hooks/useTabDirty";
import { readZoomFactor } from "../../lib/zoom";
import "./QueryConsole.css";

interface Props {
  tabId: string;
  connectionId: string;
  database: string;
}

/**
 * Snapshot of a saved query the editor is currently editing. We keep
 * the full object (not just the id) so:
 *   - `name` is available for tab-title sync without a round trip
 *   - `sql` is the baseline that drives the dirty check
 *   - other fields (`created_at`, etc.) come along for the eventual
 *     upsert call so we don't have to re-`load` the row
 *
 * Cleared whenever the user opens a new query tab, opens a different
 * saved query, or saves a brand-new untitled query (in which case it's
 * re-set to the freshly-saved row).
 */
interface LoadedSavedQuery {
  id: string;
  name: string;
  sql: string;
  created_at: number;
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

/**
 * Per-editor completion context. Monaco completion providers are
 * global per language, so a single provider is registered and it
 * resolves the context from the model being completed: each query tab
 * then completes its own connection's tables and its own history.
 */
interface CompletionContext {
  loadTablesIfNeeded: () => Promise<void>;
  loadColumnsForTable: (table: string, schema: string) => Promise<{ name: string; type: string }[]>;
  getTables: () => { name: string; schema: string }[];
  getHistory: () => HistoryEntry[];
  suggestionsEnabled: () => boolean;
}

function completionContexts(monaco: Monaco): Map<string, CompletionContext> {
  const m = monaco as any;
  if (!m.__tablioCompletionContexts) m.__tablioCompletionContexts = new Map<string, CompletionContext>();
  return m.__tablioCompletionContexts;
}

function contextForModel(monaco: Monaco, model: any): CompletionContext | undefined {
  const uri = model?.uri?.toString?.();
  return uri ? completionContexts(monaco).get(uri) : undefined;
}

function ensureCompletionProviders(monaco: Monaco) {
  const m = monaco as any;
  if (m.__tablioQueryProvidersRegistered) return;
  m.__tablioQueryProvidersRegistered = true;
  // Providers registered by older builds of this component (one per
  // mounted editor, bound to that editor's refs).
  for (const key of ["__tablioCompletionDisposable", "__tablioInlineDisposable"]) {
    if (m[key]) {
      try { m[key].dispose(); } catch {}
      m[key] = undefined;
    }
  }

  monaco.languages.registerCompletionItemProvider("sql", {
    triggerCharacters: [".", "("],
    provideCompletionItems: async (model: any, position: any) => {
      const ctx = contextForModel(monaco, model);
      if (!ctx) return { suggestions: [] };
      await ctx.loadTablesIfNeeded();
      const tables = ctx.getTables();
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
        const entry = tables.find((t) => t.name === tableName);
        const cols = await ctx.loadColumnsForTable(tableName, entry?.schema || "public");
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
      for (const mt of textBefore.matchAll(/\b(?:FROM|JOIN|UPDATE|INTO)\s+(\w+)/gi)) {
        referencedTables.add(mt[1]);
      }

      const colSuggestions: any[] = [];
      const seen = new Set<string>();
      const colPromises = [...referencedTables].map(async (tName) => {
        const entry = tables.find((t) => t.name === tName);
        return { tName, cols: await ctx.loadColumnsForTable(tName, entry?.schema || "public") };
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
      const tableSuggestions = tables.filter((t) => {
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

  monaco.languages.registerInlineCompletionsProvider("sql", {
    provideInlineCompletions(model: any, position: any) {
      const ctx = contextForModel(monaco, model);
      if (!ctx || !ctx.suggestionsEnabled()) return { items: [] };

      const textUntilCursor = model.getValueInRange({
        startLineNumber: 1, startColumn: 1,
        endLineNumber: position.lineNumber, endColumn: position.column,
      }).trimStart();

      if (textUntilCursor.length < 3) return { items: [] };

      const prefix = textUntilCursor.toLowerCase();
      const matches = ctx.getHistory()
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
  } as any);
}

const DEFAULT_SQL = "SELECT 1;";
const SQL_STORAGE_PREFIX = "tablio-query-sql:";

/** Editor text is kept per tab for the session so a reload restores it. */
function loadTabSql(tabId: string): string {
  try {
    const v = sessionStorage.getItem(SQL_STORAGE_PREFIX + tabId);
    if (v !== null) return v;
  } catch {}
  return DEFAULT_SQL;
}

function formatValidationMessage(message: string) {
  return message.replace(/[A-Za-z]/, (char) => char.toUpperCase());
}

export function QueryConsole({ tabId, connectionId, database }: Props) {
  const addToast = useToastStore((s) => s.addToast);
  const setTabTitle = useTabStore((s) => s.setTabTitle);
  const isActiveTab = useTabStore((s) => s.activeTabId === tabId);
  // Editor font size from user preferences (issue #62). Monaco
  // re-renders the option diff on every prop change so a live
  // adjustment in the Preferences dialog reflects immediately.
  const editorFontSize = useUserSettingsStore(
    (s) => s.settings.editorFontSize,
  );
  // Identity of the saved query currently in the editor, if any.
  // Drives the dirty check, the in-place Ctrl/Cmd+S path, and the
  // "Save changes" button label. `null` until the user loads a
  // saved query (or saves a brand-new one).
  const [loadedSavedQuery, setLoadedSavedQuery] = useState<LoadedSavedQuery | null>(null);
  const [sql, setSql] = useState(() => loadTabSql(tabId));
  const [result, setResult] = useState<QueryResult | null>(null);
  const [explainResult, setExplainResult] = useState<ExplainResult | null>(null);
  const [resultMode, setResultMode] = useState<ResultMode>("results");
  const [executing, setExecuting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // The last run was stopped by the user: shown as a neutral notice,
  // not as an error.
  const [cancelled, setCancelled] = useState(false);
  // Backend request id of the running execute (Stop targets it); null
  // while idle or while a non-cancellable request (Explain) runs.
  const [runningExecId, setRunningExecId] = useState<string | null>(null);
  const [cancelling, setCancelling] = useState(false);
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
  // Guards against overlapping runs (Ctrl+Enter bypasses the disabled
  // Execute button) and lets only the newest request update the UI.
  const executingRef = useRef(false);
  const requestSeqRef = useRef(0);
  const runningExecIdRef = useRef<string | null>(null);
  const cancelRequestedRef = useRef(false);
  const cancelRef = useRef<() => void>(() => {});
  const executingContextKeyRef = useRef<{ set: (v: boolean) => void } | null>(null);
  const modelUriRef = useRef<string | null>(null);
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
    // Key by `schema.table`, not just `table`. With the previous key
    // two same-named tables in different schemas (e.g. `auth.users`
    // and `app.users`) collided in this cache and autocomplete
    // returned the first-loaded set for both — wrong columns,
    // confusing UX, and very hard to spot once cached.
    const key = `${schema}.${tableName}`;
    const cached = columnsCache.current.get(key);
    if (cached) return cached;
    const { connectionId: cid, database: db } = connRef.current;
    try {
      const colInfos = await api.listColumns(cid, db, schema, tableName);
      const cols = colInfos.map((c) => ({ name: c.name, type: c.data_type }));
      columnsCache.current.set(key, cols);
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
    ensureCompletionProviders(monaco);
  };

  const completionContextRef = useRef<CompletionContext>({
    loadTablesIfNeeded,
    loadColumnsForTable,
    getTables: () => tablesRef.current,
    getHistory: () => historyRef.current,
    suggestionsEnabled: () => suggestionsEnabledRef.current,
  });

  useEffect(() => {
    // Re-register after a StrictMode remount (the editor itself stays).
    if (monacoRef.current && modelUriRef.current) {
      completionContexts(monacoRef.current).set(modelUriRef.current, completionContextRef.current);
    }
    return () => {
      const monaco = monacoRef.current;
      if (monaco && modelUriRef.current) completionContexts(monaco).delete(modelUriRef.current);
    };
  }, []);

  const handleEditorMount: OnMount = useCallback((editor, monaco) => {
    editorRef.current = editor;
    const uri = editor.getModel()?.uri?.toString();
    if (uri) {
      modelUriRef.current = uri;
      completionContexts(monaco).set(uri, completionContextRef.current);
    }
    editor.addAction({
      id: "execute-query",
      label: "Execute Query",
      keybindings: [2048 | 3],
      run: () => { executeRef.current(); },
    });
    // Escape stops a running query. The context key limits the binding
    // to while a query runs, and the suggest / find widgets keep their
    // own Escape (9 = KeyCode.Escape).
    executingContextKeyRef.current = editor.createContextKey<boolean>("tablioQueryRunning", false);
    editor.addAction({
      id: "cancel-query",
      label: "Stop Running Query",
      keybindings: [9],
      precondition: "tablioQueryRunning && !suggestWidgetVisible && !findWidgetVisible",
      run: () => { cancelRef.current(); },
    });
    // Ctrl/Cmd+S: update the loaded saved query in place, or open
    // the Save dialog for a brand-new query. The binding goes
    // through a ref so the action always sees the latest closure
    // without re-registering (Monaco's `addAction` is meant to be
    // called once per editor lifetime; re-registering on every
    // state change risks duplicate actions / stuck keybindings).
    // 2048 = KeyMod.CtrlCmd, 49 = KeyCode.KeyS  → Ctrl/Cmd+S.
    editor.addAction({
      id: "save-query",
      label: "Save Query",
      keybindings: [2048 | 49],
      run: () => { saveOrUpdateRef.current(); },
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

    const contentDisposable = editor.onDidChangeModelContent((e: { isFlush: boolean; changes: { text: string }[] }) => {
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

  const beginRequest = useCallback((): number | null => {
    if (executingRef.current) return null;
    executingRef.current = true;
    setExecuting(true);
    return ++requestSeqRef.current;
  }, []);

  const endRequest = useCallback((reqId: number) => {
    if (reqId !== requestSeqRef.current) return;
    executingRef.current = false;
    setExecuting(false);
    runningExecIdRef.current = null;
    setRunningExecId(null);
    setCancelling(false);
  }, []);

  /** Make run `reqId` cancellable and return its backend request id. */
  const startCancellable = useCallback((reqId: number): string => {
    const id = `${tabId}:${reqId}:${Date.now()}`;
    cancelRequestedRef.current = false;
    runningExecIdRef.current = id;
    setRunningExecId(id);
    setCancelled(false);
    return id;
  }, [tabId]);

  /**
   * A failed run the user had asked to stop is reported as cancelled,
   * whatever the server said (the backend already normalises it, but
   * a statement can also fail on its own just as Stop is pressed).
   */
  const isCancellation = useCallback((e: unknown) =>
    cancelRequestedRef.current || String(e) === QUERY_CANCELLED_MESSAGE, []);

  const cancelExecution = useCallback(async () => {
    const id = runningExecIdRef.current;
    if (!id || cancelRequestedRef.current) return;
    cancelRequestedRef.current = true;
    setCancelling(true);
    try {
      // The execute promise settles (as cancelled) once the backend has
      // stopped the query and cleaned up its connection.
      await api.cancelExecution(id);
    } catch (e) {
      cancelRequestedRef.current = false;
      setCancelling(false);
      addToast(`Could not cancel the query: ${String(e)}`, "error");
    }
  }, [addToast]);

  useEffect(() => { cancelRef.current = cancelExecution; }, [cancelExecution]);
  useEffect(() => { executingContextKeyRef.current?.set(runningExecId !== null); }, [runningExecId]);
  // Closing the tab mid-query stops the query instead of leaving it
  // running on the server.
  useEffect(() => () => {
    const id = runningExecIdRef.current;
    if (id) api.cancelExecution(id).catch(() => {});
  }, []);

  const executeCurrentStatement = useCallback(async () => {
    const queryToRun = getQueryText();
    if (!queryToRun) return;
    const reqId = beginRequest();
    if (reqId === null) return;
    const isCurrent = () => reqId === requestSeqRef.current;
    setError(null);
    setResultMode("results");
    lastQueryRef.current = queryToRun;
    const requestId = startCancellable(reqId);
    try {
      const res = await api.executeQuery({
        connection_id: connectionId, database, sql: queryToRun, request_id: requestId,
      });
      if (!isCurrent()) return;
      setResult(res);
      setExplainResult(null);
      res.notices?.forEach((n) => addToast(n, "info"));
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
      if (!isCurrent()) return;
      if (isCancellation(e)) {
        setCancelled(true);
        setResult(null);
        setSourceTable(null);
        return;
      }
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
      endRequest(reqId);
    }
  }, [connectionId, database, getQueryText, beginRequest, endRequest, startCancellable, isCancellation, addToast]);

  const handleReExecute = useCallback(async () => {
    const queryToRun = lastQueryRef.current;
    if (!queryToRun) return;
    const reqId = beginRequest();
    if (reqId === null) return;
    setError(null);
    const requestId = startCancellable(reqId);
    try {
      const res = await api.executeQuery({
        connection_id: connectionId, database, sql: queryToRun, request_id: requestId,
      });
      if (reqId === requestSeqRef.current) {
        setResult(res);
        res.notices?.forEach((n) => addToast(n, "info"));
      }
    } catch (e) {
      if (reqId !== requestSeqRef.current) return;
      if (isCancellation(e)) setCancelled(true);
      else setError(String(e));
    } finally {
      endRequest(reqId);
    }
  }, [connectionId, database, beginRequest, endRequest, startCancellable, isCancellation, addToast]);

  useEffect(() => { executeRef.current = executeCurrentStatement; }, [executeCurrentStatement]);

  useEffect(() => {
    if (editorRef.current) requestAnimationFrame(() => editorRef.current?.layout());
  }, [error, result]);

  const explainCurrentStatement = useCallback(async () => {
    const queryToRun = getQueryText();
    if (!queryToRun) return;
    const reqId = beginRequest();
    if (reqId === null) return;
    setError(null);
    setCancelled(false);
    setResultMode("explain");
    try {
      const res = await api.explainQuery({ connection_id: connectionId, database, sql: queryToRun });
      if (reqId !== requestSeqRef.current) return;
      setExplainResult(res);
      setResult(null);
    } catch (e) {
      if (reqId !== requestSeqRef.current) return;
      setError(String(e));
      setExplainResult(null);
    } finally {
      endRequest(reqId);
    }
  }, [connectionId, database, getQueryText, beginRequest, endRequest]);

  // ── Toolbar actions ──

  const handleExportResult = useCallback(async (format: "csv" | "json" | "sql" | "xlsx") => {
    if (!result || !result.is_select) return;
    try {
      const ext = format === "sql" ? "sql" : format;
      const label = format === "xlsx" ? "Excel" : format.toUpperCase();
      const filePath = await save({
        defaultPath: `query_result.${ext}`,
        filters: [{ name: label, extensions: [ext] }],
      });
      if (!filePath) return;
      const warning = await api.exportQueryResultToFile({
        columns: result.columns, rows: result.rows as unknown[][],
        format, table_name: null,
      }, filePath);
      addToast(`Exported query result as ${label}`);
      if (warning) addToast(warning, "info");
    } catch (e) {
      addToast(String(e), "error");
    }
  }, [result, addToast]);

  const handleFormatSql = useCallback(() => {
    try {
      setSql(formatSQL(sql, { language: "postgresql", tabWidth: 2 }));
    } catch {}
  }, [sql]);

  /**
   * "Dirty" = the editor's text has diverged from the snapshot of
   * the saved query it was loaded from. When there's no loaded
   * saved query, nothing is "dirty" — there's no baseline to
   * compare against (a brand-new untitled query). Used by:
   *
   *   - The Save button label ("Save changes" vs "Save")
   *   - Whether Ctrl/Cmd+S runs the in-place update vs opens the
   *     Save dialog
   */
  const isDirty = useMemo(
    () => loadedSavedQuery !== null && sql !== loadedSavedQuery.sql,
    [loadedSavedQuery, sql],
  );

  useReportTabDirty(tabId, "query", isDirty);

  // Session persistence of the editor text, keyed by tab.
  useEffect(() => {
    const timer = window.setTimeout(() => {
      try { sessionStorage.setItem(SQL_STORAGE_PREFIX + tabId, sql); } catch {}
    }, 300);
    return () => window.clearTimeout(timer);
  }, [sql, tabId]);
  useEffect(() => {
    // Tabs stay mounted until closed, so unmount means the tab is gone.
    return () => {
      try { sessionStorage.removeItem(SQL_STORAGE_PREFIX + tabId); } catch {}
    };
  }, [tabId]);

  const handleSaveQuery = useCallback(() => {
    setSaveQueryName("");
    setShowSaveDialog(true);
    setTimeout(() => saveInputRef.current?.focus(), 50);
  }, []);

  const handleSaveQueryConfirm = useCallback(async () => {
    const name = saveQueryName.trim();
    if (!name) return;
    // Brand-new save (no loaded saved query). Mint a fresh id, but
    // immediately adopt it as `loadedSavedQuery` so the next edit
    // already shows the "Save changes" affordance instead of
    // re-prompting for a name. Also push the chosen name through
    // to the tab title.
    const newId = crypto.randomUUID();
    const now = Date.now();
    try {
      await api.saveQuery({
        id: newId, name, sql,
        connection_id: connectionId, database,
        created_at: now, updated_at: now,
      });
      setLoadedSavedQuery({ id: newId, name, sql, created_at: now });
      setTabTitle(tabId, name);
      addToast(`Query "${name}" saved`);
    } catch (e) {
      setError(String(e));
    }
    setShowSaveDialog(false);
  }, [saveQueryName, sql, connectionId, database, tabId, setTabTitle, addToast]);

  /**
   * In-place update of the currently-loaded saved query. No dialog —
   * the name is fixed, only the SQL changes. Bound to Ctrl/Cmd+S and
   * to the "Save changes" toolbar button.
   *
   * Held in a ref so the Monaco keybinding (registered once on
   * editor mount) always sees the latest sql / loadedSavedQuery
   * without having to re-register on every keystroke.
   */
  const handleUpdateLoadedQuery = useCallback(async () => {
    if (!loadedSavedQuery) return;
    if (sql === loadedSavedQuery.sql) return; // no-op when clean
    try {
      const now = Date.now();
      await api.saveQuery({
        id: loadedSavedQuery.id,
        name: loadedSavedQuery.name,
        sql,
        connection_id: connectionId,
        database,
        created_at: loadedSavedQuery.created_at,
        updated_at: now,
      });
      setLoadedSavedQuery({ ...loadedSavedQuery, sql });
      addToast(`Saved changes to "${loadedSavedQuery.name}"`);
    } catch (e) {
      setError(String(e));
    }
  }, [loadedSavedQuery, sql, connectionId, database, addToast]);

  /**
   * Single entry point for the Save toolbar button and Ctrl/Cmd+S:
   *   - If editing a loaded saved query, update it in place.
   *   - Otherwise (brand-new query), open the Save dialog to capture
   *     a name.
   *
   * Mirrors standard editor "Save" semantics — Ctrl+S works whether
   * the file has a name yet or not.
   */
  const handleSaveOrUpdate = useCallback(() => {
    if (loadedSavedQuery) {
      void handleUpdateLoadedQuery();
    } else {
      handleSaveQuery();
    }
  }, [loadedSavedQuery, handleUpdateLoadedQuery, handleSaveQuery]);

  // Keep the Monaco-bound keybinding pointed at the latest closure
  // without having to re-register the action when state changes.
  const saveOrUpdateRef = useRef(handleSaveOrUpdate);
  useEffect(() => {
    saveOrUpdateRef.current = handleSaveOrUpdate;
  }, [handleSaveOrUpdate]);

  const handleSelectSavedQuery = useCallback((query: SavedQuery) => {
    setSql(query.sql);
    setLoadedSavedQuery({
      id: query.id,
      name: query.name,
      sql: query.sql,
      created_at: query.created_at,
    });
    setTabTitle(tabId, query.name);
    setShowSavedQueries(false);
  }, [tabId, setTabTitle]);

  // ── Resizable split ──

  const handleSplitDragStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    draggingRef.current = true;
    const startY = e.clientY;
    const startHeight = editorHeight;
    const container = consoleRef.current;
    if (!container) return;
    const totalH = container.getBoundingClientRect().height;
    const zoom = readZoomFactor();

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
          {runningExecId && (
            <button
              className="btn-secondary query-stop-btn"
              data-testid="stop-query-btn"
              onClick={cancelExecution}
              disabled={cancelling}
              title="Stop the running query (Esc)"
            >
              {cancelling ? <Loader2 size={14} className="spin" /> : <Square size={14} />}
              {cancelling ? "Stopping…" : "Stop"}
            </button>
          )}
          <button className="btn-secondary" onClick={explainCurrentStatement} disabled={executing}>
            {executing && resultMode === "explain" ? <Loader2 size={14} className="spin" /> : <Search size={14} />}
            Explain
          </button>
          <button className="btn-ghost" onClick={handleFormatSql} title="Format SQL (Ctrl+Shift+F)">
            <AlignLeft size={14} /> Format
          </button>
          <span className="query-hint">Ctrl+Enter to run</span>
          <div className="flex-spacer" />
          {loadedSavedQuery ? (
            // The user is editing a loaded saved query. Two states:
            //   - dirty → "Save changes" (active call to action)
            //   - clean → "Save" (visibly idle, disabled — no work to do)
            // In both cases the button updates the existing row by
            // id instead of minting a new one. The `data-testid` is
            // stable across all variants so e2e selectors don't have
            // to track the user-visible label or title (which now
            // legitimately changes based on state).
            <button
              className="btn-ghost"
              data-testid="save-query-btn"
              onClick={handleSaveOrUpdate}
              disabled={!isDirty}
              title={isDirty ? "Save changes (Ctrl/Cmd+S)" : "No unsaved changes"}
            >
              <BookmarkPlus size={14} /> {isDirty ? "Save changes" : "Save"}
            </button>
          ) : (
            // No loaded saved query — first save opens the dialog
            // to capture a name (still Ctrl/Cmd+S).
            <button
              className="btn-ghost"
              data-testid="save-query-btn"
              onClick={handleSaveOrUpdate}
              title="Save query (Ctrl/Cmd+S)"
            >
              <BookmarkPlus size={14} /> Save
            </button>
          )}
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
              fontSize: editorFontSize,
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
        {cancelled && !executing && !error && (
          <div className="query-cancelled" data-testid="query-cancelled" role="status">
            <Ban size={14} /> <span>{QUERY_CANCELLED_MESSAGE}</span>
          </div>
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
                  tabId={tabId}
                  isActive={isActiveTab}
                />
              )
            )}
          </>
        )}

        {!result && !explainResult && !error && !cancelled && (
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
