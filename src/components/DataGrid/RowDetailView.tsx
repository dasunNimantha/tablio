import { useState, useRef, useEffect, useMemo, useCallback } from "react";
import { X, ChevronDown, ChevronRight, Copy, Check, CheckCheck, Lock } from "lucide-react";
import type { ColumnInfo } from "../../lib/tauri";
import { isNumericLike, isNumericType, isTextLike, parseCellInput, valueCategory } from "../../lib/cellValues";
import "./RowDetailView.css";

interface Props {
  columns: ColumnInfo[];
  row: unknown[];
  rowIndex: number;
  onClose: () => void;
  onCellChange: (colIndex: number, newValue: unknown) => void;
}

function tryParseJson(value: unknown): { parsed: unknown; isJson: boolean } {
  if (typeof value === "object" && value !== null) return { parsed: value, isJson: true };
  if (typeof value !== "string") return { parsed: value, isJson: false };
  try {
    const p = JSON.parse(value as string);
    if (typeof p === "object" && p !== null) return { parsed: p, isJson: true };
  } catch {}
  return { parsed: value, isJson: false };
}

function getValueColorForOriginal(value: unknown): string {
  if (value === null || value === undefined) return "json-null";
  if (typeof value === "boolean") return "json-bool";
  if (typeof value === "number") return "json-number";
  return "json-string";
}

function getEditColorClass(col: ColumnInfo): string {
  const cat = getTypeCategory(col.data_type);
  switch (cat) {
    case "integer":
    case "number":
      return "json-number";
    case "boolean":
      return "json-bool";
    default:
      return "json-string";
  }
}

function needsQuotes(value: unknown, col: ColumnInfo): boolean {
  if (value === null || value === undefined) return false;
  if (typeof value === "boolean") return false;
  if (typeof value === "number") return false;
  const cat = getTypeCategory(col.data_type);
  if (cat === "integer" || cat === "number" || cat === "boolean") return false;
  return true;
}

function getTypeCategory(dataType: string): string {
  const t = dataType.toLowerCase();
  if (/^(smallint|integer|int2|int4|int8|bigint|smallserial|serial|bigserial|tinyint|mediumint)/.test(t)) return "integer";
  if (/^(real|double|float|numeric|decimal|money|double precision)/.test(t)) return "number";
  if (/^(bool)/.test(t)) return "boolean";
  if (/^(json|jsonb)/.test(t)) return "json";
  if (/^(uuid)/.test(t)) return "uuid";
  if (/^(date)$/.test(t)) return "date";
  if (/^(time)/.test(t)) return "time";
  if (/^(timestamp|timestamptz)/.test(t)) return "timestamp";
  if (/^(inet|cidr)/.test(t)) return "inet";
  if (/^(character varying|varchar)/.test(t)) return "varchar";
  if (/^(char|character)\b/.test(t)) return "char";
  return "text";
}

function parseMaxLength(dataType: string): number | null {
  const m = dataType.match(/\((\d+)\)/);
  return m ? parseInt(m[1], 10) : null;
}

export type EditResult = { ok: true; value: unknown } | { ok: false; error: string };

/**
 * Validate and convert the inline editor's text into the value to store.
 *
 * Text is stored exactly as typed: no trimming, and "" or "null" stay
 * strings (use the NULL button for SQL NULL). For non-text types an
 * empty value (or the word NULL) can't be a valid value, so it means
 * NULL when the column allows it. Numbers keep exact precision.
 */
export function prepareEditValue(value: string, col: ColumnInfo): EditResult {
  const dataType = col.data_type;
  const trimmed = value.trim();
  if (!isTextLike(dataType)) {
    if (trimmed === "" || trimmed.toLowerCase() === "null") {
      return col.is_nullable
        ? { ok: true, value: null }
        : { ok: false, error: "This field is NOT NULL — value required" };
    }
  }

  const category = getTypeCategory(dataType);
  switch (category) {
    case "json":
      // Validate, but send the text as typed: the backend writes it as
      // JSON text, so big numbers, key order and JSON scalars survive.
      try { JSON.parse(trimmed); return { ok: true, value: trimmed }; } catch { return { ok: false, error: "Invalid JSON syntax" }; }
    case "uuid":
      if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(trimmed))
        return { ok: false, error: "Invalid UUID (expected xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)" };
      return { ok: true, value: trimmed };
    case "date":
      if (isNaN(Date.parse(trimmed))) return { ok: false, error: "Invalid date format" };
      return { ok: true, value: trimmed };
    case "timestamp":
      if (isNaN(Date.parse(trimmed))) return { ok: false, error: "Invalid timestamp format" };
      return { ok: true, value: trimmed };
    case "varchar":
    case "char": {
      const maxLen = parseMaxLength(dataType);
      if (maxLen !== null && value.length > maxLen)
        return { ok: false, error: `Max length is ${maxLen}, got ${value.length}` };
      return { ok: true, value };
    }
  }

  const parsed = parseCellInput(value, dataType);
  if (!parsed.ok) return parsed;
  if (valueCategory(dataType) === "integer" && !/unsigned/i.test(dataType)) {
    const n = BigInt(String(parsed.value));
    const dt = dataType.toLowerCase();
    if (/^(smallint|int2)\b/.test(dt) && (n < -32768n || n > 32767n))
      return { ok: false, error: "smallint range: -32768 to 32767" };
    if (/^(integer|int4|int|serial)\b/.test(dt) && (n < -2147483648n || n > 2147483647n))
      return { ok: false, error: "integer range: -2147483648 to 2147483647" };
  }
  return parsed;
}

function JsonValue({ value, depth, filterQ }: { value: unknown; depth: number; filterQ: string }) {
  const [collapsed, setCollapsed] = useState(depth > 2);

  if (value === null || value === undefined) {
    return <span className="json-null">null</span>;
  }
  if (typeof value === "boolean") {
    return <span className="json-bool">{String(value)}</span>;
  }
  if (typeof value === "number") {
    return <span className="json-number">{String(value)}</span>;
  }
  if (typeof value === "string") {
    const { parsed, isJson } = tryParseJson(value);
    if (isJson) {
      return <JsonValue value={parsed} depth={depth} filterQ={filterQ} />;
    }

    if (value.length > 120) {
      return <span className="json-string">"{value.slice(0, 120)}…"</span>;
    }
    return <span className="json-string">"{value}"</span>;
  }

  if (Array.isArray(value)) {
    if (value.length === 0) return <span className="json-bracket">[]</span>;
    return (
      <span>
        <span className="json-toggle" onClick={() => setCollapsed(!collapsed)}>
          {collapsed ? <ChevronRight size={12} /> : <ChevronDown size={12} />}
        </span>
        <span className="json-bracket">[</span>
        {collapsed ? (
          <span className="json-collapsed-hint" onClick={() => setCollapsed(false)}>
            {value.length} items]
          </span>
        ) : (
          <>
            {value.map((item, i) => (
              <div key={i} className="json-line" style={{ paddingLeft: (depth + 1) * 18 }}>
                <JsonValue value={item} depth={depth + 1} filterQ={filterQ} />
                {i < value.length - 1 && <span className="json-comma">,</span>}
              </div>
            ))}
            <div style={{ paddingLeft: depth * 18 }}>
              <span className="json-bracket">]</span>
            </div>
          </>
        )}
      </span>
    );
  }

  if (typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>);
    if (entries.length === 0) return <span className="json-bracket">{"{}"}</span>;
    return (
      <span>
        <span className="json-toggle" onClick={() => setCollapsed(!collapsed)}>
          {collapsed ? <ChevronRight size={12} /> : <ChevronDown size={12} />}
        </span>
        <span className="json-bracket">{"{"}</span>
        {collapsed ? (
          <span className="json-collapsed-hint" onClick={() => setCollapsed(false)}>
            {entries.length} keys{"}"}
          </span>
        ) : (
          <>
            {entries.map(([k, v], i) => (
              <div key={k} className="json-line" style={{ paddingLeft: (depth + 1) * 18 }}>
                <span className="json-key">"{k}"</span>
                <span className="json-colon">: </span>
                <JsonValue value={v} depth={depth + 1} filterQ={filterQ} />
                {i < entries.length - 1 && <span className="json-comma">,</span>}
              </div>
            ))}
            <div style={{ paddingLeft: depth * 18 }}>
              <span className="json-bracket">{"}"}</span>
            </div>
          </>
        )}
      </span>
    );
  }

  return <span className="json-string">{String(value)}</span>;
}

function highlightMatch(text: string, query: string): React.ReactNode {
  if (!query) return text;
  const tokens = query.toLowerCase().split(/\s+/).filter(Boolean);
  if (!tokens.length) return text;
  const escaped = tokens.map((t) => t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  const regex = new RegExp(`(${escaped.join("|")})`, "gi");
  const parts = text.split(regex);
  if (parts.length === 1) return text;
  const test = new RegExp(`^(?:${escaped.join("|")})$`, "i");
  return (
    <>
      {parts.map((part, i) =>
        test.test(part) ? (
          <mark key={i} className="json-highlight">{part}</mark>
        ) : (
          part
        )
      )}
    </>
  );
}

export function RowDetailView({
  columns,
  row,
  rowIndex,
  onClose,
  onCellChange,
}: Props) {
  const [filterText, setFilterText] = useState("");
  const [editingKey, setEditingKey] = useState<string | null>(null);
  const [editingColIndex, setEditingColIndex] = useState(-1);
  const [editValue, setEditValue] = useState("");
  const [editError, setEditError] = useState<string | null>(null);
  const [pendingEdits, setPendingEdits] = useState<Map<number, unknown>>(new Map());
  const [copied, setCopied] = useState(false);
  const [panelWidth, setPanelWidth] = useState(440);
  const editRef = useRef<HTMLTextAreaElement>(null);
  const resizeRef = useRef<{ startX: number; startW: number } | null>(null);
  const blurCommitRef = useRef(true);
  const editStartValueRef = useRef("");
  const copyTimerRef = useRef<ReturnType<typeof setTimeout>>(undefined);
  const blurTimerRef = useRef<ReturnType<typeof setTimeout>>(undefined);

  const resizeTextarea = useCallback(() => {
    if (editRef.current) {
      editRef.current.style.height = "0";
      editRef.current.style.height = editRef.current.scrollHeight + "px";
    }
  }, []);

  useEffect(() => {
    setPendingEdits(new Map());
    setEditingKey(null);
    setEditingColIndex(-1);
    setEditError(null);
  }, [rowIndex]);

  useEffect(() => {
    if (editingKey !== null && editRef.current) {
      editRef.current.focus();
      editRef.current.select();
      resizeTextarea();
    }
  }, [editingKey, resizeTextarea]);

  useEffect(() => {
    return () => {
      resizeRef.current = null;
      clearTimeout(copyTimerRef.current);
      clearTimeout(blurTimerRef.current);
    };
  }, []);

  const handlePanelResizeStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    resizeRef.current = { startX: e.clientX, startW: panelWidth };
    const onMove = (ev: MouseEvent) => {
      if (!resizeRef.current) return;
      const delta = resizeRef.current.startX - ev.clientX;
      const newW = Math.max(280, Math.min(resizeRef.current.startW + delta, window.innerWidth * 0.7));
      setPanelWidth(newW);
    };
    const onUp = () => {
      resizeRef.current = null;
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
    };
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
  }, [panelWidth]);

  const rowObj = useMemo(() => {
    const entries: { key: string; value: unknown; colIndex: number; col: ColumnInfo }[] = [];
    columns.forEach((col, i) => {
      entries.push({ key: col.name, value: row[i], colIndex: i, col });
    });
    return entries;
  }, [columns, row]);

  const filtered = useMemo(() => {
    if (!filterText) return rowObj;
    const tokens = filterText.toLowerCase().split(/\s+/).filter(Boolean);
    if (!tokens.length) return rowObj;
    return rowObj.filter(({ key, value, colIndex }) => {
      const displayVal = pendingEdits.has(colIndex) ? pendingEdits.get(colIndex) : value;
      const keyLower = key.toLowerCase();
      const valStr = (displayVal === null || displayVal === undefined)
        ? "null"
        : typeof displayVal === "object" ? JSON.stringify(displayVal) : String(displayVal);
      const valLower = valStr.toLowerCase();
      return tokens.every((t) => keyLower.includes(t) || valLower.includes(t));
    });
  }, [rowObj, filterText, pendingEdits]);

  const handleCopyAll = () => {
    const obj: Record<string, unknown> = {};
    columns.forEach((col, i) => {
      obj[col.name] = pendingEdits.has(i) ? pendingEdits.get(i) : row[i];
    });
    navigator.clipboard.writeText(JSON.stringify(obj, null, 2));
    setCopied(true);
    clearTimeout(copyTimerRef.current);
    copyTimerRef.current = setTimeout(() => setCopied(false), 1500);
  };

  const handleStartEdit = (colIndex: number, key: string, value: unknown, col: ColumnInfo) => {
    if (col.is_primary_key) return;

    const str = value === null || value === undefined
      ? ""
      : typeof value === "object"
        ? JSON.stringify(value, null, 2)
        : String(value);
    editStartValueRef.current = str;
    setEditingKey(key);
    setEditingColIndex(colIndex);
    setEditValue(str);
    setEditError(null);
    blurCommitRef.current = true;
  };

  const closeEditor = () => {
    setEditingKey(null);
    setEditingColIndex(-1);
    setEditError(null);
  };

  const recordValue = useCallback((colIndex: number, value: unknown) => {
    const origVal = row[colIndex];
    const isSame = (value === null && (origVal === null || origVal === undefined))
      || (value !== null && JSON.stringify(value) === JSON.stringify(origVal));
    setPendingEdits((prev) => {
      if (isSame && !prev.has(colIndex)) return prev;
      const next = new Map(prev);
      if (isSame) next.delete(colIndex);
      else next.set(colIndex, value);
      return next;
    });
  }, [row]);

  const commitField = useCallback(() => {
    if (editingColIndex < 0) return;
    const col = columns[editingColIndex];

    // Nothing typed: leave the value (including NULL) as it was.
    if (editValue === editStartValueRef.current) {
      closeEditor();
      return;
    }

    const result = prepareEditValue(editValue, col);
    if (!result.ok) {
      setEditError(result.error);
      blurCommitRef.current = false;
      return;
    }

    recordValue(editingColIndex, result.value);
    closeEditor();
  }, [editValue, editingColIndex, columns, recordValue]);

  const handleSetNull = useCallback(() => {
    if (editingColIndex < 0) return;
    blurCommitRef.current = false;
    clearTimeout(blurTimerRef.current);
    recordValue(editingColIndex, null);
    closeEditor();
  }, [editingColIndex, recordValue]);

  const handleCancel = () => {
    blurCommitRef.current = false;
    setEditingKey(null);
    setEditingColIndex(-1);
    setEditError(null);
  };

  const handleBlur = useCallback(() => {
    clearTimeout(blurTimerRef.current);
    blurTimerRef.current = setTimeout(() => {
      if (blurCommitRef.current) {
        commitField();
      }
    }, 0);
  }, [commitField]);

  const handleSaveAll = () => {
    pendingEdits.forEach((value, colIndex) => {
      onCellChange(colIndex, value);
    });
    setPendingEdits(new Map());
  };

  const handleDiscardAll = () => {
    blurCommitRef.current = false;
    setPendingEdits(new Map());
    setEditingKey(null);
    setEditingColIndex(-1);
    setEditError(null);
  };

  const hasPendingEdits = pendingEdits.size > 0;

  return (
    <div className="row-detail-panel" style={{ width: panelWidth }}>
      <div className="row-detail-resize-handle" onMouseDown={handlePanelResizeStart} />
      <div className="row-detail-header">
        <h2>JSON VIEWER</h2>
        <div className="row-detail-header-actions">
          {hasPendingEdits && (
            <button className="json-save-all-btn" onClick={handleSaveAll}>
              <CheckCheck size={13} />
              Apply {pendingEdits.size} edit{pendingEdits.size > 1 ? "s" : ""}
            </button>
          )}
          {(hasPendingEdits || editError) && (
            <button className="json-discard-btn" onClick={handleDiscardAll} title="Discard all changes">
              <X size={13} />
            </button>
          )}
          <button className="btn-icon" onClick={handleCopyAll} title="Copy as JSON">
            {copied ? <Check size={14} /> : <Copy size={14} />}
          </button>
          <button className="btn-icon" onClick={onClose}>
            <X size={16} />
          </button>
        </div>
      </div>
      <div className="row-detail-filter">
        <input
          placeholder="Filter by key or value..."
          value={filterText}
          onChange={(e) => setFilterText(e.target.value)}
        />
      </div>

      <div className="row-detail-body">
        <div className="json-tree">
          <div className="json-line">
            <span className="json-bracket">{"{"}</span>
          </div>
          {filtered.map(({ key, value, colIndex, col }, i) => {
            const isLast = i === filtered.length - 1;
            const isEditing = editingKey === key;
            const isPending = pendingEdits.has(colIndex);
            const displayValue = isPending ? pendingEdits.get(colIndex) : value;
            const isPk = col.is_primary_key;

            if (isEditing) {
              const editColorClass = getEditColorClass(col);
              const showQuotes = getTypeCategory(col.data_type) !== "integer"
                && getTypeCategory(col.data_type) !== "number"
                && getTypeCategory(col.data_type) !== "boolean";

              return (
                <div
                  key={key}
                  className={`json-row ${isPending ? "json-row-modified" : ""}`}
                  style={{ paddingLeft: 18 }}
                >
                  <span className="json-key">
                    "{filterText ? highlightMatch(key, filterText) : key}"
                  </span>
                  <span className="json-colon">: </span>
                  {showQuotes && <span className={editColorClass}>"</span>}
                  <textarea
                    ref={editRef}
                    className={`json-inline-input ${editColorClass} ${editError ? "json-inline-input-error" : ""}`}
                    value={editValue}
                    onChange={(e) => { setEditValue(e.target.value); setEditError(null); blurCommitRef.current = true; resizeTextarea(); }}
                    onKeyDown={(e) => {
                      if (e.key === "Escape") { handleCancel(); }
                      if (col.is_nullable && e.key.toLowerCase() === "n" && (e.ctrlKey || e.metaKey) && e.shiftKey) {
                        e.preventDefault();
                        handleSetNull();
                      }
                      if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); blurCommitRef.current = false; commitField(); }
                    }}
                    onBlur={handleBlur}
                  />
                  {showQuotes && <span className={editColorClass}>"</span>}
                  {col.is_nullable && (
                    <button
                      type="button"
                      className="json-set-null-btn"
                      title="Set to NULL (Ctrl+Shift+N)"
                      // Fire before the textarea's blur commits the text.
                      onMouseDown={(e) => { e.preventDefault(); handleSetNull(); }}
                    >
                      NULL
                    </button>
                  )}
                  {editError && <span className="json-inline-error">{editError}</span>}
                  {!isLast && <span className="json-comma">,</span>}
                </div>
              );
            }

            const colorClass = getValueColorForOriginal(displayValue);
            const showQuotes = needsQuotes(displayValue, col);

            return (
              <div
                key={key}
                className={`json-row ${isPending ? "json-row-modified" : ""} ${isPk ? "json-row-pk" : ""}`}
                style={{ paddingLeft: 18 }}
              >
                <span className="json-key">
                  "{filterText ? highlightMatch(key, filterText) : key}"
                </span>
                <span className="json-colon">: </span>
                <span
                  className={`json-value-inline ${isPk ? "json-value-readonly" : ""}`}
                  onDoubleClick={() => handleStartEdit(colIndex, key, displayValue, col)}
                  title={isPk ? "Primary key — not editable" : "Double-click to edit"}
                >
                  {isPk && <Lock size={10} className="json-pk-icon" />}
                  {typeof displayValue === "string" && isNumericType(col.data_type) && isNumericLike(displayValue) ? (
                    // Exact big integers / decimals arrive as strings;
                    // show them as numbers, not quoted text.
                    <span className="json-number">{displayValue}</span>
                  ) : (
                    <JsonValue value={displayValue} depth={1} filterQ={filterText} />
                  )}
                </span>
                {!isLast && <span className="json-comma">,</span>}
              </div>
            );
          })}
          <div className="json-line">
            <span className="json-bracket">{"}"}</span>
          </div>
        </div>
      </div>
    </div>
  );
}
