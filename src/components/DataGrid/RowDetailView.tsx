import { useState, useRef, useEffect, useMemo, useCallback } from "react";
import { X, ChevronDown, ChevronRight, Copy, Check, Save, Lock } from "lucide-react";
import type { ColumnInfo } from "../../lib/tauri";
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

function validateValue(value: string, dataType: string, isNullable: boolean): string | null {
  const trimmed = value.trim();

  if (trimmed === "" || trimmed.toLowerCase() === "null") {
    return isNullable ? null : "This field is NOT NULL — value required";
  }

  const category = getTypeCategory(dataType);

  switch (category) {
    case "integer": {
      if (!/^-?\d+$/.test(trimmed)) return `Expected integer, got "${trimmed}"`;
      try {
        const n = BigInt(trimmed);
        const dt = dataType.toLowerCase();
        if (/^(smallint|int2)/.test(dt) && (n < -32768n || n > 32767n))
          return "smallint range: -32768 to 32767";
        if (/^(integer|int4|serial)/.test(dt) && (n < -2147483648n || n > 2147483647n))
          return "integer range: -2147483648 to 2147483647";
      } catch {
        return `Invalid integer: "${trimmed}"`;
      }
      break;
    }
    case "number": {
      if (isNaN(Number(trimmed))) return `Expected number, got "${trimmed}"`;
      break;
    }
    case "boolean":
      if (!["true", "false", "t", "f", "1", "0", "yes", "no"].includes(trimmed.toLowerCase()))
        return "Expected boolean (true/false/t/f/1/0)";
      break;
    case "json":
      try { JSON.parse(trimmed); } catch { return "Invalid JSON syntax"; }
      break;
    case "uuid":
      if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(trimmed))
        return "Invalid UUID (expected xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)";
      break;
    case "date":
      if (isNaN(Date.parse(trimmed))) return "Invalid date format";
      break;
    case "timestamp":
      if (isNaN(Date.parse(trimmed))) return "Invalid timestamp format";
      break;
    case "varchar":
    case "char": {
      const maxLen = parseMaxLength(dataType);
      if (maxLen !== null && trimmed.length > maxLen)
        return `Max length is ${maxLen}, got ${trimmed.length}`;
      break;
    }
  }

  return null;
}

function coerceForSave(value: string, col: ColumnInfo): unknown {
  const trimmed = value.trim();
  if ((trimmed === "" || trimmed.toLowerCase() === "null") && col.is_nullable) return null;

  const cat = getTypeCategory(col.data_type);
  switch (cat) {
    case "integer": {
      const n = parseInt(trimmed, 10);
      return isNaN(n) ? trimmed : n;
    }
    case "number": {
      const n = parseFloat(trimmed);
      return isNaN(n) ? trimmed : n;
    }
    case "boolean": {
      const lower = trimmed.toLowerCase();
      return ["true", "t", "1", "yes"].includes(lower);
    }
    case "json":
      try { return JSON.parse(trimmed); } catch { return trimmed; }
    default:
      return trimmed;
  }
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
    setTimeout(() => setCopied(false), 1500);
  };

  const handleStartEdit = (colIndex: number, key: string, value: unknown, col: ColumnInfo) => {
    if (col.is_primary_key) return;

    const str = value === null || value === undefined
      ? ""
      : typeof value === "object"
        ? JSON.stringify(value, null, 2)
        : String(value);
    setEditingKey(key);
    setEditingColIndex(colIndex);
    setEditValue(str);
    setEditError(null);
    blurCommitRef.current = true;
  };

  const commitField = useCallback(() => {
    if (editingColIndex < 0) return;
    const col = columns[editingColIndex];
    const v = editValue.trim();

    const error = validateValue(v, col.data_type, col.is_nullable);
    if (error) {
      setEditError(error);
      blurCommitRef.current = false;
      return;
    }

    const coerced = coerceForSave(editValue, col);
    const origVal = row[editingColIndex];

    const isSame = coerced === null && origVal === null
      || coerced === null && origVal === undefined
      || JSON.stringify(coerced) === JSON.stringify(origVal);

    if (!isSame) {
      setPendingEdits((prev) => {
        const next = new Map(prev);
        next.set(editingColIndex, coerced);
        return next;
      });
    } else {
      setPendingEdits((prev) => {
        if (!prev.has(editingColIndex)) return prev;
        const next = new Map(prev);
        next.delete(editingColIndex);
        return next;
      });
    }

    setEditingKey(null);
    setEditingColIndex(-1);
    setEditError(null);
  }, [editValue, editingColIndex, columns, row]);

  const handleCancel = () => {
    blurCommitRef.current = false;
    setEditingKey(null);
    setEditingColIndex(-1);
    setEditError(null);
  };

  const handleBlur = useCallback(() => {
    setTimeout(() => {
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
              <Save size={13} />
              Save {pendingEdits.size} change{pendingEdits.size > 1 ? "s" : ""}
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
                      if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); blurCommitRef.current = false; commitField(); }
                    }}
                    onBlur={handleBlur}
                  />
                  {showQuotes && <span className={editColorClass}>"</span>}
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
                  <JsonValue value={displayValue} depth={1} filterQ={filterText} />
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
