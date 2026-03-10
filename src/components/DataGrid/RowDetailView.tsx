import { useState, useRef, useEffect, useMemo } from "react";
import { X, ChevronDown, ChevronRight, Copy, Check, Pencil } from "lucide-react";
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

interface EditState {
  colIndex: number;
  colName: string;
  value: string;
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
  const idx = text.toLowerCase().indexOf(query.toLowerCase());
  if (idx === -1) return text;
  return (
    <>
      {text.slice(0, idx)}
      <mark className="json-highlight">{text.slice(idx, idx + query.length)}</mark>
      {text.slice(idx + query.length)}
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
  const [editState, setEditState] = useState<EditState | null>(null);
  const [copied, setCopied] = useState(false);
  const editRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    if (editState && editRef.current) {
      editRef.current.focus();
    }
  }, [editState]);

  const rowObj = useMemo(() => {
    const entries: { key: string; value: unknown; colIndex: number; col: ColumnInfo }[] = [];
    columns.forEach((col, i) => {
      entries.push({ key: col.name, value: row[i], colIndex: i, col });
    });
    return entries;
  }, [columns, row]);

  const filtered = useMemo(() => {
    if (!filterText) return rowObj;
    const q = filterText.toLowerCase();
    return rowObj.filter(({ key, value }) => {
      if (key.toLowerCase().includes(q)) return true;
      if (value === null || value === undefined) return "null".includes(q);
      const str = typeof value === "object" ? JSON.stringify(value) : String(value);
      return str.toLowerCase().includes(q);
    });
  }, [rowObj, filterText]);

  const handleCopyAll = () => {
    const obj: Record<string, unknown> = {};
    columns.forEach((col, i) => { obj[col.name] = row[i]; });
    navigator.clipboard.writeText(JSON.stringify(obj, null, 2));
    setCopied(true);
    setTimeout(() => setCopied(false), 1500);
  };

  const handleStartEdit = (colIndex: number, colName: string, value: unknown) => {
    const str = value === null || value === undefined
      ? ""
      : typeof value === "object"
        ? JSON.stringify(value, null, 2)
        : String(value);
    setEditState({ colIndex, colName, value: str });
  };

  const handleSaveEdit = () => {
    if (!editState) return;
    const v = editState.value.trim();
    const col = columns[editState.colIndex];
    if (v === "" && col.is_nullable) {
      onCellChange(editState.colIndex, null);
    } else {
      onCellChange(editState.colIndex, v);
    }
    setEditState(null);
  };

  const formatValueStr = (value: unknown): string => {
    if (value === null || value === undefined) return "null";
    if (typeof value === "object") return JSON.stringify(value);
    return String(value);
  };

  return (
    <div className="row-detail-panel">
      <div className="row-detail-header">
        <h2>JSON VIEWER</h2>
        <div className="row-detail-header-actions">
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
          placeholder="Filter keys by text or /regex/"
          value={filterText}
          onChange={(e) => setFilterText(e.target.value)}
        />
      </div>

      {editState && (
        <div className="row-detail-edit-bar">
          <div className="row-detail-edit-label">
            Editing <strong>{editState.colName}</strong>
          </div>
          <textarea
            ref={editRef}
            className="row-detail-edit-textarea"
            value={editState.value}
            onChange={(e) => setEditState({ ...editState, value: e.target.value })}
            onKeyDown={(e) => {
              if (e.key === "Escape") setEditState(null);
              if (e.key === "Enter" && e.ctrlKey) { e.preventDefault(); handleSaveEdit(); }
            }}
            rows={Math.min(Math.max(editState.value.split("\n").length, 2), 8)}
          />
          <div className="row-detail-edit-actions">
            <button className="btn-primary" onClick={handleSaveEdit} style={{ height: 28, fontSize: 12 }}>Save</button>
            <button className="btn-ghost" onClick={() => setEditState(null)} style={{ height: 28, fontSize: 12 }}>Cancel</button>
            <span className="row-detail-edit-hint">Ctrl+Enter</span>
          </div>
        </div>
      )}

      <div className="row-detail-body">
        <div className="json-tree">
          <div className="json-line">
            <span className="json-bracket">{"{"}</span>
          </div>
          {filtered.map(({ key, value, colIndex, col }, i) => {
            const valStr = formatValueStr(value);
            const isLast = i === filtered.length - 1;

            return (
              <div key={key} className="json-row" style={{ paddingLeft: 18 }}>
                <span className="json-key">
                  "{filterText ? highlightMatch(key, filterText) : key}"
                </span>
                <span className="json-colon">: </span>
                <span className="json-value-inline">
                  <JsonValue value={value} depth={1} filterQ={filterText} />
                </span>
                {!isLast && <span className="json-comma">,</span>}
                <span className="json-row-actions">
                  <button
                    className="btn-icon json-edit-btn"
                    onClick={() => handleStartEdit(colIndex, key, value)}
                    title={`Edit ${key}`}
                  >
                    <Pencil size={11} />
                  </button>
                </span>
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
