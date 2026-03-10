import { useState, useRef, useEffect } from "react";
import { ColumnInfo } from "../../lib/tauri";
import "./DataGrid.css";

interface Props {
  value: unknown;
  column: ColumnInfo;
  isModified: boolean;
  isInserted: boolean;
  onChange: (newValue: unknown) => void;
}

export function EditableCell({
  value,
  column,
  isModified,
  isInserted,
  onChange,
}: Props) {
  const [editing, setEditing] = useState(false);
  const [editValue, setEditValue] = useState("");
  const inputRef = useRef<HTMLInputElement>(null);

  const isNull = value === null || value === undefined;
  const displayValue = isNull ? "NULL" : String(value);

  const handleDoubleClick = () => {
    setEditing(true);
    setEditValue(isNull ? "" : String(value));
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") {
      commitEdit();
    } else if (e.key === "Escape") {
      setEditing(false);
    } else if (e.key === "Tab") {
      e.preventDefault();
      commitEdit();
    }
  };

  const commitEdit = () => {
    setEditing(false);
    if (editValue === "" && column.is_nullable) {
      onChange(null);
      return;
    }
    const parsed = parseValue(editValue, column.data_type);
    onChange(parsed);
  };

  useEffect(() => {
    if (editing && inputRef.current) {
      inputRef.current.focus();
      inputRef.current.select();
    }
  }, [editing]);

  const cellClass = [
    "grid-cell",
    isNull ? "cell-null" : "",
    isModified ? "cell-modified" : "",
    isInserted ? "cell-inserted" : "",
    editing ? "cell-editing" : "",
  ]
    .filter(Boolean)
    .join(" ");

  if (editing) {
    return (
      <td className={cellClass}>
        <input
          ref={inputRef}
          className="cell-input"
          value={editValue}
          onChange={(e) => setEditValue(e.target.value)}
          onBlur={commitEdit}
          onKeyDown={handleKeyDown}
        />
      </td>
    );
  }

  return (
    <td className={cellClass} onDoubleClick={handleDoubleClick}>
      <span className="cell-value">{displayValue}</span>
    </td>
  );
}

function parseValue(raw: string, dataType: string): unknown {
  const t = dataType.toLowerCase();
  if (
    t.includes("int") ||
    t === "serial" ||
    t === "bigserial" ||
    t === "smallserial"
  ) {
    const n = parseInt(raw, 10);
    return isNaN(n) ? raw : n;
  }
  if (
    t.includes("float") ||
    t.includes("double") ||
    t.includes("decimal") ||
    t.includes("numeric") ||
    t === "real"
  ) {
    const n = parseFloat(raw);
    return isNaN(n) ? raw : n;
  }
  if (t === "boolean" || t === "bool") {
    return raw.toLowerCase() === "true" || raw === "1";
  }
  return raw;
}
