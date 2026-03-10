import { useState } from "react";
import { Plus, X, Play } from "lucide-react";
import { ColumnInfo } from "../../lib/tauri";
import "./FilterBar.css";

interface FilterCondition {
  id: string;
  column: string;
  operator: string;
  value: string;
}

interface Props {
  columns: ColumnInfo[];
  onApply: (filter: string | null) => void;
  onClose: () => void;
}

const OPERATORS = [
  { value: "=", label: "=" },
  { value: "!=", label: "!=" },
  { value: ">", label: ">" },
  { value: "<", label: "<" },
  { value: ">=", label: ">=" },
  { value: "<=", label: "<=" },
  { value: "LIKE", label: "LIKE" },
  { value: "ILIKE", label: "ILIKE" },
  { value: "IS NULL", label: "IS NULL" },
  { value: "IS NOT NULL", label: "IS NOT NULL" },
];

const NO_VALUE_OPS = ["IS NULL", "IS NOT NULL"];

export function FilterBar({ columns, onApply, onClose }: Props) {
  const [conditions, setConditions] = useState<FilterCondition[]>([
    { id: "1", column: columns[0]?.name || "", operator: "=", value: "" },
  ]);

  const addCondition = () => {
    setConditions((prev) => [
      ...prev,
      {
        id: String(Date.now()),
        column: columns[0]?.name || "",
        operator: "=",
        value: "",
      },
    ]);
  };

  const removeCondition = (id: string) => {
    setConditions((prev) => {
      const next = prev.filter((c) => c.id !== id);
      return next.length === 0
        ? [{ id: "1", column: columns[0]?.name || "", operator: "=", value: "" }]
        : next;
    });
  };

  const updateCondition = (
    id: string,
    field: keyof FilterCondition,
    value: string
  ) => {
    setConditions((prev) =>
      prev.map((c) => (c.id === id ? { ...c, [field]: value } : c))
    );
  };

  const buildWhereClause = (): string | null => {
    const clauses = conditions
      .filter((c) => {
        if (NO_VALUE_OPS.includes(c.operator)) return c.column;
        return c.column && c.value;
      })
      .map((c) => {
        const col = `"${c.column.replace(/"/g, '""')}"`;
        if (NO_VALUE_OPS.includes(c.operator)) {
          return `${col} ${c.operator}`;
        }
        if (c.operator === "LIKE" || c.operator === "ILIKE") {
          return `${col} ${c.operator} '${c.value.replace(/'/g, "''")}'`;
        }
        const colInfo = columns.find((ci) => ci.name === c.column);
        const isNumeric = colInfo && /int|float|double|decimal|numeric|real|serial/i.test(colInfo.data_type);
        if (isNumeric && !isNaN(Number(c.value))) {
          return `${col} ${c.operator} ${c.value}`;
        }
        return `${col} ${c.operator} '${c.value.replace(/'/g, "''")}'`;
      });

    return clauses.length > 0 ? clauses.join(" AND ") : null;
  };

  const handleApply = () => {
    onApply(buildWhereClause());
  };

  const handleClear = () => {
    setConditions([
      { id: "1", column: columns[0]?.name || "", operator: "=", value: "" },
    ]);
    onApply(null);
  };

  return (
    <div className="filter-bar">
      <div className="filter-conditions">
        {conditions.map((cond, idx) => (
          <div key={cond.id} className="filter-condition">
            {idx > 0 && <span className="filter-and">AND</span>}
            <select
              value={cond.column}
              onChange={(e) => updateCondition(cond.id, "column", e.target.value)}
            >
              {columns.map((col) => (
                <option key={col.name} value={col.name}>
                  {col.name}
                </option>
              ))}
            </select>
            <select
              value={cond.operator}
              onChange={(e) =>
                updateCondition(cond.id, "operator", e.target.value)
              }
            >
              {OPERATORS.map((op) => (
                <option key={op.value} value={op.value}>
                  {op.label}
                </option>
              ))}
            </select>
            {!NO_VALUE_OPS.includes(cond.operator) && (
              <input
                placeholder="Value..."
                value={cond.value}
                onChange={(e) =>
                  updateCondition(cond.id, "value", e.target.value)
                }
                onKeyDown={(e) => e.key === "Enter" && handleApply()}
              />
            )}
            <button
              className="btn-icon"
              onClick={() => removeCondition(cond.id)}
              title="Remove condition"
            >
              <X size={12} />
            </button>
          </div>
        ))}
      </div>
      <div className="filter-actions">
        <button className="btn-ghost" onClick={addCondition}>
          <Plus size={12} /> Add
        </button>
        <button className="btn-primary" onClick={handleApply} style={{ height: 26, fontSize: 12 }}>
          <Play size={12} /> Apply
        </button>
        <button className="btn-ghost" onClick={handleClear}>
          Clear
        </button>
        <button className="btn-icon" onClick={onClose} title="Close filter bar">
          <X size={14} />
        </button>
      </div>
    </div>
  );
}
