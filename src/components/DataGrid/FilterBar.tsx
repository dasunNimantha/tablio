import { useState } from "react";
import { Plus, X, Play } from "lucide-react";
import { ColumnInfo } from "../../lib/tauri";
import { buildWhereClause as buildWhereClauseFromConditions, NO_VALUE_OPS, type FilterCondition } from "../../lib/filterBuilder";
import { CustomSelect } from "../CustomSelect/CustomSelect";
import "./FilterBar.css";

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

export function FilterBar({ columns, onApply, onClose }: Props) {
  const [conditions, setConditions] = useState<FilterCondition[]>([
    { id: "1", column: columns[0]?.name || "", operator: "=", value: "", join: "AND" },
  ]);

  const addCondition = () => {
    setConditions((prev) => [
      ...prev,
      {
        id: String(Date.now()),
        column: columns[0]?.name || "",
        operator: "=",
        value: "",
        join: "AND",
      },
    ]);
  };

  const removeCondition = (id: string) => {
    setConditions((prev) => {
      const next = prev.filter((c) => c.id !== id);
      return next.length === 0
        ? [{ id: "1", column: columns[0]?.name || "", operator: "=", value: "", join: "AND" }]
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

  const handleApply = () => {
    onApply(buildWhereClauseFromConditions(conditions, columns));
  };

  const handleClear = () => {
    setConditions([
      { id: "1", column: columns[0]?.name || "", operator: "=", value: "", join: "AND" },
    ]);
    onApply(null);
  };

  return (
    <div className="filter-bar">
      <div className="filter-conditions">
        {conditions.map((cond, idx) => (
          <div key={cond.id} className="filter-condition">
            {idx > 0 && (
              <button
                className="filter-join-btn"
                onClick={() =>
                  updateCondition(cond.id, "join", cond.join === "AND" ? "OR" : "AND")
                }
                title="Click to toggle AND/OR"
              >
                {cond.join}
              </button>
            )}
            <CustomSelect
              value={cond.column}
              options={columns.map((col) => ({ value: col.name, label: col.name }))}
              onChange={(v) => updateCondition(cond.id, "column", v)}
              searchable
            />
            <CustomSelect
              value={cond.operator}
              options={OPERATORS}
              onChange={(v) => updateCondition(cond.id, "operator", v)}
            />
            {!NO_VALUE_OPS.includes(cond.operator) && (
              <input
                className="filter-value-input"
                placeholder="Value..."
                value={cond.value}
                onChange={(e) =>
                  updateCondition(cond.id, "value", e.target.value)
                }
                onKeyDown={(e) => e.key === "Enter" && handleApply()}
              />
            )}
            {conditions.length > 1 && (
              <button
                className="btn-icon"
                onClick={() => removeCondition(cond.id)}
                title="Remove condition"
              >
                <X size={12} />
              </button>
            )}
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
