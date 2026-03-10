import { useState } from "react";
import { Plus, X, Play } from "lucide-react";
import { ColumnInfo } from "../../lib/tauri";
import { CustomSelect } from "../CustomSelect/CustomSelect";
import "./FilterBar.css";

type JoinType = "AND" | "OR";

interface FilterCondition {
  id: string;
  column: string;
  operator: string;
  value: string;
  join: JoinType;
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

  const buildWhereClause = (): string | null => {
    const valid = conditions.filter((c) => {
      if (NO_VALUE_OPS.includes(c.operator)) return c.column;
      return c.column && c.value;
    });

    if (valid.length === 0) return null;

    const toClause = (c: FilterCondition): string => {
      const col = `"${c.column.replace(/"/g, '""')}"`;
      if (NO_VALUE_OPS.includes(c.operator)) {
        return `${col} ${c.operator}`;
      }
      if (c.operator === "LIKE" || c.operator === "ILIKE") {
        return `${col} ${c.operator} '${c.value.replace(/'/g, "''")}'`;
      }
      const colInfo = columns.find((ci) => ci.name === c.column);
      const isNum = colInfo && /int|float|double|decimal|numeric|real|serial/i.test(colInfo.data_type);
      if (isNum && !isNaN(Number(c.value))) {
        return `${col} ${c.operator} ${c.value}`;
      }
      return `${col} ${c.operator} '${c.value.replace(/'/g, "''")}'`;
    };

    if (valid.length === 1) return toClause(valid[0]);

    const groups: { join: JoinType; clauses: string[] }[] = [];
    let currentGroup: { join: JoinType; clauses: string[] } = { join: "AND", clauses: [toClause(valid[0])] };

    for (let i = 1; i < valid.length; i++) {
      if (valid[i].join === currentGroup.join) {
        currentGroup.clauses.push(toClause(valid[i]));
      } else {
        groups.push(currentGroup);
        currentGroup = { join: valid[i].join, clauses: [toClause(valid[i])] };
      }
    }
    groups.push(currentGroup);

    if (groups.length === 1) {
      return groups[0].clauses.join(` ${groups[0].join} `);
    }

    let result = groups[0].clauses.length > 1
      ? `(${groups[0].clauses.join(` ${groups[0].join} `)})`
      : groups[0].clauses[0];

    for (let i = 1; i < groups.length; i++) {
      const g = groups[i];
      const part = g.clauses.length > 1
        ? `(${g.clauses.join(` ${g.join} `)})`
        : g.clauses[0];
      result = `${result} ${g.join} ${part}`;
    }

    return result;
  };

  const handleApply = () => {
    onApply(buildWhereClause());
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
