import { useState, useRef, useEffect } from "react";
import { api, ColumnDefinition } from "../../lib/tauri";
import { X, Plus, Trash2, Loader2, Eye, EyeOff } from "lucide-react";
import "./CreateTableDialog.css";

interface Props {
  connectionId: string;
  database: string;
  schema: string;
  onClose: () => void;
  onCreated: () => void;
}

const PG_TYPES = [
  "integer", "bigint", "smallint", "serial", "bigserial",
  "text", "varchar(255)", "char(1)",
  "boolean",
  "timestamp", "timestamptz", "date", "time",
  "numeric", "real", "double precision",
  "uuid", "json", "jsonb",
  "bytea",
];

interface ColumnRow {
  name: string;
  data_type: string;
  is_nullable: boolean;
  is_primary_key: boolean;
  default_value: string;
}

const emptyColumn = (): ColumnRow => ({
  name: "",
  data_type: "text",
  is_nullable: true,
  is_primary_key: false,
  default_value: "",
});

export function CreateTableDialog({
  connectionId,
  database,
  schema,
  onClose,
  onCreated,
}: Props) {
  const [tableName, setTableName] = useState("");
  const [columns, setColumns] = useState<ColumnRow[]>([
    { name: "id", data_type: "serial", is_nullable: false, is_primary_key: true, default_value: "" },
    emptyColumn(),
  ]);
  const [showPreview, setShowPreview] = useState(false);
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const dialogRef = useRef<HTMLDivElement>(null);
  const previewRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (showPreview) {
      const frame = window.requestAnimationFrame(() => {
        const dialog = dialogRef.current;
        const preview = previewRef.current;
        if (!dialog || !preview) return;
        dialog.scrollTo({
          top: Math.max(0, preview.offsetTop - 16),
          behavior: "smooth",
        });
      });
      return () => window.cancelAnimationFrame(frame);
    }
  }, [showPreview]);

  const updateColumn = (idx: number, field: keyof ColumnRow, value: unknown) => {
    setColumns((prev) => {
      const next = [...prev];
      next[idx] = { ...next[idx], [field]: value };
      return next;
    });
  };

  const addColumn = () => setColumns((prev) => [...prev, emptyColumn()]);

  const removeColumn = (idx: number) => {
    if (columns.length <= 1) return;
    setColumns((prev) => prev.filter((_, i) => i !== idx));
  };

  const validColumns = columns.filter((c) => c.name.trim() !== "");

  const generateDDL = (): string => {
    const pkCols = validColumns.filter((c) => c.is_primary_key);
    const colDefs = validColumns.map((c) => {
      let def = `    "${c.name}" ${c.data_type}`;
      if (!c.is_nullable) def += " NOT NULL";
      if (c.default_value) def += ` DEFAULT ${c.default_value}`;
      return def;
    });
    if (pkCols.length > 0) {
      colDefs.push(
        `    PRIMARY KEY (${pkCols.map((c) => `"${c.name}"`).join(", ")})`
      );
    }
    return `CREATE TABLE "${schema}"."${tableName}" (\n${colDefs.join(",\n")}\n);`;
  };

  const handleCreate = async () => {
    if (!tableName.trim()) {
      setError("Table name is required");
      return;
    }
    if (validColumns.length === 0) {
      setError("At least one column is required");
      return;
    }

    setCreating(true);
    setError(null);
    try {
      const colDefs: ColumnDefinition[] = validColumns.map((c) => ({
        name: c.name,
        data_type: c.data_type,
        is_nullable: c.is_nullable,
        is_primary_key: c.is_primary_key,
        default_value: c.default_value || null,
      }));

      await api.createTable({
        connection_id: connectionId,
        database,
        schema,
        table_name: tableName,
        columns: colDefs,
      });

      onCreated();
      onClose();
    } catch (e) {
      setError(String(e));
    } finally {
      setCreating(false);
    }
  };

  return (
    <div className="dialog-overlay" onClick={onClose}>
      <div
        className="dialog create-table-dialog"
        ref={dialogRef}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="dialog-header">
          <h2>Create Table</h2>
          <button className="btn-icon" onClick={onClose}>
            <X size={18} />
          </button>
        </div>

        <div className="dialog-body">
          <div className="form-row">
            <div className="form-group flex-1">
              <label>Schema</label>
              <input value={`${database}.${schema}`} disabled />
            </div>
            <div className="form-group flex-1">
              <label>Table Name</label>
              <input
                value={tableName}
                onChange={(e) => setTableName(e.target.value)}
                placeholder="my_table"
                autoFocus
              />
            </div>
          </div>

          <div className="form-group">
            <label>Columns</label>
            <div className="create-table-columns">
              <div className="create-table-columns-header">
                <span style={{ flex: 2 }}>Name</span>
                <span style={{ flex: 2 }}>Type</span>
                <span style={{ width: 60, textAlign: "center" }}>Nullable</span>
                <span style={{ width: 40, textAlign: "center" }}>PK</span>
                <span style={{ flex: 1 }}>Default</span>
                <span style={{ width: 32 }}></span>
              </div>
              {columns.map((col, idx) => (
                <div key={idx} className="create-table-column-row">
                  <input
                    style={{ flex: 2 }}
                    value={col.name}
                    onChange={(e) => updateColumn(idx, "name", e.target.value)}
                    placeholder="column_name"
                  />
                  <select
                    style={{ flex: 2 }}
                    value={col.data_type}
                    onChange={(e) => updateColumn(idx, "data_type", e.target.value)}
                  >
                    {PG_TYPES.map((t) => (
                      <option key={t} value={t}>
                        {t}
                      </option>
                    ))}
                  </select>
                  <div style={{ width: 60, display: "flex", justifyContent: "center" }}>
                    <input
                      type="checkbox"
                      checked={col.is_nullable}
                      onChange={(e) => updateColumn(idx, "is_nullable", e.target.checked)}
                    />
                  </div>
                  <div style={{ width: 40, display: "flex", justifyContent: "center" }}>
                    <input
                      type="checkbox"
                      checked={col.is_primary_key}
                      onChange={(e) =>
                        updateColumn(idx, "is_primary_key", e.target.checked)
                      }
                    />
                  </div>
                  <input
                    style={{ flex: 1 }}
                    value={col.default_value}
                    onChange={(e) => updateColumn(idx, "default_value", e.target.value)}
                    placeholder="default"
                  />
                  <button
                    className="btn-icon"
                    onClick={() => removeColumn(idx)}
                    disabled={columns.length <= 1}
                  >
                    <Trash2 size={12} />
                  </button>
                </div>
              ))}
              <button
                className="btn-ghost add-column-btn"
                onClick={addColumn}
              >
                <Plus size={14} /> Add Column
              </button>
            </div>
          </div>

          {showPreview && (
            <div className="ddl-preview" ref={previewRef}>
              <pre>{generateDDL()}</pre>
            </div>
          )}

          {error && (
            <div className="test-result error">
              {error}
            </div>
          )}
        </div>

        <div className="dialog-footer">
          <button
            className={`btn-ghost ${showPreview ? "active-filter" : ""}`}
            onClick={() => setShowPreview(!showPreview)}
          >
            {showPreview ? <EyeOff size={14} /> : <Eye size={14} />}
            {showPreview ? "Hide SQL" : "Preview SQL"}
          </button>
          <div className="dialog-footer-right">
            <button className="btn-secondary" onClick={onClose}>
              Cancel
            </button>
            <button
              className="btn-primary"
              onClick={handleCreate}
              disabled={creating}
            >
              {creating && <Loader2 size={14} className="spin" />}
              Create Table
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
