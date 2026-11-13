import { useState, useRef, useEffect, useId, useMemo } from "react";
import { api, ColumnDefinition } from "../../lib/tauri";
import { useConnectionStore } from "../../stores/connectionStore";
import {
  buildCreateTableSql,
  columnTypesFor,
  defaultColumnType,
  defaultIdType,
  supportsNullability,
  validateColumnNames,
} from "../../lib/columnTypes";
import { Modal } from "../Modal/Modal";
import { X, Plus, Trash2, Loader2, Eye, EyeOff } from "lucide-react";
import "./CreateTableDialog.css";

interface Props {
  connectionId: string;
  database: string;
  schema: string;
  onClose: () => void;
  onCreated: () => void;
}

interface ColumnRow {
  name: string;
  data_type: string;
  is_nullable: boolean;
  is_primary_key: boolean;
  default_value: string;
}

const emptyColumn = (dataType: string): ColumnRow => ({
  name: "",
  data_type: dataType,
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
  const dbType = useConnectionStore(
    (s) => s.connections.find((c) => c.id === connectionId)?.db_type,
  );
  const typeOptions = columnTypesFor(dbType);
  const newColumnType = defaultColumnType(dbType);
  const showNullability = supportsNullability(dbType);

  const initialColumns = useMemo<ColumnRow[]>(
    () => [
      {
        name: "id",
        data_type: defaultIdType(dbType),
        is_nullable: false,
        is_primary_key: true,
        default_value: "",
      },
      emptyColumn(defaultColumnType(dbType)),
    ],
    [dbType],
  );

  const [tableName, setTableName] = useState("");
  const [columns, setColumns] = useState<ColumnRow[]>(initialColumns);
  const [showPreview, setShowPreview] = useState(false);
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [submitted, setSubmitted] = useState(false);
  const previewRef = useRef<HTMLDivElement>(null);
  const tableNameRef = useRef<HTMLInputElement>(null);
  const titleId = useId();
  const idBase = useId();

  const dirty =
    tableName.trim() !== "" || JSON.stringify(columns) !== JSON.stringify(initialColumns);

  useEffect(() => {
    if (showPreview) {
      const frame = window.requestAnimationFrame(() => {
        const preview = previewRef.current;
        const scroller = preview?.closest(".create-table-dialog");
        if (!preview || !scroller) return;
        scroller.scrollTo({
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

  const addColumn = () => setColumns((prev) => [...prev, emptyColumn(newColumnType)]);

  const removeColumn = (idx: number) => {
    if (columns.length <= 1) return;
    setColumns((prev) => prev.filter((_, i) => i !== idx));
  };

  // A row with no name that the user never touched is skipped (the dialog
  // starts with one). A row with a type/default/PK set but no name is an
  // error: dropping it silently would lose the user's input.
  const isPristineBlank = (c: ColumnRow) =>
    c.name.trim() === "" &&
    c.data_type === newColumnType &&
    !c.default_value.trim() &&
    !c.is_primary_key;
  const unnamed = columns
    .map((c, i) => (c.name.trim() === "" && !isPristineBlank(c) ? i : -1))
    .filter((i) => i >= 0);
  const { duplicates, message: duplicateMessage } = validateColumnNames(columns);
  const columnsError =
    duplicateMessage ??
    (unnamed.length > 0
      ? `Column ${unnamed.map((i) => i + 1).join(", ")} ${unnamed.length > 1 ? "have" : "has"} no name — name ${unnamed.length > 1 ? "them" : "it"} or remove the row`
      : null);
  const columnsErrorId = `${idBase}-columns-error`;

  const validColumns = columns.filter((c) => c.name.trim() !== "");

  const handleCreate = async () => {
    setSubmitted(true);
    if (!tableName.trim()) {
      setError("Table name is required");
      tableNameRef.current?.focus();
      return;
    }
    if (columnsError) {
      setError(null);
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
        name: c.name.trim(),
        data_type: c.data_type,
        is_nullable: c.is_nullable,
        is_primary_key: c.is_primary_key,
        default_value: c.default_value || null,
      }));

      await api.createTable({
        connection_id: connectionId,
        database,
        schema,
        table_name: tableName.trim(),
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

  const typeList = (current: string) =>
    typeOptions.includes(current) ? typeOptions : [current, ...typeOptions];

  return (
    <Modal
      onClose={onClose}
      className="dialog create-table-dialog"
      labelledBy={titleId}
      busy={creating}
      closeOnOverlayClick={!dirty}
      initialFocusRef={tableNameRef}
    >
      <div className="dialog-header">
        <h2 id={titleId}>Create Table</h2>
        <button className="btn-icon" onClick={onClose} aria-label="Close" title="Close">
          <X size={18} />
        </button>
      </div>

      <div className="dialog-body">
        <div className="form-row">
          <div className="form-group flex-1">
            <label htmlFor={`${idBase}-schema`}>Schema</label>
            <input id={`${idBase}-schema`} value={`${database}.${schema}`} disabled />
          </div>
          <div className="form-group flex-1">
            <label htmlFor={`${idBase}-table`}>Table Name</label>
            <input
              id={`${idBase}-table`}
              ref={tableNameRef}
              value={tableName}
              onChange={(e) => setTableName(e.target.value)}
              placeholder="my_table"
              aria-invalid={submitted && !tableName.trim() ? true : undefined}
            />
          </div>
        </div>

        <div className="form-group">
          <span className="form-group-label" id={`${idBase}-columns`}>Columns</span>
          <div
            className="create-table-columns"
            role="group"
            aria-labelledby={`${idBase}-columns`}
            aria-describedby={columnsError ? columnsErrorId : undefined}
          >
            <div className="create-table-columns-header" aria-hidden="true">
              <span style={{ flex: 2 }}>Name</span>
              <span style={{ flex: 2 }}>Type</span>
              {showNullability && (
                <span style={{ width: 60, textAlign: "center" }}>Nullable</span>
              )}
              <span style={{ width: 40, textAlign: "center" }}>PK</span>
              {showNullability && <span style={{ flex: 1 }}>Default</span>}
              <span style={{ width: 32 }}></span>
            </div>
            {columns.map((col, idx) => {
              const label = col.name.trim() || `column ${idx + 1}`;
              const invalid = duplicates.has(idx) || unnamed.includes(idx);
              return (
                <div key={idx} className="create-table-column-row">
                  <input
                    style={{ flex: 2 }}
                    value={col.name}
                    onChange={(e) => updateColumn(idx, "name", e.target.value)}
                    placeholder="column_name"
                    aria-label={`Column ${idx + 1} name`}
                    aria-invalid={invalid || undefined}
                    className={invalid ? "input-invalid" : undefined}
                  />
                  <select
                    style={{ flex: 2 }}
                    value={col.data_type}
                    onChange={(e) => updateColumn(idx, "data_type", e.target.value)}
                    aria-label={`Type of ${label}`}
                  >
                    {typeList(col.data_type).map((t) => (
                      <option key={t} value={t}>
                        {t}
                      </option>
                    ))}
                  </select>
                  {showNullability && (
                    <div style={{ width: 60, display: "flex", justifyContent: "center" }}>
                      <input
                        type="checkbox"
                        checked={col.is_nullable}
                        onChange={(e) => updateColumn(idx, "is_nullable", e.target.checked)}
                        aria-label={`${label} nullable`}
                      />
                    </div>
                  )}
                  <div style={{ width: 40, display: "flex", justifyContent: "center" }}>
                    <input
                      type="checkbox"
                      checked={col.is_primary_key}
                      onChange={(e) => updateColumn(idx, "is_primary_key", e.target.checked)}
                      aria-label={`${label} primary key`}
                    />
                  </div>
                  {showNullability && (
                    <input
                      style={{ flex: 1 }}
                      value={col.default_value}
                      onChange={(e) => updateColumn(idx, "default_value", e.target.value)}
                      placeholder="default"
                      aria-label={`${label} default value`}
                    />
                  )}
                  <button
                    className="btn-icon"
                    onClick={() => removeColumn(idx)}
                    disabled={columns.length <= 1}
                    aria-label={`Remove column ${label}`}
                    title={`Remove column ${label}`}
                  >
                    <Trash2 size={12} />
                  </button>
                </div>
              );
            })}
            <button className="btn-ghost add-column-btn" onClick={addColumn}>
              <Plus size={14} /> Add Column
            </button>
          </div>
          {columnsError && (
            <div
              id={columnsErrorId}
              className="create-table-columns-error"
              role={submitted ? "alert" : undefined}
            >
              {columnsError}
            </div>
          )}
        </div>

        {showPreview && (
          <div className="ddl-preview" ref={previewRef}>
            <pre>{buildCreateTableSql(dbType, database, schema, tableName, validColumns)}</pre>
          </div>
        )}

        {error && (
          <div className="test-result error" role="alert">
            {error}
          </div>
        )}
      </div>

      <div className="dialog-footer">
        <button
          className={`btn-ghost ${showPreview ? "active-filter" : ""}`}
          onClick={() => setShowPreview(!showPreview)}
          aria-pressed={showPreview}
        >
          {showPreview ? <EyeOff size={14} /> : <Eye size={14} />}
          {showPreview ? "Hide SQL" : "Preview SQL"}
        </button>
        <div className="dialog-footer-right">
          <button className="btn-secondary" onClick={onClose} disabled={creating}>
            Cancel
          </button>
          <button
            className="btn-primary"
            onClick={handleCreate}
            disabled={creating || !!columnsError}
          >
            {creating && <Loader2 size={14} className="spin" />}
            Create Table
          </button>
        </div>
      </div>
    </Modal>
  );
}
