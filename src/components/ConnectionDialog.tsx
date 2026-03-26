import { useMemo, useRef, useState } from "react";
import { useConnectionStore } from "../stores/connectionStore";
import { api, ConnectionConfig } from "../lib/tauri";
import { X, Loader2, CheckCircle, XCircle } from "lucide-react";
import "./ConnectionDialog.css";

const COLORS = [
  "#89b4fa", "#a6e3a1", "#f9e2af", "#f38ba8",
  "#cba6f7", "#89dceb", "#fab387", "#94e2d5",
];

const DB_TYPES = [
  { value: "postgres" as const, label: "PostgreSQL", defaultPort: 5432 },
  { value: "mysql" as const, label: "MySQL", defaultPort: 3306 },
  { value: "sqlite" as const, label: "SQLite", defaultPort: 0 },
];

type ValidationField = "name" | "host" | "port" | "user" | "database";
type ValidationErrors = Partial<Record<ValidationField, string>>;

function normalizeConnectionForm(form: ConnectionConfig): ConnectionConfig {
  return {
    ...form,
    name: form.name.trim(),
    host: form.db_type === "sqlite" ? "" : form.host.trim(),
    user: form.db_type === "sqlite" ? form.user : form.user.trim(),
    database: form.database.trim(),
    group: form.group?.trim() ? form.group.trim() : null,
  };
}

function validateConnectionForm(
  form: ConnectionConfig,
  existingConnections: ConnectionConfig[],
): ValidationErrors {
  const errors: ValidationErrors = {};
  const normalized = normalizeConnectionForm(form);

  if (!normalized.name) {
    errors.name = "Connection name is required.";
  } else {
    const duplicateName = existingConnections.some(
      (conn) =>
        conn.id !== normalized.id &&
        conn.name.trim().toLowerCase() === normalized.name.toLowerCase(),
    );
    if (duplicateName) {
      errors.name = "A connection with this name already exists.";
    }
  }

  if (normalized.db_type === "sqlite") {
    if (!normalized.database) {
      errors.database = "Database file path is required.";
    }
    return errors;
  }

  if (!normalized.host) {
    errors.host = "Host is required.";
  }

  if (!Number.isInteger(normalized.port) || normalized.port < 1 || normalized.port > 65535) {
    errors.port = "Port must be between 1 and 65535.";
  }

  if (!normalized.user) {
    errors.user = "Username is required.";
  }

  if (!normalized.database) {
    errors.database = "Database name is required.";
  }

  return errors;
}

interface Props {
  onClose: () => void;
  editConfig?: ConnectionConfig;
  duplicate?: boolean;
}

export function ConnectionDialog({ onClose, editConfig, duplicate }: Props) {
  const addConnection = useConnectionStore((s) => s.addConnection);
  const updateConnection = useConnectionStore((s) => s.updateConnection);
  const connections = useConnectionStore((s) => s.connections);
  const isEdit = !!editConfig && !duplicate;

  const [form, setForm] = useState<ConnectionConfig>(() => {
    if (editConfig && duplicate) {
      return {
        ...editConfig,
        id: crypto.randomUUID(),
        name: `${editConfig.name} (copy)`,
      };
    }
    return editConfig || {
      id: crypto.randomUUID(),
      name: "",
      db_type: "postgres",
      host: "localhost",
      port: 5432,
      user: "",
      password: "",
      database: "",
      color: COLORS[0],
      ssl: false,
    };
  });

  const [testResult, setTestResult] = useState<"success" | "error" | null>(null);
  const [testError, setTestError] = useState("");
  const [saving, setSaving] = useState(false);
  const [showValidation, setShowValidation] = useState(false);
  const [touched, setTouched] = useState<Partial<Record<ValidationField, boolean>>>({});
  const testingRef = useRef(false);

  const isSqlite = form.db_type === "sqlite";
  const validationErrors = useMemo(
    () => validateConnectionForm(form, connections),
    [form, connections],
  );

  const touchField = (field: ValidationField) => {
    setTouched((prev) => (prev[field] ? prev : { ...prev, [field]: true }));
  };

  const getFieldError = (field: ValidationField) =>
    showValidation || touched[field] ? validationErrors[field] : undefined;

  const updateField = <K extends keyof ConnectionConfig>(field: K, value: ConnectionConfig[K]) => {
    setForm((prev) => ({ ...prev, [field]: value }));
    setTestResult(null);
    setTestError("");
  };

  const validateBeforeSubmit = () => {
    const normalized = normalizeConnectionForm(form);
    setForm(normalized);
    setShowValidation(true);
    const nextErrors = validateConnectionForm(normalized, connections);
    if (Object.keys(nextErrors).length > 0) {
      setTestResult(null);
      setTestError("Please fix the highlighted fields.");
      return null;
    }
    setTestError("");
    return normalized;
  };

  const handleDbTypeChange = (dbType: ConnectionConfig["db_type"]) => {
    const info = DB_TYPES.find((d) => d.value === dbType)!;
    setForm((f) => ({
      ...f,
      db_type: dbType,
      port: info.defaultPort,
      host: dbType === "sqlite" ? "" : f.host || "localhost",
    }));
    setTestResult(null);
    setTestError("");
  };

  const [testing, setTesting] = useState(false);

  const handleTest = async () => {
    if (testingRef.current) return;
    const normalized = validateBeforeSubmit();
    if (!normalized) return;
    testingRef.current = true;
    setTestResult(null);
    setTestError("");
    setTesting(true);
    try {
      await api.testConnection(normalized);
      setTestResult("success");
    } catch (e) {
      setTestResult("error");
      setTestError(String(e));
    } finally {
      testingRef.current = false;
      setTesting(false);
    }
  };

  const handleSave = async () => {
    const normalized = validateBeforeSubmit();
    if (!normalized) return;
    setSaving(true);
    try {
      if (isEdit) {
        await updateConnection(normalized);
      } else {
        await addConnection(normalized);
      }
      onClose();
    } catch (e) {
      setTestError(String(e));
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="dialog-overlay">
      <div className="dialog">
        <div className="dialog-header">
          <h2>{isEdit ? "Edit Connection" : "New Connection"}</h2>
          <button className="btn-icon" onClick={onClose}>
            <X size={18} />
          </button>
        </div>

        <div className="dialog-body">
          <div className="form-group">
            <label>Database Type</label>
            <div className="db-type-selector">
              {DB_TYPES.map((dt) => (
                <button
                  key={dt.value}
                  className={`db-type-btn ${form.db_type === dt.value ? "active" : ""}`}
                  onClick={() => handleDbTypeChange(dt.value)}
                >
                  {dt.label}
                </button>
              ))}
            </div>
          </div>

          <div className="form-row">
            <div className={`form-group flex-1${getFieldError("name") ? " form-group--error" : ""}`}>
              <label>Connection Name</label>
              <input
                value={form.name}
                onChange={(e) => updateField("name", e.target.value)}
                onBlur={() => touchField("name")}
                placeholder="My Database"
                aria-invalid={!!getFieldError("name")}
              />
              {getFieldError("name") && <div className="field-error">{getFieldError("name")}</div>}
            </div>
            <div className="form-group">
              <label>Color</label>
              <div className="color-picker">
                {COLORS.map((c) => (
                  <button
                    key={c}
                    className={`color-dot ${form.color === c ? "active" : ""}`}
                    style={{ background: c }}
                    onClick={() => setForm((f) => ({ ...f, color: c }))}
                  />
                ))}
              </div>
            </div>
          </div>

          {isSqlite ? (
            <div className={`form-group${getFieldError("database") ? " form-group--error" : ""}`}>
              <label>Database File Path</label>
              <input
                value={form.database}
                onChange={(e) => updateField("database", e.target.value)}
                onBlur={() => touchField("database")}
                placeholder="/path/to/database.db"
                aria-invalid={!!getFieldError("database")}
              />
              {getFieldError("database") && (
                <div className="field-error">{getFieldError("database")}</div>
              )}
            </div>
          ) : (
            <>
              <div className="form-row">
                <div className={`form-group flex-1${getFieldError("host") ? " form-group--error" : ""}`}>
                  <label>Host</label>
                  <input
                    value={form.host}
                    onChange={(e) => updateField("host", e.target.value)}
                    onBlur={() => touchField("host")}
                    placeholder="localhost"
                    aria-invalid={!!getFieldError("host")}
                  />
                  {getFieldError("host") && <div className="field-error">{getFieldError("host")}</div>}
                </div>
                <div className={`form-group${getFieldError("port") ? " form-group--error" : ""}`} style={{ width: 100 }}>
                  <label>Port</label>
                  <input
                    type="number"
                    value={form.port}
                    onChange={(e) => updateField("port", parseInt(e.target.value, 10) || 0)}
                    onBlur={() => touchField("port")}
                    min={1}
                    max={65535}
                    aria-invalid={!!getFieldError("port")}
                  />
                  {getFieldError("port") && <div className="field-error">{getFieldError("port")}</div>}
                </div>
              </div>

              <div className="form-row">
                <div className={`form-group flex-1${getFieldError("user") ? " form-group--error" : ""}`}>
                  <label>Username</label>
                  <input
                    value={form.user}
                    onChange={(e) => updateField("user", e.target.value)}
                    onBlur={() => touchField("user")}
                    placeholder="postgres"
                    aria-invalid={!!getFieldError("user")}
                  />
                  {getFieldError("user") && <div className="field-error">{getFieldError("user")}</div>}
                </div>
                <div className="form-group flex-1">
                  <label>Password</label>
                  <input
                    type="password"
                    value={form.password}
                    onChange={(e) => updateField("password", e.target.value)}
                  />
                </div>
              </div>

              <div className={`form-group${getFieldError("database") ? " form-group--error" : ""}`}>
                <label>Database</label>
                <input
                  value={form.database}
                  onChange={(e) => updateField("database", e.target.value)}
                  onBlur={() => touchField("database")}
                  placeholder="mydb"
                  aria-invalid={!!getFieldError("database")}
                />
                {getFieldError("database") && (
                  <div className="field-error">{getFieldError("database")}</div>
                )}
              </div>
            </>
          )}

          <div className="form-group">
            <label>Group (optional)</label>
            <input
              value={form.group || ""}
              onChange={(e) => updateField("group", e.target.value || null)}
              placeholder="e.g. Production, Development"
            />
          </div>

          {testError && <div className="connection-form-error">{testError}</div>}

        </div>

        <div className="dialog-footer">
          <button
            className={`btn-test-conn ${testing ? "btn-test-conn--testing" : ""} ${!testing && testResult === "success" ? "btn-test-conn--success" : ""} ${!testing && testResult === "error" ? "btn-test-conn--error" : ""}`}
            onClick={handleTest}
          >
            {testing ? (
              <><Loader2 size={14} className="spin" /> Testing…</>
            ) : testResult === "success" ? (
              <><CheckCircle size={14} /> Connected</>
            ) : testResult === "error" ? (
              <><XCircle size={14} /> Failed</>
            ) : (
              "Test Connection"
            )}
          </button>
          <div className="dialog-footer-right">
            <button className="btn-secondary" onClick={onClose}>
              Cancel
            </button>
            <button className="btn-primary" onClick={handleSave} disabled={saving}>
              {saving ? <Loader2 size={14} className="spin" /> : null}
              {isEdit ? "Save Changes" : "Save"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
