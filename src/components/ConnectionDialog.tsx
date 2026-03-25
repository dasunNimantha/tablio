import { useRef, useState } from "react";
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

interface Props {
  onClose: () => void;
  editConfig?: ConnectionConfig;
  duplicate?: boolean;
}

export function ConnectionDialog({ onClose, editConfig, duplicate }: Props) {
  const addConnection = useConnectionStore((s) => s.addConnection);
  const updateConnection = useConnectionStore((s) => s.updateConnection);
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
  const testingRef = useRef(false);

  const isSqlite = form.db_type === "sqlite";

  const handleDbTypeChange = (dbType: ConnectionConfig["db_type"]) => {
    const info = DB_TYPES.find((d) => d.value === dbType)!;
    setForm((f) => ({
      ...f,
      db_type: dbType,
      port: info.defaultPort,
      host: dbType === "sqlite" ? "" : f.host || "localhost",
    }));
  };

  const [testing, setTesting] = useState(false);

  const handleTest = async () => {
    if (testingRef.current) return;
    testingRef.current = true;
    setTestResult(null);
    setTestError("");
    setTesting(true);
    try {
      await api.testConnection(form);
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
    setSaving(true);
    try {
      if (isEdit) {
        await updateConnection(form);
      } else {
        await addConnection(form);
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
            <div className="form-group flex-1">
              <label>Connection Name</label>
              <input
                value={form.name}
                onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))}
                placeholder="My Database"
              />
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
            <div className="form-group">
              <label>Database File Path</label>
              <input
                value={form.database}
                onChange={(e) => setForm((f) => ({ ...f, database: e.target.value }))}
                placeholder="/path/to/database.db"
              />
            </div>
          ) : (
            <>
              <div className="form-row">
                <div className="form-group flex-1">
                  <label>Host</label>
                  <input
                    value={form.host}
                    onChange={(e) => setForm((f) => ({ ...f, host: e.target.value }))}
                    placeholder="localhost"
                  />
                </div>
                <div className="form-group" style={{ width: 100 }}>
                  <label>Port</label>
                  <input
                    type="number"
                    value={form.port}
                    onChange={(e) => setForm((f) => ({ ...f, port: parseInt(e.target.value) || 0 }))}
                  />
                </div>
              </div>

              <div className="form-row">
                <div className="form-group flex-1">
                  <label>Username</label>
                  <input
                    value={form.user}
                    onChange={(e) => setForm((f) => ({ ...f, user: e.target.value }))}
                    placeholder="postgres"
                  />
                </div>
                <div className="form-group flex-1">
                  <label>Password</label>
                  <input
                    type="password"
                    value={form.password}
                    onChange={(e) => setForm((f) => ({ ...f, password: e.target.value }))}
                  />
                </div>
              </div>

              <div className="form-group">
                <label>Database</label>
                <input
                  value={form.database}
                  onChange={(e) => setForm((f) => ({ ...f, database: e.target.value }))}
                  placeholder="mydb"
                />
              </div>
            </>
          )}

          <div className="form-group">
            <label>Group (optional)</label>
            <input
              value={form.group || ""}
              onChange={(e) => setForm((f) => ({ ...f, group: e.target.value || null }))}
              placeholder="e.g. Production, Development"
            />
          </div>


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
