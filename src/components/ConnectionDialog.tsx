import { useState } from "react";
import { useConnectionStore } from "../stores/connectionStore";
import { api, ConnectionConfig } from "../lib/tauri";
import { X, Loader2, CheckCircle, XCircle, ChevronDown, ChevronRight } from "lucide-react";
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
}

export function ConnectionDialog({ onClose, editConfig }: Props) {
  const { addConnection, updateConnection } = useConnectionStore();
  const isEdit = !!editConfig;

  const [form, setForm] = useState<ConnectionConfig>(
    editConfig || {
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
    }
  );

  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<"success" | "error" | null>(null);
  const [testError, setTestError] = useState("");
  const [saving, setSaving] = useState(false);
  const [showSsh, setShowSsh] = useState(!!editConfig?.ssh_enabled);

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

  const handleTest = async () => {
    setTesting(true);
    setTestResult(null);
    setTestError("");
    try {
      await api.testConnection(form);
      setTestResult("success");
    } catch (e) {
      setTestResult("error");
      setTestError(String(e));
    } finally {
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
    <div className="dialog-overlay" onClick={onClose}>
      <div className="dialog" onClick={(e) => e.stopPropagation()}>
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

              <div className="form-row">
                <div className="form-group flex-1">
                  <label>Database</label>
                  <input
                    value={form.database}
                    onChange={(e) => setForm((f) => ({ ...f, database: e.target.value }))}
                    placeholder="mydb"
                  />
                </div>
                <div className="form-group">
                  <label className="checkbox-label">
                    <input
                      type="checkbox"
                      checked={form.ssl}
                      onChange={(e) => setForm((f) => ({ ...f, ssl: e.target.checked }))}
                    />
                    Use SSL
                  </label>
                </div>
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

          {!isSqlite && (
            <div className="ssh-section">
              <button
                className="btn-ghost ssh-toggle"
                type="button"
                onClick={() => setShowSsh(!showSsh)}
              >
                {showSsh ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                SSH Tunnel
                {form.ssh_enabled && <span className="ssh-active-badge">Active</span>}
              </button>
              {showSsh && (
                <div className="ssh-fields">
                  <div className="form-group">
                    <label className="checkbox-label">
                      <input
                        type="checkbox"
                        checked={form.ssh_enabled || false}
                        onChange={(e) => setForm((f) => ({ ...f, ssh_enabled: e.target.checked }))}
                      />
                      Enable SSH Tunnel
                    </label>
                  </div>
                  {form.ssh_enabled && (
                    <>
                      <div className="form-row">
                        <div className="form-group flex-1">
                          <label>SSH Host</label>
                          <input
                            value={form.ssh_host || ""}
                            onChange={(e) => setForm((f) => ({ ...f, ssh_host: e.target.value }))}
                            placeholder="bastion.example.com"
                          />
                        </div>
                        <div className="form-group" style={{ width: 100 }}>
                          <label>SSH Port</label>
                          <input
                            type="number"
                            value={form.ssh_port || 22}
                            onChange={(e) => setForm((f) => ({ ...f, ssh_port: parseInt(e.target.value) || 22 }))}
                          />
                        </div>
                      </div>
                      <div className="form-row">
                        <div className="form-group flex-1">
                          <label>SSH Username</label>
                          <input
                            value={form.ssh_user || ""}
                            onChange={(e) => setForm((f) => ({ ...f, ssh_user: e.target.value }))}
                            placeholder="ubuntu"
                          />
                        </div>
                        <div className="form-group flex-1">
                          <label>SSH Password</label>
                          <input
                            type="password"
                            value={form.ssh_password || ""}
                            onChange={(e) => setForm((f) => ({ ...f, ssh_password: e.target.value }))}
                            placeholder="(optional if using key)"
                          />
                        </div>
                      </div>
                      <div className="form-group">
                        <label>SSH Private Key Path</label>
                        <input
                          value={form.ssh_key_path || ""}
                          onChange={(e) => setForm((f) => ({ ...f, ssh_key_path: e.target.value }))}
                          placeholder="~/.ssh/id_rsa"
                        />
                      </div>
                    </>
                  )}
                </div>
              )}
            </div>
          )}

          {testResult && (
            <div className={`test-result ${testResult}`}>
              {testResult === "success" ? (
                <>
                  <CheckCircle size={16} /> Connection successful
                </>
              ) : (
                <>
                  <XCircle size={16} /> {testError}
                </>
              )}
            </div>
          )}
        </div>

        <div className="dialog-footer">
          <button className="btn-secondary" onClick={handleTest} disabled={testing}>
            {testing ? <Loader2 size={14} className="spin" /> : null}
            Test Connection
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
