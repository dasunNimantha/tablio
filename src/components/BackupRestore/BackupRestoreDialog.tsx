import { useState } from "react";
import { api, BackupRequest, RestoreRequest } from "../../lib/tauri";
import { X, Loader2, Database, Upload, Download } from "lucide-react";
import "./BackupRestoreDialog.css";

interface Props {
  connectionId: string;
  database: string;
  onClose: () => void;
}

type Tab = "backup" | "restore";

const BACKUP_FORMATS = [
  { value: "plain", label: "Plain SQL" },
  { value: "custom", label: "Custom" },
  { value: "tar", label: "Tar" },
  { value: "directory", label: "Directory" },
];

const RESTORE_FORMATS = [
  { value: "auto", label: "Auto-detect" },
  { value: "plain", label: "Plain" },
  { value: "custom", label: "Custom" },
  { value: "tar", label: "Tar" },
  { value: "directory", label: "Directory" },
];

export function BackupRestoreDialog({
  connectionId,
  database,
  onClose,
}: Props) {
  const [tab, setTab] = useState<Tab>("backup");

  // Backup state
  const [outputPath, setOutputPath] = useState("");
  const [backupFormat, setBackupFormat] = useState("plain");
  const [schemaOnly, setSchemaOnly] = useState(false);
  const [dataOnly, setDataOnly] = useState(false);

  // Restore state
  const [inputPath, setInputPath] = useState("");
  const [restoreFormat, setRestoreFormat] = useState("auto");

  // Operation state
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const resetResult = () => {
    setResult(null);
    setError(null);
  };

  const handleBackup = async () => {
    if (!outputPath.trim()) {
      setError("Output file path is required");
      return;
    }

    setLoading(true);
    setError(null);
    setResult(null);

    try {
      const request: BackupRequest = {
        connection_id: connectionId,
        database,
        output_path: outputPath.trim(),
        format: backupFormat,
        schema_only: schemaOnly,
        data_only: dataOnly,
      };
      const message = await api.backupDatabase(request);
      setResult(message);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  };

  const handleRestore = async () => {
    if (!inputPath.trim()) {
      setError("Input file path is required");
      return;
    }

    setLoading(true);
    setError(null);
    setResult(null);

    try {
      const request: RestoreRequest = {
        connection_id: connectionId,
        database,
        input_path: inputPath.trim(),
        format: restoreFormat,
      };
      const message = await api.restoreDatabase(request);
      setResult(message);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  };

  const handleTabChange = (newTab: Tab) => {
    setTab(newTab);
    resetResult();
  };

  return (
    <div className="dialog-overlay" onClick={onClose}>
      <div
        className="dialog backup-restore-dialog"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="dialog-header">
          <h2>
            <Database size={18} />
            Backup / Restore
          </h2>
          <button className="btn-icon" onClick={onClose}>
            <X size={18} />
          </button>
        </div>

        <div className="backup-tabs">
          <button
            type="button"
            className={`backup-tab ${tab === "backup" ? "active" : ""}`}
            onClick={() => handleTabChange("backup")}
          >
            <Download size={16} />
            Backup
          </button>
          <button
            type="button"
            className={`backup-tab ${tab === "restore" ? "active" : ""}`}
            onClick={() => handleTabChange("restore")}
          >
            <Upload size={16} />
            Restore
          </button>
        </div>

        <div className="dialog-body">
          {tab === "backup" && (
            <div className="backup-form">
              <div className="form-group">
                <label>Target Database</label>
                <input
                  type="text"
                  value={database}
                  disabled
                  className="backup-input-disabled"
                />
              </div>
              <div className="form-group">
                <label>Output file path</label>
                <input
                  type="text"
                  value={outputPath}
                  onChange={(e) => {
                    setOutputPath(e.target.value);
                    resetResult();
                  }}
                  placeholder="/path/to/backup.sql"
                  disabled={loading}
                />
              </div>
              <div className="form-group">
                <label>Format</label>
                <select
                  value={backupFormat}
                  onChange={(e) => setBackupFormat(e.target.value)}
                  disabled={loading}
                >
                  {BACKUP_FORMATS.map((f) => (
                    <option key={f.value} value={f.value}>
                      {f.label}
                    </option>
                  ))}
                </select>
              </div>
              <div className="backup-checkboxes">
                <label className="checkbox-label">
                  <input
                    type="checkbox"
                    checked={schemaOnly}
                    onChange={(e) => {
                      setSchemaOnly(e.target.checked);
                      if (e.target.checked) setDataOnly(false);
                    }}
                    disabled={loading}
                  />
                  Schema only
                </label>
                <label className="checkbox-label">
                  <input
                    type="checkbox"
                    checked={dataOnly}
                    onChange={(e) => {
                      setDataOnly(e.target.checked);
                      if (e.target.checked) setSchemaOnly(false);
                    }}
                    disabled={loading}
                  />
                  Data only
                </label>
              </div>
              <button
                className="btn-primary backup-action-btn"
                onClick={handleBackup}
                disabled={loading}
              >
                {loading && <Loader2 size={14} className="spin" />}
                Start Backup
              </button>
            </div>
          )}

          {tab === "restore" && (
            <div className="backup-form">
              <div className="form-group">
                <label>Target Database</label>
                <input
                  type="text"
                  value={database}
                  disabled
                  className="backup-input-disabled"
                />
              </div>
              <div className="form-group">
                <label>Input file path</label>
                <input
                  type="text"
                  value={inputPath}
                  onChange={(e) => {
                    setInputPath(e.target.value);
                    resetResult();
                  }}
                  placeholder="/path/to/backup.sql"
                  disabled={loading}
                />
              </div>
              <div className="form-group">
                <label>Format</label>
                <select
                  value={restoreFormat}
                  onChange={(e) => setRestoreFormat(e.target.value)}
                  disabled={loading}
                >
                  {RESTORE_FORMATS.map((f) => (
                    <option key={f.value} value={f.value}>
                      {f.label}
                    </option>
                  ))}
                </select>
              </div>
              <button
                className="btn-primary backup-action-btn"
                onClick={handleRestore}
                disabled={loading}
              >
                {loading && <Loader2 size={14} className="spin" />}
                Start Restore
              </button>
            </div>
          )}

          {(loading || result || error) && (
            <div className="backup-result-area">
              {loading && (
                <div className="backup-result loading">
                  <Loader2 size={18} className="spin" />
                  <span>
                    {tab === "backup" ? "Backing up database..." : "Restoring database..."}
                  </span>
                </div>
              )}
              {result && !loading && (
                <div className="backup-result success">{result}</div>
              )}
              {error && !loading && (
                <div className="backup-result error">{error}</div>
              )}
            </div>
          )}
        </div>

        <div className="dialog-footer">
          <button className="btn-secondary" onClick={onClose}>
            Close
          </button>
        </div>
      </div>
    </div>
  );
}
