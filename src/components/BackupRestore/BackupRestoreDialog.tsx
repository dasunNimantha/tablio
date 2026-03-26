import { useState, useCallback, useEffect, useMemo, useRef } from "react";
import { api, BackupRequest, RestoreRequest } from "../../lib/tauri";
import { useConnectionStore } from "../../stores/connectionStore";
import {
  X,
  Loader2,
  Upload,
  Download,
  FolderOpen,
  CheckCircle2,
  XCircle,
  AlertTriangle,
  ChevronDown,
} from "lucide-react";
import { save, open } from "@tauri-apps/plugin-dialog";
import "./BackupRestoreDialog.css";

interface Props {
  connectionId: string;
  database: string;
  onClose: () => void;
}

type Tab = "backup" | "restore";

const BACKUP_FORMATS = [
  { value: "plain", label: "Plain SQL", meta: ".sql", ext: "sql" },
  { value: "custom", label: "Custom", meta: ".dump", ext: "dump" },
  { value: "tar", label: "Tar", meta: ".tar", ext: "tar" },
  { value: "directory", label: "Directory", meta: "folder", ext: "" },
] as const;

const RESTORE_FORMATS = [
  { value: "auto", label: "Auto-detect", meta: "auto" },
  { value: "plain", label: "Plain SQL", meta: ".sql" },
  { value: "custom", label: "Custom", meta: ".dump" },
  { value: "tar", label: "Tar", meta: ".tar" },
  { value: "directory", label: "Directory", meta: "folder" },
] as const;

const CONTENT_MODES = [
  { value: "all", label: "All content" },
  { value: "schema", label: "Schema only" },
  { value: "data", label: "Data only" },
] as const;

interface SelectOption {
  value: string;
  label: string;
}

function CustomFormatSelect({
  id,
  value,
  onChange,
  options,
  disabled,
  open,
  onOpenChange,
}: {
  id: string;
  value: string;
  onChange: (v: string) => void;
  options: SelectOption[];
  disabled?: boolean;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const rootRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!open) return;
    const onDoc = (e: MouseEvent) => {
      if (rootRef.current && !rootRef.current.contains(e.target as Node)) {
        onOpenChange(false);
      }
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onOpenChange(false);
    };
    document.addEventListener("mousedown", onDoc);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDoc);
      document.removeEventListener("keydown", onKey);
    };
  }, [open, onOpenChange]);

  const selected = options.find((o) => o.value === value) ?? options[0];

  return (
    <div className="br-custom-select" ref={rootRef}>
      <button
        type="button"
        id={id}
        className="br-custom-select-trigger"
        disabled={disabled}
        aria-haspopup="listbox"
        aria-expanded={open}
        onClick={() => !disabled && onOpenChange(!open)}
      >
        <span className="br-custom-select-value">{selected?.label}</span>
        <ChevronDown
          size={14}
          className={`br-custom-select-chevron${open ? " br-custom-select-chevron--open" : ""}`}
          aria-hidden
        />
      </button>
      {open && (
        <div className="br-custom-select-menu" role="listbox" aria-labelledby={id}>
          {options.map((opt) => (
            <button
              key={opt.value}
              type="button"
              role="option"
              aria-selected={value === opt.value}
              className={`br-custom-select-option${value === opt.value ? " br-custom-select-option--active" : ""}`}
              onClick={() => {
                onChange(opt.value);
                onOpenChange(false);
              }}
            >
              {opt.label}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

export function BackupRestoreDialog({
  connectionId,
  database,
  onClose,
}: Props) {
  const connections = useConnectionStore((s) => s.connections);
  const [tab, setTab] = useState<Tab>("backup");

  const [outputPath, setOutputPath] = useState("");
  const [backupFormat, setBackupFormat] = useState("plain");
  const [contentMode, setContentMode] = useState<"all" | "schema" | "data">("all");

  const [inputPath, setInputPath] = useState("");
  const [restoreFormat, setRestoreFormat] = useState("auto");

  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [openDropdown, setOpenDropdown] = useState<
    null | "backupFormat" | "restoreFormat" | "backupContent"
  >(null);
  const sourceConn = connections.find((c) => c.id === connectionId);

  const backupFormatOptions = useMemo(
    () =>
      BACKUP_FORMATS.map((f) => ({
        value: f.value,
        label:
          f.meta === "folder" ? `${f.label} (folder)` : `${f.label} (${f.meta})`,
      })),
    [],
  );

  const restoreFormatOptions = useMemo(
    () =>
      RESTORE_FORMATS.map((f) => ({
        value: f.value,
        label:
          f.meta === "auto"
            ? f.label
            : f.meta === "folder"
              ? `${f.label} (folder)`
              : `${f.label} (${f.meta})`,
      })),
    [],
  );

  const backupContentOptions = useMemo(
    () => CONTENT_MODES.map((m) => ({ value: m.value, label: m.label })),
    [],
  );

  const resetResult = () => {
    setResult(null);
    setError(null);
  };

  const handleBrowseOutput = useCallback(async () => {
    try {
      const fmt = BACKUP_FORMATS.find((f) => f.value === backupFormat);
      const filters = fmt?.ext
        ? [{ name: fmt.label, extensions: [fmt.ext] }]
        : [];
      const path = await save({
        defaultPath: fmt?.ext ? `${database}_backup.${fmt.ext}` : `${database}_backup`,
        filters,
      });
      if (path) setOutputPath(path);
    } catch {
      /* user cancelled */
    }
  }, [backupFormat, database]);

  const handleBrowseInput = useCallback(async () => {
    try {
      const path = await open({
        multiple: false,
        filters: [
          { name: "Backup files", extensions: ["sql", "dump", "tar", "gz"] },
          { name: "All files", extensions: ["*"] },
        ],
      });
      if (path) setInputPath(path as string);
    } catch {
      /* user cancelled */
    }
  }, []);

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
        schema_only: contentMode === "schema",
        data_only: contentMode === "data",
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
    setOpenDropdown(null);
    resetResult();
  };

  const canBackup = outputPath.trim().length > 0 && !loading;
  const canRestore = inputPath.trim().length > 0 && !loading;

  return (
    <div className="dialog-overlay" onClick={loading ? undefined : onClose}>
      <div className="dialog br-dialog" onClick={(e) => e.stopPropagation()}>
        <div className="dialog-header">
          <h2>Backup &amp; Restore</h2>
          <button className="btn-icon" onClick={onClose} disabled={loading}>
            <X size={16} />
          </button>
        </div>

        <div className="dialog-body">
          {/* Database read-only field */}
          <div className="form-group">
            <label>Source database</label>
            <div className="dr-readonly-field">
              <span className="dr-dot" style={{ background: sourceConn?.color || "#6d9eff" }} />
              <span>{sourceConn?.name || "Connection"}</span>
              <span className="dr-sep">/</span>
              <span className="dr-mono">{database}</span>
              {sourceConn && (
                <span className="dr-host">{sourceConn.host}:{sourceConn.port}</span>
              )}
            </div>
          </div>

          <div className="form-group">
            <label>Operation</label>
            <div className="br-tabs">
              <button
                type="button"
                className={`br-tab ${tab === "backup" ? "br-tab--active" : ""}`}
                onClick={() => handleTabChange("backup")}
                disabled={loading}
              >
                <Download size={14} />
                Backup
              </button>
              <button
                type="button"
                className={`br-tab ${tab === "restore" ? "br-tab--active" : ""}`}
                onClick={() => handleTabChange("restore")}
                disabled={loading}
              >
                <Upload size={14} />
                Restore
              </button>
            </div>
          </div>

          {/* Backup form */}
          {tab === "backup" && (
            <>
              <div className="form-group">
                <label>Output file path</label>
                <div className="br-path-row">
                  <input
                    type="text"
                    value={outputPath}
                    onChange={(e) => {
                      setOutputPath(e.target.value);
                      resetResult();
                    }}
                    placeholder={`${database}_backup.sql`}
                    disabled={loading}
                  />
                  <span className="br-path-sep" aria-hidden />
                  <button
                    type="button"
                    className="br-browse-btn"
                    onClick={handleBrowseOutput}
                    disabled={loading}
                    title="Browse..."
                  >
                    <FolderOpen size={14} />
                  </button>
                </div>
              </div>

              <div className="form-group">
                <div className="br-two-col-labels">
                  <label htmlFor="br-backup-format">Format</label>
                  <label htmlFor="br-backup-content">Content</label>
                </div>
                <div className="br-two-col">
                  <CustomFormatSelect
                    id="br-backup-format"
                    value={backupFormat}
                    onChange={(v) => {
                      setBackupFormat(v);
                      resetResult();
                    }}
                    options={backupFormatOptions}
                    disabled={loading}
                    open={openDropdown === "backupFormat"}
                    onOpenChange={(o) => setOpenDropdown(o ? "backupFormat" : null)}
                  />
                  <CustomFormatSelect
                    id="br-backup-content"
                    value={contentMode}
                    onChange={(v) => {
                      setContentMode(v as "all" | "schema" | "data");
                      resetResult();
                    }}
                    options={backupContentOptions}
                    disabled={loading}
                    open={openDropdown === "backupContent"}
                    onOpenChange={(o) => setOpenDropdown(o ? "backupContent" : null)}
                  />
                </div>
              </div>
            </>
          )}

          {/* Restore form */}
          {tab === "restore" && (
            <>
              <div className="form-group">
                <label>Input file path</label>
                <div className="br-path-row">
                  <input
                    type="text"
                    value={inputPath}
                    onChange={(e) => {
                      setInputPath(e.target.value);
                      resetResult();
                    }}
                    placeholder="Select a backup file..."
                    disabled={loading}
                  />
                  <span className="br-path-sep" aria-hidden />
                  <button
                    type="button"
                    className="br-browse-btn"
                    onClick={handleBrowseInput}
                    disabled={loading}
                    title="Browse..."
                  >
                    <FolderOpen size={14} />
                  </button>
                </div>
              </div>

              <div className="form-group">
                <label htmlFor="br-restore-format">Format</label>
                <CustomFormatSelect
                  id="br-restore-format"
                  value={restoreFormat}
                  onChange={(v) => {
                    setRestoreFormat(v);
                    resetResult();
                  }}
                  options={restoreFormatOptions}
                  disabled={loading}
                  open={openDropdown === "restoreFormat"}
                  onOpenChange={(o) => setOpenDropdown(o ? "restoreFormat" : null)}
                />
              </div>

              {inputPath.trim() && (
                <div className="dr-banner dr-banner--warn">
                  <AlertTriangle size={14} />
                  <div>
                    Restoring will overwrite existing data in <strong>{database}</strong>.
                  </div>
                </div>
              )}
            </>
          )}

          {/* Status banners */}
          {result && !loading && (
            <div className="dr-banner dr-banner--success">
              <CheckCircle2 size={14} />
              <div>
                <strong>{tab === "backup" ? "Backup complete" : "Restore complete"}</strong>
                <p className="dr-banner-sub">{result}</p>
              </div>
            </div>
          )}
          {error && !loading && (
            <div className="dr-banner dr-banner--error">
              <XCircle size={14} />
              <div>
                <strong>{tab === "backup" ? "Backup failed" : "Restore failed"}</strong>
                <p className="dr-banner-sub">{error}</p>
              </div>
            </div>
          )}
        </div>

        <div className="dialog-footer br-footer">
          <button className="btn-secondary" onClick={onClose} disabled={loading}>
            Cancel
          </button>
          {tab === "backup" ? (
            <button
              className="btn-primary"
              onClick={handleBackup}
              disabled={!canBackup}
            >
              {loading ? <Loader2 size={14} className="spin" /> : <Download size={14} />}
              {loading ? "Backing up..." : "Start Backup"}
            </button>
          ) : (
            <button
              className="btn-primary"
              onClick={handleRestore}
              disabled={!canRestore}
            >
              {loading ? <Loader2 size={14} className="spin" /> : <Upload size={14} />}
              {loading ? "Restoring..." : "Start Restore"}
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
