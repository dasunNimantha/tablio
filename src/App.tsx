import { useEffect, useState, useRef, useCallback } from "react";
import { ObjectTree } from "./components/Sidebar/ObjectTree";
import { TabBar } from "./components/Tabs/TabBar";
import { TabContent } from "./components/Tabs/TabContent";
import { ConnectionDialog } from "./components/ConnectionDialog";
import { CreateTableDialog } from "./components/CreateTable/CreateTableDialog";
import { AlterTableDialog } from "./components/AlterTable/AlterTableDialog";
import { ImportDialog } from "./components/ImportDialog/ImportDialog";
import { BackupRestoreDialog } from "./components/BackupRestore/BackupRestoreDialog";
import { DumpRestoreDialog } from "./components/DumpRestore/DumpRestoreDialog";
import { KeyboardShortcuts } from "./components/KeyboardShortcuts/KeyboardShortcuts";
import { ToastContainer } from "./components/Toast/Toast";
import { useConnectionStore } from "./stores/connectionStore";
import { useTabStore } from "./stores/tabStore";
import { Database, Plus, Keyboard, Palette, Check, Cpu, MemoryStick } from "lucide-react";
import { themes, getThemeById, applyTheme } from "./lib/themes";
import { api } from "./lib/tauri";

interface CreateTableTarget {
  connectionId: string;
  connectionColor: string;
  database: string;
  schema: string;
}

interface AlterTableTarget {
  connectionId: string;
  connectionColor: string;
  database: string;
  schema: string;
  tableName: string;
}

interface ImportTarget {
  connectionId: string;
  connectionColor: string;
  database: string;
  schema: string;
  tableName: string;
}

interface BackupTarget {
  connectionId: string;
  database: string;
}

interface DumpRestoreTarget {
  connectionId: string;
  database: string;
}

function ResourceMonitor() {
  const [usage, setUsage] = useState({ memory_mb: 0, cpu_percent: 0 });
  useEffect(() => {
    let alive = true;
    const poll = () => {
      api.getAppResourceUsage().then((r) => {
        if (alive) setUsage(r);
      }).catch(() => {});
    };
    poll();
    const id = setInterval(poll, 3000);
    return () => { alive = false; clearInterval(id); };
  }, []);
  return (
    <div className="statusbar-left">
      <span className="statusbar-resource" title="Memory Usage">
        <MemoryStick size={13} />
        {usage.memory_mb < 1024
          ? `${usage.memory_mb.toFixed(1)} MB`
          : `${(usage.memory_mb / 1024).toFixed(2)} GB`}
      </span>
      <span className="statusbar-resource" title="CPU Usage">
        <Cpu size={13} />
        {usage.cpu_percent.toFixed(1)}%
      </span>
    </div>
  );
}

export default function App() {
  const loadConnections = useConnectionStore((s) => s.loadConnections);
  const hasTabs = useTabStore((s) => s.tabs.length > 0);
  const [showConnDialog, setShowConnDialog] = useState(false);
  const [createTableTarget, setCreateTableTarget] = useState<CreateTableTarget | null>(null);
  const [alterTableTarget, setAlterTableTarget] = useState<AlterTableTarget | null>(null);
  const [importTarget, setImportTarget] = useState<ImportTarget | null>(null);
  const [backupTarget, setBackupTarget] = useState<BackupTarget | null>(null);
  const [dumpRestoreTarget, setDumpRestoreTarget] = useState<DumpRestoreTarget | null>(null);
  const [showShortcuts, setShowShortcuts] = useState(false);
  const [showZoom, setShowZoom] = useState(false);
  const [showThemePicker, setShowThemePicker] = useState(false);
  const zoomTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const themePickerRef = useRef<HTMLDivElement>(null);
  const [themeId, setThemeId] = useState<string>(() => {
    return localStorage.getItem("dbstudio-theme") || "dark";
  });
  const [zoom, setZoom] = useState<number>(() => {
    const saved = localStorage.getItem("dbstudio-zoom");
    return saved ? parseFloat(saved) : 110;
  });
  const [sidebarWidth, setSidebarWidth] = useState(() => {
    const saved = localStorage.getItem("dbstudio-sidebar-width");
    return saved ? parseInt(saved, 10) : 270;
  });
  const sidebarWidthRef = useRef(sidebarWidth);
  const sidebarElRef = useRef<HTMLDivElement>(null);

  const handleResizeStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";

    const onMove = (ev: MouseEvent) => {
      const w = Math.max(200, Math.min(ev.clientX, 500));
      sidebarWidthRef.current = w;
      if (sidebarElRef.current) {
        sidebarElRef.current.style.width = `${w}px`;
        sidebarElRef.current.style.minWidth = `${w}px`;
      }
    };
    const onUp = () => {
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
      setSidebarWidth(sidebarWidthRef.current);
      localStorage.setItem("dbstudio-sidebar-width", String(sidebarWidthRef.current));
    };
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
  }, []);

  useEffect(() => {
    return () => {
      if (zoomTimerRef.current) clearTimeout(zoomTimerRef.current);
    };
  }, []);

  useEffect(() => {
    loadConnections();
  }, [loadConnections]);


  useEffect(() => {
    const t = getThemeById(themeId);
    applyTheme(t);
    localStorage.setItem("dbstudio-theme", themeId);
  }, [themeId]);

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (themePickerRef.current && !themePickerRef.current.contains(e.target as Node)) {
        setShowThemePicker(false);
      }
    };
    if (showThemePicker) {
      document.addEventListener("mousedown", handleClickOutside);
    }
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, [showThemePicker]);

  const applyZoom = useCallback((level: number) => {
    const clamped = Math.min(200, Math.max(50, Math.round(level)));
    setZoom(clamped);
    localStorage.setItem("dbstudio-zoom", String(clamped));
    document.documentElement.style.zoom = `${clamped}%`;
    document.documentElement.style.setProperty("--app-zoom", String(clamped / 100));
    setShowZoom(true);
    if (zoomTimerRef.current) clearTimeout(zoomTimerRef.current);
    zoomTimerRef.current = setTimeout(() => setShowZoom(false), 1500);
  }, []);

  useEffect(() => {
    document.documentElement.style.zoom = `${zoom}%`;
    document.documentElement.style.setProperty("--app-zoom", String(zoom / 100));
  }, []);

  useEffect(() => {
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === "?" && e.ctrlKey) {
        e.preventDefault();
        setShowShortcuts((prev) => !prev);
      }
      if (e.ctrlKey && (e.key === "=" || e.key === "+")) {
        e.preventDefault();
        setZoom((prev) => { const n = Math.min(200, prev + 10); applyZoom(n); return n; });
      }
      if (e.ctrlKey && e.key === "-") {
        e.preventDefault();
        setZoom((prev) => { const n = Math.max(50, prev - 10); applyZoom(n); return n; });
      }
      if (e.ctrlKey && e.key === "0") {
        e.preventDefault();
        applyZoom(110);
      }
    };
    const handleWheel = (e: WheelEvent) => {
      if (!e.ctrlKey) return;
      e.preventDefault();
      setZoom((prev) => {
        const delta = e.deltaY > 0 ? -10 : 10;
        const n = Math.min(200, Math.max(50, prev + delta));
        applyZoom(n);
        return n;
      });
    };
    window.addEventListener("keydown", handleKey);
    window.addEventListener("wheel", handleWheel, { passive: false });
    return () => {
      window.removeEventListener("keydown", handleKey);
      window.removeEventListener("wheel", handleWheel);
    };
  }, [applyZoom]);

  const onAddConnection = useCallback(() => setShowConnDialog(true), []);
  const onCreateTable = useCallback(
    (connectionId: string, connectionColor: string, database: string, schema: string) =>
      setCreateTableTarget({ connectionId, connectionColor, database, schema }),
    [],
  );
  const onAlterTable = useCallback(
    (connectionId: string, connectionColor: string, database: string, schema: string, tableName: string) =>
      setAlterTableTarget({ connectionId, connectionColor, database, schema, tableName }),
    [],
  );
  const onImportData = useCallback(
    (connectionId: string, connectionColor: string, database: string, schema: string, tableName: string) =>
      setImportTarget({ connectionId, connectionColor, database, schema, tableName }),
    [],
  );
  const onBackupRestore = useCallback(
    (connectionId: string, _connectionColor: string, database: string) =>
      setBackupTarget({ connectionId, database }),
    [],
  );
  const onDumpRestore = useCallback(
    (connectionId: string, database: string) =>
      setDumpRestoreTarget({ connectionId, database }),
    [],
  );

  return (
    <div className="app-layout">
      <div ref={sidebarElRef} className="sidebar" style={{ width: sidebarWidth, minWidth: sidebarWidth }}>
        <ObjectTree
          onAddConnection={onAddConnection}
          onCreateTable={onCreateTable}
          onAlterTable={onAlterTable}
          onImportData={onImportData}
          onBackupRestore={onBackupRestore}
          onDumpRestore={onDumpRestore}
        />
      </div>
      <div className="sidebar-resize-handle" onMouseDown={handleResizeStart} />
      <div className="main-content">
        {hasTabs ? (
          <>
            <TabBar />
            <TabContent />
          </>
        ) : (
          <div className="empty-state">
            <Database size={56} strokeWidth={1.2} />
            <div className="empty-state-text">
              <h2>Welcome to DB Studio</h2>
              <p>
                Connect to a database and open a table or start a new query to get started.
              </p>
            </div>
            <button className="btn-primary" onClick={() => setShowConnDialog(true)} style={{ marginTop: 8 }}>
              <Plus size={16} /> New Connection
            </button>
          </div>
        )}
        <div className="statusbar">
          <ResourceMonitor />
          <div className="statusbar-right">
            {showZoom && (
              <span
                className="zoom-indicator"
                onClick={() => applyZoom(110)}
                title="Reset Zoom (Ctrl+0)"
              >
                {zoom}%
              </span>
            )}
            <button
              className="btn-icon"
              onClick={() => setShowShortcuts(true)}
              title="Keyboard Shortcuts (Ctrl+?)"
            >
              <Keyboard size={14} />
            </button>
            <div className="theme-picker-wrapper" ref={themePickerRef}>
              <button
                className="btn-icon"
                onClick={() => setShowThemePicker((p) => !p)}
                title="Change Theme"
              >
                <Palette size={14} />
              </button>
              {showThemePicker && (
                <div className="theme-picker-popover">
                  <div className="theme-picker-group">
                    <div className="theme-picker-group-label">Dark</div>
                    {themes.filter((t) => t.group === "dark").map((t) => (
                      <button
                        key={t.id}
                        className={`theme-picker-item${t.id === themeId ? " active" : ""}`}
                        onClick={() => { setThemeId(t.id); setShowThemePicker(false); }}
                      >
                        <span className="theme-picker-name">{t.name}</span>
                        {t.id === themeId && <Check size={13} />}
                      </button>
                    ))}
                  </div>
                  <div className="theme-picker-group">
                    <div className="theme-picker-group-label">Light</div>
                    {themes.filter((t) => t.group === "light").map((t) => (
                      <button
                        key={t.id}
                        className={`theme-picker-item${t.id === themeId ? " active" : ""}`}
                        onClick={() => { setThemeId(t.id); setShowThemePicker(false); }}
                      >
                        <span className="theme-picker-name">{t.name}</span>
                        {t.id === themeId && <Check size={13} />}
                      </button>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
      {showConnDialog && (
        <ConnectionDialog onClose={() => setShowConnDialog(false)} />
      )}
      {createTableTarget && (
        <CreateTableDialog
          connectionId={createTableTarget.connectionId}
          database={createTableTarget.database}
          schema={createTableTarget.schema}
          onClose={() => setCreateTableTarget(null)}
          onCreated={() => setCreateTableTarget(null)}
        />
      )}
      {alterTableTarget && (
        <AlterTableDialog
          connectionId={alterTableTarget.connectionId}
          database={alterTableTarget.database}
          schema={alterTableTarget.schema}
          tableName={alterTableTarget.tableName}
          onClose={() => setAlterTableTarget(null)}
          onAltered={() => setAlterTableTarget(null)}
        />
      )}
      {importTarget && (
        <ImportDialog
          connectionId={importTarget.connectionId}
          database={importTarget.database}
          schema={importTarget.schema}
          tableName={importTarget.tableName}
          onClose={() => setImportTarget(null)}
          onImported={() => setImportTarget(null)}
        />
      )}
      {backupTarget && (
        <BackupRestoreDialog
          connectionId={backupTarget.connectionId}
          database={backupTarget.database}
          onClose={() => setBackupTarget(null)}
        />
      )}
      {dumpRestoreTarget && (
        <DumpRestoreDialog
          sourceConnectionId={dumpRestoreTarget.connectionId}
          sourceDatabase={dumpRestoreTarget.database}
          onClose={() => setDumpRestoreTarget(null)}
        />
      )}
      {showShortcuts && (
        <KeyboardShortcuts onClose={() => setShowShortcuts(false)} />
      )}
      <ToastContainer />
    </div>
  );
}
