import { useEffect, useState, useRef, useCallback } from "react";
import { ObjectTree } from "./components/Sidebar/ObjectTree";
import { TabBar } from "./components/Tabs/TabBar";
import { TabContent } from "./components/Tabs/TabContent";
import { ConnectionDialog } from "./components/ConnectionDialog";
import { CreateTableDialog } from "./components/CreateTable/CreateTableDialog";
import { AlterTableDialog } from "./components/AlterTable/AlterTableDialog";
import { ImportDialog } from "./components/ImportDialog/ImportDialog";
import { BackupRestoreDialog } from "./components/BackupRestore/BackupRestoreDialog";
import { KeyboardShortcuts } from "./components/KeyboardShortcuts/KeyboardShortcuts";
import { ToastContainer } from "./components/Toast/Toast";
import { useConnectionStore } from "./stores/connectionStore";
import { useTabStore } from "./stores/tabStore";
import { Database, Plus, Sun, Moon, Keyboard } from "lucide-react";

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

export default function App() {
  const { loadConnections } = useConnectionStore();
  const { tabs } = useTabStore();
  const [showConnDialog, setShowConnDialog] = useState(false);
  const [createTableTarget, setCreateTableTarget] = useState<CreateTableTarget | null>(null);
  const [alterTableTarget, setAlterTableTarget] = useState<AlterTableTarget | null>(null);
  const [importTarget, setImportTarget] = useState<ImportTarget | null>(null);
  const [backupTarget, setBackupTarget] = useState<BackupTarget | null>(null);
  const [showShortcuts, setShowShortcuts] = useState(false);
  const [theme, setTheme] = useState<"dark" | "light">(() => {
    return (localStorage.getItem("dbstudio-theme") as "dark" | "light") || "dark";
  });
  const [sidebarWidth, setSidebarWidth] = useState(() => {
    const saved = localStorage.getItem("dbstudio-sidebar-width");
    return saved ? parseInt(saved, 10) : 270;
  });
  const sidebarWidthRef = useRef(sidebarWidth);

  const handleResizeStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";

    const onMove = (ev: MouseEvent) => {
      const w = Math.max(200, Math.min(ev.clientX, 500));
      sidebarWidthRef.current = w;
      setSidebarWidth(w);
    };
    const onUp = () => {
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
      localStorage.setItem("dbstudio-sidebar-width", String(sidebarWidthRef.current));
    };
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
  }, []);

  useEffect(() => {
    loadConnections();
  }, [loadConnections]);


  useEffect(() => {
    document.documentElement.setAttribute("data-theme", theme);
    localStorage.setItem("dbstudio-theme", theme);
  }, [theme]);

  useEffect(() => {
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === "?" && e.ctrlKey) {
        e.preventDefault();
        setShowShortcuts((prev) => !prev);
      }
    };
    window.addEventListener("keydown", handleKey);
    return () => window.removeEventListener("keydown", handleKey);
  }, []);

  return (
    <div className="app-layout">
      <div className="sidebar" style={{ width: sidebarWidth, minWidth: sidebarWidth }}>
        <ObjectTree
          onAddConnection={() => setShowConnDialog(true)}
          onCreateTable={(connectionId, connectionColor, database, schema) =>
            setCreateTableTarget({ connectionId, connectionColor, database, schema })
          }
          onAlterTable={(connectionId, connectionColor, database, schema, tableName) =>
            setAlterTableTarget({ connectionId, connectionColor, database, schema, tableName })
          }
          onImportData={(connectionId, connectionColor, database, schema, tableName) =>
            setImportTarget({ connectionId, connectionColor, database, schema, tableName })
          }
          onBackupRestore={(connectionId, _connectionColor, database) =>
            setBackupTarget({ connectionId, database })
          }
        />
      </div>
      <div className="sidebar-resize-handle" onMouseDown={handleResizeStart} />
      <div className="main-content">
        {tabs.length > 0 ? (
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
          <div className="statusbar-left" />
          <div className="statusbar-right">
            <button
              className="btn-icon"
              onClick={() => setShowShortcuts(true)}
              title="Keyboard Shortcuts (Ctrl+?)"
            >
              <Keyboard size={14} />
            </button>
            <button
              className="btn-icon"
              onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
              title="Toggle Theme"
            >
              {theme === "dark" ? <Sun size={14} /> : <Moon size={14} />}
            </button>
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
      {showShortcuts && (
        <KeyboardShortcuts onClose={() => setShowShortcuts(false)} />
      )}
      <ToastContainer />
    </div>
  );
}
