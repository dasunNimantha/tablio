import { useConnectionStore } from "../../stores/connectionStore";
import { useTabStore } from "../../stores/tabStore";
import { useShallow } from "zustand/react/shallow";
import {
  Plus,
  Power,
  PowerOff,
  Trash2,
  Edit,
  Loader2,
  FolderOpen,
  Folder,
  ChevronRight,
  ChevronDown,
} from "lucide-react";
import { useState, useMemo } from "react";
import { ConnectionDialog } from "../ConnectionDialog";
import { ConfirmDialog } from "../ConfirmDialog";
import { ConnectionConfig } from "../../lib/tauri";
import "./Sidebar.css";

interface Props {
  onAddConnection: () => void;
}

export function ConnectionList({ onAddConnection }: Props) {
  const { connections, activeConnections, connectTo, disconnectFrom, removeConnection } =
    useConnectionStore(useShallow((s) => ({
      connections: s.connections,
      activeConnections: s.activeConnections,
      connectTo: s.connectTo,
      disconnectFrom: s.disconnectFrom,
      removeConnection: s.removeConnection,
    })));
  const [editingConn, setEditingConn] = useState<ConnectionConfig | null>(null);
  const [connectingId, setConnectingId] = useState<string | null>(null);
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(new Set());
  const [deleteTarget, setDeleteTarget] = useState<ConnectionConfig | null>(null);

  const openTab = useTabStore((s) => s.openTab);

  const handleConnect = async (config: ConnectionConfig) => {
    setConnectingId(config.id);
    try {
      await connectTo(config);
      openTab({
        id: `activity:${config.id}`,
        type: "activity",
        title: `Activity: ${config.name || config.database}`,
        connectionId: config.id,
        connectionColor: config.color || "#5284e0",
        database: config.database,
        schema: "",
      });
    } catch (e) {
      console.error("Connection failed:", e);
    } finally {
      setConnectingId(null);
    }
  };

  const handleDisconnect = async (id: string) => {
    await disconnectFrom(id);
  };

  const handleConfirmDelete = async () => {
    if (deleteTarget) {
      await removeConnection(deleteTarget.id);
      setDeleteTarget(null);
    }
  };

  const toggleGroup = (group: string) => {
    setCollapsedGroups((prev) => {
      const next = new Set(prev);
      if (next.has(group)) next.delete(group);
      else next.add(group);
      return next;
    });
  };

  const grouped = useMemo(() => {
    const groups: Record<string, ConnectionConfig[]> = {};
    const canonicalKey: Record<string, string> = {};
    const ungrouped: ConnectionConfig[] = [];
    for (const conn of connections) {
      const g = conn.group?.trim();
      if (g) {
        const lower = g.toLowerCase();
        if (!canonicalKey[lower]) canonicalKey[lower] = g;
        const key = canonicalKey[lower];
        if (!groups[key]) groups[key] = [];
        groups[key].push(conn);
      } else {
        ungrouped.push(conn);
      }
    }
    return { groups, ungrouped };
  }, [connections]);

  const renderConnection = (conn: ConnectionConfig) => {
    const isActive = activeConnections.has(conn.id);
    const isConnecting = connectingId === conn.id;
    return (
      <div
        key={conn.id}
        className={`connection-item ${isActive ? "active" : ""}`}
      >
        <div
          className="connection-dot"
          style={{ background: conn.color }}
        />
        <div className="connection-info">
          <span className="connection-name">{conn.name || conn.database}</span>
          <span className="connection-detail">
            {conn.db_type === "sqlite"
              ? conn.database
              : `${conn.host}:${conn.port}/${conn.database}`}
          </span>
        </div>
        <div className="connection-actions">
          {isConnecting ? (
            <Loader2 size={14} className="spin" />
          ) : isActive ? (
            <button
              className="btn-icon"
              onClick={() => handleDisconnect(conn.id)}
              title="Disconnect"
            >
              <PowerOff size={14} />
            </button>
          ) : (
            <button
              className="btn-icon"
              onClick={() => handleConnect(conn)}
              title="Connect"
            >
              <Power size={14} />
            </button>
          )}
          <button
            className="btn-icon"
            onClick={() => setEditingConn(conn)}
            title="Edit"
          >
            <Edit size={14} />
          </button>
          <button
            className="btn-icon"
            onClick={() => setDeleteTarget(conn)}
            title="Delete"
          >
            <Trash2 size={14} />
          </button>
        </div>
      </div>
    );
  };

  return (
    <>
      <div className="connection-list">
        {connections.length === 0 ? (
          <div className="connection-empty">
            <p>No connections yet</p>
            <button className="btn-secondary" onClick={onAddConnection}>
              <Plus size={14} /> Add Connection
            </button>
          </div>
        ) : (
          <>
            {Object.entries(grouped.groups)
              .sort(([a], [b]) => a.localeCompare(b))
              .map(([groupName, conns], groupIndex) => {
                const isCollapsed = collapsedGroups.has(groupName);
                return (
                  <div
                    key={groupName}
                    className={`connection-group ${groupIndex === 0 ? "connection-group-first" : ""}`}
                  >
                    <div
                      className="connection-group-header"
                      onClick={() => toggleGroup(groupName)}
                    >
                      <span className="connection-group-chevron">
                        {isCollapsed ? <ChevronRight size={12} /> : <ChevronDown size={12} />}
                      </span>
                      {isCollapsed ? (
                        <Folder size={13} className="connection-group-folder-icon" />
                      ) : (
                        <FolderOpen size={13} className="connection-group-folder-icon" />
                      )}
                      <span className="connection-group-name">{groupName}</span>
                      <span className="connection-group-count">{conns.length}</span>
                    </div>
                    {!isCollapsed && conns.map(renderConnection)}
                  </div>
                );
              })}
            {grouped.ungrouped.map(renderConnection)}
          </>
        )}
      </div>
      {editingConn && (
        <ConnectionDialog
          editConfig={editingConn}
          onClose={() => setEditingConn(null)}
        />
      )}
      {deleteTarget && (
        <ConfirmDialog
          title="Delete Connection"
          message={`Are you sure you want to delete "${deleteTarget.name || deleteTarget.database}"? This action cannot be undone.`}
          confirmLabel="Delete"
          danger
          onConfirm={handleConfirmDelete}
          onCancel={() => setDeleteTarget(null)}
        />
      )}
    </>
  );
}
