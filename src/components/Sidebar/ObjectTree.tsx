import { useState, useCallback } from "react";
import { useConnectionStore } from "../../stores/connectionStore";
import { useTabStore, TabInfo } from "../../stores/tabStore";
import { api, DatabaseInfo, SchemaInfo, TableInfo, FunctionInfo, ConnectionConfig } from "../../lib/tauri";
import { ConfirmDialog } from "../ConfirmDialog";
import { ConnectionDialog } from "../ConnectionDialog";
import {
  ChevronRight,
  ChevronDown,
  Database,
  Table2,
  FolderOpen,
  Folder,
  Eye,
  Terminal,
  Loader2,
  Zap,
  SearchIcon,
  Plus,
} from "lucide-react";
import "./Sidebar.css";

interface TreeNode {
  id: string;
  label: string;
  type: "connection" | "database" | "schema" | "table-group" | "view-group" | "function-group" | "table" | "view" | "function";
  children?: TreeNode[];
  isLoading?: boolean;
  connectionId: string;
  connectionColor: string;
  database?: string;
  schema?: string;
  tableName?: string;
  tableType?: string;
}

interface ContextMenuState {
  x: number;
  y: number;
  node: TreeNode;
}

interface ObjectTreeProps {
  onAddConnection?: () => void;
  onCreateTable?: (connectionId: string, connectionColor: string, database: string, schema: string) => void;
  onAlterTable?: (connectionId: string, connectionColor: string, database: string, schema: string, tableName: string) => void;
  onImportData?: (connectionId: string, connectionColor: string, database: string, schema: string, tableName: string) => void;
  onBackupRestore?: (connectionId: string, connectionColor: string, database: string) => void;
}

export function ObjectTree({ onAddConnection, onCreateTable, onAlterTable, onImportData, onBackupRestore }: ObjectTreeProps) {
  const { connections, activeConnections, connectTo, disconnectFrom, removeConnection } = useConnectionStore();
  const { openTab } = useTabStore();
  const [editingConn, setEditingConn] = useState<import("../../lib/tauri").ConnectionConfig | null>(null);
  const [connectingId, setConnectingId] = useState<string | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<import("../../lib/tauri").ConnectionConfig | null>(null);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [childrenMap, setChildrenMap] = useState<Record<string, TreeNode[]>>({});
  const [loadingNodes, setLoadingNodes] = useState<Set<string>>(new Set());
  const [contextMenu, setContextMenu] = useState<ContextMenuState | null>(null);
  const [searchQuery, setSearchQuery] = useState("");

  const [confirmAction, setConfirmAction] = useState<{
    title: string;
    message: string;
    action: () => Promise<void>;
    node: TreeNode;
  } | null>(null);


  const toggleExpand = useCallback(
    async (node: TreeNode) => {
      const nodeId = node.id;

      if (expanded.has(nodeId)) {
        setExpanded((prev) => {
          const next = new Set(prev);
          next.delete(nodeId);
          return next;
        });
        return;
      }

      setExpanded((prev) => new Set(prev).add(nodeId));

      if (childrenMap[nodeId]) return;

      setLoadingNodes((prev) => new Set(prev).add(nodeId));

      try {
        let children: TreeNode[] = [];

        if (node.type === "connection") {
          const dbs: DatabaseInfo[] = await api.listDatabases(node.connectionId);
          const templateDbs = new Set(["template0", "template1"]);
          const userDbs = dbs.filter((db) => !templateDbs.has(db.name));

          if (userDbs.length === 1) {
            const db = userDbs[0];
            const dbNodeId = `${node.connectionId}:${db.name}`;
            const dbNode: TreeNode = {
              id: dbNodeId,
              label: db.name,
              type: "database" as const,
              connectionId: node.connectionId,
              connectionColor: node.connectionColor,
              database: db.name,
            };
            setExpanded((prev) => new Set(prev).add(dbNodeId));
            children = [dbNode];
            setTimeout(() => toggleExpand(dbNode), 50);
          } else {
            children = userDbs.map((db) => ({
              id: `${node.connectionId}:${db.name}`,
              label: db.name,
              type: "database" as const,
              connectionId: node.connectionId,
              connectionColor: node.connectionColor,
              database: db.name,
            }));
          }
        } else if (node.type === "database") {
          const allSchemas: SchemaInfo[] = await api.listSchemas(
            node.connectionId,
            node.database!
          );
          const systemSchemas = new Set([
            "pg_catalog", "information_schema", "pg_toast",
            "pg_temp_1", "pg_toast_temp_1",
          ]);
          const userSchemas = allSchemas.filter(
            (s) => !systemSchemas.has(s.name) && !s.name.startsWith("pg_temp_") && !s.name.startsWith("pg_toast_temp_")
          );
          const schemasToShow = userSchemas.length > 0 ? userSchemas : allSchemas;

          if (schemasToShow.length === 1) {
            const s = schemasToShow[0];
            children = [
              {
                id: `${node.connectionId}:${node.database}:${s.name}:tables`,
                label: "Tables",
                type: "table-group" as const,
                connectionId: node.connectionId,
                connectionColor: node.connectionColor,
                database: node.database,
                schema: s.name,
              },
              {
                id: `${node.connectionId}:${node.database}:${s.name}:views`,
                label: "Views",
                type: "view-group" as const,
                connectionId: node.connectionId,
                connectionColor: node.connectionColor,
                database: node.database,
                schema: s.name,
              },
              {
                id: `${node.connectionId}:${node.database}:${s.name}:functions`,
                label: "Functions",
                type: "function-group" as const,
                connectionId: node.connectionId,
                connectionColor: node.connectionColor,
                database: node.database,
                schema: s.name,
              },
            ];
          } else {
            children = schemasToShow.map((s) => ({
              id: `${node.connectionId}:${node.database}:${s.name}`,
              label: s.name,
              type: "schema" as const,
              connectionId: node.connectionId,
              connectionColor: node.connectionColor,
              database: node.database,
              schema: s.name,
            }));
          }
        } else if (node.type === "schema") {
          children = [
            {
              id: `${node.id}:tables`,
              label: "Tables",
              type: "table-group" as const,
              connectionId: node.connectionId,
              connectionColor: node.connectionColor,
              database: node.database,
              schema: node.schema,
            },
            {
              id: `${node.id}:views`,
              label: "Views",
              type: "view-group" as const,
              connectionId: node.connectionId,
              connectionColor: node.connectionColor,
              database: node.database,
              schema: node.schema,
            },
            {
              id: `${node.id}:functions`,
              label: "Functions",
              type: "function-group" as const,
              connectionId: node.connectionId,
              connectionColor: node.connectionColor,
              database: node.database,
              schema: node.schema,
            },
          ];
        } else if (node.type === "function-group") {
          const funcs: FunctionInfo[] = await api.listFunctions(
            node.connectionId,
            node.database!,
            node.schema!
          );
          children = funcs.map((f) => ({
            id: `${node.connectionId}:${node.database}:${node.schema}:fn:${f.name}`,
            label: `${f.name}() → ${f.return_type}`,
            type: "function" as const,
            connectionId: node.connectionId,
            connectionColor: node.connectionColor,
            database: node.database,
            schema: node.schema,
            tableName: f.name,
            tableType: f.kind,
          }));
        } else if (node.type === "table-group" || node.type === "view-group") {
          const tables: TableInfo[] = await api.listTables(
            node.connectionId,
            node.database!,
            node.schema!
          );
          const isTableGroup = node.type === "table-group";
          children = tables
            .filter((t) =>
              isTableGroup
                ? t.table_type === "BASE TABLE"
                : t.table_type === "VIEW"
            )
            .map((t) => ({
              id: `${node.connectionId}:${node.database}:${node.schema}:${t.name}`,
              label: t.name,
              type: isTableGroup ? ("table" as const) : ("view" as const),
              connectionId: node.connectionId,
              connectionColor: node.connectionColor,
              database: node.database,
              schema: node.schema,
              tableName: t.name,
              tableType: t.table_type,
            }));
        }

        setChildrenMap((prev) => ({ ...prev, [nodeId]: children }));
      } catch (err) {
        console.error("Failed to load tree children:", err);
      } finally {
        setLoadingNodes((prev) => {
          const next = new Set(prev);
          next.delete(nodeId);
          return next;
        });
      }
    },
    [expanded, childrenMap]
  );

  const handleDoubleClick = async (node: TreeNode) => {
    if (node.type === "connection") {
      const isActive = activeConnections.has(node.connectionId);
      if (!isActive) {
        const conn = connections.find((c) => c.id === node.connectionId);
        if (conn) {
          setConnectingId(conn.id);
          try {
            await connectTo(conn);
            setTimeout(() => toggleExpand(node), 100);
          } catch {}
          finally { setConnectingId(null); }
        }
      }
      return;
    }

    if (node.type !== "table" && node.type !== "view") return;

    const tabId = `${node.connectionId}:${node.database}:${node.schema}:${node.tableName}`;
    const tab: TabInfo = {
      id: tabId,
      type: "table",
      title: `${node.database}.${node.schema}.${node.tableName}`,
      connectionId: node.connectionId,
      connectionColor: node.connectionColor,
      database: node.database!,
      schema: node.schema!,
      table: node.tableName,
    };
    openTab(tab);
  };

  const handleOpenQueryTab = (connId: string, color: string, dbName: string) => {
    const tabId = `query:${connId}:${dbName}:${Date.now()}`;
    const tab: TabInfo = {
      id: tabId,
      type: "query",
      title: `Query - ${dbName}`,
      connectionId: connId,
      connectionColor: color,
      database: dbName,
      schema: "",
    };
    openTab(tab);
  };

  const handleViewDdl = (node: TreeNode) => {
    const objType = node.type === "view" ? "VIEW" : "TABLE";
    const tabId = `ddl:${node.connectionId}:${node.database}:${node.schema}:${node.tableName}`;
    const tab: TabInfo = {
      id: tabId,
      type: "ddl",
      title: `DDL: ${node.tableName}`,
      connectionId: node.connectionId,
      connectionColor: node.connectionColor,
      database: node.database!,
      schema: node.schema!,
      table: node.tableName,
      objectType: objType,
    };
    openTab(tab);
  };

  const handleViewStructure = (node: TreeNode) => {
    const tabId = `structure:${node.connectionId}:${node.database}:${node.schema}:${node.tableName}`;
    const tab: TabInfo = {
      id: tabId,
      type: "structure",
      title: `Structure: ${node.tableName}`,
      connectionId: node.connectionId,
      connectionColor: node.connectionColor,
      database: node.database!,
      schema: node.schema!,
      table: node.tableName,
    };
    openTab(tab);
  };

  const handleDropObject = (node: TreeNode) => {
    const objType = node.type === "view" ? "VIEW" : "TABLE";
    setConfirmAction({
      title: `Drop ${objType}`,
      message: `Are you sure you want to drop ${objType.toLowerCase()} "${node.schema}.${node.tableName}"? This action cannot be undone.`,
      node,
      action: async () => {
        await api.dropObject({
          connection_id: node.connectionId,
          database: node.database!,
          schema: node.schema!,
          object_name: node.tableName!,
          object_type: objType,
        });
        const parentId = node.type === "view"
          ? `${node.connectionId}:${node.database}:${node.schema}:views`
          : `${node.connectionId}:${node.database}:${node.schema}:tables`;
        setChildrenMap((prev) => {
          const next = { ...prev };
          delete next[parentId];
          return next;
        });
        setExpanded((prev) => {
          const next = new Set(prev);
          next.delete(parentId);
          return next;
        });
        setTimeout(() => {
          const parentNode: TreeNode = {
            id: parentId,
            label: node.type === "view" ? "Views" : "Tables",
            type: node.type === "view" ? "view-group" : "table-group",
            connectionId: node.connectionId,
            connectionColor: node.connectionColor,
            database: node.database,
            schema: node.schema,
          };
          toggleExpand(parentNode);
        }, 50);
      },
    });
  };

  const handleTruncateTable = (node: TreeNode) => {
    setConfirmAction({
      title: "Truncate Table",
      message: `Are you sure you want to truncate table "${node.schema}.${node.tableName}"? All data will be permanently deleted.`,
      node,
      action: async () => {
        await api.truncateTable({
          connection_id: node.connectionId,
          database: node.database!,
          schema: node.schema!,
          table_name: node.tableName!,
        });
      },
    });
  };

  const handleViewStats = (node: TreeNode) => {
    const tabId = `stats:${node.connectionId}:${node.database}:${node.schema}:${node.tableName}`;
    const tab: TabInfo = {
      id: tabId,
      type: "stats",
      title: `Stats: ${node.tableName}`,
      connectionId: node.connectionId,
      connectionColor: node.connectionColor,
      database: node.database!,
      schema: node.schema!,
      table: node.tableName,
    };
    openTab(tab);
  };

  const handleOpenActivity = (node: TreeNode) => {
    const tabId = `activity:${node.connectionId}:${Date.now()}`;
    const tab: TabInfo = {
      id: tabId,
      type: "activity",
      title: `Activity: ${node.label}`,
      connectionId: node.connectionId,
      connectionColor: node.connectionColor,
      database: node.database || "",
      schema: "",
    };
    openTab(tab);
  };

  const handleRefreshNode = (node: TreeNode) => {
    setChildrenMap((prev) => {
      const next = { ...prev };
      delete next[node.id];
      return next;
    });
    setExpanded((prev) => {
      const next = new Set(prev);
      next.delete(node.id);
      return next;
    });
    setTimeout(() => toggleExpand(node), 50);
  };

  const handleContextMenu = (e: React.MouseEvent, node: TreeNode) => {
    e.preventDefault();
    e.stopPropagation();
    setContextMenu({ x: e.clientX, y: e.clientY, node });
  };

  const renderNode = (node: TreeNode, depth: number) => {
    const isExpanded = expanded.has(node.id);
    const isLoading = loadingNodes.has(node.id);
    const children = childrenMap[node.id];
    const isLeaf = node.type === "table" || node.type === "view" || node.type === "function";

    const Icon = () => {
      if (isLoading) return <Loader2 size={14} className="spin" />;
      switch (node.type) {
        case "database":
          return <Database size={14} />;
        case "table":
          return <Table2 size={14} />;
        case "view":
          return <Eye size={14} />;
        case "function":
          return <Zap size={14} />;
        case "table-group":
        case "view-group":
        case "function-group":
          return isExpanded ? <FolderOpen size={14} /> : <Folder size={14} />;
        default:
          return <Database size={14} />;
      }
    };

    return (
      <div key={node.id}>
        <div
          className={`tree-node ${isLeaf ? "leaf" : ""}`}
          style={{ paddingLeft: depth * 12 + 6 }}
          onClick={() => !isLeaf && toggleExpand(node)}
          onDoubleClick={() => handleDoubleClick(node)}
          onContextMenu={(e) => handleContextMenu(e, node)}
        >
          {!isLeaf && (
            <span className="tree-chevron">
              {isExpanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
            </span>
          )}
          {isLeaf && <span className="tree-chevron-spacer" />}
          <span className="tree-icon">
            <Icon />
          </span>
          <span className="tree-label">{node.label}</span>
          {node.type === "database" && (
            <button
              className="btn-icon tree-action"
              onClick={(e) => {
                e.stopPropagation();
                handleOpenQueryTab(node.connectionId, node.connectionColor, node.database!);
              }}
              title="New Query"
            >
              <Terminal size={12} />
            </button>
          )}
        </div>
        {isExpanded && children && (
          <div className="tree-children">
            {children.length === 0 ? (
              <div
                className="tree-node tree-empty"
                style={{ paddingLeft: (depth + 1) * 12 + 6 }}
              >
                <span className="tree-label text-muted">(empty)</span>
              </div>
            ) : (
              filterNodes(children).map((child) => renderNode(child, depth + 1))
            )}
          </div>
        )}
      </div>
    );
  };

  const renderContextMenu = () => {
    if (!contextMenu) return null;
    const { node } = contextMenu;

    const items: { label: string; action: () => void }[] = [];

    if (node.type === "table" || node.type === "view") {
      items.push({
        label: "Open Table",
        action: () => handleDoubleClick(node),
      });
      items.push({
        label: "View Structure",
        action: () => handleViewStructure(node),
      });
      items.push({
        label: "View DDL",
        action: () => handleViewDdl(node),
      });
      items.push({
        label: `Drop ${node.type === "view" ? "View" : "Table"}`,
        action: () => handleDropObject(node),
      });
      if (node.type === "table") {
        items.push({
          label: "Alter Table",
          action: () => onAlterTable?.(node.connectionId, node.connectionColor, node.database!, node.schema!, node.tableName!),
        });
        items.push({
          label: "View Stats",
          action: () => handleViewStats(node),
        });
        items.push({
          label: "Import Data",
          action: () => onImportData?.(node.connectionId, node.connectionColor, node.database!, node.schema!, node.tableName!),
        });
        items.push({
          label: "Truncate Table",
          action: () => handleTruncateTable(node),
        });
      }
    }

    if (node.type === "function") {
      items.push({
        label: "View DDL",
        action: () => {
          const tabId = `ddl:${node.connectionId}:${node.database}:${node.schema}:fn:${node.tableName}`;
          const tab: TabInfo = {
            id: tabId,
            type: "ddl",
            title: `DDL: ${node.tableName}()`,
            connectionId: node.connectionId,
            connectionColor: node.connectionColor,
            database: node.database!,
            schema: node.schema!,
            table: node.tableName,
            objectType: "FUNCTION",
          };
          openTab(tab);
        },
      });
    }

    if (node.type === "schema" || node.type === "table-group") {
      items.push({
        label: "Create Table",
        action: () => {
          onCreateTable?.(
            node.connectionId,
            node.connectionColor,
            node.database!,
            node.schema!
          );
        },
      });
    }

    if (node.type === "connection") {
      const isActive = activeConnections.has(node.connectionId);
      const conn = connections.find((c) => c.id === node.connectionId);

      if (isActive) {
        items.push({
          label: "Disconnect",
          action: async () => { await disconnectFrom(node.connectionId); },
        });
        items.push({
          label: "Server Activity",
          action: () => handleOpenActivity(node),
        });
        items.push({
          label: "Manage Roles",
          action: () => {
            const tabId = `roles:${node.connectionId}`;
            const tab: TabInfo = {
              id: tabId,
              type: "roles",
              title: `Roles: ${node.label}`,
              connectionId: node.connectionId,
              connectionColor: node.connectionColor,
              database: "",
              schema: "",
            };
            openTab(tab);
          },
        });
      } else {
        items.push({
          label: "Connect",
          action: async () => {
            if (conn) {
              setConnectingId(conn.id);
              try { await connectTo(conn); } catch {} finally { setConnectingId(null); }
            }
          },
        });
      }

      if (conn) {
        items.push({
          label: "Edit Connection",
          action: () => setEditingConn(conn),
        });
        items.push({
          label: "Delete Connection",
          action: () => setDeleteTarget(conn),
        });
      }
    }

    if (node.type === "database") {
      items.push({
        label: "New Query",
        action: () =>
          handleOpenQueryTab(node.connectionId, node.connectionColor, node.database!),
      });
      items.push({
        label: "Backup / Restore",
        action: () => onBackupRestore?.(node.connectionId, node.connectionColor, node.database!),
      });
    }

    if (!["table", "view"].includes(node.type)) {
      items.push({
        label: "Refresh",
        action: () => handleRefreshNode(node),
      });
    }

    return (
      <>
        <div className="context-backdrop" onClick={() => setContextMenu(null)} />
        <div
          className="context-menu"
          style={{ left: contextMenu.x, top: contextMenu.y }}
        >
          {items.map((item, i) => (
            <button
              key={i}
              onClick={() => {
                item.action();
                setContextMenu(null);
              }}
            >
              {item.label}
            </button>
          ))}
        </div>
      </>
    );
  };

  const filterNodes = (nodes: TreeNode[]): TreeNode[] => {
    if (!searchQuery.trim()) return nodes;
    const q = searchQuery.toLowerCase();
    return nodes.filter((n) => n.label.toLowerCase().includes(q));
  };

  return (
    <>
      <div className="sidebar-header">
        <span>Explorer</span>
        <button className="btn-icon" onClick={onAddConnection} title="New Connection">
          <Plus size={16} />
        </button>
      </div>
      <div className="sidebar-content">
        {connections.length === 0 ? (
          <div className="tree-empty-state">
            <p>No connections yet</p>
            <button className="btn-secondary" onClick={onAddConnection} style={{ marginTop: 8 }}>
              <Plus size={14} /> Add Connection
            </button>
          </div>
        ) : (
          <>
            <div className="tree-search">
              <SearchIcon size={13} />
              <input
                placeholder="Search objects..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
              />
            </div>
            {connections.map((conn) => {
              const isActive = activeConnections.has(conn.id);
              const isConnecting = connectingId === conn.id;
              const node: TreeNode = {
                id: conn.id,
                label: conn.name || conn.database,
                type: "connection",
                connectionId: conn.id,
                connectionColor: conn.color,
              };
              return (
                <div key={conn.id} className="tree-root">
                  <div className="tree-root-header">
                    <div
                      className="connection-dot"
                      style={{
                        background: isActive ? "var(--success)" : "var(--text-muted)",
                        boxShadow: isActive ? "0 0 6px var(--success)" : "none",
                      }}
                    />
                    <div style={{ flex: 1, minWidth: 0 }}>
                      {isActive ? (
                        renderNode(node, 0)
                      ) : (
                        <div
                          className="tree-node"
                          style={{ paddingLeft: 6 }}
                          onDoubleClick={() => handleDoubleClick(node)}
                          onContextMenu={(e) => handleContextMenu(e, node)}
                        >
                          {isConnecting ? (
                            <Loader2 size={14} className="spin" style={{ color: "var(--accent)" }} />
                          ) : (
                            <span className="tree-icon"><Database size={14} /></span>
                          )}
                          <span className="tree-label" style={{ color: "var(--text-muted)" }}>
                            {conn.name || conn.database}
                          </span>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              );
            })}
          </>
        )}
        {renderContextMenu()}
        {confirmAction && (
          <ConfirmDialog
            title={confirmAction.title}
            message={confirmAction.message}
            confirmLabel={confirmAction.title}
            danger
            onConfirm={async () => {
              try {
                await confirmAction.action();
              } catch (err) {
                console.error("Action failed:", err);
              } finally {
                setConfirmAction(null);
              }
            }}
            onCancel={() => setConfirmAction(null)}
          />
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
          onConfirm={async () => {
            await removeConnection(deleteTarget.id);
            setDeleteTarget(null);
          }}
          onCancel={() => setDeleteTarget(null)}
        />
      )}
    </>
  );
}
