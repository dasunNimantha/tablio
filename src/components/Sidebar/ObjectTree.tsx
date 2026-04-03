import { useState, useCallback, useRef, useEffect, useMemo, memo } from "react";
import { createPortal } from "react-dom";
import { useConnectionStore } from "../../stores/connectionStore";
import { useTabStore, TabInfo } from "../../stores/tabStore";
import { useShallow } from "zustand/react/shallow";
import { api, DatabaseInfo, SchemaInfo, TableInfo, FunctionInfo } from "../../lib/tauri";
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
  Filter,
  Power,
  PowerOff,
  Pencil,
  Trash2,
  Check,
  X,
  Layers,
} from "lucide-react";
import "./Sidebar.css";

const MAX_FOLDER_NAME_LENGTH = 50;

interface TreeNode {
  id: string;
  label: string;
  type: "connection" | "database" | "schema" | "table-group" | "view-group" | "function-group" | "table" | "view" | "function";
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
  onDumpRestore?: (connectionId: string, database: string) => void;
}

export const ObjectTree = memo(function ObjectTree({ onAddConnection, onCreateTable, onAlterTable, onImportData, onBackupRestore, onDumpRestore }: ObjectTreeProps) {
  const { connections, activeConnections, connectTo, disconnectFrom, removeConnection, updateConnection } = useConnectionStore(useShallow((s) => ({
    connections: s.connections,
    activeConnections: s.activeConnections,
    connectTo: s.connectTo,
    disconnectFrom: s.disconnectFrom,
    removeConnection: s.removeConnection,
    updateConnection: s.updateConnection,
  })));
  const openTab = useTabStore((s) => s.openTab);
  const [editingConn, setEditingConn] = useState<import("../../lib/tauri").ConnectionConfig | null>(null);
  const [duplicatingConn, setDuplicatingConn] = useState<import("../../lib/tauri").ConnectionConfig | null>(null);
  const [connectingId, setConnectingId] = useState<string | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<import("../../lib/tauri").ConnectionConfig | null>(null);
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(new Set());
  const [creatingFolder, setCreatingFolder] = useState(false);
  const [newFolderName, setNewFolderName] = useState("");
  const [renamingGroup, setRenamingGroup] = useState<string | null>(null);
  const [renameValue, setRenameValue] = useState("");
  const [groupContextMenu, setGroupContextMenu] = useState<{ x: number; y: number; group: string } | null>(null);
  const [dragOverGroup, setDragOverGroup] = useState<string | null>(null);
  const [draggingConnId, setDraggingConnId] = useState<string | null>(null);
  const [emptyGroups, setEmptyGroups] = useState<Set<string>>(() => {
    try {
      const raw = localStorage.getItem("tablio-empty-folders");
      return raw ? new Set(JSON.parse(raw) as string[]) : new Set();
    } catch { return new Set(); }
  });
  const dragCounterRef = useRef<Record<string, number>>({});
  const newFolderInputRef = useRef<HTMLInputElement>(null);
  const renameInputRef = useRef<HTMLInputElement>(null);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [childrenMap, setChildrenMap] = useState<Record<string, TreeNode[]>>({});
  const [loadingNodes, setLoadingNodes] = useState<Set<string>>(new Set());
  const [contextMenu, setContextMenu] = useState<ContextMenuState | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [showTypeFilter, setShowTypeFilter] = useState(false);
  const [typeFilters, setTypeFilters] = useState({ tables: true, views: true, functions: true });
  const typeFilterRef = useRef<HTMLDivElement>(null);

  const [confirmAction, setConfirmAction] = useState<{
    title: string;
    message: string;
    action: () => Promise<void>;
    node: TreeNode;
  } | null>(null);

  useEffect(() => {
    if (!showTypeFilter) return;
    const handleClick = (e: MouseEvent) => {
      if (typeFilterRef.current && !typeFilterRef.current.contains(e.target as Node)) {
        setShowTypeFilter(false);
      }
    };
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [showTypeFilter]);

  useEffect(() => {
    try {
      localStorage.setItem("tablio-empty-folders", JSON.stringify([...emptyGroups]));
    } catch {}
  }, [emptyGroups]);

  useEffect(() => {
    if (creatingFolder && newFolderInputRef.current) {
      newFolderInputRef.current.focus();
    }
  }, [creatingFolder]);

  useEffect(() => {
    if (renamingGroup && renameInputRef.current) {
      renameInputRef.current.focus();
      renameInputRef.current.select();
    }
  }, [renamingGroup]);

  useEffect(() => {
    if (!groupContextMenu) return;
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setGroupContextMenu(null);
    };
    document.addEventListener("keydown", handleKey);
    return () => document.removeEventListener("keydown", handleKey);
  }, [groupContextMenu]);

  const handleCreateFolder = () => {
    const name = newFolderName.trim().slice(0, MAX_FOLDER_NAME_LENGTH);
    if (name) {
      setEmptyGroups((prev) => new Set(prev).add(name));
      setCollapsedGroups((prev) => {
        const next = new Set(prev);
        next.delete(name);
        return next;
      });
    }
    setCreatingFolder(false);
    setNewFolderName("");
  };

  const handleRenameGroup = async (oldName: string) => {
    const newName = renameValue.trim().slice(0, MAX_FOLDER_NAME_LENGTH);
    if (!newName || newName === oldName) {
      setRenamingGroup(null);
      return;
    }
    const oldLower = oldName.toLowerCase();
    const toUpdate = connections.filter((c) => c.group?.trim().toLowerCase() === oldLower);
    for (const conn of toUpdate) {
      await updateConnection({ ...conn, group: newName });
    }
    setEmptyGroups((prev) => {
      const next = new Set([...prev].filter((g) => g.toLowerCase() !== oldLower));
      if (toUpdate.length === 0) next.add(newName);
      return next;
    });
    setCollapsedGroups((prev) => {
      const next = new Set([...prev].filter((g) => g.toLowerCase() !== oldLower));
      return next;
    });
    setRenamingGroup(null);
  };

  const handleDeleteGroup = async (groupName: string) => {
    const lower = groupName.toLowerCase();
    const toUpdate = connections.filter((c) => c.group?.trim().toLowerCase() === lower);
    for (const conn of toUpdate) {
      await updateConnection({ ...conn, group: null });
    }
    setEmptyGroups((prev) => {
      return new Set([...prev].filter((g) => g.toLowerCase() !== lower));
    });
  };

  const preserveSourceGroup = (conn: import("../../lib/tauri").ConnectionConfig) => {
    const sourceGroup = conn.group?.trim();
    if (sourceGroup) {
      const sourceLower = sourceGroup.toLowerCase();
      const othersInGroup = connections.filter((c) => c.id !== conn.id && c.group?.trim().toLowerCase() === sourceLower);
      if (othersInGroup.length === 0) {
        setEmptyGroups((prev) => new Set(prev).add(sourceGroup));
      }
    }
  };

  const handleMoveToGroup = async (conn: import("../../lib/tauri").ConnectionConfig, group: string | null) => {
    preserveSourceGroup(conn);
    await updateConnection({ ...conn, group });
  };

  const handleDrop = async (e: React.DragEvent, targetGroup: string | null) => {
    e.preventDefault();
    setDragOverGroup(null);
    setDraggingConnId(null);
    dragCounterRef.current = {};
    const connId = e.dataTransfer.getData("text/connection-id");
    if (!connId) return;
    const conn = connections.find((c) => c.id === connId);
    if (conn && (conn.group?.trim()?.toLowerCase() || null) !== (targetGroup?.toLowerCase() || null)) {
      preserveSourceGroup(conn);
      await updateConnection({ ...conn, group: targetGroup });
    }
  };

  const expandNode = useCallback(
    async (node: TreeNode) => {
      const nodeId = node.id;
      if (expanded.has(nodeId)) return;

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
            setTimeout(() => expandNode(dbNode), 50);
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

      await expandNode(node);
    },
    [expanded, expandNode]
  );

  const handleConnectDirect = async (conn: import("../../lib/tauri").ConnectionConfig) => {
    if (connectingId) return;
    setConnectingId(conn.id);
    try {
      await connectTo(conn);
      openTab({
        id: `activity:${conn.id}`,
        type: "activity",
        title: `Activity: ${conn.name || conn.database}`,
        connectionId: conn.id,
        connectionColor: conn.color || "#5284e0",
        database: conn.database,
        schema: "",
      });
    } catch (e) {
      console.error("Connection failed:", e);
    } finally { setConnectingId(null); }
  };

  const handleDoubleClick = async (node: TreeNode) => {
    if (node.type === "connection") {
      const isActive = activeConnections.has(node.connectionId);
      if (!isActive) {
        const conn = connections.find((c) => c.id === node.connectionId);
        if (conn) {
          setConnectingId(conn.id);
          try {
            await connectTo(conn);
            openTab({
              id: `activity:${conn.id}`,
              type: "activity",
              title: `Activity: ${conn.name || conn.database}`,
              connectionId: conn.id,
              connectionColor: conn.color || "#5284e0",
              database: conn.database,
              schema: "",
            });
            await expandNode(node);
          } catch (e) {
            console.error("Connection failed:", e);
          } finally { setConnectingId(null); }
        }
      }
      return;
    }

    if (node.type !== "table" && node.type !== "view") return;

    const tabId = `${node.connectionId}:${node.database}:${node.schema}:${node.tableName}`;
    const tab: TabInfo = {
      id: tabId,
      type: "table",
      title: node.database === node.schema
        ? `${node.schema}.${node.tableName}`
        : `${node.database}.${node.schema}.${node.tableName}`,
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
    const z = parseFloat(document.documentElement.style.zoom || "100") / 100;
    setContextMenu({ x: e.clientX / z, y: e.clientY / z, node });
  };

  const renderNode = (node: TreeNode, depth: number) => {
    const children = childrenMap[node.id];
    const isLeaf = node.type === "table" || node.type === "view" || node.type === "function";
    const q = searchQuery.trim().toLowerCase();
    const selfMatches = hasActiveFilter && q && node.label.toLowerCase().includes(q);
    const descendantMatches = hasActiveFilter && !selfMatches && children?.some((c) => nodeMatchesFilter(c, q));
    const isExpanded = expanded.has(node.id) || (hasActiveFilter && !!descendantMatches);
    const isLoading = loadingNodes.has(node.id);

    const getIconInfo = (): { icon: React.ReactNode; cls: string } => {
      if (isLoading) return { icon: <Loader2 size={15} className="spin" />, cls: "" };
      switch (node.type) {
        case "connection":
          return { icon: <Database size={15} />, cls: "icon-connection" };
        case "database":
          return { icon: <Database size={15} />, cls: "icon-database" };
        case "schema":
          return { icon: <Layers size={15} />, cls: "icon-schema" };
        case "table":
          return { icon: <Table2 size={15} />, cls: "icon-table" };
        case "view":
          return { icon: <Eye size={15} />, cls: "icon-view" };
        case "function":
          return { icon: <Zap size={15} />, cls: "icon-function" };
        case "table-group":
        case "view-group":
        case "function-group":
          return { icon: isExpanded ? <FolderOpen size={15} /> : <Folder size={15} />, cls: "icon-folder" };
        default:
          return { icon: <Database size={15} />, cls: "icon-database" };
      }
    };
    const iconInfo = getIconInfo();

    return (
      <div key={node.id}>
        <div
          className={`tree-node ${isLeaf ? "leaf" : ""}`}
          style={{ paddingLeft: depth * 12 + 6 }}
          onClick={() => {
            if (isLeaf) {
              handleDoubleClick(node);
            } else {
              toggleExpand(node);
            }
          }}
          onDoubleClick={() => !isLeaf && handleDoubleClick(node)}
          onContextMenu={(e) => handleContextMenu(e, node)}
        >
          {!isLeaf && (
            <span className="tree-chevron">
              {isExpanded ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
            </span>
          )}
          {isLeaf && <span className="tree-chevron-spacer" />}
          <span className={`tree-icon ${iconInfo.cls}`}>
            {iconInfo.icon}
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

    const items: { label: string; action: () => void; children?: { label: string; action: () => void }[] }[] = [];

    if (node.type === "table" || node.type === "view") {
      items.push({
        label: "Open Table",
        action: () => handleDoubleClick(node),
      });
      items.push({
        label: "Query",
        action: () => {
          const tabId = `query:${node.connectionId}:${node.database}:${node.tableName}:${Date.now()}`;
          const tab: TabInfo = {
            id: tabId,
            type: "query",
            title: `Query - ${node.tableName}`,
            connectionId: node.connectionId,
            connectionColor: node.connectionColor,
            database: node.database!,
            schema: "",
          };
          openTab(tab);
        },
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
        label: "View ERD",
        action: () => {
          const tabId = `erd:${node.connectionId}:${node.database}:${node.schema}`;
          const tab: TabInfo = {
            id: tabId,
            type: "erd",
            title: `ERD: ${node.schema}`,
            connectionId: node.connectionId,
            connectionColor: node.connectionColor,
            database: node.database!,
            schema: node.schema!,
          };
          openTab(tab);
        },
      });
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
          label: "Query Statistics",
          action: () => {
            const tabId = `querystats:${node.connectionId}:${Date.now()}`;
            const tab: TabInfo = {
              id: tabId,
              type: "querystats",
              title: `Query Stats: ${node.label}`,
              connectionId: node.connectionId,
              connectionColor: node.connectionColor,
              database: "",
              schema: "",
            };
            openTab(tab);
          },
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
              try {
                await connectTo(conn);
                openTab({
                  id: `activity:${conn.id}`,
                  type: "activity",
                  title: `Activity: ${conn.name || conn.database}`,
                  connectionId: conn.id,
                  connectionColor: conn.color || "#5284e0",
                  database: conn.database,
                  schema: "",
                });
              } catch (e) { console.error("Connection failed:", e); } finally { setConnectingId(null); }
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
          label: "Duplicate Connection",
          action: () => setDuplicatingConn(conn),
        });

        const existingGroups = Object.keys(grouped.groups).sort();
        const currentGroupLower = conn.group?.trim()?.toLowerCase() || null;
        const moveChildren: { label: string; action: () => void }[] = [];
        for (const g of existingGroups) {
          if (g.toLowerCase() !== currentGroupLower) {
            moveChildren.push({
              label: g,
              action: () => handleMoveToGroup(conn, g),
            });
          }
        }
        if (currentGroupLower) {
          if (moveChildren.length > 0) {
            moveChildren.push({ label: "---", action: () => {} });
          }
          moveChildren.push({
            label: "Remove from folder",
            action: () => handleMoveToGroup(conn, null),
          });
        }
        if (moveChildren.length > 0) {
          items.push({ label: "---", action: () => {} });
          items.push({
            label: "Move to",
            action: () => {},
            children: moveChildren,
          });
        }

        items.push({ label: "---", action: () => {} });
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
      items.push({
        label: "Dump & Restore To…",
        action: () => onDumpRestore?.(node.connectionId, node.database!),
      });
    }

    if (!["table", "view"].includes(node.type)) {
      items.push({
        label: "Refresh",
        action: () => handleRefreshNode(node),
      });
    }

    return createPortal(
      <>
        <div className="context-backdrop" onClick={() => setContextMenu(null)} />
        <div
          className="context-menu"
          style={{ left: contextMenu.x, top: contextMenu.y }}
          ref={(el) => {
            if (!el) return;
            const z = parseFloat(document.documentElement.style.zoom || "100") / 100;
            const cssVh = window.innerHeight / z;
            const cssVw = window.innerWidth / z;
            if (contextMenu.y + el.offsetHeight > cssVh) {
              el.style.top = `${Math.max(4, cssVh - el.offsetHeight)}px`;
            }
            if (contextMenu.x + el.offsetWidth > cssVw) {
              el.style.left = `${Math.max(4, cssVw - el.offsetWidth)}px`;
            }
          }}
        >
          {items.map((item, i) =>
            item.label === "---" ? (
              <div key={i} className="context-menu-separator" />
            ) : item.children ? (
              <div key={i} className="context-menu-submenu">
                <button className="context-menu-submenu-trigger">
                  {item.label}
                  <ChevronRight size={12} />
                </button>
                <div className="context-menu-submenu-content">
                  {item.children.map((child, j) =>
                    child.label === "---" ? (
                      <div key={j} className="context-menu-separator" />
                    ) : (
                      <button
                        key={j}
                        onClick={() => {
                          child.action();
                          setContextMenu(null);
                        }}
                      >
                        {child.label}
                      </button>
                    )
                  )}
                </div>
              </div>
            ) : (
              <button
                key={i}
                onClick={() => {
                  item.action();
                  setContextMenu(null);
                }}
              >
                {item.label}
              </button>
            )
          )}
        </div>
      </>,
      document.body
    );
  };

  const isTypeVisible = useCallback(
    (node: TreeNode): boolean => {
      if (node.type === "table" || node.type === "table-group") return typeFilters.tables;
      if (node.type === "view" || node.type === "view-group") return typeFilters.views;
      if (node.type === "function" || node.type === "function-group") return typeFilters.functions;
      return true;
    },
    [typeFilters]
  );

  const nodeMatchesFilter = useCallback(
    (node: TreeNode, q: string): boolean => {
      if (!isTypeVisible(node)) return false;
      const textMatch = !q || node.label.toLowerCase().includes(q);
      if (textMatch && (node.type === "table" || node.type === "view" || node.type === "function")) return true;
      if (textMatch && !q && node.type !== "table" && node.type !== "view" && node.type !== "function") return true;
      const children = childrenMap[node.id];
      if (children) {
        return children.some((child) => nodeMatchesFilter(child, q));
      }
      return !q;
    },
    [childrenMap, isTypeVisible]
  );

  const allTypesEnabled = typeFilters.tables && typeFilters.views && typeFilters.functions;
  const hasActiveFilter = !!searchQuery.trim() || !allTypesEnabled;

  const filterNodes = (nodes: TreeNode[]): TreeNode[] => {
    if (!hasActiveFilter) return nodes;
    const q = searchQuery.trim().toLowerCase();
    return nodes.filter((n) => nodeMatchesFilter(n, q));
  };

  const grouped = useMemo(() => {
    const groups: Record<string, typeof connections> = {};
    const canonicalKey: Record<string, string> = {};
    const ungrouped: typeof connections = [];
    for (const g of emptyGroups) {
      const lower = g.toLowerCase();
      if (!canonicalKey[lower]) canonicalKey[lower] = g;
      const key = canonicalKey[lower];
      if (!groups[key]) groups[key] = [];
    }
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
  }, [connections, emptyGroups]);

  const toggleGroup = (group: string) => {
    setCollapsedGroups((prev) => {
      const next = new Set(prev);
      if (next.has(group)) next.delete(group);
      else next.add(group);
      return next;
    });
  };

  const renderConnectionNode = (conn: import("../../lib/tauri").ConnectionConfig) => {
    const isActive = activeConnections.has(conn.id);
    const isConn = connectingId === conn.id;
    const node: TreeNode = {
      id: conn.id,
      label: conn.name || conn.database,
      type: "connection",
      connectionId: conn.id,
      connectionColor: conn.color,
    };
    return (
      <div
        key={conn.id}
        className={`tree-root${draggingConnId === conn.id ? " tree-root--dragging" : ""}`}
        draggable
        onDragStart={(e) => {
          e.dataTransfer.setData("text/connection-id", conn.id);
          e.dataTransfer.effectAllowed = "move";
          setDraggingConnId(conn.id);
        }}
        onDragEnd={() => {
          setDraggingConnId(null);
          setDragOverGroup(null);
          dragCounterRef.current = {};
        }}
      >
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
              <div className="tree-conn-row">
                {renderNode(node, 0)}
                <div className="tree-conn-actions">
                  <button
                    className="btn-icon tree-action"
                    onClick={(e) => { e.stopPropagation(); disconnectFrom(conn.id); }}
                    title="Disconnect"
                  >
                    <PowerOff size={12} />
                  </button>
                  <button
                    className="btn-icon tree-action"
                    onClick={(e) => { e.stopPropagation(); setEditingConn(conn); }}
                    title="Edit"
                  >
                    <Pencil size={12} />
                  </button>
                  <button
                    className="btn-icon tree-action tree-action-danger"
                    onClick={(e) => {
                      e.stopPropagation();
                      setConfirmAction({
                        title: "Delete Connection",
                        message: `Delete "${conn.name || conn.database}"? This cannot be undone.`,
                        action: () => removeConnection(conn.id),
                        node,
                      });
                    }}
                    title="Delete"
                  >
                    <Trash2 size={12} />
                  </button>
                </div>
              </div>
            ) : (
              <div
                className="tree-node tree-conn-row"
                style={{ paddingLeft: 6 }}
                onDoubleClick={() => handleDoubleClick(node)}
                onContextMenu={(e) => handleContextMenu(e, node)}
              >
                {isConn ? (
                  <Loader2 size={14} className="spin" style={{ color: "var(--accent)" }} />
                ) : (
                  <span className="tree-icon icon-connection"><Database size={14} /></span>
                )}
                <span className="tree-label" style={{ color: "var(--text-muted)" }}>
                  {conn.name || conn.database}
                </span>
                <div className="tree-conn-actions">
                  <button
                    className="btn-icon tree-action"
                    onClick={(e) => { e.stopPropagation(); handleConnectDirect(conn); }}
                    title="Connect"
                  >
                    <Power size={12} />
                  </button>
                  <button
                    className="btn-icon tree-action"
                    onClick={(e) => { e.stopPropagation(); setEditingConn(conn); }}
                    title="Edit"
                  >
                    <Pencil size={12} />
                  </button>
                  <button
                    className="btn-icon tree-action tree-action-danger"
                    onClick={(e) => {
                      e.stopPropagation();
                      setConfirmAction({
                        title: "Delete Connection",
                        message: `Delete "${conn.name || conn.database}"? This cannot be undone.`,
                        action: () => removeConnection(conn.id),
                        node,
                      });
                    }}
                    title="Delete"
                  >
                    <Trash2 size={12} />
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    );
  };

  return (
    <>
      <div className="sidebar-header">
        <span>Explorer</span>
        <div style={{ display: "flex", gap: 2 }}>
          <button className="btn-icon" onClick={onAddConnection} title="New Connection">
            <Plus size={16} />
          </button>
          <button className="btn-icon" onClick={() => { setCreatingFolder(true); setNewFolderName(""); }} title="New Folder">
            <Folder size={15} />
          </button>
        </div>
      </div>
      <div
        className={`sidebar-content ${dragOverGroup === "__ungrouped__" ? "sidebar-drop-active" : ""}`}
        onDragOver={(e) => { e.preventDefault(); }}
        onDragEnter={(e) => {
          e.preventDefault();
          if (e.currentTarget !== e.target && (e.target as HTMLElement).closest?.(".connection-group")) return;
          if (draggingConnId) {
            const dragged = connections.find((c) => c.id === draggingConnId);
            if (dragged?.group?.trim()) {
              setDragOverGroup("__ungrouped__");
            }
          }
        }}
        onDragLeave={(e) => {
          if (e.currentTarget === e.target || !(e.relatedTarget instanceof Node && e.currentTarget.contains(e.relatedTarget))) {
            if (dragOverGroup === "__ungrouped__") setDragOverGroup(null);
          }
        }}
        onDrop={(e) => handleDrop(e, null)}
      >
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
                placeholder="Filter objects..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
              />
              <div className="tree-type-filter-wrapper" ref={typeFilterRef}>
                <button
                  className={`btn-icon tree-filter-btn ${!allTypesEnabled ? "tree-filter-active" : ""}`}
                  onClick={() => setShowTypeFilter((v) => !v)}
                  title="Filter by type"
                >
                  <Filter size={13} />
                </button>
                {showTypeFilter && (
                  <div className="tree-type-filter-dropdown">
                    <label className="tree-type-filter-item">
                      <input
                        type="checkbox"
                        checked={typeFilters.tables}
                        onChange={(e) => setTypeFilters((f) => ({ ...f, tables: e.target.checked }))}
                      />
                      <Table2 size={13} />
                      <span>Tables</span>
                    </label>
                    <label className="tree-type-filter-item">
                      <input
                        type="checkbox"
                        checked={typeFilters.views}
                        onChange={(e) => setTypeFilters((f) => ({ ...f, views: e.target.checked }))}
                      />
                      <Eye size={13} />
                      <span>Views</span>
                    </label>
                    <label className="tree-type-filter-item">
                      <input
                        type="checkbox"
                        checked={typeFilters.functions}
                        onChange={(e) => setTypeFilters((f) => ({ ...f, functions: e.target.checked }))}
                      />
                      <Zap size={13} />
                      <span>Functions</span>
                    </label>
                  </div>
                )}
              </div>
            </div>
            {creatingFolder && (
              <div className="connection-group-create">
                <Folder size={13} />
                <input
                  ref={newFolderInputRef}
                  className="folder-name-input"
                  value={newFolderName}
                  onChange={(e) => setNewFolderName(e.target.value.slice(0, MAX_FOLDER_NAME_LENGTH))}
                  maxLength={MAX_FOLDER_NAME_LENGTH}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") handleCreateFolder();
                    if (e.key === "Escape") { setCreatingFolder(false); setNewFolderName(""); }
                  }}
                  placeholder="Enter folder name"
                />
                <button className="btn-icon" onClick={handleCreateFolder} title="Create"><Check size={13} /></button>
                <button className="btn-icon" onClick={() => { setCreatingFolder(false); setNewFolderName(""); }} title="Cancel"><X size={13} /></button>
              </div>
            )}
            {Object.entries(grouped.groups)
              .sort(([a], [b]) => a.localeCompare(b))
              .map(([groupName, conns], groupIndex) => {
                const isCollapsed = collapsedGroups.has(groupName);
                const isRenaming = renamingGroup === groupName;
                return (
                  <div
                    key={`group:${groupName}`}
                    className={`connection-group ${groupIndex === 0 ? "connection-group-first" : ""} ${dragOverGroup === groupName ? "drag-over" : ""}`}
                    onDragOver={(e) => { e.preventDefault(); e.stopPropagation(); }}
                    onDragEnter={(e) => {
                      e.preventDefault();
                      e.stopPropagation();
                      const key = `group:${groupName}`;
                      dragCounterRef.current[key] = (dragCounterRef.current[key] || 0) + 1;
                      if (draggingConnId) {
                        const dragged = connections.find((c) => c.id === draggingConnId);
                        if ((dragged?.group?.trim()?.toLowerCase() || null) !== groupName.toLowerCase()) {
                          setDragOverGroup(groupName);
                        }
                      }
                    }}
                    onDragLeave={(e) => {
                      e.stopPropagation();
                      const key = `group:${groupName}`;
                      dragCounterRef.current[key] = (dragCounterRef.current[key] || 1) - 1;
                      if (dragCounterRef.current[key] <= 0) {
                        dragCounterRef.current[key] = 0;
                        if (dragOverGroup === groupName) setDragOverGroup(null);
                      }
                    }}
                    onDrop={(e) => { e.stopPropagation(); handleDrop(e, groupName); }}
                  >
                    <div
                      className="connection-group-header"
                      onClick={() => !isRenaming && toggleGroup(groupName)}
                      onContextMenu={(e) => {
                        e.preventDefault();
                        const z = parseFloat(document.documentElement.style.zoom || "100") / 100;
                        setGroupContextMenu({ x: e.clientX / z, y: e.clientY / z, group: groupName });
                      }}
                    >
                      <span className="connection-group-chevron">
                        {isCollapsed ? <ChevronRight size={12} /> : <ChevronDown size={12} />}
                      </span>
                      {isCollapsed ? (
                        <Folder size={13} className="connection-group-folder-icon" />
                      ) : (
                        <FolderOpen size={13} className="connection-group-folder-icon" />
                      )}
                      {isRenaming ? (
                        <input
                          ref={renameInputRef}
                          className="folder-name-input"
                          value={renameValue}
                          onChange={(e) => setRenameValue(e.target.value.slice(0, MAX_FOLDER_NAME_LENGTH))}
                          maxLength={MAX_FOLDER_NAME_LENGTH}
                          onKeyDown={(e) => {
                            if (e.key === "Enter") handleRenameGroup(groupName);
                            if (e.key === "Escape") setRenamingGroup(null);
                          }}
                          onBlur={() => handleRenameGroup(groupName)}
                          onClick={(e) => e.stopPropagation()}
                        />
                      ) : (
                        <span className="connection-group-name">{groupName}</span>
                      )}
                    </div>
                    {!isCollapsed && (conns.length > 0
                      ? conns.map(renderConnectionNode)
                      : <div className="connection-group-empty">Drag connections here</div>
                    )}
                  </div>
                );
              })}
            {grouped.ungrouped.map(renderConnectionNode)}
            {groupContextMenu && createPortal(
              <>
                <div className="context-backdrop" onClick={() => setGroupContextMenu(null)} />
                <div
                  className="context-menu"
                  style={{ left: groupContextMenu.x, top: groupContextMenu.y }}
                  ref={(el) => {
                    if (!el) return;
                    const z = parseFloat(document.documentElement.style.zoom || "100") / 100;
                    const cssVh = window.innerHeight / z;
                    const cssVw = window.innerWidth / z;
                    if (groupContextMenu.y + el.offsetHeight > cssVh) {
                      el.style.top = `${Math.max(4, cssVh - el.offsetHeight)}px`;
                    }
                    if (groupContextMenu.x + el.offsetWidth > cssVw) {
                      el.style.left = `${Math.max(4, cssVw - el.offsetWidth)}px`;
                    }
                  }}
                >
                  <button onClick={() => {
                    setRenamingGroup(groupContextMenu.group);
                    setRenameValue(groupContextMenu.group);
                    setGroupContextMenu(null);
                  }}>Rename Folder</button>
                  <button onClick={() => {
                    handleDeleteGroup(groupContextMenu.group);
                    setGroupContextMenu(null);
                  }}>Delete Folder</button>
                </div>
              </>,
              document.body
            )}
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
      {duplicatingConn && (
        <ConnectionDialog
          editConfig={duplicatingConn}
          duplicate
          onClose={() => setDuplicatingConn(null)}
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
});
