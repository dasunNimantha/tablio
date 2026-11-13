import { useState, useCallback, useRef, useEffect, useMemo, memo } from "react";
import { useConnectionStore } from "../../stores/connectionStore";
import { useTabStore, TabInfo } from "../../stores/tabStore";
import { useShallow } from "zustand/react/shallow";
import { api, DatabaseInfo, SchemaInfo, TableInfo, FunctionInfo } from "../../lib/tauri";
import { readZoomFactor } from "../../lib/zoom";
import { useToastStore } from "../../stores/toastStore";
import { wasCancelledByUser } from "../../stores/connectionStore";
import { errorText, formatConnectFailure } from "../../lib/connectionErrors";
import { ContextMenu, type MenuEntry } from "../Menu/ContextMenu";
import { SCHEMA_POLL_INTERVAL_MS, structureSignature } from "../../lib/schemaSync";
import { usePolling } from "../../hooks/usePolling";
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
  AlertTriangle,
  RotateCw,
  GitBranch,
  ShieldCheck,
} from "lucide-react";
import "./Sidebar.css";

const MAX_FOLDER_NAME_LENGTH = 50;
/** Minimum gap between automatic schema checks triggered by window focus. */
const FOCUS_REFRESH_MIN_INTERVAL_MS = 5_000;
/** When the largest partition is >= this multiple of the median, we badge
 *  the parent as skewed. Picked conservatively so that small fixtures with
 *  natural variance (e.g. one ~3x partition) don't false-positive. */
const PARTITION_SKEW_RATIO = 5;

/** Buckets a flat TableInfo[] into top-level rows + a parent->children map.
 *  A row is "top-level" when it has no `parent_table` *or* its parent isn't
 *  in the same schema (which we treat as orphan and surface at top). */
const groupTablesByParent = (tables: TableInfo[], schema: string) => {
  const childrenByParent: Map<string, TableInfo[]> = new Map();
  const topLevel: TableInfo[] = [];
  for (const t of tables) {
    const parentKey = t.parent_table ?? null;
    const parentInSameSchema = parentKey?.startsWith(`${schema}.`) ?? false;
    if (parentKey && parentInSameSchema) {
      const arr = childrenByParent.get(parentKey) ?? [];
      arr.push(t);
      childrenByParent.set(parentKey, arr);
    } else {
      topLevel.push(t);
    }
  }
  return { topLevel, childrenByParent };
};

/** Returns true if (max non-empty partition size / median non-empty size)
 *  exceeds PARTITION_SKEW_RATIO. Ignores empty partitions so a brand-new
 *  partition coming online doesn't trigger the warning. */
const detectSizeSkew = (children: TableInfo[]): boolean => {
  const sizes = children
    .map((c) => c.total_bytes ?? 0)
    .filter((s) => s > 0)
    .sort((a, b) => a - b);
  if (sizes.length < 3) return false;
  const median = sizes[Math.floor(sizes.length / 2)];
  const max = sizes[sizes.length - 1];
  return median > 0 && max / median >= PARTITION_SKEW_RATIO;
};

/** Recursively build a TreeNode for a single TableInfo. If the table is a
 *  partitioned parent, also eagerly build a synthetic "Partitions"
 *  collection containing all of its direct children (and, recursively,
 *  their own Partitions collections for sub-partitioning). The pre-built
 *  subtree gets attached so the user just expands the chevron and sees it
 *  immediately — no extra round-trip to the backend. */
const buildTableNode = (
  table: TableInfo,
  parentBaseId: string,
  connectionId: string,
  connectionColor: string,
  database: string | undefined,
  schema: string | undefined,
  childrenByParent: Map<string, TableInfo[]>,
): TreeNode => {
  const qualified = `${schema}.${table.name}`;
  const directChildren = childrenByParent.get(qualified) ?? [];
  const isPartitionedParent = !!table.partition_strategy && directChildren.length > 0;
  const tableId = `${parentBaseId}:${table.name}`;

  let partitionGroupChildren: TreeNode[] | undefined;
  if (isPartitionedParent) {
    const baseId = `${tableId}:partitions`;
    partitionGroupChildren = directChildren.map((child) =>
      buildTableNode(
        child,
        baseId,
        connectionId,
        connectionColor,
        database,
        schema,
        childrenByParent,
      ),
    );
  }

  const defaultChild = directChildren.find((p) => p.is_default_partition);
  return {
    id: tableId,
    label: table.name,
    type: "table",
    connectionId,
    connectionColor,
    database,
    schema,
    tableName: table.name,
    tableType: table.table_type,
    partitionStrategy: table.partition_strategy ?? null,
    partitionBound: table.partition_bound ?? null,
    isDefaultPartition: !!table.is_default_partition,
    rowCountEstimate: table.row_count_estimate,
    totalBytes: table.total_bytes ?? null,
    preloadedChildren: partitionGroupChildren
      ? [
          {
            id: `${tableId}:partitions`,
            label: "Partitions",
            type: "partition-group",
            connectionId,
            connectionColor,
            database,
            schema,
            tableName: table.name,
            preloadedChildren: partitionGroupChildren,
          },
        ]
      : undefined,
    hasSkewWarning: isPartitionedParent ? detectSizeSkew(directChildren) : false,
    hasDefaultPartitionRows:
      isPartitionedParent && !!defaultChild && (defaultChild.row_count_estimate ?? 0) > 0,
  };
};

interface TreeNode {
  id: string;
  label: string;
  type:
    | "connection"
    | "database"
    | "schema"
    | "table-group"
    | "view-group"
    | "function-group"
    | "partition-group"
    | "table"
    | "view"
    | "function";
  connectionId: string;
  connectionColor: string;
  database?: string;
  schema?: string;
  tableName?: string;
  tableType?: string;
  /** Partition strategy when this node represents a partitioned parent. */
  partitionStrategy?: "range" | "list" | "hash" | null;
  /** Human-readable bound when this node is a partition child. */
  partitionBound?: string | null;
  /** True when this is the DEFAULT partition. */
  isDefaultPartition?: boolean;
  /** Estimated row count for tables / partitions. */
  rowCountEstimate?: number | null;
  /** Disk size including indexes/toast (bytes) for tables / partitions. */
  totalBytes?: number | null;
  /** Set on partitioned-parent nodes: the partition children, used to build
      the synthetic Partitions sub-collection on expand. */
  partitionChildren?: TableInfo[];
  /** Set on partition-group nodes: pre-built children to render. */
  preloadedChildren?: TreeNode[];
  /** True if a partitioned parent shows skewed sizes across its partitions. */
  hasSkewWarning?: boolean;
  /** True if the DEFAULT partition under this parent has rows in it. */
  hasDefaultPartitionRows?: boolean;
}

interface ContextMenuState {
  x: number;
  y: number;
  node: TreeNode;
  /** Tree row to return focus to when the menu closes. */
  returnFocusTo: HTMLElement | null;
}

/** What the keyboard handler needs to know about each rendered tree row. */
interface TreeItemEntry {
  expandable: boolean;
  expanded: boolean;
  toggle: () => void;
  activate: () => void;
  openMenu: (el: HTMLElement) => void;
  rename?: () => void;
}

/** Context-menu position for a row opened from the keyboard. */
function menuPointFor(el: HTMLElement): { x: number; y: number } {
  const r = el.getBoundingClientRect();
  const z = readZoomFactor();
  return { x: (r.left + Math.min(r.width / 2, 120)) / z, y: r.bottom / z };
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
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(new Set());
  const [creatingFolder, setCreatingFolder] = useState(false);
  const [newFolderName, setNewFolderName] = useState("");
  const [renamingGroup, setRenamingGroup] = useState<string | null>(null);
  const [renameValue, setRenameValue] = useState("");
  const [groupContextMenu, setGroupContextMenu] = useState<{
    x: number;
    y: number;
    group: string;
    returnFocusTo: HTMLElement | null;
  } | null>(null);
  const addToast = useToastStore((s) => s.addToast);
  const [errorNodes, setErrorNodes] = useState<Record<string, string>>({});
  const [focusedId, setFocusedId] = useState<string | null>(null);
  const treeRef = useRef<HTMLDivElement>(null);
  const renamingBusyRef = useRef(false);
  // Rebuilt on every render by renderNode / the folder rows; read by the
  // tree's keyboard handler.
  const treeItems = useRef(new Map<string, TreeItemEntry>());
  treeItems.current = new Map();
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
    confirmLabel?: string;
    /** Rejecting keeps the dialog open and shows the error inline. */
    action: () => Promise<void>;
    node: TreeNode;
  } | null>(null);

  /** Report a failed store action as an error toast. */
  const reportError = useCallback(
    (prefix: string, err: unknown) => {
      console.error(prefix, err);
      addToast(`${prefix}: ${errorText(err)}`, "error");
    },
    [addToast],
  );

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
    if (!showTypeFilter) return;
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setShowTypeFilter(false);
    };
    document.addEventListener("keydown", handleKey);
    return () => document.removeEventListener("keydown", handleKey);
  }, [showTypeFilter]);

  const folderExists = (name: string, except?: string) => {
    const lower = name.toLowerCase();
    if (except && except.toLowerCase() === lower) return false;
    return Object.keys(grouped.groups).some((g) => g.toLowerCase() === lower);
  };

  const handleCreateFolder = () => {
    const name = newFolderName.trim().slice(0, MAX_FOLDER_NAME_LENGTH);
    if (name && folderExists(name)) {
      addToast(`A folder named "${name}" already exists.`, "error");
      newFolderInputRef.current?.focus();
      return;
    }
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
    // Enter commits and unmounts the input, whose blur would commit again.
    if (renamingBusyRef.current) return;
    const newName = renameValue.trim().slice(0, MAX_FOLDER_NAME_LENGTH);
    if (!newName || newName === oldName) {
      setRenamingGroup(null);
      return;
    }
    if (folderExists(newName, oldName)) {
      // Renaming onto an existing folder used to merge the two silently.
      addToast(`A folder named "${newName}" already exists.`, "error");
      setRenameValue(oldName);
      setRenamingGroup(null);
      return;
    }
    renamingBusyRef.current = true;
    const oldLower = oldName.toLowerCase();
    const toUpdate = connections.filter((c) => c.group?.trim().toLowerCase() === oldLower);
    try {
      for (const conn of toUpdate) {
        await updateConnection({ ...conn, group: newName });
      }
    } catch (e) {
      reportError(`Couldn't rename folder "${oldName}"`, e);
    } finally {
      renamingBusyRef.current = false;
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
    try {
      for (const conn of toUpdate) {
        await updateConnection({ ...conn, group: null });
      }
    } catch (e) {
      reportError(`Couldn't delete folder "${groupName}"`, e);
      return;
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
    try {
      await updateConnection({ ...conn, group });
    } catch (e) {
      reportError(`Couldn't move "${conn.name || conn.database}"`, e);
    }
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
      await handleMoveToGroup(conn, targetGroup);
    }
  };

  const fetchChildren = useCallback(async (node: TreeNode): Promise<TreeNode[]> => {
    let children: TreeNode[] = [];

    if (node.type === "connection") {
      const dbs: DatabaseInfo[] = await api.listDatabases(node.connectionId);
      const templateDbs = new Set(["template0", "template1"]);
      const userDbs = dbs.filter((db) => !templateDbs.has(db.name));
      children = userDbs.map((db) => ({
        id: `${node.connectionId}:${db.name}`,
        label: db.name,
        type: "database" as const,
        connectionId: node.connectionId,
        connectionColor: node.connectionColor,
        database: db.name,
      }));
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
      const filtered = tables.filter((t) =>
        isTableGroup ? t.table_type === "BASE TABLE" : t.table_type === "VIEW"
      );

      if (isTableGroup) {
        // Partition children are hidden from the flat Tables list and
        // surface only inside their parent's "Partitions" sub-collection.
        const { topLevel, childrenByParent } = groupTablesByParent(filtered, node.schema!);
        const baseId = `${node.connectionId}:${node.database}:${node.schema}`;
        children = topLevel.map((t) =>
          buildTableNode(
            t,
            baseId,
            node.connectionId,
            node.connectionColor,
            node.database,
            node.schema,
            childrenByParent,
          )
        );
      } else {
        children = filtered.map((t) => ({
          id: `${node.connectionId}:${node.database}:${node.schema}:${t.name}`,
          label: t.name,
          type: "view" as const,
          connectionId: node.connectionId,
          connectionColor: node.connectionColor,
          database: node.database,
          schema: node.schema,
          tableName: t.name,
          tableType: t.table_type,
        }));
      }
    } else if (
      (node.type === "partition-group" || node.type === "table") &&
      node.preloadedChildren
    ) {
      // Partition tree was pre-built recursively at table-group expand
      // time (see buildTableNode). Hand the children straight back —
      // no extra backend call.
      children = node.preloadedChildren;
    }

    return children;
  }, []);

  const loadChildren = useCallback(
    async (node: TreeNode) => {
      const nodeId = node.id;
      setLoadingNodes((prev) => new Set(prev).add(nodeId));
      setErrorNodes((prev) => {
        if (!(nodeId in prev)) return prev;
        const { [nodeId]: _gone, ...rest } = prev;
        return rest;
      });

      try {
        const children = await fetchChildren(node);

        // A connection with a single user database auto-expands into it.
        if (node.type === "connection" && children.length === 1) {
          const dbNode = children[0];
          setExpanded((prev) => new Set(prev).add(dbNode.id));
          setTimeout(() => loadChildren(dbNode), 50);
        }

        setChildrenMap((prev) => ({ ...prev, [nodeId]: children }));
      } catch (err) {
        // Shown as an error row with Retry under the node (see renderNode).
        console.error("Failed to load tree children:", err);
        setErrorNodes((prev) => ({ ...prev, [nodeId]: errorText(err) }));
      } finally {
        setLoadingNodes((prev) => {
          const next = new Set(prev);
          next.delete(nodeId);
          return next;
        });
      }
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [fetchChildren]
  );

  const expandNode = useCallback(
    async (node: TreeNode) => {
      const nodeId = node.id;
      if (expanded.has(nodeId)) return;
      setExpanded((prev) => new Set(prev).add(nodeId));
      if (childrenMap[nodeId]) return;
      await loadChildren(node);
    },
    [expanded, childrenMap, loadChildren]
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

  /** Connect, open the server's Activity tab and (optionally) expand the
   *  connection. Failures are reported as an error toast; a prompt the
   *  user cancelled (passphrase / host key) is not a failure. */
  const connectAndOpen = async (
    conn: import("../../lib/tauri").ConnectionConfig,
    opts?: { expand?: boolean },
  ) => {
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
      if (opts?.expand) {
        await expandNode({
          id: conn.id,
          label: conn.name || conn.database,
          type: "connection",
          connectionId: conn.id,
          connectionColor: conn.color,
        });
      }
    } catch (e) {
      console.error("Connection failed:", e);
      if (!wasCancelledByUser(e)) addToast(formatConnectFailure(e, conn), "error");
    } finally {
      setConnectingId(null);
    }
  };

  const handleDisconnect = async (connectionId: string) => {
    try {
      await disconnectFrom(connectionId);
    } catch (e) {
      const conn = connections.find((c) => c.id === connectionId);
      reportError(`Couldn't disconnect "${conn?.name || conn?.database || "connection"}"`, e);
    }
  };

  const requestDeleteConnection = (conn: import("../../lib/tauri").ConnectionConfig, node: TreeNode) => {
    setConfirmAction({
      title: "Delete Connection",
      confirmLabel: "Delete",
      message: `Delete "${conn.name || conn.database}"? This action cannot be undone.`,
      action: () => removeConnection(conn.id),
      node,
    });
  };

  const handleDoubleClick = async (node: TreeNode) => {
    if (node.type === "connection") {
      const isActive = activeConnections.has(node.connectionId);
      if (!isActive) {
        const conn = connections.find((c) => c.id === node.connectionId);
        if (conn) await connectAndOpen(conn, { expand: true });
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

  /**
   * Opens the unified per-table tab and focuses a specific sub-tab inside
   * the TableView. Used by both right-click "Open Inspector" (subTab =
   * "columns") and the partition strategy chip (subTab = "partitions").
   * If the table tab is already open, it activates and switches sub-tab
   * in place — no duplicate tabs.
   */
  const openTableTabAt = (node: TreeNode, subTab: string) => {
    if (!node.tableName) return;
    const tabId = `table:${node.connectionId}:${node.database}:${node.schema}:${node.tableName}`;
    const tab: TabInfo = {
      id: tabId,
      type: "table",
      title: node.tableName,
      connectionId: node.connectionId,
      connectionColor: node.connectionColor,
      database: node.database!,
      schema: node.schema!,
      table: node.tableName,
      subTab,
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
        addToast(`Dropped ${objType.toLowerCase()} ${node.schema}.${node.tableName}`, "success");
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
        addToast(`Truncated ${node.schema}.${node.tableName}`, "success");
      },
    });
  };

  const handleOpenPartitions = (node: TreeNode) => {
    if (!node.tableName) return;
    // Partitions are now a sub-tab of the unified table view, so this
    // routes through the same single tab as Data/Columns/Indexes/etc.
    const tabId = `table:${node.connectionId}:${node.database}:${node.schema}:${node.tableName}`;
    const tab: TabInfo = {
      id: tabId,
      type: "table",
      title: node.tableName,
      connectionId: node.connectionId,
      connectionColor: node.connectionColor,
      database: node.database!,
      schema: node.schema!,
      table: node.tableName,
      subTab: "schema:partitions",
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

  const expandedRef = useRef(expanded);
  expandedRef.current = expanded;
  const childrenMapRef = useRef(childrenMap);
  childrenMapRef.current = childrenMap;

  /** Re-fetch `root` and every expanded descendant in place, so schema
   *  changes made outside the app show up without collapsing the tree.
   *  Cached children of collapsed descendants are dropped and reload lazily.
   *  Resolves to true when objects were added, removed or renamed. */
  const refreshSubtree = useCallback(
    async (root: TreeNode, opts?: { silent?: boolean }): Promise<boolean> => {
      const prefix = `${root.id}:`;
      const inSubtree = (id: string) => id === root.id || id.startsWith(prefix);
      const expandedNow = expandedRef.current;
      const refreshed = new Set<string>();
      let failed = false;
      let firstError: unknown = null;
      let structureChanged = false;

      if (!opts?.silent) setLoadingNodes((prev) => new Set(prev).add(root.id));
      try {
        const queue: TreeNode[] = [root];
        while (queue.length > 0) {
          const node = queue.shift()!;
          let children: TreeNode[];
          try {
            children = await fetchChildren(node);
          } catch (err) {
            console.error("Failed to refresh tree children:", err);
            failed = true;
            firstError ??= err;
            continue;
          }
          refreshed.add(node.id);
          const previous = childrenMapRef.current[node.id];
          if (previous && structureSignature(previous) !== structureSignature(children)) {
            structureChanged = true;
          }
          if (!previous || JSON.stringify(previous) !== JSON.stringify(children)) {
            setChildrenMap((prev) => ({ ...prev, [node.id]: children }));
          }
          for (const child of children) {
            if (expandedNow.has(child.id) && !child.preloadedChildren) queue.push(child);
          }
        }
      } finally {
        const stale = failed
          ? []
          : Object.keys(childrenMapRef.current).filter((id) => inSubtree(id) && !refreshed.has(id));
        if (stale.length > 0) {
          setChildrenMap((prev) => {
            const next = { ...prev };
            for (const id of stale) delete next[id];
            return next;
          });
          setExpanded((prev) => {
            const next = new Set(prev);
            for (const id of stale) next.delete(id);
            return next;
          });
        }
        if (!opts?.silent) {
          setLoadingNodes((prev) => {
            const next = new Set(prev);
            next.delete(root.id);
            return next;
          });
        }
      }
      // Background polls stay quiet; an explicit Refresh reports failure.
      if (failed && !opts?.silent) {
        addToast(`Couldn't refresh "${root.label}": ${errorText(firstError)}`, "error");
      }
      return structureChanged;
    },
    [fetchChildren, addToast]
  );

  const connectionsRef = useRef(connections);
  connectionsRef.current = connections;
  const activeConnectionsRef = useRef(activeConnections);
  activeConnectionsRef.current = activeConnections;

  const schemaVersions = useConnectionStore((s) => s.schemaVersions);
  const connectionsLoaded = useConnectionStore((s) => s.connectionsLoaded);
  const invalidateSchema = useConnectionStore((s) => s.invalidateSchema);
  // Connections whose next schema-version bump came from this tree, which
  // has already refreshed itself and only needs to notify open tabs.
  const selfInvalidatedRef = useRef(new Set<string>());
  const refreshingRef = useRef(new Set<string>());

  const refreshConnectionTree = useCallback(
    async (connectionId: string, silent: boolean) => {
      const conn = connectionsRef.current.find((c) => c.id === connectionId);
      if (!conn || !activeConnectionsRef.current.has(connectionId)) return;
      if (!expandedRef.current.has(conn.id) && !childrenMapRef.current[conn.id]) return;
      if (refreshingRef.current.has(connectionId)) return;
      refreshingRef.current.add(connectionId);
      try {
        const changed = await refreshSubtree(
          {
            id: conn.id,
            label: conn.name || conn.database,
            type: "connection",
            connectionId: conn.id,
            connectionColor: conn.color,
          },
          { silent }
        );
        if (changed) {
          selfInvalidatedRef.current.add(connectionId);
          invalidateSchema(connectionId);
        }
      } finally {
        refreshingRef.current.delete(connectionId);
      }
    },
    [refreshSubtree, invalidateSchema]
  );

  const seenSchemaVersionsRef = useRef(schemaVersions);
  useEffect(() => {
    const seen = seenSchemaVersionsRef.current;
    seenSchemaVersionsRef.current = schemaVersions;
    for (const [id, version] of Object.entries(schemaVersions)) {
      if (seen[id] === version) continue;
      if (selfInvalidatedRef.current.delete(id)) continue;
      refreshConnectionTree(id, false);
    }
  }, [schemaVersions, refreshConnectionTree]);

  const checkActiveConnections = useCallback(async () => {
    for (const id of activeConnectionsRef.current) {
      await refreshConnectionTree(id, true);
    }
  }, [refreshConnectionTree]);

  // Migrations are usually run from a terminal or another tool: check right
  // away when the user switches back to the app, and periodically while
  // it's visible.
  useEffect(() => {
    let lastCheck = 0;
    const onFocus = () => {
      const now = Date.now();
      if (now - lastCheck < FOCUS_REFRESH_MIN_INTERVAL_MS) return;
      lastCheck = now;
      checkActiveConnections();
    };
    window.addEventListener("focus", onFocus);
    return () => window.removeEventListener("focus", onFocus);
  }, [checkActiveConnections]);

  usePolling(checkActiveConnections, SCHEMA_POLL_INTERVAL_MS, activeConnections.size > 0, {
    immediate: false,
  });

  const handleRefreshNode = async (node: TreeNode) => {
    if (!expanded.has(node.id) && !childrenMap[node.id]) {
      expandNode(node);
      return;
    }
    setExpanded((prev) => new Set(prev).add(node.id));
    const changed = await refreshSubtree(node);
    if (changed) {
      selfInvalidatedRef.current.add(node.connectionId);
      invalidateSchema(node.connectionId);
    }
  };

  const handleContextMenu = (e: React.MouseEvent, node: TreeNode) => {
    e.preventDefault();
    e.stopPropagation();
    const z = readZoomFactor();
    const item = (e.currentTarget as HTMLElement).closest<HTMLElement>("[role='treeitem']");
    setContextMenu({ x: e.clientX / z, y: e.clientY / z, node, returnFocusTo: item });
  };

  const openContextMenuFor = (node: TreeNode, el: HTMLElement) => {
    setContextMenu({ ...menuPointFor(el), node, returnFocusTo: el });
  };

  /** Props shared by every `role="treeitem"` (roving tabindex). The tree
   *  is "flat" in ARIA terms: each treeitem is just its row, with the
   *  hierarchy given by aria-level / aria-setsize / aria-posinset, so a
   *  row's accessible name is its own label and never swallows the text
   *  of its descendants. */
  const treeItemProps = (
    id: string,
    opts: { level: number; label: string; expanded?: boolean; className?: string; pos?: [number, number] },
  ) => ({
    role: "treeitem" as const,
    "aria-level": opts.level,
    "aria-expanded": opts.expanded,
    "aria-posinset": opts.pos?.[0],
    "aria-setsize": opts.pos?.[1],
    "aria-selected": focusedId === id,
    "aria-label": opts.label,
    tabIndex: focusedId === id ? 0 : -1,
    "data-tree-id": id,
    className: `tree-item${opts.className ? ` ${opts.className}` : ""}`,
    onFocus: (e: React.FocusEvent<HTMLElement>) => {
      if (e.target === e.currentTarget) setFocusedId(id);
    },
  });

  /** Buttons inside a row are only tab stops while that row is current. */
  const rowTabIndex = (id: string) => (focusedId === id ? 0 : -1);

  /** Non-object rows ("(empty)", load errors) are treeitems too, so arrow
   *  keys don't skip them; Enter on an error row retries. */
  const registerPlaceholder = (id: string, activate?: () => void) => () => {
    treeItems.current.set(id, {
      expandable: false,
      expanded: false,
      toggle: () => {},
      activate: activate ?? (() => {}),
      openMenu: () => {},
    });
  };

  interface RenderNodeOptions {
    /** aria-level of this node (folders shift connections down a level). */
    level: number;
    /** [position, set size] among the node's siblings. */
    pos?: [number, number];
    labelSuffix?: React.ReactNode;
    /** Extra controls rendered at the end of the row (connection actions). */
    actions?: React.ReactNode;
    rowClassName?: string;
    labelClassName?: string;
    /** Overrides the default expandability (inactive connections can't expand). */
    expandable?: boolean;
    icon?: React.ReactNode;
  }

  const renderNode = (node: TreeNode, depth: number, opts: RenderNodeOptions) => {
    const { level, labelSuffix } = opts;
    // A node is expandable when it ships preloadedChildren (partitioned
    // parent / partition-group) or when it's a non-leaf branch type.
    const isPartitionedParent = node.type === "table" && !!node.preloadedChildren;
    const baseLeaf =
      node.type === "table" || node.type === "view" || node.type === "function";
    const isLeaf = opts.expandable === undefined ? baseLeaf && !isPartitionedParent : !opts.expandable;
    const children = isPartitionedParent
      ? node.preloadedChildren!
      : childrenMap[node.id];
    const q = searchQuery.trim().toLowerCase();
    const selfMatches = hasActiveFilter && q && node.label.toLowerCase().includes(q);
    const descendantMatches = hasActiveFilter && !selfMatches && children?.some((c) => nodeMatchesFilter(c, q));
    const isExpanded = !isLeaf && (expanded.has(node.id) || (hasActiveFilter && !!descendantMatches));
    const isLoading = loadingNodes.has(node.id);
    const loadError = errorNodes[node.id];

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
        case "partition-group":
          return { icon: <GitBranch size={15} />, cls: "icon-partition-group" };
        case "table-group":
        case "view-group":
        case "function-group":
          return { icon: isExpanded ? <FolderOpen size={15} /> : <Folder size={15} />, cls: "icon-folder" };
        default:
          return { icon: <Database size={15} />, cls: "icon-database" };
      }
    };
    const iconInfo = getIconInfo();

    // Sidebar shows only the lightweight summary: a strategy chip on the
    // partitioned parent, a "default" chip and italic on default partitions,
    // a warning icon when the parent has problems, and a count on the
    // Partitions sub-collection. Row count and size are intentionally omitted
    // because the row count is a stale planner estimate that confuses users
    // when it disagrees with a live COUNT(*); both metrics live in the
    // Schema page and the Partitions tab where they can be presented
    // accurately and with the context that they're estimates.
    const isDefaultPartition = node.type === "table" && !!node.isDefaultPartition;
    const showStrategyChip = isPartitionedParent && !!node.partitionStrategy;
    const showWarning = node.hasDefaultPartitionRows || node.hasSkewWarning;

    const partitionGroupCount =
      node.type === "partition-group" ? children?.length ?? 0 : null;

    const activate = () => {
      // Partitioned parent tables are still tables — opening their data
      // tab on a single click matches a regular table; expansion happens
      // via the chevron (or double-click on a non-leaf branch type).
      if (baseLeaf) {
        handleDoubleClick(node);
      } else if (node.type === "connection" && isLeaf) {
        handleDoubleClick(node);
      } else {
        toggleExpand(node);
      }
    };
    treeItems.current.set(node.id, {
      expandable: !isLeaf,
      expanded: isExpanded,
      toggle: () => toggleExpand(node),
      activate,
      openMenu: (el) => openContextMenuFor(node, el),
    });
    const tab = rowTabIndex(node.id);
    // The accessible name includes any visible chip text so it matches
    // what's on screen (WCAG 2.5.3).
    const rowName = [
      node.label,
      showStrategyChip ? node.partitionStrategy : null,
      isDefaultPartition ? "default" : null,
      partitionGroupCount ? String(partitionGroupCount) : null,
    ]
      .filter(Boolean)
      .join(" ");
    const childList = isExpanded && children ? filterNodes(children) : [];

    return (
      <div key={node.id} className="tree-subtree">
        <div
          {...treeItemProps(node.id, {
            level,
            label: rowName,
            expanded: isLeaf ? undefined : isExpanded,
            pos: opts.pos,
            className: `tree-node ${isLeaf ? "leaf" : ""} ${isDefaultPartition ? "tree-node-default-partition" : ""}${opts.rowClassName ? ` ${opts.rowClassName}` : ""}`,
          })}
          style={{ paddingLeft: depth * 12 + 6 }}
          onClick={() => {
            if (node.type === "connection" && isLeaf) return; // inactive: double-click connects
            activate();
          }}
          onDoubleClick={() => !baseLeaf && handleDoubleClick(node)}
          onContextMenu={(e) => handleContextMenu(e, node)}
        >
          {!isLeaf && (
            <span
              className="tree-chevron"
              aria-hidden="true"
              onClick={(e) => {
                e.stopPropagation();
                toggleExpand(node);
              }}
            >
              {isExpanded ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
            </span>
          )}
          {/* Disconnected connections sit flush, as before (no spacer). */}
          {isLeaf && node.type !== "connection" && <span className="tree-chevron-spacer" aria-hidden="true" />}
          {opts.icon ?? (
            <span className={`tree-icon ${iconInfo.cls}`} aria-hidden="true">
              {iconInfo.icon}
            </span>
          )}
          <span className={`tree-label${opts.labelClassName ? ` ${opts.labelClassName}` : ""}`} title={node.label}>
            {node.label}
          </span>
          {/* Word break for assistive tech between the label and chips
              (whitespace-only text is dropped from the flex layout). */}
          {" "}
          {labelSuffix}
          {showStrategyChip && (
            <button
              className={`tree-chip tree-chip-partition-${node.partitionStrategy} tree-chip-clickable`}
              tabIndex={tab}
              onClick={(e) => {
                e.stopPropagation();
                handleOpenPartitions(node);
              }}
              // Visible text names it; the title adds what it does.
              title="Open Partitions view"
            >
              {node.partitionStrategy}
            </button>
          )}
          {isDefaultPartition && (
            <span className="tree-chip tree-chip-default" title="Default partition">
              default
            </span>
          )}
          {partitionGroupCount != null && partitionGroupCount > 0 && (
            <span className="tree-meta">{partitionGroupCount}</span>
          )}
          {showWarning && (
            <span
              className="tree-warning"
              role="img"
              aria-label={
                node.hasDefaultPartitionRows
                  ? "Default partition has rows — likely a bound mismatch"
                  : "Partition sizes are skewed"
              }
              title={
                node.hasDefaultPartitionRows
                  ? "Default partition has rows — likely a bound mismatch"
                  : "Partition sizes are skewed"
              }
            >
              <AlertTriangle size={12} aria-hidden="true" />
            </span>
          )}
          {node.type === "database" && (
            <button
              className="btn-icon tree-action"
              tabIndex={tab}
              onClick={(e) => {
                e.stopPropagation();
                handleOpenQueryTab(node.connectionId, node.connectionColor, node.database!);
              }}
              title="New Query"
              aria-label={`New query on ${node.label}`}
            >
              <Terminal size={12} />
            </button>
          )}
          {opts.actions}
        </div>
        {isExpanded && (children || loadError) && (
          <div className="tree-children">
            {loadError && !children ? (
              <div
                {...treeItemProps(`${node.id}::error`, {
                  level: level + 1,
                  label: `Couldn't load ${node.label}: ${loadError}`,
                  className: "tree-node tree-error",
                })}
                style={{ paddingLeft: (depth + 1) * 12 + 6 }}
                ref={registerPlaceholder(`${node.id}::error`, () => loadChildren(node))}
              >
                <AlertTriangle size={13} aria-hidden="true" className="tree-error-icon" />
                <span className="tree-label tree-error-text" title={loadError}>
                  Couldn't load: {loadError}
                </span>
                <button
                  className="btn-icon tree-error-retry"
                  tabIndex={rowTabIndex(`${node.id}::error`)}
                  onClick={(e) => {
                    e.stopPropagation();
                    loadChildren(node);
                  }}
                  title="Retry"
                  aria-label={`Retry loading ${node.label}`}
                >
                  <RotateCw size={12} />
                </button>
              </div>
            ) : children!.length === 0 ? (
              <div
                {...treeItemProps(`${node.id}::empty`, {
                  level: level + 1,
                  label: "(empty)",
                  className: "tree-node tree-empty",
                })}
                style={{ paddingLeft: (depth + 1) * 12 + 6 }}
                ref={registerPlaceholder(`${node.id}::empty`)}
              >
                <span className="tree-label text-muted">(empty)</span>
              </div>
            ) : (
              childList.map((child, i) =>
                renderNode(child, depth + 1, { level: level + 1, pos: [i + 1, childList.length] }),
              )
            )}
          </div>
        )}
      </div>
    );
  };

  const renderContextMenu = () => {
    if (!contextMenu) return null;
    const { node } = contextMenu;

    const items: MenuEntry[] = [];

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
        label: "View Schema",
        action: () => openTableTabAt(node, "schema"),
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
          action: () => { void handleDisconnect(node.connectionId); },
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
          action: () => {
            if (conn) void connectAndOpen(conn);
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
        const moveChildren: MenuEntry[] = [];
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
          action: () => requestDeleteConnection(conn, node),
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

    return (
      <ContextMenu
        x={contextMenu.x}
        y={contextMenu.y}
        items={items}
        label={`${node.label} actions`}
        returnFocusTo={contextMenu.returnFocusTo}
        onClose={() => setContextMenu(null)}
      />
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
      // preloadedChildren is populated synchronously for partition trees;
      // childrenMap for everything else that's been expanded at least once.
      const children = node.preloadedChildren ?? childrenMap[node.id];
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

  const renderConnectionNode = (
    conn: import("../../lib/tauri").ConnectionConfig,
    level: number,
    pos: [number, number],
  ) => {
    const isActive = activeConnections.has(conn.id);
    const isConn = connectingId === conn.id;
    const name = conn.name || conn.database;
    const node: TreeNode = {
      id: conn.id,
      label: name,
      type: "connection",
      connectionId: conn.id,
      connectionColor: conn.color,
    };
    const tab = rowTabIndex(conn.id);
    const sshBadge = conn.ssh_enabled && (
      <span
        className={`tree-conn-ssh-badge${isActive ? "" : " tree-conn-ssh-badge--idle"}`}
        role="img"
        aria-label="SSH tunnel"
        title={`${isActive ? "Tunneled" : "Will tunnel"} through ${conn.ssh_user || "ssh"}@${conn.ssh_host || "bastion"}:${conn.ssh_port || 22}`}
      >
        <ShieldCheck size={11} aria-hidden="true" />
      </span>
    );
    const actions = (
      <div className="tree-conn-actions">
        {isActive ? (
          <button
            className="btn-icon tree-action"
            tabIndex={tab}
            onClick={(e) => { e.stopPropagation(); void handleDisconnect(conn.id); }}
            title="Disconnect"
            aria-label={`Disconnect ${name}`}
          >
            <PowerOff size={12} />
          </button>
        ) : (
          <button
            className="btn-icon tree-action"
            tabIndex={tab}
            onClick={(e) => { e.stopPropagation(); void connectAndOpen(conn); }}
            title="Connect"
            aria-label={`Connect ${name}`}
          >
            <Power size={12} />
          </button>
        )}
        <button
          className="btn-icon tree-action"
          tabIndex={tab}
          onClick={(e) => { e.stopPropagation(); setEditingConn(conn); }}
          title="Edit"
          aria-label={`Edit ${name}`}
        >
          <Pencil size={12} />
        </button>
        <button
          className="btn-icon tree-action tree-action-danger"
          tabIndex={tab}
          onClick={(e) => { e.stopPropagation(); requestDeleteConnection(conn, node); }}
          title="Delete"
          aria-label={`Delete ${name}`}
        >
          <Trash2 size={12} />
        </button>
      </div>
    );
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
            aria-hidden="true"
            style={{
              background: isActive ? "var(--success)" : "var(--text-muted)",
              boxShadow: isActive ? "0 0 6px var(--success)" : "none",
            }}
          />
          <div style={{ flex: 1, minWidth: 0 }}>
            {renderNode(node, 0, {
              level,
              pos,
              labelSuffix: sshBadge,
              actions,
              rowClassName: "tree-conn-row",
              expandable: isActive,
              labelClassName: isActive ? undefined : "tree-label--inactive",
              icon: isConn ? (
                <span className="tree-icon" aria-hidden="true">
                  <Loader2 size={14} className="spin" style={{ color: "var(--accent)" }} />
                </span>
              ) : undefined,
            })}
          </div>
        </div>
      </div>
    );
  };

  const onTreeKeyDown = (e: React.KeyboardEvent<HTMLDivElement>) => {
    const target = e.target as HTMLElement;
    // Keys typed into the folder-rename input or pressed on a row button
    // keep their normal meaning.
    if (target.getAttribute("role") !== "treeitem") return;
    const id = target.dataset.treeId;
    const entry = id ? treeItems.current.get(id) : undefined;
    const tree = treeRef.current;
    if (!entry || !tree) return;
    const items = Array.from(tree.querySelectorAll<HTMLElement>("[role='treeitem']"));
    const idx = items.indexOf(target);
    const levelOf = (el: HTMLElement) => Number(el.getAttribute("aria-level") ?? 1);
    const focusAt = (i: number) => items[Math.max(0, Math.min(items.length - 1, i))]?.focus();
    const handled = () => {
      e.preventDefault();
      e.stopPropagation();
    };

    if ((e.key === "F10" && e.shiftKey) || e.key === "ContextMenu") {
      handled();
      entry.openMenu(target);
      return;
    }
    if (e.ctrlKey || e.metaKey || e.altKey) return;
    switch (e.key) {
      case "ArrowDown":
        handled();
        focusAt(idx + 1);
        return;
      case "ArrowUp":
        handled();
        focusAt(idx - 1);
        return;
      case "Home":
        handled();
        focusAt(0);
        return;
      case "End":
        handled();
        focusAt(items.length - 1);
        return;
      case "ArrowRight": {
        handled();
        if (entry.expandable && !entry.expanded) {
          entry.toggle();
        } else if (entry.expandable && entry.expanded) {
          const next = items[idx + 1];
          if (next && levelOf(next) === levelOf(target) + 1) next.focus();
        } else if (!entry.expandable && id && connections.some((c) => c.id === id)) {
          // A disconnected connection "expands" by connecting.
          entry.activate();
        }
        return;
      }
      case "ArrowLeft": {
        handled();
        if (entry.expandable && entry.expanded) {
          entry.toggle();
        } else {
          // Parent = nearest earlier row one level up.
          const level = levelOf(target);
          for (let i = idx - 1; i >= 0; i--) {
            if (levelOf(items[i]) < level) {
              items[i].focus();
              break;
            }
          }
        }
        return;
      }
      case "Enter":
      case " ":
        handled();
        entry.activate();
        return;
      case "F2":
        if (entry.rename) {
          handled();
          entry.rename();
        }
        return;
    }
  };

  // Keep exactly one tab stop in the tree: when the current row goes away
  // (collapsed parent, filter, disconnect) fall back to the first row.
  useEffect(() => {
    const tree = treeRef.current;
    if (!tree) return;
    if (tree.querySelector("[role='treeitem'][tabindex='0']")) return;
    const first = tree.querySelector<HTMLElement>("[role='treeitem']");
    const id = first?.dataset.treeId ?? null;
    if (id !== focusedId) setFocusedId(id);
  });

  const renderGroup = (
    groupName: string,
    conns: typeof connections,
    groupIndex: number,
    pos: [number, number],
  ) => {
    const isCollapsed = collapsedGroups.has(groupName);
    const isRenaming = renamingGroup === groupName;
    const groupId = `group:${groupName}`;
    const startRename = () => {
      setRenamingGroup(groupName);
      setRenameValue(groupName);
    };
    treeItems.current.set(groupId, {
      expandable: true,
      expanded: !isCollapsed,
      toggle: () => toggleGroup(groupName),
      activate: () => toggleGroup(groupName),
      openMenu: (el) => setGroupContextMenu({ ...menuPointFor(el), group: groupName, returnFocusTo: el }),
      rename: startRename,
    });
    return (
      <div
        key={groupId}
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
          {...treeItemProps(groupId, {
            level: 1,
            label: `${groupName} folder`,
            expanded: !isCollapsed,
            pos,
            className: "connection-group-header",
          })}
          onClick={() => !isRenaming && toggleGroup(groupName)}
          onContextMenu={(e) => {
            e.preventDefault();
            const z = readZoomFactor();
            const item = (e.currentTarget as HTMLElement).closest<HTMLElement>("[role='treeitem']");
            setGroupContextMenu({ x: e.clientX / z, y: e.clientY / z, group: groupName, returnFocusTo: item });
          }}
        >
          <span className="connection-group-chevron" aria-hidden="true">
            {isCollapsed ? <ChevronRight size={12} /> : <ChevronDown size={12} />}
          </span>
          {isCollapsed ? (
            <Folder size={13} className="connection-group-folder-icon" aria-hidden="true" />
          ) : (
            <FolderOpen size={13} className="connection-group-folder-icon" aria-hidden="true" />
          )}
          {isRenaming ? (
            <input
              ref={renameInputRef}
              className="folder-name-input"
              aria-label={`Rename folder ${groupName}`}
              value={renameValue}
              onChange={(e) => setRenameValue(e.target.value.slice(0, MAX_FOLDER_NAME_LENGTH))}
              maxLength={MAX_FOLDER_NAME_LENGTH}
              onKeyDown={(e) => {
                e.stopPropagation();
                if (e.key === "Enter") {
                  e.preventDefault();
                  void handleRenameGroup(groupName).then(() =>
                    treeRef.current?.querySelector<HTMLElement>(`[data-tree-id="${CSS.escape(`group:${renameValue.trim() || groupName}`)}"]`)?.focus(),
                  );
                }
                if (e.key === "Escape") {
                  e.preventDefault();
                  setRenamingGroup(null);
                  (e.currentTarget.closest("[role='treeitem']") as HTMLElement | null)?.focus();
                }
              }}
              onBlur={() => handleRenameGroup(groupName)}
              onClick={(e) => e.stopPropagation()}
            />
          ) : (
            <span className="connection-group-name" title={groupName}>{groupName}</span>
          )}
        </div>
        {!isCollapsed && (
          <div className="connection-group-children">
            {conns.length > 0 ? (
              conns.map((c, i) => renderConnectionNode(c, 2, [i + 1, conns.length]))
            ) : (
              <div
                {...treeItemProps(`${groupId}::empty`, {
                  level: 2,
                  label: "Empty folder. Drag connections here",
                  className: "connection-group-empty",
                })}
                ref={registerPlaceholder(`${groupId}::empty`)}
              >
                Drag connections here
              </div>
            )}
          </div>
        )}
      </div>
    );
  };

  return (
    <>
      <div className="sidebar-header">
        <span>Explorer</span>
        <div style={{ display: "flex", gap: 2 }}>
          <button className="btn-icon" onClick={onAddConnection} title="New Connection" aria-label="New connection">
            <Plus size={16} />
          </button>
          <button className="btn-icon" onClick={() => { setCreatingFolder(true); setNewFolderName(""); }} title="New Folder" aria-label="New folder">
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
        {connections.length === 0 && !connectionsLoaded ? (
          <div className="tree-empty-state tree-loading-state" role="status" aria-live="polite">
            <Loader2 size={16} className="spin" aria-hidden="true" />
            <p>Loading connections…</p>
          </div>
        ) : connections.length === 0 ? (
          <div className="tree-empty-state">
            <p>No connections yet</p>
            <button className="btn-secondary" onClick={onAddConnection} style={{ marginTop: 8 }}>
              <Plus size={14} /> Add Connection
            </button>
          </div>
        ) : (
          <>
            <div className="tree-search">
              <SearchIcon size={13} aria-hidden="true" />
              <input
                placeholder="Filter objects..."
                aria-label="Filter objects"
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
              />
              <div className="tree-type-filter-wrapper" ref={typeFilterRef}>
                <button
                  className={`btn-icon tree-filter-btn ${!allTypesEnabled ? "tree-filter-active" : ""}`}
                  onClick={() => setShowTypeFilter((v) => !v)}
                  title="Filter by type"
                  aria-label="Filter by type"
                  aria-expanded={showTypeFilter}
                >
                  <Filter size={13} />
                </button>
                {showTypeFilter && (
                  <div className="tree-type-filter-dropdown" role="group" aria-label="Object types">
                    <label className="tree-type-filter-item">
                      <input
                        type="checkbox"
                        checked={typeFilters.tables}
                        onChange={(e) => setTypeFilters((f) => ({ ...f, tables: e.target.checked }))}
                      />
                      <Table2 size={13} aria-hidden="true" />
                      <span>Tables</span>
                    </label>
                    <label className="tree-type-filter-item">
                      <input
                        type="checkbox"
                        checked={typeFilters.views}
                        onChange={(e) => setTypeFilters((f) => ({ ...f, views: e.target.checked }))}
                      />
                      <Eye size={13} aria-hidden="true" />
                      <span>Views</span>
                    </label>
                    <label className="tree-type-filter-item">
                      <input
                        type="checkbox"
                        checked={typeFilters.functions}
                        onChange={(e) => setTypeFilters((f) => ({ ...f, functions: e.target.checked }))}
                      />
                      <Zap size={13} aria-hidden="true" />
                      <span>Functions</span>
                    </label>
                  </div>
                )}
              </div>
            </div>
            {creatingFolder && (
              <div className="connection-group-create">
                <Folder size={13} aria-hidden="true" />
                <input
                  ref={newFolderInputRef}
                  className="folder-name-input"
                  aria-label="New folder name"
                  value={newFolderName}
                  onChange={(e) => setNewFolderName(e.target.value.slice(0, MAX_FOLDER_NAME_LENGTH))}
                  maxLength={MAX_FOLDER_NAME_LENGTH}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") handleCreateFolder();
                    if (e.key === "Escape") { setCreatingFolder(false); setNewFolderName(""); }
                  }}
                  placeholder="Enter folder name"
                />
                <button className="btn-icon" onClick={handleCreateFolder} title="Create" aria-label="Create folder"><Check size={13} /></button>
                <button className="btn-icon" onClick={() => { setCreatingFolder(false); setNewFolderName(""); }} title="Cancel" aria-label="Cancel new folder"><X size={13} /></button>
              </div>
            )}
            <div
              ref={treeRef}
              role="tree"
              aria-label="Connections"
              className="object-tree"
              onKeyDown={onTreeKeyDown}
            >
              {(() => {
                const groups = Object.entries(grouped.groups).sort(([a], [b]) => a.localeCompare(b));
                const total = groups.length + grouped.ungrouped.length;
                return (
                  <>
                    {groups.map(([groupName, conns], i) => renderGroup(groupName, conns, i, [i + 1, total]))}
                    {grouped.ungrouped.map((c, i) => renderConnectionNode(c, 1, [groups.length + i + 1, total]))}
                  </>
                );
              })()}
            </div>
            {groupContextMenu && (
              <ContextMenu
                x={groupContextMenu.x}
                y={groupContextMenu.y}
                label={`${groupContextMenu.group} folder actions`}
                returnFocusTo={groupContextMenu.returnFocusTo}
                onClose={() => setGroupContextMenu(null)}
                items={[
                  {
                    label: "Rename Folder",
                    action: () => {
                      setRenamingGroup(groupContextMenu.group);
                      setRenameValue(groupContextMenu.group);
                    },
                  },
                  {
                    label: "Delete Folder",
                    action: () => { void handleDeleteGroup(groupContextMenu.group); },
                  },
                ]}
              />
            )}
          </>
        )}
        {renderContextMenu()}
        {confirmAction && (
          <ConfirmDialog
            title={confirmAction.title}
            message={confirmAction.message}
            confirmLabel={confirmAction.confirmLabel ?? confirmAction.title}
            danger
            onConfirm={async () => {
              // A rejection propagates to ConfirmDialog, which stays open
              // and shows the error instead of closing as if it worked.
              await confirmAction.action();
              setConfirmAction(null);
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
    </>
  );
});
