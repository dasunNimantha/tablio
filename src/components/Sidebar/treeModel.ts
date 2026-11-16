import { createContext } from "react";
import { createStore, type StoreApi } from "zustand/vanilla";
import type { TableInfo } from "../../lib/tauri";

export interface TreeNode {
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

/** The text / type filter as applied to the tree (see ObjectTree). */
export interface TreeFilter {
  /** False when no text is typed and every object type is shown. */
  active: boolean;
  /** Trimmed, lower-cased filter text. */
  q: string;
  tables: boolean;
  views: boolean;
  functions: boolean;
}

export const NO_FILTER: TreeFilter = { active: false, q: "", tables: true, views: true, functions: true };

/** Tree state that individual rows subscribe to, so a change re-renders
 *  only the rows whose own slice of it changed (not the whole tree). */
export interface TreeState {
  /** The roving-tabindex row (the tree's single tab stop). */
  focusedId: string | null;
  expanded: Set<string>;
  childrenMap: Record<string, TreeNode[]>;
  loadingNodes: Set<string>;
  errorNodes: Record<string, string>;
  filter: TreeFilter;
}

export type TreeStore = StoreApi<TreeState>;

export const createTreeStore = (): TreeStore =>
  createStore<TreeState>(() => ({
    focusedId: null,
    expanded: new Set(),
    childrenMap: {},
    loadingNodes: new Set(),
    errorNodes: {},
    filter: NO_FILTER,
  }));

const LEAF_TYPES = new Set<TreeNode["type"]>(["table", "view", "function"]);
export const isLeafType = (type: TreeNode["type"]) => LEAF_TYPES.has(type);

const isTypeVisible = (node: TreeNode, f: TreeFilter): boolean => {
  if (node.type === "table" || node.type === "table-group") return f.tables;
  if (node.type === "view" || node.type === "view-group") return f.views;
  if (node.type === "function" || node.type === "function-group") return f.functions;
  return true;
};

type Matcher = (node: TreeNode) => boolean;
const matcherCache = new WeakMap<TreeFilter, { childrenMap: Record<string, TreeNode[]>; match: Matcher }>();

/**
 * Whether a node passes the filter: an object whose name matches, or a
 * branch with a matching descendant among its loaded children. Results are
 * memoised per (children, filter), so each node is evaluated once per filter
 * value instead of once per rendered ancestor row.
 */
export function getMatcher(childrenMap: Record<string, TreeNode[]>, filter: TreeFilter): Matcher {
  const cached = matcherCache.get(filter);
  if (cached && cached.childrenMap === childrenMap) return cached.match;
  const { q } = filter;
  const memo = new WeakMap<TreeNode, boolean>();
  const match: Matcher = (node) => {
    const known = memo.get(node);
    if (known !== undefined) return known;
    memo.set(node, false); // guards against id cycles
    let result: boolean;
    if (!isTypeVisible(node, filter)) {
      result = false;
    } else {
      const textMatch = !q || node.label.toLowerCase().includes(q);
      const leaf = isLeafType(node.type);
      if (textMatch && (leaf || !q)) {
        result = true;
      } else {
        // preloadedChildren is populated synchronously for partition trees;
        // childrenMap for everything else that's been expanded at least once.
        const children = node.preloadedChildren ?? childrenMap[node.id];
        result = children ? children.some(match) : !q;
      }
    }
    memo.set(node, result);
    return result;
  };
  matcherCache.set(filter, { childrenMap, match });
  return match;
}

/** What the keyboard handler needs to know about each rendered tree row. */
export interface TreeItemEntry {
  expandable: boolean;
  expanded: boolean;
  toggle: () => void;
  activate: () => void;
  openMenu: (el: HTMLElement) => void;
  rename?: () => void;
}

/** ObjectTree actions the rows call (kept in a ref, so always current). */
export interface TreeRowHandlers {
  handleDoubleClick: (node: TreeNode) => void;
  toggleExpand: (node: TreeNode) => void;
  loadChildren: (node: TreeNode) => void;
  handleContextMenu: (e: React.MouseEvent, node: TreeNode) => void;
  openContextMenuFor: (node: TreeNode, el: HTMLElement) => void;
  handleOpenPartitions: (node: TreeNode) => void;
  handleOpenQueryTab: (connId: string, color: string, dbName: string) => void;
}

export interface TreeContextValue {
  store: TreeStore;
  handlers: { current: TreeRowHandlers };
  /** Keyboard metadata of every mounted row, by tree id. */
  items: Map<string, TreeItemEntry>;
  /** Restores a tab stop if the current row went away. */
  scheduleTabStopCheck: () => void;
}

export const TreeContext = createContext<TreeContextValue | null>(null);
