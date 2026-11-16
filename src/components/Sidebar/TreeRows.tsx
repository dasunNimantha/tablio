import { memo, useContext, useEffect, useLayoutEffect, useMemo, type ReactNode } from "react";
import { useStore } from "zustand";
import { useShallow } from "zustand/react/shallow";
import {
  AlertTriangle,
  ChevronDown,
  ChevronRight,
  Database,
  Eye,
  Folder,
  FolderOpen,
  GitBranch,
  Layers,
  Loader2,
  RotateCw,
  Table2,
  Terminal,
  Zap,
} from "lucide-react";
import { getMatcher, isLeafType, TreeContext, type TreeContextValue, type TreeItemEntry, type TreeNode } from "./treeModel";

const NO_CHILDREN: TreeNode[] = [];

function useTreeContext(): TreeContextValue {
  const ctx = useContext(TreeContext);
  if (!ctx) throw new Error("Tree rows must render inside TreeContext");
  return ctx;
}

/** Registers a row's keyboard metadata for as long as it is mounted. */
function useRegisterItem(ctx: TreeContextValue, id: string, entry: TreeItemEntry) {
  useLayoutEffect(() => {
    ctx.items.set(id, entry);
    return () => {
      if (ctx.items.get(id) === entry) ctx.items.delete(id);
    };
  });
}

/** When the tab-stop row unmounts (collapsed parent, filter, …) another
 *  row has to become the tab stop. */
function useTabStopGuard(ctx: TreeContextValue, focused: boolean) {
  useEffect(() => {
    if (!focused) return;
    return () => ctx.scheduleTabStopCheck();
  }, [ctx, focused]);
}

interface TreeRowProps {
  node: TreeNode;
  depth: number;
  /** aria-level of this node (folders shift connections down a level). */
  level: number;
  /** Position and set size among the node's siblings. */
  posinset?: number;
  setsize?: number;
  labelSuffix?: ReactNode;
  /** Extra controls rendered at the end of the row (connection actions);
   *  receives the tabIndex the row's buttons should use. */
  renderActions?: (tabIndex: number) => ReactNode;
  rowClassName?: string;
  labelClassName?: string;
  /** Overrides the default expandability (inactive connections can't expand). */
  expandable?: boolean;
  icon?: ReactNode;
}

/**
 * One tree row plus, when expanded, its children. Memoised and subscribed
 * to its own slice of the tree store, so moving focus re-renders only the
 * previously and newly focused rows, and expanding a node renders only its
 * subtree.
 *
 * The tree is "flat" in ARIA terms: each treeitem is just its row, with the
 * hierarchy given by aria-level / aria-setsize / aria-posinset, so a row's
 * accessible name is its own label and never swallows the text of its
 * descendants.
 */
export const TreeRow = memo(function TreeRow({
  node,
  depth,
  level,
  posinset,
  setsize,
  labelSuffix,
  renderActions,
  rowClassName,
  labelClassName,
  expandable,
  icon,
}: TreeRowProps) {
  const ctx = useTreeContext();
  const h = ctx.handlers;
  // A node is expandable when it ships preloadedChildren (partitioned
  // parent / partition-group) or when it's a non-leaf branch type.
  const isPartitionedParent = node.type === "table" && !!node.preloadedChildren;
  const baseLeaf = isLeafType(node.type);
  const isLeaf = expandable === undefined ? baseLeaf && !isPartitionedParent : !expandable;

  const { focused, expandedRaw, children, isLoading, loadError, matcher, autoExpand } = useStore(
    ctx.store,
    useShallow((s) => {
      const children = isPartitionedParent ? node.preloadedChildren : s.childrenMap[node.id];
      const f = s.filter;
      const matcher = f.active ? getMatcher(s.childrenMap, f) : null;
      let autoExpand = false;
      if (matcher && !isLeaf && children) {
        // Branches open by themselves to show matching descendants.
        const selfMatches = !!f.q && node.label.toLowerCase().includes(f.q);
        autoExpand = !selfMatches && children.some(matcher);
      }
      return {
        focused: s.focusedId === node.id,
        expandedRaw: s.expanded.has(node.id),
        children,
        isLoading: s.loadingNodes.has(node.id),
        loadError: s.errorNodes[node.id] as string | undefined,
        matcher,
        autoExpand,
      };
    }),
  );
  const isExpanded = !isLeaf && (expandedRaw || autoExpand);
  const childList = useMemo(
    () => (isExpanded && children ? (matcher ? children.filter(matcher) : children) : NO_CHILDREN),
    [isExpanded, children, matcher],
  );

  const activate = () => {
    // Partitioned parent tables are still tables — opening their data
    // tab on a single click matches a regular table; expansion happens
    // via the chevron (or double-click on a non-leaf branch type).
    if (baseLeaf) {
      h.current.handleDoubleClick(node);
    } else if (node.type === "connection" && isLeaf) {
      h.current.handleDoubleClick(node);
    } else {
      h.current.toggleExpand(node);
    }
  };
  useRegisterItem(ctx, node.id, {
    expandable: !isLeaf,
    expanded: isExpanded,
    toggle: () => h.current.toggleExpand(node),
    activate,
    openMenu: (el) => h.current.openContextMenuFor(node, el),
  });
  useTabStopGuard(ctx, focused);

  const iconInfo = ((): { icon: ReactNode; cls: string } => {
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
  })();

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
  const partitionGroupCount = node.type === "partition-group" ? children?.length ?? 0 : null;

  // Buttons inside a row are only tab stops while that row is current.
  const tab = focused ? 0 : -1;
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

  return (
    <div className="tree-subtree">
      <div
        role="treeitem"
        aria-level={level}
        aria-expanded={isLeaf ? undefined : isExpanded}
        aria-posinset={posinset}
        aria-setsize={setsize}
        aria-selected={focused}
        aria-label={rowName}
        tabIndex={tab}
        data-tree-id={node.id}
        className={`tree-item tree-node ${isLeaf ? "leaf" : ""} ${isDefaultPartition ? "tree-node-default-partition" : ""}${rowClassName ? ` ${rowClassName}` : ""}`}
        onFocus={(e) => {
          if (e.target === e.currentTarget) ctx.store.setState({ focusedId: node.id });
        }}
        style={{ paddingLeft: depth * 12 + 6 }}
        onClick={() => {
          if (node.type === "connection" && isLeaf) return; // inactive: double-click connects
          activate();
        }}
        onDoubleClick={() => !baseLeaf && h.current.handleDoubleClick(node)}
        onContextMenu={(e) => h.current.handleContextMenu(e, node)}
      >
        {!isLeaf && (
          <span
            className="tree-chevron"
            aria-hidden="true"
            onClick={(e) => {
              e.stopPropagation();
              h.current.toggleExpand(node);
            }}
          >
            {isExpanded ? <ChevronDown size={13} /> : <ChevronRight size={13} />}
          </span>
        )}
        {/* Disconnected connections sit flush, as before (no spacer). */}
        {isLeaf && node.type !== "connection" && <span className="tree-chevron-spacer" aria-hidden="true" />}
        {icon ?? (
          <span className={`tree-icon ${iconInfo.cls}`} aria-hidden="true">
            {iconInfo.icon}
          </span>
        )}
        <span className={`tree-label${labelClassName ? ` ${labelClassName}` : ""}`} title={node.label}>
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
              h.current.handleOpenPartitions(node);
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
              h.current.handleOpenQueryTab(node.connectionId, node.connectionColor, node.database!);
            }}
            title="New Query"
            aria-label={`New query on ${node.label}`}
          >
            <Terminal size={12} />
          </button>
        )}
        {renderActions?.(tab)}
      </div>
      {isExpanded && (children || loadError) && (
        <div className="tree-children">
          {loadError && !children ? (
            <TreeErrorRow node={node} error={loadError} depth={depth + 1} level={level + 1} />
          ) : children!.length === 0 ? (
            <TreeEmptyRow id={`${node.id}::empty`} depth={depth + 1} level={level + 1} />
          ) : (
            childList.map((child, i) => (
              <TreeRow key={child.id} node={child} depth={depth + 1} level={level + 1} posinset={i + 1} setsize={childList.length} />
            ))
          )}
        </div>
      )}
    </div>
  );
});

const NOOP = () => {};

/** Non-object rows ("(empty)", load errors) are treeitems too, so arrow
 *  keys don't skip them; Enter on an error row retries. */
function usePlaceholderRow(id: string, activate?: () => void) {
  const ctx = useTreeContext();
  const focused = useStore(ctx.store, (s) => s.focusedId === id);
  useRegisterItem(ctx, id, {
    expandable: false,
    expanded: false,
    toggle: NOOP,
    activate: activate ?? NOOP,
    openMenu: NOOP,
  });
  useTabStopGuard(ctx, focused);
  return {
    focused,
    rowProps: {
      role: "treeitem" as const,
      "aria-selected": focused,
      tabIndex: focused ? 0 : -1,
      "data-tree-id": id,
      onFocus: (e: React.FocusEvent<HTMLElement>) => {
        if (e.target === e.currentTarget) ctx.store.setState({ focusedId: id });
      },
    },
  };
}

const TreeErrorRow = memo(function TreeErrorRow({
  node,
  error,
  depth,
  level,
}: {
  node: TreeNode;
  error: string;
  depth: number;
  level: number;
}) {
  const id = `${node.id}::error`;
  const ctx = useTreeContext();
  const retry = () => ctx.handlers.current.loadChildren(node);
  const { focused, rowProps } = usePlaceholderRow(id, retry);
  return (
    <div
      {...rowProps}
      aria-level={level}
      aria-label={`Couldn't load ${node.label}: ${error}`}
      className="tree-item tree-node tree-error"
      style={{ paddingLeft: depth * 12 + 6 }}
    >
      <AlertTriangle size={13} aria-hidden="true" className="tree-error-icon" />
      <span className="tree-label tree-error-text" title={error}>
        Couldn't load: {error}
      </span>
      <button
        className="btn-icon tree-error-retry"
        tabIndex={focused ? 0 : -1}
        onClick={(e) => {
          e.stopPropagation();
          retry();
        }}
        title="Retry"
        aria-label={`Retry loading ${node.label}`}
      >
        <RotateCw size={12} />
      </button>
    </div>
  );
});

const TreeEmptyRow = memo(function TreeEmptyRow({ id, depth, level }: { id: string; depth: number; level: number }) {
  const { rowProps } = usePlaceholderRow(id);
  return (
    <div
      {...rowProps}
      aria-level={level}
      aria-label="(empty)"
      className="tree-item tree-node tree-empty"
      style={{ paddingLeft: depth * 12 + 6 }}
    >
      <span className="tree-label text-muted">(empty)</span>
    </div>
  );
});
