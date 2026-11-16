import { memo, useCallback, useContext, useEffect, useLayoutEffect, useMemo, useRef, useState, type ReactNode } from "react";
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

/** Child lists longer than this render only the rows near the viewport. */
export const VIRTUALIZE_THRESHOLD = 200;
/** A collapsed row's vertical pitch: 36px `.tree-node` + its 1px margin
 *  (vertical margins between rows collapse to one). Rows that render are
 *  measured; this is only the estimate for rows that never have. */
const DEFAULT_ROW_EXTENT = 37;
const ROW_MARGIN = 1;
/** Extra rows rendered above and below the viewport, in CSS px. */
const OVERSCAN_PX = 600;
const INITIAL_WINDOW = 40;
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
          ) : childList.length > VIRTUALIZE_THRESHOLD ? (
            <VirtualChildList items={childList} depth={depth + 1} level={level + 1} />
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

/** First index i with arr[i] > x (arr ascending). */
function upperBound(arr: Float64Array, x: number): number {
  let lo = 0;
  let hi = arr.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (arr[mid] > x) hi = mid;
    else lo = mid + 1;
  }
  return lo;
}

function scrollParentOf(el: HTMLElement): HTMLElement | null {
  for (let p = el.parentElement; p; p = p.parentElement) {
    const { overflowY } = getComputedStyle(p);
    if (overflowY === "auto" || overflowY === "scroll") return p;
  }
  return null;
}

/**
 * A long child list that renders only the rows near the viewport (plus the
 * focused row and its neighbours, so arrow keys always have a next row),
 * with spacers standing in for the rest. Spacers carry `data-tree-gap` and
 * the ids of the first / last row they hide, which the tree's keyboard
 * handler uses to reveal rows (End, or arrowing into a gap).
 */
const VirtualChildList = memo(function VirtualChildList({
  items,
  depth,
  level,
}: {
  items: TreeNode[];
  depth: number;
  level: number;
}) {
  const ctx = useTreeContext();
  const n = items.length;

  // Measured vertical extent of rows by id (expanded rows are taller).
  const extents = useRef(new Map<string, number>());
  const [measureVersion, setMeasureVersion] = useState(0);
  // Row i starts at offsets[i]; measureVersion re-sums after a measurement.
  const offsets = useMemo(() => {
    const o = new Float64Array(n + 1);
    for (let i = 0; i < n; i++) o[i + 1] = o[i] + (extents.current.get(items[i].id) ?? DEFAULT_ROW_EXTENT);
    return o;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [items, n, measureVersion]);
  const offsetsRef = useRef(offsets);
  offsetsRef.current = offsets;

  const [range, setRange] = useState(() => ({ start: 0, end: Math.min(n, INITIAL_WINDOW) }));
  const indexById = useMemo(() => new Map(items.map((c, i) => [c.id, i])), [items]);
  // The focused row only matters here when it (or a neighbour) is outside
  // the window, so moving focus within the window doesn't re-render this.
  const focusedIndex = useStore(ctx.store, (s) => {
    const i = s.focusedId == null ? -1 : indexById.get(s.focusedId) ?? -1;
    return i > range.start && i < range.end - 2 ? -1 : i;
  });
  const topGapRef = useRef<HTMLDivElement>(null);
  const renderedRef = useRef<number[]>([]);

  // Rows to render: the viewport window plus the focused row ± 1.
  const segments: [number, number][] = [];
  const add = (a: number, b: number) => {
    a = Math.max(0, a);
    b = Math.min(n, b);
    if (a < b) segments.push([a, b]);
  };
  add(range.start, Math.min(range.end, n));
  if (focusedIndex >= 0) add(focusedIndex - 1, focusedIndex + 2);
  segments.sort((x, y) => x[0] - y[0]);
  const merged: [number, number][] = [];
  for (const seg of segments) {
    const last = merged[merged.length - 1];
    if (last && seg[0] <= last[1]) last[1] = Math.max(last[1], seg[1]);
    else merged.push([seg[0], seg[1]]);
  }

  const gap = (a: number, b: number, key: string, ref?: React.Ref<HTMLDivElement>) => {
    const count = b - a;
    return (
      <div
        key={key}
        ref={ref}
        className="tree-virtual-gap"
        aria-hidden="true"
        // Margins between rows collapse to one; a spacer interrupts that,
        // so it is one margin shorter than the rows it replaces.
        style={{ height: count > 0 ? offsets[b] - offsets[a] - ROW_MARGIN : 0 }}
        {...(count > 0 ? { "data-tree-gap": "", "data-gap-first": items[a].id, "data-gap-last": items[b - 1].id } : {})}
      />
    );
  };

  const out: ReactNode[] = [];
  const rendered: number[] = [];
  let cursor = 0;
  merged.forEach(([a, b], si) => {
    if (si === 0) out.push(gap(0, a, "gap-top", topGapRef));
    else if (a > cursor) out.push(gap(cursor, a, `gap-${cursor}`));
    for (let i = a; i < b; i++) {
      rendered.push(i);
      out.push(
        <TreeRow key={items[i].id} node={items[i]} depth={depth} level={level} posinset={i + 1} setsize={n} />,
      );
    }
    cursor = b;
  });
  if (merged.length === 0) out.push(gap(0, 0, "gap-top", topGapRef));
  out.push(gap(cursor, n, "gap-bottom"));
  renderedRef.current = rendered;

  // Measure rendered rows and re-sum offsets when an extent changed: after
  // every render here, and when a row's own subtree expands or collapses
  // (which re-renders only that row, but resizes the list).
  const itemsRef = useRef(items);
  itemsRef.current = items;
  const measure = useCallback(() => {
    const list = topGapRef.current?.parentElement;
    if (!list) return;
    let changed = false;
    let k = 0;
    for (const el of Array.from(list.children) as HTMLElement[]) {
      if (el.classList.contains("tree-virtual-gap")) continue;
      const i = renderedRef.current[k++];
      const item = i === undefined ? undefined : itemsRef.current[i];
      if (!item) break;
      const extent = el.offsetHeight + ROW_MARGIN;
      const prev = extents.current.get(item.id) ?? DEFAULT_ROW_EXTENT;
      if (Math.abs(prev - extent) > 0.5) {
        extents.current.set(item.id, extent);
        changed = true;
      }
    }
    if (changed) setMeasureVersion((v) => v + 1);
  }, []);
  useLayoutEffect(measure);

  // Track the viewport of the scroll container.
  useLayoutEffect(() => {
    const top = topGapRef.current;
    const list = top?.parentElement;
    const scroller = list ? scrollParentOf(list) : null;
    if (!list || !scroller) {
      setRange({ start: 0, end: n });
      return;
    }
    const compute = () => {
      const o = offsetsRef.current;
      const lr = list.getBoundingClientRect();
      const sr = scroller.getBoundingClientRect();
      // Client rects are in zoomed pixels; offsets are in layout pixels.
      const scale = list.offsetHeight > 0 ? lr.height / list.offsetHeight : 1;
      const viewTop = (sr.top - lr.top) / scale - OVERSCAN_PX;
      const viewBottom = (sr.bottom - lr.top) / scale + OVERSCAN_PX;
      const start = Math.max(0, Math.min(n, upperBound(o, viewTop) - 1));
      const end = Math.max(start, Math.min(n, upperBound(o, viewBottom)));
      setRange((prev) => (prev.start === start && prev.end === end ? prev : { start, end }));
    };
    compute();
    let frame = 0;
    const onScroll = () => {
      if (frame) return;
      frame = requestAnimationFrame(() => {
        frame = 0;
        compute();
      });
    };
    scroller.addEventListener("scroll", onScroll, { passive: true });
    const ro =
      typeof ResizeObserver === "undefined"
        ? null
        : new ResizeObserver(() => {
            measure();
            onScroll();
          });
    ro?.observe(scroller);
    ro?.observe(list);
    return () => {
      if (frame) cancelAnimationFrame(frame);
      scroller.removeEventListener("scroll", onScroll);
      ro?.disconnect();
    };
  }, [n, offsets, measure]);

  return <>{out}</>;
});
