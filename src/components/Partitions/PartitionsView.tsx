import { useEffect, useMemo, useState } from "react";
import {
  Loader2,
  Search,
  X,
  AlertTriangle,
  ArrowUp,
  ArrowDown,
  ChevronsUpDown,
  GitBranch,
} from "lucide-react";
import { api, TableInfo } from "../../lib/tauri";
import { useTabStore, TabInfo } from "../../stores/tabStore";
import "./PartitionsView.css";

interface Props {
  connectionId: string;
  connectionColor: string;
  database: string;
  schema: string;
  /** Bare table name of the partitioned parent. */
  parent: string;
  /** When rendered inside TableView, the parent header already supplies
   *  the table identity and high-level stats. */
  embedded?: boolean;
}

type SortKey = "name" | "bound" | "rows" | "bytes" | "pct";
type SortDir = "asc" | "desc";

interface PartitionRow {
  table: TableInfo;
  qualified: string;
  isSubPartitioned: boolean;
  rows: number;
  bytes: number;
  pctOfTotal: number;
  isDefault: boolean;
  defaultHasRows: boolean;
  isWarning: boolean;
}

const formatBytes = (bytes: number): string => {
  if (bytes <= 0) return "—";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let v = bytes;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v < 10 && i > 0 ? v.toFixed(1) : Math.round(v)} ${units[i]}`;
};

const formatRows = (n: number | null | undefined): string => {
  if (n == null || n < 0) return "—";
  return n.toLocaleString();
};

export function PartitionsView({
  connectionId,
  connectionColor,
  database,
  schema,
  parent,
  embedded = false,
}: Props) {
  const openTab = useTabStore((s) => s.openTab);
  const [tables, setTables] = useState<TableInfo[] | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("bytes");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await api.listTables(connectionId, database, schema);
        if (!cancelled) setTables(data);
      } catch (e) {
        if (!cancelled) setError(String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    load();
    return () => {
      cancelled = true;
    };
  }, [connectionId, database, schema]);

  const parentQualified = `${schema}.${parent}`;
  const parentTable = tables?.find((t) => t.name === parent && t.schema === schema);

  // Build the set of qualified names that are "this parent or any descendant"
  // so we can index sub-partitioning correctly.
  const childNamesByParent = useMemo(() => {
    const map = new Map<string, TableInfo[]>();
    if (!tables) return map;
    for (const t of tables) {
      if (!t.parent_table) continue;
      const arr = map.get(t.parent_table) ?? [];
      arr.push(t);
      map.set(t.parent_table, arr);
    }
    return map;
  }, [tables]);

  const directChildren = childNamesByParent.get(parentQualified) ?? [];

  const rows: PartitionRow[] = useMemo(() => {
    const totalBytes = directChildren.reduce(
      (sum, t) => sum + (t.total_bytes ?? 0),
      0
    );
    const totalRows = directChildren.reduce(
      (sum, t) => sum + (t.row_count_estimate ?? 0),
      0
    );
    return directChildren.map((t) => {
      const qualified = `${t.schema}.${t.name}`;
      const isSub = childNamesByParent.has(qualified);
      const bytes = t.total_bytes ?? 0;
      const rowCount = t.row_count_estimate ?? 0;
      const pctSource = totalBytes > 0 ? bytes / totalBytes : totalRows > 0 ? rowCount / totalRows : 0;
      const isDefault = !!t.is_default_partition;
      const defaultHasRows = isDefault && rowCount > 0;
      return {
        table: t,
        qualified,
        isSubPartitioned: isSub,
        rows: rowCount,
        bytes,
        pctOfTotal: pctSource * 100,
        isDefault,
        defaultHasRows,
        isWarning: defaultHasRows,
      };
    });
  }, [directChildren, childNamesByParent]);

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return rows;
    return rows.filter(
      (r) =>
        r.table.name.toLowerCase().includes(q) ||
        (r.table.partition_bound ?? "").toLowerCase().includes(q)
    );
  }, [rows, search]);

  const sorted = useMemo(() => {
    const arr = [...filtered];
    const dir = sortDir === "asc" ? 1 : -1;
    arr.sort((a, b) => {
      switch (sortKey) {
        case "name":
          return a.table.name.localeCompare(b.table.name) * dir;
        case "bound":
          return (a.table.partition_bound ?? "").localeCompare(b.table.partition_bound ?? "") * dir;
        case "rows":
          return (a.rows - b.rows) * dir;
        case "pct":
          return (a.pctOfTotal - b.pctOfTotal) * dir;
        case "bytes":
        default:
          return (a.bytes - b.bytes) * dir;
      }
    });
    return arr;
  }, [filtered, sortKey, sortDir]);

  const maxBytes = useMemo(
    () => Math.max(1, ...rows.map((r) => r.bytes)),
    [rows]
  );

  const summary = useMemo(() => {
    const totalRows = rows.reduce((s, r) => s + r.rows, 0);
    const totalBytes = rows.reduce((s, r) => s + r.bytes, 0);
    const skewedRow = [...rows].filter((r) => r.bytes > 0).sort((a, b) => b.bytes - a.bytes)[0];
    const median = (() => {
      const sizes = rows.map((r) => r.bytes).filter((b) => b > 0).sort((a, b) => a - b);
      if (sizes.length < 3) return 0;
      return sizes[Math.floor(sizes.length / 2)];
    })();
    const skewed = median > 0 && skewedRow && skewedRow.bytes / median >= 5;
    const defaultRow = rows.find((r) => r.isDefault);
    return {
      totalRows,
      totalBytes,
      partitionCount: rows.length,
      skewedRow: skewed ? skewedRow : null,
      defaultRow: defaultRow && defaultRow.rows > 0 ? defaultRow : null,
    };
  }, [rows]);

  const onSort = (key: SortKey) => {
    if (key === sortKey) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir(key === "name" || key === "bound" ? "asc" : "desc");
    }
  };

  const SortIcon = ({ k }: { k: SortKey }) =>
    sortKey !== k ? (
      <ChevronsUpDown size={12} className="pv-sort-icon-muted" />
    ) : sortDir === "asc" ? (
      <ArrowUp size={12} />
    ) : (
      <ArrowDown size={12} />
    );

  const openPartition = (row: PartitionRow) => {
    const tabId = `table:${connectionId}:${database}:${schema}:${row.table.name}`;
    if (row.isSubPartitioned) {
      // Sub-partitioned children open in Schema mode anchored at the
      // Partitions section, so the user lands directly on the next
      // level of the partition hierarchy.
      const tab: TabInfo = {
        id: tabId,
        type: "table",
        title: row.table.name,
        connectionId,
        connectionColor,
        database,
        schema,
        table: row.table.name,
        subTab: "schema:partitions",
      };
      openTab(tab);
      return;
    }
    openTab({
      id: tabId,
      type: "table",
      title: row.table.name,
      connectionId,
      connectionColor,
      database,
      schema,
      table: row.table.name,
      subTab: "data",
    });
  };

  if (loading) {
    return (
      <div className="pv-loading">
        <Loader2 size={24} className="spin" />
        <span>Loading partitions…</span>
      </div>
    );
  }

  if (error) {
    return <div className="pv-error">{error}</div>;
  }

  if (!parentTable) {
    return (
      <div className="pv-empty">
        <AlertTriangle size={20} />
        <p>Could not find table {parentQualified}.</p>
      </div>
    );
  }

  if (!parentTable.partition_strategy || rows.length === 0) {
    return (
      <div className="pv-empty">
        <GitBranch size={20} />
        <p>{parentQualified} is not a partitioned table.</p>
      </div>
    );
  }

  const strategy = parentTable.partition_strategy;

  return (
    <div className="pv">
      {!embedded && (
        <div className="pv-header">
          <div className="pv-header-main">
            <span className="pv-title">{parentQualified}</span>
            <span className={`pv-chip pv-chip-strategy-${strategy}`}>{strategy}</span>
            <span className="pv-header-count">
              {summary.partitionCount} {summary.partitionCount === 1 ? "partition" : "partitions"}
            </span>
          </div>
          <div className="pv-header-stats">
            <span>
              <strong>{summary.totalRows.toLocaleString()}</strong> rows
            </span>
            <span className="pv-header-sep">·</span>
            <span>
              <strong>{formatBytes(summary.totalBytes)}</strong> total
            </span>
          </div>
        </div>
      )}

      {(summary.defaultRow || summary.skewedRow) && (
        <div className="pv-warnings">
          {summary.defaultRow && (
            <div className="pv-warning">
              <AlertTriangle size={14} />
              <span>
                Default partition <code>{summary.defaultRow.table.name}</code> holds{" "}
                <strong>{summary.defaultRow.rows.toLocaleString()}</strong> rows. Likely a
                missing explicit bound.
              </span>
            </div>
          )}
          {summary.skewedRow && (
            <div className="pv-warning">
              <AlertTriangle size={14} />
              <span>
                Largest partition <code>{summary.skewedRow.table.name}</code> is{" "}
                <strong>{formatBytes(summary.skewedRow.bytes)}</strong> — sizes are skewed.
              </span>
            </div>
          )}
        </div>
      )}

      <div className="pv-toolbar">
        <div className="pv-search">
          <Search size={13} />
          <input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Filter by name or bound…"
          />
          {search && (
            <button className="pv-search-clear" onClick={() => setSearch("")}>
              <X size={12} />
            </button>
          )}
        </div>
        <div className="pv-toolbar-info">
          {filtered.length !== rows.length && (
            <span className="pv-match-count">
              {filtered.length} of {rows.length}
            </span>
          )}
        </div>
      </div>

      <div className="pv-table-wrapper">
        <table className="pv-table">
          <colgroup>
            <col className="pv-col-name" />
            <col className="pv-col-bound" />
            <col className="pv-col-rows" />
            <col className="pv-col-size" />
            <col className="pv-col-share" />
          </colgroup>
          <thead>
            <tr>
              <th className="pv-th-name pv-th-sortable" onClick={() => onSort("name")}>
                <span className="pv-th-content">
                  <span>Name</span>
                  <SortIcon k="name" />
                </span>
              </th>
              <th className="pv-th-sortable" onClick={() => onSort("bound")}>
                <span className="pv-th-content">
                  <span>Bound</span>
                  <SortIcon k="bound" />
                </span>
              </th>
              <th className="pv-th-sortable pv-th-num" onClick={() => onSort("rows")}>
                <span className="pv-th-content">
                  <span>Rows</span>
                  <SortIcon k="rows" />
                </span>
              </th>
              <th className="pv-th-sortable pv-th-num" onClick={() => onSort("bytes")}>
                <span className="pv-th-content">
                  <span>Size</span>
                  <SortIcon k="bytes" />
                </span>
              </th>
              <th className="pv-th-sortable pv-th-share" onClick={() => onSort("pct")}>
                <span className="pv-th-content">
                  <span>% of total</span>
                  <SortIcon k="pct" />
                </span>
              </th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((row) => (
              <tr
                key={row.qualified}
                className={`pv-row ${row.isWarning ? "pv-row-warning" : ""}`}
                onDoubleClick={() => openPartition(row)}
                title="Double-click to open"
              >
                <td className="pv-td-name">
                  {row.isSubPartitioned && (
                    <GitBranch size={12} className="pv-td-name-icon" />
                  )}
                  <span>{row.table.name}</span>
                  {row.table.partition_strategy && (
                    <span
                      className={`pv-chip pv-chip-strategy-${row.table.partition_strategy} pv-chip-inline`}
                    >
                      {row.table.partition_strategy}
                    </span>
                  )}
                </td>
                <td className="pv-td-bound">
                  {row.isDefault ? (
                    <span className="pv-chip pv-chip-default">DEFAULT</span>
                  ) : (
                    <code>{row.table.partition_bound ?? "—"}</code>
                  )}
                </td>
                <td className="pv-td-num">{formatRows(row.rows)}</td>
                <td className="pv-td-num">{formatBytes(row.bytes)}</td>
                <td className="pv-td-share">
                  <div className="pv-share-bar">
                    <div
                      className="pv-share-fill"
                      style={{
                        width: `${Math.max(0.5, (row.bytes / maxBytes) * 100)}%`,
                      }}
                    />
                  </div>
                  <span className="pv-share-text">
                    {row.pctOfTotal.toFixed(row.pctOfTotal < 10 ? 1 : 0)}%
                  </span>
                </td>
              </tr>
            ))}
            {sorted.length === 0 && (
              <tr>
                <td colSpan={6} className="pv-empty-row">
                  No matching partitions.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
