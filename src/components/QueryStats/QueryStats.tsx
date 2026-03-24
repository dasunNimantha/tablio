import { useEffect, useState, useCallback, useMemo, useRef } from "react";
import { api, QueryStatEntry, QueryStatsResponse } from "../../lib/tauri";
import { formatQueryDuration as formatDuration, cacheHitClass, speedClass } from "../../lib/dashboardUtils";
import {
  Loader2,
  RefreshCw,
  AlertTriangle,
  ChevronDown,
  ChevronRight,
  ChevronLeft,
  ChevronsLeft,
  ChevronsRight,
  Database,
  Search,
  Pause,
  Play,
} from "lucide-react";
import { CustomSelect } from "../CustomSelect/CustomSelect";
import "./QueryStats.css";

interface Props {
  connectionId: string;
}

type SortKey =
  | "total_exec_time_ms"
  | "calls"
  | "mean_exec_time_ms"
  | "max_exec_time_ms"
  | "rows";

const SORT_OPTIONS: { value: SortKey; label: string }[] = [
  { value: "total_exec_time_ms", label: "Total Time" },
  { value: "calls", label: "Calls" },
  { value: "mean_exec_time_ms", label: "Mean Time" },
  { value: "max_exec_time_ms", label: "Max Time" },
  { value: "rows", label: "Rows" },
];

const PAGE_SIZE = 50;

function formatNumber(n: number): string {
  return n.toLocaleString();
}

export function QueryStats({ connectionId }: Props) {
  const [data, setData] = useState<QueryStatsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sortKey, setSortKey] = useState<SortKey>("total_exec_time_ms");
  const [expandedIdx, setExpandedIdx] = useState<number | null>(null);
  const [search, setSearch] = useState("");
  const [userFilter, setUserFilter] = useState<string>("__all__");
  const [page, setPage] = useState(0);
  const [paused, setPaused] = useState(false);
  const [enabling, setEnabling] = useState(false);
  const [checking, setChecking] = useState(false);
  const [enableError, setEnableError] = useState<string | null>(null);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const fetchStats = useCallback(async () => {
    try {
      const result = await api.getQueryStats({ connection_id: connectionId });
      setData(result);
      setError(null);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, [connectionId]);

  const handleEnableExtension = useCallback(async () => {
    setEnabling(true);
    setEnableError(null);
    try {
      await api.executeQuery({
        connection_id: connectionId,
        database: "",
        sql: "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;",
      });
      setLoading(true);
      await fetchStats();
    } catch (e) {
      setEnableError(String(e));
    } finally {
      setEnabling(false);
    }
  }, [connectionId, fetchStats]);

  useEffect(() => {
    fetchStats();
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [fetchStats]);

  useEffect(() => {
    if (intervalRef.current) clearInterval(intervalRef.current);
    if (!paused && data?.available) {
      intervalRef.current = setInterval(fetchStats, 10000);
    }
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [paused, fetchStats, data?.available]);

  const uniqueUsers = useMemo(() => {
    if (!data?.entries) return [];
    const users = new Set(data.entries.map((e) => e.user).filter(Boolean));
    return Array.from(users).sort();
  }, [data]);

  const filtered = useMemo(() => {
    if (!data?.entries) return [];
    let entries = data.entries;
    if (userFilter !== "__all__") {
      entries = entries.filter((e) => e.user === userFilter);
    }
    if (search.trim()) {
      const q = search.toLowerCase();
      entries = entries.filter((e) => e.query.toLowerCase().includes(q));
    }
    return [...entries].sort(
      (a, b) => (b[sortKey] as number) - (a[sortKey] as number)
    );
  }, [data, sortKey, search, userFilter]);

  useEffect(() => {
    setPage(0);
    setExpandedIdx(null);
  }, [search, sortKey, userFilter]);

  const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
  const pageEntries = filtered.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  if (loading) {
    return (
      <div className="qs-loading">
        <Loader2 size={24} className="spin" />
        <span>Loading query statistics...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="qs-dashboard">
        <div className="qs-toolbar">
          <span className="qs-title">Query Statistics</span>
          <div style={{ flex: 1 }} />
          <button className="btn-ghost" onClick={() => { setLoading(true); fetchStats(); }}>
            <RefreshCw size={14} /> Refresh
          </button>
        </div>
        <div className="qs-error">{error}</div>
      </div>
    );
  }

  if (data && !data.available) {
    return (
      <div className="qs-dashboard">
        <div className="qs-toolbar">
          <span className="qs-title">Query Statistics</span>
        </div>
        <div className="qs-unavailable">
          <div className="qs-unavailable-icon">
            <Database size={36} />
          </div>
          <h3>pg_stat_statements not enabled</h3>
          <p>This extension is required to track query performance statistics.</p>

          <div className="qs-setup-card">
            <div className="qs-setup-steps">
              <div className="qs-step">
                <span className="qs-step-num">1</span>
                <div>
                  <strong>Add to postgresql.conf</strong>
                  <code>shared_preload_libraries = 'pg_stat_statements'</code>
                </div>
              </div>
              <div className="qs-step">
                <span className="qs-step-num">2</span>
                <div>
                  <strong>Restart PostgreSQL</strong>
                  <code>sudo systemctl restart postgresql</code>
                </div>
              </div>
              <div className="qs-step">
                <span className="qs-step-num">3</span>
                <div>
                  <strong>Create the extension</strong>
                  <code>CREATE EXTENSION pg_stat_statements;</code>
                </div>
              </div>
            </div>

            <div className="qs-setup-actions">
              <button
                className="btn-primary qs-enable-btn"
                onClick={handleEnableExtension}
                disabled={enabling}
              >
                {enabling ? <><Loader2 size={14} className="spin" /> Enabling...</> : "Enable Extension"}
              </button>
              <button className="btn-ghost" disabled={checking} onClick={async () => { setChecking(true); await fetchStats(); setChecking(false); }}>
                {checking ? <Loader2 size={14} className="spin" /> : <RefreshCw size={14} />} {checking ? "Checking..." : "Check"}
              </button>
            </div>
            {enableError && (
              <div className="qs-enable-error">
                <AlertTriangle size={13} /> {enableError}
              </div>
            )}
          </div>
        </div>
      </div>
    );
  }

  const toggleExpand = (globalIdx: number) => {
    setExpandedIdx(expandedIdx === globalIdx ? null : globalIdx);
  };

  return (
    <div className="qs-dashboard">
      <div className="qs-toolbar">
        <span className="qs-title">Query Statistics</span>
        <span className="qs-count">
          {filtered.length === (data?.entries?.length ?? 0)
            ? `${filtered.length} queries`
            : `${filtered.length} of ${data?.entries?.length ?? 0} queries`}
        </span>
        <div style={{ flex: 1 }} />
        <div className="qs-search">
          <Search size={14} className="qs-search-icon" />
          <input
            type="text"
            placeholder="Search queries..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        </div>
        <div className="qs-sort-label">
          User
          <CustomSelect
            value={userFilter}
            options={[
              { value: "__all__", label: "All Users" },
              ...uniqueUsers.map((u) => ({ value: u, label: u })),
            ]}
            onChange={(v) => setUserFilter(v)}
            className="qs-sort-select"
          />
        </div>
        <div className="qs-sort-label">
          Sort
          <CustomSelect
            value={sortKey}
            options={SORT_OPTIONS.map((o) => ({ value: o.value, label: o.label }))}
            onChange={(v) => setSortKey(v as SortKey)}
            className="qs-sort-select"
          />
        </div>
        <button
          className={`btn-ghost ${!paused ? "active" : ""}`}
          onClick={() => setPaused(!paused)}
          title={paused ? "Resume auto-refresh" : "Pause auto-refresh"}
        >
          {paused ? <Play size={14} /> : <Pause size={14} />}
          {paused ? "Stream" : "Live"}
        </button>
        <button className="btn-ghost" onClick={() => { setLoading(true); fetchStats(); }}>
          <RefreshCw size={14} /> Refresh
        </button>
      </div>

      <div className="qs-content">
        <table className="info-table qs-table">
          <thead>
            <tr>
              <th style={{ width: 32 }}></th>
              <th style={{ maxWidth: 420 }}>Query</th>
              <th style={{ width: 90 }} title="Total number of times this query has been executed">Calls</th>
              <th style={{ width: 90 }} title="PostgreSQL role that executed the query">User</th>
              <th style={{ width: 110 }} title="Cumulative execution time across all calls">Total Time</th>
              <th style={{ width: 100 }} title="Average execution time per call">Mean Time</th>
              <th style={{ width: 100 }} title="Longest single execution of this query">Max Time</th>
              <th style={{ width: 90 }} title="Total number of rows returned by all executions">Rows</th>
              <th style={{ width: 120 }} title="Percentage of reads served from buffer cache vs disk">Cache Hit</th>
            </tr>
          </thead>
          <tbody>
            {pageEntries.length === 0 ? (
              <tr>
                <td colSpan={9} className="info-empty">
                  {search ? "No queries match your search" : "No query statistics available"}
                </td>
              </tr>
            ) : (
              pageEntries.map((entry, localIdx) => {
                const globalIdx = page * PAGE_SIZE + localIdx;
                return (
                  <QueryRow
                    key={`${entry.queryid ?? "q"}-${globalIdx}`}
                    entry={entry}
                    expanded={expandedIdx === globalIdx}
                    onToggle={() => toggleExpand(globalIdx)}
                  />
                );
              })
            )}
          </tbody>
        </table>
      </div>

      {totalPages > 1 && (
        <div className="qs-pagination">
          <div className="qs-pagination-info">
            {filtered.length} queries
            {totalPages > 1 && (
              <span className="qs-pagination-range">
                {" · "}Showing {page * PAGE_SIZE + 1} -{" "}
                {Math.min((page + 1) * PAGE_SIZE, filtered.length)}
              </span>
            )}
          </div>
          <div className="qs-pagination-controls">
            <button className="btn-icon" disabled={page === 0} onClick={() => setPage(0)}>
              <ChevronsLeft size={16} />
            </button>
            <button className="btn-icon" disabled={page === 0} onClick={() => setPage((p) => p - 1)}>
              <ChevronLeft size={16} />
            </button>
            <span className="qs-page-info">
              Page {page + 1} of {totalPages}
            </span>
            <button className="btn-icon" disabled={page >= totalPages - 1} onClick={() => setPage((p) => p + 1)}>
              <ChevronRight size={16} />
            </button>
            <button className="btn-icon" disabled={page >= totalPages - 1} onClick={() => setPage(totalPages - 1)}>
              <ChevronsRight size={16} />
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

function QueryRow({
  entry,
  expanded,
  onToggle,
}: {
  entry: QueryStatEntry;
  expanded: boolean;
  onToggle: () => void;
}) {
  const meanClass = speedClass(entry.mean_exec_time_ms);
  const cacheClass = cacheHitClass(entry.cache_hit_ratio);

  return (
    <>
      <tr className={`qs-row ${meanClass}`} onClick={onToggle}>
        <td className="qs-expand-cell">
          {expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
        </td>
        <td className="qs-query-cell" title={entry.query}>
          {entry.query.substring(0, 80)}
          {entry.query.length > 80 ? "..." : ""}
        </td>
        <td className="qs-num">{formatNumber(entry.calls)}</td>
        <td className="qs-user-cell">{entry.user}</td>
        <td className="qs-num">{formatDuration(entry.total_exec_time_ms)}</td>
        <td className={`qs-num ${meanClass}`}>
          {formatDuration(entry.mean_exec_time_ms)}
          {entry.mean_exec_time_ms >= 1000 && (
            <AlertTriangle size={12} className="qs-warn-icon" />
          )}
        </td>
        <td className="qs-num">{formatDuration(entry.max_exec_time_ms)}</td>
        <td className="qs-num">{formatNumber(entry.rows)}</td>
        <td>
          <div className={`qs-cache-bar ${cacheClass}`}>
            <div
              className="qs-cache-fill"
              style={{ width: `${Math.min(entry.cache_hit_ratio, 100)}%` }}
            />
            <span className="qs-cache-label">
              {entry.cache_hit_ratio.toFixed(1)}%
            </span>
          </div>
        </td>
      </tr>
      {expanded && (
        <tr className="qs-detail-row">
          <td colSpan={9}>
            <QueryDetail entry={entry} />
          </td>
        </tr>
      )}
    </>
  );
}

function QueryDetail({ entry }: { entry: QueryStatEntry }) {
  const rowsPerCall = entry.calls > 0 ? entry.rows / entry.calls : 0;

  return (
    <div className="qs-detail">
      <div className="qs-detail-query">
        <pre>{entry.query}</pre>
      </div>
      <div className="qs-detail-grid">
        <div className="qs-detail-section">
          <h4>Execution</h4>
          <dl>
            <dt>Total calls</dt>
            <dd>{formatNumber(entry.calls)}</dd>
            <dt>Total exec time</dt>
            <dd>{formatDuration(entry.total_exec_time_ms)}</dd>
            <dt>Mean exec time</dt>
            <dd>{formatDuration(entry.mean_exec_time_ms)}</dd>
            <dt>Min exec time</dt>
            <dd>{formatDuration(entry.min_exec_time_ms)}</dd>
            <dt>Max exec time</dt>
            <dd>{formatDuration(entry.max_exec_time_ms)}</dd>
          </dl>
        </div>
        <div className="qs-detail-section">
          <h4>Planning</h4>
          <dl>
            <dt>Total plan time</dt>
            <dd>{entry.total_plan_time_ms != null ? formatDuration(entry.total_plan_time_ms) : "N/A"}</dd>
            <dt>Mean plan time</dt>
            <dd>{entry.mean_plan_time_ms != null ? formatDuration(entry.mean_plan_time_ms) : "N/A"}</dd>
          </dl>
        </div>
        <div className="qs-detail-section">
          <h4>Rows</h4>
          <dl>
            <dt>Total rows</dt>
            <dd>{formatNumber(entry.rows)}</dd>
            <dt>Rows per call</dt>
            <dd>{rowsPerCall.toFixed(1)}</dd>
          </dl>
        </div>
        <div className="qs-detail-section">
          <h4>Cache</h4>
          <dl>
            <dt>Shared blocks hit</dt>
            <dd>{formatNumber(entry.shared_blks_hit)}</dd>
            <dt>Shared blocks read</dt>
            <dd>{formatNumber(entry.shared_blks_read)}</dd>
            <dt>Hit ratio</dt>
            <dd>
              <div className={`qs-cache-bar qs-cache-bar-lg ${cacheHitClass(entry.cache_hit_ratio)}`}>
                <div
                  className="qs-cache-fill"
                  style={{ width: `${Math.min(entry.cache_hit_ratio, 100)}%` }}
                />
                <span className="qs-cache-label">
                  {entry.cache_hit_ratio.toFixed(2)}%
                </span>
              </div>
            </dd>
          </dl>
        </div>
      </div>
    </div>
  );
}
