import { useEffect, useState, useRef, useCallback, useMemo } from "react";
import { api, LockInfo } from "../../lib/tauri";
import { Loader2, ChevronUp, ChevronDown, Search, Lock } from "lucide-react";

interface Props {
  connectionId: string;
  paused: boolean;
}

type SortKey = "pid" | "locktype" | "relation" | "mode" | "granted" | "user" | "state" | "duration_ms";

export function DashboardLocks({ connectionId, paused }: Props) {
  const [locks, setLocks] = useState<LockInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sortKey, setSortKey] = useState<SortKey>("granted");
  const [sortAsc, setSortAsc] = useState(true);
  const [search, setSearch] = useState("");
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const fetchLocks = useCallback(async () => {
    try {
      const result = await api.getLocks({ connection_id: connectionId });
      setLocks(result);
      setError(null);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, [connectionId]);

  useEffect(() => {
    fetchLocks();
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [fetchLocks]);

  useEffect(() => {
    if (intervalRef.current) clearInterval(intervalRef.current);
    if (!paused) {
      intervalRef.current = setInterval(fetchLocks, 2000);
    }
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [paused, fetchLocks]);

  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortAsc(!sortAsc);
    } else {
      setSortKey(key);
      setSortAsc(true);
    }
  };

  const filtered = useMemo(() => {
    let list = locks;
    if (search.trim()) {
      const q = search.toLowerCase();
      list = list.filter(
        (l) =>
          l.pid.toString().includes(q) ||
          l.locktype.toLowerCase().includes(q) ||
          l.relation.toLowerCase().includes(q) ||
          l.mode.toLowerCase().includes(q) ||
          l.user.toLowerCase().includes(q) ||
          l.query.toLowerCase().includes(q)
      );
    }
    return list;
  }, [locks, search]);

  const sorted = useMemo(() => {
    const copy = [...filtered];
    copy.sort((a, b) => {
      let cmp = 0;
      const av = a[sortKey];
      const bv = b[sortKey];
      if (typeof av === "boolean") {
        cmp = (av ? 1 : 0) - (bv ? 1 : 0);
      } else if (typeof av === "number" || typeof av === "object") {
        cmp = ((av as number) ?? 0) - ((bv as number) ?? 0);
      } else {
        cmp = String(av).localeCompare(String(bv));
      }
      return sortAsc ? cmp : -cmp;
    });
    return copy;
  }, [filtered, sortKey, sortAsc]);

  const waitingCount = locks.filter((l) => !l.granted).length;
  const grantedCount = locks.filter((l) => l.granted).length;

  const formatDuration = (ms: number | null): string => {
    if (ms === null) return "-";
    if (ms < 1000) return `${Math.round(ms)}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
    return `${(ms / 60000).toFixed(1)}m`;
  };

  const SortIcon = ({ col }: { col: SortKey }) => {
    if (sortKey !== col) return null;
    return sortAsc ? <ChevronUp size={12} /> : <ChevronDown size={12} />;
  };

  if (loading) {
    return (
      <div className="activity-loading">
        <Loader2 size={24} className="spin" />
        <span>Loading locks...</span>
      </div>
    );
  }

  return (
    <div className="dashboard-sub-content">
      {error && <div className="activity-error">{error}</div>}

      <div className="activity-sub-toolbar">
        <span className="activity-count">{locks.length} locks</span>
        {grantedCount > 0 && (
          <span className="lock-granted-count">{grantedCount} granted</span>
        )}
        {waitingCount > 0 && (
          <span className="lock-waiting-badge">{waitingCount} waiting</span>
        )}
        <div style={{ flex: 1 }} />
        <div className="config-search">
          <Search size={14} />
          <input
            type="text"
            placeholder="Filter locks..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        </div>
      </div>

      <div className="activity-content">
        {sorted.length === 0 ? (
          <div className="dashboard-empty-state">
            <Lock size={32} strokeWidth={1.2} />
            <span>{search ? "No matching locks" : "No active locks"}</span>
          </div>
        ) : (
          <table className="info-table">
            <thead>
              <tr>
                <th onClick={() => handleSort("pid")} className="sortable-th">PID <SortIcon col="pid" /></th>
                <th onClick={() => handleSort("locktype")} className="sortable-th">Lock Type <SortIcon col="locktype" /></th>
                <th onClick={() => handleSort("relation")} className="sortable-th">Relation <SortIcon col="relation" /></th>
                <th onClick={() => handleSort("mode")} className="sortable-th">Mode <SortIcon col="mode" /></th>
                <th onClick={() => handleSort("granted")} className="sortable-th">Granted <SortIcon col="granted" /></th>
                <th onClick={() => handleSort("user")} className="sortable-th">User <SortIcon col="user" /></th>
                <th onClick={() => handleSort("state")} className="sortable-th">State <SortIcon col="state" /></th>
                <th>Query</th>
                <th onClick={() => handleSort("duration_ms")} className="sortable-th">Duration <SortIcon col="duration_ms" /></th>
              </tr>
            </thead>
            <tbody>
              {sorted.map((lock, i) => (
                <tr key={`${lock.pid}-${i}`} className={lock.granted ? "" : "lock-waiting-row"}>
                  <td className="info-cell-muted">{lock.pid}</td>
                  <td>{lock.locktype}</td>
                  <td className="lock-relation">{lock.relation || "-"}</td>
                  <td className="lock-mode">{lock.mode}</td>
                  <td>
                    <span className={`lock-granted-badge ${lock.granted ? "granted" : "waiting"}`}>
                      {lock.granted ? "Granted" : "Waiting"}
                    </span>
                  </td>
                  <td>{lock.user || "-"}</td>
                  <td>
                    <span className={`activity-state ${lock.state.toLowerCase().replace(/\s+/g, "-")}`}>
                      {lock.state || "-"}
                    </span>
                  </td>
                  <td className="activity-query" title={lock.query}>
                    {lock.query.substring(0, 60)}
                    {lock.query.length > 60 ? "..." : ""}
                  </td>
                  <td className="info-cell-muted">{formatDuration(lock.duration_ms)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
