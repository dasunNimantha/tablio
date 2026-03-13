import { useState, useCallback, useEffect, useRef, useMemo } from "react";
import { api, ServerActivity } from "../../lib/tauri";
import { Loader2, XCircle, Search, Users } from "lucide-react";
import { ConfirmDialog } from "../ConfirmDialog";

interface Props {
  connectionId: string;
  paused: boolean;
}

export function DashboardSessions({ connectionId, paused }: Props) {
  const [activities, setActivities] = useState<ServerActivity[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [killTarget, setKillTarget] = useState<ServerActivity | null>(null);
  const [search, setSearch] = useState("");
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const fetchActivity = useCallback(async () => {
    try {
      const result = await api.getServerActivity({ connection_id: connectionId });
      setActivities(result);
      setError(null);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, [connectionId]);

  useEffect(() => {
    fetchActivity();
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [fetchActivity]);

  useEffect(() => {
    if (intervalRef.current) clearInterval(intervalRef.current);
    if (!paused) {
      intervalRef.current = setInterval(fetchActivity, 2000);
    }
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [paused, fetchActivity]);

  const handleKill = async () => {
    if (!killTarget) return;
    try {
      await api.cancelQuery({
        connection_id: connectionId,
        pid: killTarget.pid,
      });
      setKillTarget(null);
      fetchActivity();
    } catch (e) {
      setError(String(e));
      setKillTarget(null);
    }
  };

  const filtered = useMemo(() => {
    if (!search.trim()) return activities;
    const q = search.toLowerCase();
    return activities.filter(
      (a) =>
        a.pid.toLowerCase().includes(q) ||
        a.user.toLowerCase().includes(q) ||
        a.database.toLowerCase().includes(q) ||
        a.query.toLowerCase().includes(q) ||
        a.state.toLowerCase().includes(q) ||
        (a.client_addr && a.client_addr.toLowerCase().includes(q))
    );
  }, [activities, search]);

  const activeCount = activities.filter((a) => a.state === "active").length;

  const formatDuration = (ms: number | null): string => {
    if (ms === null) return "-";
    if (ms < 1000) return `${Math.round(ms)}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
    return `${(ms / 60000).toFixed(1)}m`;
  };

  if (loading) {
    return (
      <div className="activity-loading">
        <Loader2 size={24} className="spin" />
        <span>Loading sessions...</span>
      </div>
    );
  }

  return (
    <div className="dashboard-sub-content">
      {error && <div className="activity-error">{error}</div>}

      <div className="activity-sub-toolbar">
        <span className="activity-count">{activities.length} connections</span>
        {activeCount > 0 && (
          <span className="session-active-badge">{activeCount} active</span>
        )}
        <div style={{ flex: 1 }} />
        <div className="config-search">
          <Search size={14} />
          <input
            type="text"
            placeholder="Filter sessions..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        </div>
      </div>

      <div className="activity-content">
        {filtered.length === 0 ? (
          <div className="dashboard-empty-state">
            <Users size={32} strokeWidth={1.2} />
            <span>{search ? "No matching sessions" : "No active connections"}</span>
          </div>
        ) : (
          <table className="info-table">
            <thead>
              <tr>
                <th>PID</th>
                <th>User</th>
                <th>Database</th>
                <th>State</th>
                <th>Duration</th>
                <th>Client</th>
                <th>Query</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((act) => (
                <tr key={act.pid}>
                  <td className="info-cell-muted">{act.pid}</td>
                  <td>{act.user}</td>
                  <td>{act.database}</td>
                  <td>
                    <span className={`activity-state ${act.state.toLowerCase().replace(/\s+/g, "-")}`}>
                      {act.state}
                    </span>
                  </td>
                  <td className="info-cell-muted">{formatDuration(act.duration_ms)}</td>
                  <td className="info-cell-muted">{act.client_addr || "-"}</td>
                  <td className="activity-query" title={act.query}>
                    {act.query.substring(0, 80)}
                    {act.query.length > 80 ? "..." : ""}
                  </td>
                  <td>
                    {act.state === "active" && (
                      <button
                        className="btn-icon"
                        onClick={() => setKillTarget(act)}
                        title="Cancel query"
                      >
                        <XCircle size={14} />
                      </button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {killTarget && (
        <ConfirmDialog
          title="Cancel Query"
          message={`Cancel query on PID ${killTarget.pid}? This will terminate the running query.`}
          confirmLabel="Cancel Query"
          danger
          onConfirm={handleKill}
          onCancel={() => setKillTarget(null)}
        />
      )}
    </div>
  );
}
