import { useEffect, useState, useRef, useCallback } from "react";
import { api, ServerActivity } from "../../lib/tauri";
import { Loader2, Pause, Play, XCircle } from "lucide-react";
import { ConfirmDialog } from "../ConfirmDialog";
import "./ActivityDashboard.css";

interface Props {
  connectionId: string;
}

export function ActivityDashboard({ connectionId }: Props) {
  const [activities, setActivities] = useState<ServerActivity[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [paused, setPaused] = useState(false);
  const [killTarget, setKillTarget] = useState<ServerActivity | null>(null);
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
        <span>Loading server activity...</span>
      </div>
    );
  }

  return (
    <div className="activity-dashboard">
      <div className="activity-toolbar">
        <span className="activity-title">Server Activity</span>
        <span className="activity-count">{activities.length} connections</span>
        <div style={{ flex: 1 }} />
        <button
          className={`btn-ghost ${paused ? "" : "active"}`}
          onClick={() => setPaused(!paused)}
          title={paused ? "Resume auto-refresh" : "Pause auto-refresh"}
        >
          {paused ? <Play size={14} /> : <Pause size={14} />}
          {paused ? "Resume" : "Live"}
        </button>
      </div>

      {error && <div className="activity-error">{error}</div>}

      <div className="activity-content">
        <table className="info-table">
          <thead>
            <tr>
              <th>PID</th>
              <th>User</th>
              <th>Database</th>
              <th>State</th>
              <th>Duration</th>
              <th>Query</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {activities.length === 0 ? (
              <tr>
                <td colSpan={7} className="info-empty">
                  No active connections
                </td>
              </tr>
            ) : (
              activities.map((act) => (
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
              ))
            )}
          </tbody>
        </table>
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
