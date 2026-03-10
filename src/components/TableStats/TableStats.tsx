import { useEffect, useState } from "react";
import { api, TableStats as TableStatsType } from "../../lib/tauri";
import { Loader2 } from "lucide-react";
import "./TableStats.css";

interface Props {
  connectionId: string;
  database: string;
  schema: string;
  table: string;
}

function parseSize(s: string): number {
  const match = s.match(/([\d.]+)\s*(bytes|kB|MB|GB|TB)/i);
  if (!match) return 0;
  const val = parseFloat(match[1]);
  const unit = match[2].toLowerCase();
  const multipliers: Record<string, number> = {
    bytes: 1,
    kb: 1024,
    mb: 1024 * 1024,
    gb: 1024 * 1024 * 1024,
    tb: 1024 * 1024 * 1024 * 1024,
  };
  return val * (multipliers[unit] || 1);
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return (bytes / Math.pow(1024, i)).toFixed(1) + " " + units[i];
}

function SizeBar({ label, value, max, color }: { label: string; value: number; max: number; color: string }) {
  const pct = max > 0 ? Math.max((value / max) * 100, 0.5) : 0;
  return (
    <div className="stats-bar-row">
      <span className="stats-bar-label">{label}</span>
      <div className="stats-bar-track">
        <div className="stats-bar-fill" style={{ width: `${pct}%`, background: color }} />
      </div>
      <span className="stats-bar-value">{formatBytes(value)}</span>
    </div>
  );
}

export function TableStats({
  connectionId,
  database,
  schema,
  table,
}: Props) {
  const [stats, setStats] = useState<TableStatsType | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const load = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await api.getTableStats(
          connectionId,
          database,
          schema,
          table
        );
        setStats(data);
      } catch (e) {
        setError(String(e));
      } finally {
        setLoading(false);
      }
    };
    load();
  }, [connectionId, database, schema, table]);

  if (loading) {
    return (
      <div className="table-stats-loading">
        <Loader2 size={24} className="spin" />
        <span>Loading statistics...</span>
      </div>
    );
  }

  if (error) {
    return <div className="table-stats-error">{error}</div>;
  }

  if (!stats) {
    return null;
  }

  const dataBytes = parseSize(stats.data_size);
  const indexBytes = parseSize(stats.index_size);
  const totalBytes = parseSize(stats.total_size);
  const otherBytes = Math.max(totalBytes - dataBytes - indexBytes, 0);

  const liveTuples = stats.live_tuples ?? 0;
  const deadTuples = stats.dead_tuples ?? 0;
  const totalTuples = liveTuples + deadTuples;
  const livePct = totalTuples > 0 ? (liveTuples / totalTuples) * 100 : 100;
  const deadPct = totalTuples > 0 ? (deadTuples / totalTuples) * 100 : 0;

  return (
    <div className="table-stats">
      <div className="table-stats-toolbar">
        <span className="table-stats-name">
          {schema}.{table}
        </span>
      </div>
      <div className="table-stats-body">
        <div className="table-stats-grid">
          <div className="table-stats-card">
            <span className="table-stats-label">Row Count</span>
            <span className="table-stats-value">{stats.row_count.toLocaleString()}</span>
          </div>
          <div className="table-stats-card">
            <span className="table-stats-label">Total Size</span>
            <span className="table-stats-value">{stats.total_size}</span>
          </div>
          <div className="table-stats-card">
            <span className="table-stats-label">Last Vacuum</span>
            <span className="table-stats-value">{stats.last_vacuum ?? "—"}</span>
          </div>
          <div className="table-stats-card">
            <span className="table-stats-label">Last Analyze</span>
            <span className="table-stats-value">{stats.last_analyze ?? "—"}</span>
          </div>
        </div>

        <div className="stats-charts-row">
          <div className="stats-chart-panel">
            <h3 className="stats-chart-title">Storage Breakdown</h3>
            <div className="stats-bars">
              <SizeBar label="Data" value={dataBytes} max={totalBytes} color="var(--accent)" />
              <SizeBar label="Indexes" value={indexBytes} max={totalBytes} color="var(--purple)" />
              {otherBytes > 0 && (
                <SizeBar label="Overhead" value={otherBytes} max={totalBytes} color="var(--text-muted)" />
              )}
            </div>
            <div className="stats-size-total">
              Total: <strong>{stats.total_size}</strong>
            </div>
          </div>

          <div className="stats-chart-panel">
            <h3 className="stats-chart-title">Tuple Health</h3>
            <div className="stats-donut-container">
              <svg viewBox="0 0 120 120" className="stats-donut">
                <circle cx="60" cy="60" r="48" fill="none" stroke="var(--bg-elevated)" strokeWidth="14" />
                {totalTuples > 0 && (
                  <>
                    <circle
                      cx="60" cy="60" r="48"
                      fill="none"
                      stroke="var(--success)"
                      strokeWidth="14"
                      strokeDasharray={`${livePct * 3.016} ${301.6}`}
                      strokeDashoffset="0"
                      transform="rotate(-90 60 60)"
                      strokeLinecap="round"
                    />
                    {deadPct > 0.5 && (
                      <circle
                        cx="60" cy="60" r="48"
                        fill="none"
                        stroke="var(--error)"
                        strokeWidth="14"
                        strokeDasharray={`${deadPct * 3.016} ${301.6}`}
                        strokeDashoffset={`${-livePct * 3.016}`}
                        transform="rotate(-90 60 60)"
                        strokeLinecap="round"
                      />
                    )}
                  </>
                )}
                <text x="60" y="56" textAnchor="middle" fill="var(--text-primary)" fontSize="18" fontWeight="600" fontFamily="var(--font-mono)">
                  {totalTuples.toLocaleString()}
                </text>
                <text x="60" y="72" textAnchor="middle" fill="var(--text-muted)" fontSize="10" fontWeight="500">
                  TUPLES
                </text>
              </svg>
            </div>
            <div className="stats-donut-legend">
              <span className="stats-legend-item">
                <span className="stats-legend-dot" style={{ background: "var(--success)" }} />
                Live: {liveTuples.toLocaleString()}
              </span>
              <span className="stats-legend-item">
                <span className="stats-legend-dot" style={{ background: "var(--error)" }} />
                Dead: {deadTuples.toLocaleString()}
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
