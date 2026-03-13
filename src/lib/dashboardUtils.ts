import type { DatabaseStats } from "./tauri";

export interface RatePoint {
  tps_commit: number;
  tps_rollback: number;
  tup_ins: number;
  tup_upd: number;
  tup_del: number;
  tup_fetch: number;
  blks_read: number;
  blks_hit: number;
}

export function computeRate(prev: DatabaseStats, cur: DatabaseStats): RatePoint {
  const dt = (cur.timestamp_ms - prev.timestamp_ms) / 1000;
  if (dt <= 0) {
    return { tps_commit: 0, tps_rollback: 0, tup_ins: 0, tup_upd: 0, tup_del: 0, tup_fetch: 0, blks_read: 0, blks_hit: 0 };
  }
  return {
    tps_commit: Math.max(0, (cur.xact_commit - prev.xact_commit) / dt),
    tps_rollback: Math.max(0, (cur.xact_rollback - prev.xact_rollback) / dt),
    tup_ins: Math.max(0, (cur.tup_inserted - prev.tup_inserted) / dt),
    tup_upd: Math.max(0, (cur.tup_updated - prev.tup_updated) / dt),
    tup_del: Math.max(0, (cur.tup_deleted - prev.tup_deleted) / dt),
    tup_fetch: Math.max(0, (cur.tup_fetched - prev.tup_fetched) / dt),
    blks_read: Math.max(0, (cur.blks_read - prev.blks_read) / dt),
    blks_hit: Math.max(0, (cur.blks_hit - prev.blks_hit) / dt),
  };
}

export function formatVal(v: number): string {
  if (v >= 1000000) return `${(v / 1000000).toFixed(1)}M`;
  if (v >= 1000) return `${(v / 1000).toFixed(1)}K`;
  if (v >= 100) return Math.round(v).toString();
  if (v >= 1) return v.toFixed(1);
  if (v > 0) return v.toFixed(2);
  return "0";
}

export function makeLabels(count: number): string[] {
  const labels: string[] = [];
  for (let i = 0; i < count; i++) {
    const secsAgo = (count - 1 - i) * 2;
    if (secsAgo === 0) labels.push("now");
    else if (secsAgo % 10 === 0) labels.push(`${secsAgo}s ago`);
    else labels.push("");
  }
  return labels;
}

export function formatDuration(ms: number | null): string {
  if (ms === null) return "-";
  if (ms < 1000) return `${Math.round(ms)}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  return `${(ms / 60000).toFixed(1)}m`;
}

export function formatQueryDuration(ms: number): string {
  if (ms < 1) return `${(ms * 1000).toFixed(0)}µs`;
  if (ms < 1000) return `${ms.toFixed(1)}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(2)}s`;
  return `${(ms / 60000).toFixed(1)}m`;
}

export function cacheHitClass(ratio: number): string {
  if (ratio >= 90) return "qs-cache-good";
  if (ratio >= 70) return "qs-cache-warn";
  return "qs-cache-bad";
}

export function speedClass(meanMs: number): string {
  if (meanMs >= 5000) return "qs-speed-slow";
  if (meanMs >= 1000) return "qs-speed-warn";
  return "";
}

export function filterSessions<T extends { pid: string; user: string; database: string; query: string; state: string; client_addr?: string | null }>(
  items: T[],
  search: string
): T[] {
  if (!search.trim()) return items;
  const q = search.toLowerCase();
  return items.filter(
    (a) =>
      a.pid.toLowerCase().includes(q) ||
      a.user.toLowerCase().includes(q) ||
      a.database.toLowerCase().includes(q) ||
      a.query.toLowerCase().includes(q) ||
      a.state.toLowerCase().includes(q) ||
      (a.client_addr && a.client_addr.toLowerCase().includes(q))
  );
}
