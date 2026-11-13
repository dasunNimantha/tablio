/**
 * Query console run history (per tab, in memory).
 *
 * Pins live on the entries themselves, so they survive new runs (the
 * list used to track pins by array index and reset them on every run)
 * and pinned entries never fall out of the size cap.
 */

export interface HistoryEntry {
  /** Stable identity for React keys and pin toggling. */
  id: string;
  sql: string;
  timestamp: number;
  executionTimeMs: number;
  rowCount: number;
  error?: string;
  pinned?: boolean;
}

/** Unpinned entries kept per console. */
export const MAX_UNPINNED_HISTORY = 100;

let nextHistoryId = 1;

export function newHistoryId(): string {
  return `h${Date.now().toString(36)}-${nextHistoryId++}`;
}

/**
 * Record a run at the top of the history. A previous entry with the
 * same SQL is replaced, keeping its pin. Only unpinned entries count
 * towards `maxUnpinned`.
 */
export function addHistoryEntry(
  prev: HistoryEntry[],
  entry: Omit<HistoryEntry, "id" | "pinned"> & { id?: string },
  maxUnpinned = MAX_UNPINNED_HISTORY,
): HistoryEntry[] {
  const sql = entry.sql.trim();
  const previous = prev.find((h) => h.sql.trim() === sql);
  const next: HistoryEntry = {
    ...entry,
    id: entry.id ?? previous?.id ?? newHistoryId(),
    pinned: previous?.pinned ?? false,
  };
  const rest = prev.filter((h) => h.sql.trim() !== sql);
  const out: HistoryEntry[] = [next];
  let unpinned = next.pinned ? 0 : 1;
  for (const h of rest) {
    if (h.pinned) {
      out.push(h);
    } else if (unpinned < maxUnpinned) {
      out.push(h);
      unpinned++;
    }
  }
  return out;
}

export function toggleHistoryPin(prev: HistoryEntry[], id: string): HistoryEntry[] {
  return prev.map((h) => (h.id === id ? { ...h, pinned: !h.pinned } : h));
}

/** Pinned entries first; otherwise newest first (the stored order). */
export function sortHistoryForDisplay(history: HistoryEntry[]): HistoryEntry[] {
  return [...history.filter((h) => h.pinned), ...history.filter((h) => !h.pinned)];
}

/** SQL a new console starts with. */
export const DEFAULT_SQL = "SELECT 1;";

/**
 * Whether the editor holds SQL the user would lose: edits to a loaded
 * saved query, or anything typed into an untitled console.
 */
export function hasUnsavedSql(sql: string, savedSql: string | null): boolean {
  if (savedSql !== null) return sql !== savedSql;
  const t = sql.trim();
  return t !== "" && t !== DEFAULT_SQL;
}

/**
 * Loading other SQL into the editor needs confirmation when it would
 * discard unsaved text. Text that is already in this console's history
 * (it was run) can be recovered from there, so it doesn't ask.
 */
export function needsReplaceConfirm(
  sql: string,
  savedSql: string | null,
  incoming: string,
  historySqls: string[],
): boolean {
  if (sql === incoming || !hasUnsavedSql(sql, savedSql)) return false;
  return !historySqls.some((h) => h.trim() === sql.trim());
}

