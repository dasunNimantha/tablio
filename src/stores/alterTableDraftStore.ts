import { create } from "zustand";
import type { AlterTableOperation } from "../lib/tauri";
import type { PendingNewColumn } from "../components/AlterTable/operations";

/**
 * Per-table draft state for the Alter Table editor (issue #59).
 *
 * Persists pending edits so the user can switch away from the
 * Schema tab (or close the modal) and come back without losing
 * work. Keyed by the (connection, db, schema, table) tuple rather
 * than by tabId so the same draft is visible from both the in-tab
 * editor and the right-click → Alter Table modal — opening either
 * surface on the same table picks up where the other left off.
 *
 * Backed by `sessionStorage` (same pattern as
 * [tabStore](./tabStore.ts)) so the draft survives page reloads
 * within a session but not across full app restarts. A full
 * restart wipes everything that could be stale, which is what we
 * want — the table schema on the server may have changed under us
 * between sessions.
 */

/** Composite key: `${connectionId}:${database}:${schema}:${table}`. */
export type DraftKey = string;

export function draftKey(
  connectionId: string,
  database: string,
  schema: string,
  table: string,
): DraftKey {
  return `${connectionId}:${database}:${schema}:${table}`;
}

export interface AlterTableDraft {
  /**
   * The local copy of the table name. Tracked separately from the
   * "rename" operation so the user's keystrokes survive across
   * tab switches even before they commit.
   */
  tableNameLocal: string;
  /** Operations queued against existing columns / the table itself. */
  operations: AlterTableOperation[];
  /** Brand-new columns the user is mid-typing (no row in the DB yet). */
  pendingNewColumns: PendingNewColumn[];
  /** Whether the SQL preview pane is expanded. */
  showPreview: boolean;
  /** Column-name filter string (#60 + #59 share this). */
  columnFilter: string;
}

interface State {
  drafts: Record<DraftKey, AlterTableDraft>;
  getDraft: (key: DraftKey) => AlterTableDraft | undefined;
  /** Shallow merge — only the keys present in `patch` are written. */
  setDraft: (key: DraftKey, patch: Partial<AlterTableDraft>) => void;
  /** Used after a successful Apply, or on user-initiated Discard. */
  clearDraft: (key: DraftKey) => void;
  /** Mirrors `useTabStore.pruneStaleConnections` so deleted connections
   *  don't leak drafts in `sessionStorage`. */
  pruneStaleConnections: (validConnectionIds: Set<string>) => void;
}

const STORAGE_KEY = "tablio-alter-table-drafts";

function loadPersisted(): Record<DraftKey, AlterTableDraft> {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY);
    if (!raw) return {};
    const data = JSON.parse(raw);
    // Defensive: if a sessionStorage entry got corrupted, fall back
    // to an empty draft set rather than crashing the editor.
    if (data && typeof data === "object" && !Array.isArray(data)) {
      return data as Record<DraftKey, AlterTableDraft>;
    }
  } catch {
    // ignore — corrupted blob, treat as empty
  }
  return {};
}

// Debounced persist to keep keystrokes cheap. 500 ms matches tabStore.
let _persistTimer: ReturnType<typeof setTimeout> | null = null;
function persist(drafts: Record<DraftKey, AlterTableDraft>) {
  if (_persistTimer) clearTimeout(_persistTimer);
  _persistTimer = setTimeout(() => {
    try {
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify(drafts));
    } catch {
      // sessionStorage can throw on quota — swallowing is fine,
      // the in-memory draft still works for the session.
    }
  }, 500);
}

/** Connection id parsed back out of the composite key. */
function connectionIdFromKey(key: DraftKey): string {
  // Composite is `${connectionId}:${database}:${schema}:${table}` —
  // split off just the leading id (connection ids in Tablio are UUIDs,
  // so they don't contain `:`).
  const idx = key.indexOf(":");
  return idx === -1 ? key : key.slice(0, idx);
}

export const useAlterTableDraftStore = create<State>((set, get) => ({
  drafts: loadPersisted(),

  getDraft: (key) => get().drafts[key],

  setDraft: (key, patch) => {
    set((s) => {
      const existing: AlterTableDraft = s.drafts[key] ?? {
        tableNameLocal: "",
        operations: [],
        pendingNewColumns: [],
        showPreview: false,
        columnFilter: "",
      };
      const next: AlterTableDraft = { ...existing, ...patch };
      const drafts = { ...s.drafts, [key]: next };
      persist(drafts);
      return { drafts };
    });
  },

  clearDraft: (key) => {
    set((s) => {
      if (!(key in s.drafts)) return s;
      const drafts = { ...s.drafts };
      delete drafts[key];
      persist(drafts);
      return { drafts };
    });
  },

  pruneStaleConnections: (validConnectionIds) => {
    set((s) => {
      const drafts = { ...s.drafts };
      let mutated = false;
      for (const key of Object.keys(drafts)) {
        if (!validConnectionIds.has(connectionIdFromKey(key))) {
          delete drafts[key];
          mutated = true;
        }
      }
      if (!mutated) return s;
      persist(drafts);
      return { drafts };
    });
  },
}));
