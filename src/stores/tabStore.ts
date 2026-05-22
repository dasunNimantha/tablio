import { create } from "zustand";

export type TabType = "table" | "query" | "ddl" | "structure" | "inspector" | "activity" | "stats" | "roles" | "chart" | "erd" | "querystats" | "partitions";

export interface TabInfo {
  id: string;
  type: TabType;
  title: string;
  connectionId: string;
  connectionColor: string;
  database: string;
  schema: string;
  table?: string;
  objectType?: string;
  /** Active sub-tab inside a TableView ("data" | "columns" | ...).
   *  Persisted so the user comes back to the same panel they left. */
  subTab?: string;
}

const STORAGE_KEY = "tablio-open-tabs";

try { localStorage.removeItem(STORAGE_KEY); } catch {}

function loadPersistedTabs(): { tabs: TabInfo[]; activeTabId: string | null } {
  try {
    const raw = sessionStorage.getItem(STORAGE_KEY);
    if (!raw) return { tabs: [], activeTabId: null };
    const data = JSON.parse(raw);
    if (Array.isArray(data.tabs)) {
      return { tabs: data.tabs, activeTabId: data.activeTabId || null };
    }
  } catch {}
  return { tabs: [], activeTabId: null };
}

let _persistTimer: ReturnType<typeof setTimeout> | null = null;
function persistTabs(tabs: TabInfo[], activeTabId: string | null) {
  if (_persistTimer) clearTimeout(_persistTimer);
  _persistTimer = setTimeout(() => {
    try {
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify({ tabs, activeTabId }));
    } catch {}
  }, 500);
}

interface TabState {
  tabs: TabInfo[];
  activeTabId: string | null;

  openTab: (tab: TabInfo) => void;
  closeTab: (id: string) => void;
  closeOtherTabs: (id: string) => void;
  closeAllTabs: () => void;
  setActiveTab: (id: string) => void;
  setTabSubTab: (id: string, subTab: string) => void;
  /**
   * Replace just the `title` field on a tab. Used when a query tab
   * loads a saved query (the tab should adopt the saved name) and
   * after the first "Save…" of a brand-new query (the dialog name
   * becomes the tab title). No-op if `id` doesn't exist.
   */
  setTabTitle: (id: string, title: string) => void;
  reorderTabs: (fromIndex: number, toIndex: number) => void;
  pruneStaleConnections: (validConnectionIds: Set<string>) => void;
}

const initial = loadPersistedTabs();

export const useTabStore = create<TabState>((set, get) => ({
  tabs: initial.tabs,
  activeTabId: initial.activeTabId,

  openTab: (tab) => {
    const existing = get().tabs.find((t) => t.id === tab.id);
    if (existing) {
      // If the caller is asking us to focus an existing tab AND switch to a
      // specific sub-tab (e.g. right-click "Open Inspector" on a tab whose
      // Data view is already open), we update subTab so the existing tab
      // jumps to the requested panel instead of staying where it was.
      if (tab.subTab && tab.subTab !== existing.subTab) {
        const newTabs = get().tabs.map((t) =>
          t.id === tab.id ? { ...t, subTab: tab.subTab } : t
        );
        set({ tabs: newTabs, activeTabId: tab.id });
        persistTabs(newTabs, tab.id);
        return;
      }
      set({ activeTabId: tab.id });
      persistTabs(get().tabs, tab.id);
      return;
    }
    const newTabs = [...get().tabs, tab];
    set({ tabs: newTabs, activeTabId: tab.id });
    persistTabs(newTabs, tab.id);
  },

  closeTab: (id) => {
    set((s) => {
      const idx = s.tabs.findIndex((t) => t.id === id);
      const newTabs = s.tabs.filter((t) => t.id !== id);
      let newActive = s.activeTabId;
      if (s.activeTabId === id) {
        if (newTabs.length === 0) {
          newActive = null;
        } else {
          const newIdx = Math.min(idx, newTabs.length - 1);
          newActive = newTabs[newIdx].id;
        }
      }
      persistTabs(newTabs, newActive);
      return { tabs: newTabs, activeTabId: newActive };
    });
  },

  closeOtherTabs: (id) => {
    const newTabs = get().tabs.filter((t) => t.id === id);
    set({ tabs: newTabs, activeTabId: id });
    persistTabs(newTabs, id);
  },

  closeAllTabs: () => {
    set({ tabs: [], activeTabId: null });
    persistTabs([], null);
  },

  setActiveTab: (id) => {
    set({ activeTabId: id });
    persistTabs(get().tabs, id);
  },

  setTabSubTab: (id, subTab) => {
    set((s) => {
      const newTabs = s.tabs.map((t) =>
        t.id === id ? { ...t, subTab } : t
      );
      persistTabs(newTabs, s.activeTabId);
      return { tabs: newTabs };
    });
  },

  setTabTitle: (id, title) => {
    set((s) => {
      // Bail out cheaply when the title is unchanged so we don't
      // trip a re-render of every consumer of `tabs` for a no-op.
      const existing = s.tabs.find((t) => t.id === id);
      if (!existing || existing.title === title) return s;
      const newTabs = s.tabs.map((t) =>
        t.id === id ? { ...t, title } : t
      );
      persistTabs(newTabs, s.activeTabId);
      return { tabs: newTabs };
    });
  },

  reorderTabs: (fromIndex, toIndex) => {
    set((s) => {
      const newTabs = [...s.tabs];
      const [moved] = newTabs.splice(fromIndex, 1);
      newTabs.splice(toIndex, 0, moved);
      persistTabs(newTabs, s.activeTabId);
      return { tabs: newTabs };
    });
  },

  pruneStaleConnections: (validConnectionIds) => {
    set((s) => {
      const newTabs = s.tabs.filter((t) => validConnectionIds.has(t.connectionId));
      if (newTabs.length === s.tabs.length) return s;
      const newActive =
        s.activeTabId && newTabs.some((t) => t.id === s.activeTabId)
          ? s.activeTabId
          : newTabs.length > 0
          ? newTabs[0].id
          : null;
      persistTabs(newTabs, newActive);
      return { tabs: newTabs, activeTabId: newActive };
    });
  },
}));
