import { useConnectionStore } from "../stores/connectionStore";
import { useTabStore, type TabInfo } from "../stores/tabStore";

/**
 * Ctrl/Cmd+N: a new untitled query console for the connection of the
 * active tab (falling back to the first open connection). Returns false
 * when nothing is connected.
 */
export function openNewQueryTab(): boolean {
  const { tabs, activeTabId, openTab } = useTabStore.getState();
  const { connections, activeConnections } = useConnectionStore.getState();
  const active = tabs.find((t) => t.id === activeTabId);
  let target: { connectionId: string; color: string; database: string } | null = null;
  if (active && activeConnections.has(active.connectionId)) {
    target = { connectionId: active.connectionId, color: active.connectionColor, database: active.database };
  } else {
    const conn = connections.find((c) => activeConnections.has(c.id));
    if (conn) target = { connectionId: conn.id, color: conn.color, database: conn.database };
  }
  if (!target) return false;
  const tab: TabInfo = {
    id: `query:${target.connectionId}:${target.database}:${Date.now()}`,
    type: "query",
    title: `Query - ${target.database}`,
    connectionId: target.connectionId,
    connectionColor: target.color,
    database: target.database,
    schema: "",
  };
  openTab(tab);
  return true;
}
