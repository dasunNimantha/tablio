import { describe, it, expect, beforeEach } from "vitest";
import { useTabStore, type TabInfo } from "./tabStore";

function makeTab(id: string, connectionId = "conn1"): TabInfo {
  return {
    id,
    type: "table",
    title: `Tab ${id}`,
    connectionId,
    connectionColor: "#fff",
    database: "db",
    schema: "public",
    table: `table_${id}`,
  };
}

describe("tabStore", () => {
  beforeEach(() => {
    useTabStore.setState({ tabs: [], activeTabId: null });
  });

  describe("openTab", () => {
    it("adds a new tab and sets it active", () => {
      useTabStore.getState().openTab(makeTab("t1"));
      const state = useTabStore.getState();
      expect(state.tabs).toHaveLength(1);
      expect(state.activeTabId).toBe("t1");
    });

    it("does not duplicate existing tab, just activates it", () => {
      useTabStore.getState().openTab(makeTab("t1"));
      useTabStore.getState().openTab(makeTab("t2"));
      useTabStore.getState().openTab(makeTab("t1"));
      const state = useTabStore.getState();
      expect(state.tabs).toHaveLength(2);
      expect(state.activeTabId).toBe("t1");
    });

    it("supports multiple different tabs", () => {
      useTabStore.getState().openTab(makeTab("t1"));
      useTabStore.getState().openTab(makeTab("t2"));
      useTabStore.getState().openTab(makeTab("t3"));
      expect(useTabStore.getState().tabs).toHaveLength(3);
      expect(useTabStore.getState().activeTabId).toBe("t3");
    });
  });

  describe("closeTab", () => {
    it("removes the tab", () => {
      useTabStore.getState().openTab(makeTab("t1"));
      useTabStore.getState().openTab(makeTab("t2"));
      useTabStore.getState().closeTab("t1");
      expect(useTabStore.getState().tabs).toHaveLength(1);
      expect(useTabStore.getState().tabs[0].id).toBe("t2");
    });

    it("activates next tab when closing the active tab", () => {
      useTabStore.getState().openTab(makeTab("t1"));
      useTabStore.getState().openTab(makeTab("t2"));
      useTabStore.getState().openTab(makeTab("t3"));
      useTabStore.getState().setActiveTab("t2");
      useTabStore.getState().closeTab("t2");
      expect(useTabStore.getState().activeTabId).toBe("t3");
    });

    it("activates previous tab when closing last tab in list", () => {
      useTabStore.getState().openTab(makeTab("t1"));
      useTabStore.getState().openTab(makeTab("t2"));
      useTabStore.getState().closeTab("t2");
      expect(useTabStore.getState().activeTabId).toBe("t1");
    });

    it("sets activeTabId to null when closing the only tab", () => {
      useTabStore.getState().openTab(makeTab("t1"));
      useTabStore.getState().closeTab("t1");
      expect(useTabStore.getState().tabs).toHaveLength(0);
      expect(useTabStore.getState().activeTabId).toBeNull();
    });

    it("does not change active tab when closing a non-active tab", () => {
      useTabStore.getState().openTab(makeTab("t1"));
      useTabStore.getState().openTab(makeTab("t2"));
      useTabStore.getState().openTab(makeTab("t3"));
      useTabStore.getState().closeTab("t1");
      expect(useTabStore.getState().activeTabId).toBe("t3");
    });
  });

  describe("closeOtherTabs", () => {
    it("keeps only the specified tab", () => {
      useTabStore.getState().openTab(makeTab("t1"));
      useTabStore.getState().openTab(makeTab("t2"));
      useTabStore.getState().openTab(makeTab("t3"));
      useTabStore.getState().closeOtherTabs("t2");
      expect(useTabStore.getState().tabs).toHaveLength(1);
      expect(useTabStore.getState().tabs[0].id).toBe("t2");
      expect(useTabStore.getState().activeTabId).toBe("t2");
    });
  });

  describe("closeAllTabs", () => {
    it("removes all tabs", () => {
      useTabStore.getState().openTab(makeTab("t1"));
      useTabStore.getState().openTab(makeTab("t2"));
      useTabStore.getState().closeAllTabs();
      expect(useTabStore.getState().tabs).toHaveLength(0);
      expect(useTabStore.getState().activeTabId).toBeNull();
    });
  });

  describe("setActiveTab", () => {
    it("changes the active tab", () => {
      useTabStore.getState().openTab(makeTab("t1"));
      useTabStore.getState().openTab(makeTab("t2"));
      useTabStore.getState().setActiveTab("t1");
      expect(useTabStore.getState().activeTabId).toBe("t1");
    });
  });

  describe("reorderTabs", () => {
    it("moves a tab from one index to another", () => {
      useTabStore.getState().openTab(makeTab("t1"));
      useTabStore.getState().openTab(makeTab("t2"));
      useTabStore.getState().openTab(makeTab("t3"));
      useTabStore.getState().reorderTabs(0, 2);
      const ids = useTabStore.getState().tabs.map((t) => t.id);
      expect(ids).toEqual(["t2", "t3", "t1"]);
    });

    it("does not change active tab", () => {
      useTabStore.getState().openTab(makeTab("t1"));
      useTabStore.getState().openTab(makeTab("t2"));
      useTabStore.getState().reorderTabs(0, 1);
      expect(useTabStore.getState().activeTabId).toBe("t2");
    });
  });

  describe("pruneStaleConnections", () => {
    it("removes tabs with invalid connection IDs", () => {
      useTabStore.getState().openTab(makeTab("t1", "conn1"));
      useTabStore.getState().openTab(makeTab("t2", "conn2"));
      useTabStore.getState().openTab(makeTab("t3", "conn1"));
      useTabStore.getState().pruneStaleConnections(new Set(["conn1"]));
      expect(useTabStore.getState().tabs).toHaveLength(2);
      expect(useTabStore.getState().tabs.every((t) => t.connectionId === "conn1")).toBe(true);
    });

    it("updates active tab when pruned tab was active", () => {
      useTabStore.getState().openTab(makeTab("t1", "conn1"));
      useTabStore.getState().openTab(makeTab("t2", "conn2"));
      useTabStore.getState().setActiveTab("t2");
      useTabStore.getState().pruneStaleConnections(new Set(["conn1"]));
      expect(useTabStore.getState().activeTabId).toBe("t1");
    });

    it("sets null when all tabs are pruned", () => {
      useTabStore.getState().openTab(makeTab("t1", "conn1"));
      useTabStore.getState().pruneStaleConnections(new Set(["conn2"]));
      expect(useTabStore.getState().tabs).toHaveLength(0);
      expect(useTabStore.getState().activeTabId).toBeNull();
    });

    it("does nothing when all connections are valid", () => {
      useTabStore.getState().openTab(makeTab("t1", "conn1"));
      useTabStore.getState().openTab(makeTab("t2", "conn1"));
      const before = useTabStore.getState();
      useTabStore.getState().pruneStaleConnections(new Set(["conn1"]));
      const after = useTabStore.getState();
      expect(after.tabs).toEqual(before.tabs);
    });
  });
});
