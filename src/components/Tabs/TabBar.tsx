import { useTabStore } from "../../stores/tabStore";
import { useShallow } from "zustand/react/shallow";
import { X, Table2, Terminal, Code, Activity, BarChart3, Shield, TrendingUp } from "lucide-react";
import { useState, useRef, useEffect } from "react";
import { readZoomFactor } from "../../lib/zoom";
import { ConfirmDialog } from "../ConfirmDialog";
import "./Tabs.css";

export function TabBar() {
  const { tabs, activeTabId, setActiveTab, closeTab, closeOtherTabs, closeAllTabs } =
    useTabStore(useShallow((s) => ({
      tabs: s.tabs,
      activeTabId: s.activeTabId,
      setActiveTab: s.setActiveTab,
      closeTab: s.closeTab,
      closeOtherTabs: s.closeOtherTabs,
      closeAllTabs: s.closeAllTabs,
    })));
  const [contextMenu, setContextMenu] = useState<{
    x: number;
    y: number;
    tabId: string;
  } | null>(null);
  const [dragIdx, setDragIdx] = useState<number | null>(null);
  const [pendingClose, setPendingClose] = useState<{ titles: string[]; action: () => void } | null>(null);

  /** Close tabs, confirming first when any of them holds unsaved work. */
  const requestClose = (ids: string[], action: () => void) => {
    const { isTabDirty, tabs: current } = useTabStore.getState();
    const dirty = current.filter((t) => ids.includes(t.id) && isTabDirty(t.id));
    if (dirty.length === 0) {
      action();
      return;
    }
    setPendingClose({ titles: dirty.map((t) => t.title), action });
  };
  const tabsRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!activeTabId || !tabsRef.current) return;
    const el = tabsRef.current.querySelector(`[data-tab-id="${activeTabId}"]`) as HTMLElement | null;
    if (el) el.scrollIntoView({ behavior: "smooth", block: "nearest", inline: "nearest" });
  }, [activeTabId, tabs.length]);

  const handleContextMenu = (e: React.MouseEvent, tabId: string) => {
    e.preventDefault();
    const z = readZoomFactor();
    setContextMenu({ x: e.clientX / z, y: e.clientY / z, tabId });
  };

  const handleDragStart = (e: React.DragEvent, idx: number) => {
    setDragIdx(idx);
    e.dataTransfer.effectAllowed = "move";
  };

  const handleDragOver = (e: React.DragEvent, idx: number) => {
    e.preventDefault();
    if (dragIdx !== null && dragIdx !== idx) {
      useTabStore.getState().reorderTabs(dragIdx, idx);
      setDragIdx(idx);
    }
  };

  const handleDragEnd = () => {
    setDragIdx(null);
  };

  return (
    <>
      <div className="tab-bar" ref={tabsRef}>
        {tabs.map((tab, idx) => (
          <div
            key={tab.id}
            data-tab-id={tab.id}
            className={`tab ${tab.id === activeTabId ? "active" : ""}`}
            onClick={() => setActiveTab(tab.id)}
            onContextMenu={(e) => handleContextMenu(e, tab.id)}
            draggable
            onDragStart={(e) => handleDragStart(e, idx)}
            onDragOver={(e) => handleDragOver(e, idx)}
            onDragEnd={handleDragEnd}
          >
            <span
              className="tab-color-dot"
              style={{ background: tab.connectionColor }}
            />
            <span className="tab-icon">
              {tab.type === "table" ||
              tab.type === "structure" ||
              tab.type === "inspector" ||
              tab.type === "partitions" ? (
                // All per-table tab variants render TableView, so they
                // share the same icon — fewer visual variants in the
                // tab bar makes scanning across many tabs easier.
                <Table2 size={12} />
              ) : tab.type === "ddl" ? (
                <Code size={12} />
              ) : tab.type === "activity" ? (
                <Activity size={12} />
              ) : tab.type === "stats" ? (
                <BarChart3 size={12} />
              ) : tab.type === "roles" ? (
                <Shield size={12} />
              ) : tab.type === "querystats" ? (
                <TrendingUp size={12} />
              ) : (
                <Terminal size={12} />
              )}
            </span>
            <span className="tab-title">
              {tab.title}
            </span>
            <button
              className="tab-close"
              aria-label={`Close ${tab.title}`}
              onClick={(e) => {
                e.stopPropagation();
                requestClose([tab.id], () => closeTab(tab.id));
              }}
            >
              <X size={12} />
            </button>
          </div>
        ))}
      </div>

      {contextMenu && (
        <>
          <div
            className="context-backdrop"
            onClick={() => setContextMenu(null)}
          />
          <div
            className="context-menu"
            style={{ left: contextMenu.x, top: contextMenu.y }}
            ref={(el) => {
              if (!el) return;
              const z = readZoomFactor();
              const cssVh = window.innerHeight / z;
              const cssVw = window.innerWidth / z;
              if (contextMenu.y + el.offsetHeight > cssVh) {
                el.style.top = `${Math.max(4, cssVh - el.offsetHeight)}px`;
              }
              if (contextMenu.x + el.offsetWidth > cssVw) {
                el.style.left = `${Math.max(4, cssVw - el.offsetWidth)}px`;
              }
            }}
          >
            <button
              onClick={() => {
                const id = contextMenu.tabId;
                setContextMenu(null);
                requestClose([id], () => closeTab(id));
              }}
            >
              Close
            </button>
            <button
              onClick={() => {
                const id = contextMenu.tabId;
                setContextMenu(null);
                requestClose(
                  tabs.filter((t) => t.id !== id).map((t) => t.id),
                  () => closeOtherTabs(id),
                );
              }}
            >
              Close Others
            </button>
            <button
              onClick={() => {
                setContextMenu(null);
                requestClose(tabs.map((t) => t.id), () => closeAllTabs());
              }}
            >
              Close All
            </button>
          </div>
        </>
      )}

      {pendingClose && (
        <ConfirmDialog
          title="Discard unsaved changes?"
          message={
            pendingClose.titles.length === 1
              ? `"${pendingClose.titles[0]}" has unsaved changes. Close it and discard them?`
              : `These tabs have unsaved changes: ${pendingClose.titles.join(", ")}. Close them and discard the changes?`
          }
          confirmLabel="Discard and close"
          danger
          onConfirm={() => {
            const { action } = pendingClose;
            setPendingClose(null);
            action();
          }}
          onCancel={() => setPendingClose(null)}
        />
      )}
    </>
  );
}
