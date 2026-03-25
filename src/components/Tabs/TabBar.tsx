import { useTabStore } from "../../stores/tabStore";
import { useShallow } from "zustand/react/shallow";
import { X, Table2, Terminal, Code, Columns3, Activity, BarChart3, Shield, TrendingUp } from "lucide-react";
import { useState, useRef } from "react";
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
  const tabsRef = useRef<HTMLDivElement>(null);

  const handleContextMenu = (e: React.MouseEvent, tabId: string) => {
    e.preventDefault();
    const z = parseFloat(document.documentElement.style.zoom || "100") / 100;
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
              {tab.type === "table" ? (
                <Table2 size={12} />
              ) : tab.type === "ddl" ? (
                <Code size={12} />
              ) : tab.type === "structure" ? (
                <Columns3 size={12} />
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
            <span className="tab-title" title={tab.title}>
              {tab.title}
            </span>
            <button
              className="tab-close"
              onClick={(e) => {
                e.stopPropagation();
                closeTab(tab.id);
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
              const z = parseFloat(document.documentElement.style.zoom || "100") / 100;
              const rect = el.getBoundingClientRect();
              const vh = window.innerHeight / z;
              const vw = window.innerWidth / z;
              const menuH = rect.height / z;
              const menuW = rect.width / z;
              if (contextMenu.y + menuH > vh) {
                el.style.top = `${Math.max(4, contextMenu.y - menuH)}px`;
              }
              if (contextMenu.x + menuW > vw) {
                el.style.left = `${Math.max(4, contextMenu.x - menuW)}px`;
              }
            }}
          >
            <button
              onClick={() => {
                closeTab(contextMenu.tabId);
                setContextMenu(null);
              }}
            >
              Close
            </button>
            <button
              onClick={() => {
                closeOtherTabs(contextMenu.tabId);
                setContextMenu(null);
              }}
            >
              Close Others
            </button>
            <button
              onClick={() => {
                closeAllTabs();
                setContextMenu(null);
              }}
            >
              Close All
            </button>
          </div>
        </>
      )}
    </>
  );
}
