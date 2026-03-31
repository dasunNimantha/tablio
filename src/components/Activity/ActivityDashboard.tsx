import { useState } from "react";
import { Pause, Play, BarChart3, Users, Lock, Settings } from "lucide-react";
import { DashboardOverview } from "./DashboardOverview";
import { DashboardSessions } from "./DashboardSessions";
import { DashboardLocks } from "./DashboardLocks";
import { DashboardConfig } from "./DashboardConfig";
import "./ActivityDashboard.css";

interface Props {
  connectionId: string;
}

type SubTab = "overview" | "sessions" | "locks" | "config";

const SUB_TABS: { key: SubTab; label: string; icon: React.ReactNode }[] = [
  { key: "overview", label: "Overview", icon: <BarChart3 size={14} /> },
  { key: "sessions", label: "Sessions", icon: <Users size={14} /> },
  { key: "locks", label: "Locks", icon: <Lock size={14} /> },
  { key: "config", label: "Configuration", icon: <Settings size={14} /> },
];

export function ActivityDashboard({ connectionId }: Props) {
  const [subTab, setSubTab] = useState<SubTab>("overview");
  const [paused, setPaused] = useState(false);

  const showLiveToggle = subTab !== "config";
  const currentTab = SUB_TABS.find((t) => t.key === subTab);

  return (
    <div className="activity-dashboard">
      <div className="activity-toolbar">
        <div className="activity-toolbar-left">
          <div className="activity-toolbar-heading">
            <span className="activity-toolbar-kicker">Activity</span>
            <span className="activity-toolbar-title">{currentTab?.label ?? "Overview"}</span>
          </div>
          <div className="dashboard-sub-tabs">
            {SUB_TABS.map((t) => (
              <button
                key={t.key}
                className={`dashboard-sub-tab ${subTab === t.key ? "active" : ""}`}
                onClick={() => setSubTab(t.key)}
                aria-pressed={subTab === t.key}
              >
                {t.icon}
                {t.label}
              </button>
            ))}
          </div>
        </div>

        <div className="activity-toolbar-right">
          {showLiveToggle && (
            <button
              className={`activity-live-toggle ${paused ? "paused" : "live"}`}
              onClick={() => setPaused(!paused)}
              title={paused ? "Resume auto-refresh" : "Pause auto-refresh"}
            >
              <span className="activity-live-dot" />
              {paused ? <Play size={14} /> : <Pause size={14} />}
              {paused ? "Paused" : "Live"}
            </button>
          )}
        </div>
      </div>

      {subTab === "overview" && (
        <DashboardOverview connectionId={connectionId} paused={paused} />
      )}
      {subTab === "sessions" && (
        <DashboardSessions connectionId={connectionId} paused={paused} />
      )}
      {subTab === "locks" && (
        <DashboardLocks connectionId={connectionId} paused={paused} />
      )}
      {subTab === "config" && (
        <DashboardConfig connectionId={connectionId} />
      )}
    </div>
  );
}
