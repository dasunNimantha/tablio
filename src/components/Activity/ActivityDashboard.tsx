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
  { key: "overview", label: "Overview", icon: <BarChart3 size={13} /> },
  { key: "sessions", label: "Sessions", icon: <Users size={13} /> },
  { key: "locks", label: "Locks", icon: <Lock size={13} /> },
  { key: "config", label: "Configuration", icon: <Settings size={13} /> },
];

export function ActivityDashboard({ connectionId }: Props) {
  const [subTab, setSubTab] = useState<SubTab>("overview");
  const [paused, setPaused] = useState(false);

  const showLiveToggle = subTab !== "config";

  return (
    <div className="activity-dashboard">
      <div className="activity-toolbar">
        <div className="dashboard-sub-tabs">
          {SUB_TABS.map((t) => (
            <button
              key={t.key}
              className={`dashboard-sub-tab ${subTab === t.key ? "active" : ""}`}
              onClick={() => setSubTab(t.key)}
            >
              {t.icon}
              {t.label}
            </button>
          ))}
        </div>

        <div style={{ flex: 1 }} />

        {showLiveToggle && (
          <button
            className={`btn-ghost ${paused ? "" : "active"}`}
            onClick={() => setPaused(!paused)}
            title={paused ? "Resume auto-refresh" : "Pause auto-refresh"}
          >
            {paused ? <Play size={14} /> : <Pause size={14} />}
            {paused ? "Stream" : "Live"}
          </button>
        )}
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
