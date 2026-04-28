import { useTabStore } from "../../stores/tabStore";
import { useShallow } from "zustand/react/shallow";
import { QueryConsole } from "../QueryConsole/QueryConsole";
import { DDLViewer } from "../DDLViewer/DDLViewer";
import { ActivityDashboard } from "../Activity/ActivityDashboard";
import { TableStats } from "../TableStats/TableStats";
import { RoleManager } from "../RoleManager/RoleManager";
import { ERDView } from "../ERD/ERDView";
import { QueryStats } from "../QueryStats/QueryStats";
import { TableView, TableSubTab } from "../TableView/TableView";

// Map legacy tab types onto the integrated TableView so stale tabs from
// before the Data/Schema mode refactor still land somewhere sensible.
function legacySubTab(type: string): TableSubTab | undefined {
  switch (type) {
    case "inspector":
    case "structure":
      return "schema";
    case "partitions":
      return "schema:partitions";
    case "stats":
      return "schema:stats";
    default:
      return undefined;
  }
}

const TABLE_VIEW_TYPES = new Set([
  "table",
  "inspector",
  "structure",
  "partitions",
]);

export function TabContent() {
  const { tabs, activeTabId } = useTabStore(useShallow((s) => ({ tabs: s.tabs, activeTabId: s.activeTabId })));
  const activeTab = tabs.find((t) => t.id === activeTabId);

  if (!activeTab) return null;

  return (
    <div style={{ flex: 1, overflow: "hidden" }}>
      {tabs.map((tab) => (
        <div
          key={tab.id}
          style={{
            display: tab.id === activeTabId ? "flex" : "none",
            flexDirection: "column",
            height: "100%",
          }}
        >
          {TABLE_VIEW_TYPES.has(tab.type) && (
            <TableView
              tabId={tab.id}
              connectionId={tab.connectionId}
              connectionColor={tab.connectionColor}
              database={tab.database}
              schema={tab.schema}
              table={tab.table!}
              initialSubTab={
                (tab.subTab as TableSubTab | undefined) ??
                legacySubTab(tab.type) ??
                "data"
              }
            />
          )}
          {tab.type === "query" && (
            <QueryConsole
              connectionId={tab.connectionId}
              database={tab.database}
            />
          )}
          {tab.type === "ddl" && (
            <DDLViewer
              connectionId={tab.connectionId}
              database={tab.database}
              schema={tab.schema}
              objectName={tab.table!}
              objectType={tab.objectType || "TABLE"}
            />
          )}
          {tab.type === "activity" && (
            <ActivityDashboard connectionId={tab.connectionId} />
          )}
          {tab.type === "stats" && (
            // Standalone Stats tab is legacy — new code routes Stats
            // through the TableView sub-tab. Kept for back-compat.
            <TableStats
              connectionId={tab.connectionId}
              database={tab.database}
              schema={tab.schema}
              table={tab.table!}
            />
          )}
          {tab.type === "roles" && (
            <RoleManager connectionId={tab.connectionId} />
          )}
          {tab.type === "erd" && (
            <ERDView
              connectionId={tab.connectionId}
              database={tab.database}
              schema={tab.schema}
            />
          )}
          {tab.type === "querystats" && (
            <QueryStats connectionId={tab.connectionId} />
          )}
        </div>
      ))}
    </div>
  );
}
