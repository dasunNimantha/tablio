import { useTabStore } from "../../stores/tabStore";
import { DataGrid } from "../DataGrid/DataGrid";
import { QueryConsole } from "../QueryConsole/QueryConsole";
import { DDLViewer } from "../DDLViewer/DDLViewer";
import { TableInfo } from "../TableInfo/TableInfo";
import { ActivityDashboard } from "../Activity/ActivityDashboard";
import { TableStats } from "../TableStats/TableStats";
import { RoleManager } from "../RoleManager/RoleManager";
import { ERDView } from "../ERD/ERDView";
import { QueryStats } from "../QueryStats/QueryStats";

export function TabContent() {
  const { tabs, activeTabId } = useTabStore();
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
          {tab.type === "table" && (
            <DataGrid
              connectionId={tab.connectionId}
              database={tab.database}
              schema={tab.schema}
              table={tab.table!}
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
          {tab.type === "structure" && (
            <TableInfo
              connectionId={tab.connectionId}
              database={tab.database}
              schema={tab.schema}
              table={tab.table!}
            />
          )}
          {tab.type === "activity" && (
            <ActivityDashboard connectionId={tab.connectionId} />
          )}
          {tab.type === "stats" && (
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
