import { useEffect, useMemo, useState, useCallback } from "react";
import {
  Loader2,
  AlertCircle,
  Table2,
  Layers,
} from "lucide-react";
import {
  api,
  ColumnInfo,
  IndexInfo,
  ForeignKeyInfo,
  ReferencingTableInfo,
  TriggerInfo,
  TableInfo as TableInfoType,
} from "../../lib/tauri";
import { useTabStore } from "../../stores/tabStore";
import { DataGrid } from "../DataGrid/DataGrid";
import { SchemaPage } from "./SchemaPage";
import {
  parseSubTab,
  serializeSubTab,
  type SchemaAnchor,
  type TableMode,
} from "./subTab";
import "./TableView.css";

export type { TableMode } from "./subTab";

/** Persisted view state on the tab. See ./subTab for the grammar. */
export type TableSubTab = string;

interface Props {
  tabId: string;
  connectionId: string;
  connectionColor: string;
  database: string;
  schema: string;
  table: string;
  initialSubTab?: TableSubTab;
}

interface CheapMetadata {
  columns: ColumnInfo[];
  indexes: IndexInfo[];
  fks: ForeignKeyInfo[];
  refs: ReferencingTableInfo[];
  triggers: TriggerInfo[];
  meta: TableInfoType | null;
}

export function TableView({
  tabId,
  connectionId,
  connectionColor,
  database,
  schema,
  table,
  initialSubTab,
}: Props) {
  const setTabSubTab = useTabStore((s) => s.setTabSubTab);

  const initialParsed = useMemo(() => parseSubTab(initialSubTab), [initialSubTab]);
  const [mode, setMode] = useState<TableMode>(initialParsed.mode);
  const [focusAnchor, setFocusAnchor] = useState<SchemaAnchor | undefined>(
    initialParsed.anchor
  );
  // Both panels stay mounted after first activation so toggling doesn't
  // refetch / reset scroll / drop edit state.
  const [mountedModes, setMountedModes] = useState<Set<TableMode>>(
    () => new Set([initialParsed.mode])
  );

  const [meta, setMeta] = useState<CheapMetadata | null>(null);
  const [metaLoading, setMetaLoading] = useState(true);
  const [metaError, setMetaError] = useState<string | null>(null);

  // Re-sync if the parent prop changes (e.g. right-click "Open Inspector"
  // on the same table while the Data view is open).
  useEffect(() => {
    const parsed = parseSubTab(initialSubTab);
    setMode(parsed.mode);
    setFocusAnchor(parsed.anchor);
    setMountedModes((prev) => {
      if (prev.has(parsed.mode)) return prev;
      const next = new Set(prev);
      next.add(parsed.mode);
      return next;
    });
  }, [initialSubTab]);

  useEffect(() => {
    let cancelled = false;
    setMetaLoading(true);
    setMetaError(null);
    const load = async () => {
      try {
        const [columns, indexes, fks, refs, triggers, allTables] = await Promise.all([
          api.listColumns(connectionId, database, schema, table),
          api.listIndexes(connectionId, database, schema, table),
          api.listForeignKeys(connectionId, database, schema, table),
          api
            .listReferencedBy(connectionId, database, schema, table)
            .catch(() => [] as ReferencingTableInfo[]),
          api
            .listTriggers(connectionId, database, schema, table)
            .catch(() => [] as TriggerInfo[]),
          api
            .listTables(connectionId, database, schema)
            .catch(() => [] as TableInfoType[]),
        ]);
        if (cancelled) return;
        setMeta({
          columns,
          indexes,
          fks,
          refs,
          triggers,
          meta: allTables.find((t) => t.name === table) ?? null,
        });
      } catch (e) {
        if (cancelled) return;
        setMetaError(String(e));
      } finally {
        if (!cancelled) setMetaLoading(false);
      }
    };
    load();
    return () => {
      cancelled = true;
    };
  }, [connectionId, database, schema, table]);

  const selectMode = useCallback(
    (next: TableMode) => {
      setMode(next);
      setMountedModes((prev) => {
        if (prev.has(next)) return prev;
        const updated = new Set(prev);
        updated.add(next);
        return updated;
      });
      // Drop the anchor when the user manually flips modes — they're
      // now driving navigation, not the deep link.
      setFocusAnchor(undefined);
      setTabSubTab(tabId, serializeSubTab(next));
    },
    [tabId, setTabSubTab]
  );

  const partitionStrategy = meta?.meta?.partition_strategy ?? null;
  const isPartitioned = !!partitionStrategy;

  return (
    <div className="tv">
      <div className="tv-header">
        <div className="tv-header-left">
          <span className="tv-name">
            <span className="tv-name-schema">{schema}.</span>
            {table}
          </span>
          {meta?.meta?.table_type && (
            <span className="tv-pill">{meta.meta.table_type}</span>
          )}
          {isPartitioned && (
            <span className={`tv-pill tv-pill-${partitionStrategy}`}>
              {partitionStrategy}
            </span>
          )}
        </div>

        <div className="tv-header-right">
          <div className="tv-mode-switch" role="tablist" aria-label="View mode">
            <button
              role="tab"
              aria-selected={mode === "data"}
              className={`tv-mode-switch-btn ${mode === "data" ? "active" : ""}`}
              onClick={() => selectMode("data")}
            >
              <Table2 size={13} aria-hidden="true" />
              <span>Data</span>
            </button>
            <button
              role="tab"
              aria-selected={mode === "schema"}
              className={`tv-mode-switch-btn ${mode === "schema" ? "active" : ""}`}
              onClick={() => selectMode("schema")}
            >
              <Layers size={13} aria-hidden="true" />
              <span>Schema</span>
            </button>
          </div>
        </div>
      </div>

      {metaError && (
        <div className="tv-banner tv-banner-error">
          <AlertCircle size={14} />
          <span>Inspector data failed to load: {metaError}</span>
        </div>
      )}

      <div className="tv-body">
        {mountedModes.has("data") && (
          <div
            className="tv-panel"
            style={{ display: mode === "data" ? "flex" : "none" }}
          >
            <DataGrid
              connectionId={connectionId}
              database={database}
              schema={schema}
              table={table}
              hideTitle
            />
          </div>
        )}

        {mountedModes.has("schema") && (
          <div
            className="tv-panel"
            style={{ display: mode === "schema" ? "flex" : "none" }}
          >
            {metaLoading ? (
              <div className="tv-panel-loader">
                <Loader2 size={20} className="spin" />
                <span>Loading schema...</span>
              </div>
            ) : (
              <SchemaPage
                tabId={tabId}
                connectionId={connectionId}
                connectionColor={connectionColor}
                database={database}
                schema={schema}
                table={table}
                columns={meta?.columns ?? []}
                indexes={meta?.indexes ?? []}
                foreignKeys={meta?.fks ?? []}
                references={meta?.refs ?? []}
                triggers={meta?.triggers ?? []}
                tableMeta={meta?.meta ?? null}
                focusAnchor={focusAnchor}
                onAnchorConsumed={() => setFocusAnchor(undefined)}
              />
            )}
          </div>
        )}
      </div>
    </div>
  );
}
