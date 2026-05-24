import { useEffect, useMemo, useState } from "react";
import {
  Columns3,
  KeyRound,
  Boxes,
  ArrowRightLeft,
  CornerDownLeft,
  Zap,
  GitBranch,
  BarChart3,
} from "lucide-react";
import {
  ColumnInfo,
  IndexInfo,
  ForeignKeyInfo,
  ReferencingTableInfo,
  TriggerInfo,
  TableInfo as TableInfoType,
} from "../../lib/tauri";
import { TableStats } from "../TableStats/TableStats";
import { PartitionsView } from "../Partitions/PartitionsView";
import {
  ColumnsPanel,
  ConstraintsPanel,
  IndexesPanel,
  ForeignKeysPanel,
  ReferencesPanel,
  TriggersPanel,
} from "./panels";
import { AlterTableEditor } from "../AlterTable/AlterTableEditor";
import { useTabStore, TabInfo } from "../../stores/tabStore";

/**
 * The "Schema" half of the Data/Schema mode toggle in TableView.
 *
 * Shows a sub-tab strip (Columns / Constraints / Indexes / Foreign Keys
 * / References / Triggers / Partitions / Statistics) with one panel
 * mounted at a time. Activated panels stay mounted so flipping back
 * preserves any local state (search input, sort, scroll).
 *
 * Data fetching for the cheap metadata (columns/indexes/fks/refs/
 * triggers) is owned by TableView so flipping between Data and Schema
 * modes never refetches.
 */

// SchemaAnchor / SCHEMA_ANCHORS are owned by ./subTab so the parser
// and the renderer can never drift out of sync.
import { SCHEMA_ANCHORS, type SchemaAnchor } from "./subTab";
export type { SchemaAnchor };

interface Props {
  tabId: string;
  connectionId: string;
  connectionColor: string;
  database: string;
  schema: string;
  table: string;
  columns: ColumnInfo[];
  indexes: IndexInfo[];
  foreignKeys: ForeignKeyInfo[];
  references: ReferencingTableInfo[];
  triggers: TriggerInfo[];
  tableMeta: TableInfoType | null;
  /** Sub-tab to start on, set by deep-links (partition strategy chip,
   *  legacy `partitions`/`stats` tabs). */
  focusAnchor?: SchemaAnchor;
  onAnchorConsumed?: () => void;
  /**
   * Fired when the in-tab editor (issue #59) saves successfully so
   * `TableView` can refresh its cheap metadata fetch and the columns
   * list immediately reflects the alteration.
   */
  onAltered?: () => void;
}

export function SchemaPage({
  tabId,
  connectionId,
  connectionColor,
  database,
  schema,
  table,
  columns,
  indexes,
  foreignKeys,
  references,
  triggers,
  tableMeta,
  focusAnchor,
  onAnchorConsumed,
  onAltered,
}: Props) {
  const partitionStrategy = tableMeta?.partition_strategy ?? null;
  const isPartitioned = !!partitionStrategy;
  const setTabSubTab = useTabStore((s) => s.setTabSubTab);
  const openTab = useTabStore((s) => s.openTab);

  const [active, setActive] = useState<SchemaAnchor>(
    focusAnchor && SCHEMA_ANCHORS.includes(focusAnchor) ? focusAnchor : "columns"
  );
  const [mounted, setMounted] = useState<Set<SchemaAnchor>>(
    () => new Set([active])
  );
  // Edit/View toggle on the Columns sub-tab (issue #59).
  // `false` → render the read-only `ColumnsPanel`.
  // `true`  → render `AlterTableEditor` inline.
  // Persisted edits live in the draft store, so flipping this back
  // to `false` is non-destructive.
  const [columnsEditMode, setColumnsEditMode] = useState(false);

  // Honour deep-link anchors that arrive after mount (e.g. user clicks
  // the partition strategy chip while this tab is already open).
  useEffect(() => {
    if (!focusAnchor) return;
    if (!SCHEMA_ANCHORS.includes(focusAnchor)) return;
    setActive(focusAnchor);
    setMounted((prev) => {
      if (prev.has(focusAnchor)) return prev;
      const next = new Set(prev);
      next.add(focusAnchor);
      return next;
    });
    onAnchorConsumed?.();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [focusAnchor]);

  // If the metadata flips and we're sitting on Partitions for a table
  // that isn't partitioned, fall back to Columns.
  useEffect(() => {
    if (active === "partitions" && !isPartitioned) {
      setActive("columns");
    }
  }, [active, isPartitioned]);

  const constraintCount = useMemo(() => {
    const pkCols = columns.filter((c) => c.is_primary_key).map((c) => c.name);
    const pkSig = pkCols.join(",");
    const uniques = indexes.filter(
      (idx) => idx.is_unique && idx.columns.join(",") !== pkSig
    ).length;
    const notNullGroup = columns.some((c) => !c.is_nullable) ? 1 : 0;
    return (
      (pkCols.length ? 1 : 0) + uniques + foreignKeys.length + notNullGroup
    );
  }, [columns, indexes, foreignKeys]);

  const partitionCount = useMemo(() => {
    if (!tableMeta || !isPartitioned) return undefined;
    // The cheap metadata fetch in TableView only loaded *this* table; we
    // don't have a child count here. Show no count to avoid lying.
    return undefined;
  }, [tableMeta, isPartitioned]);

  const select = (anchor: SchemaAnchor) => {
    setActive(anchor);
    setMounted((prev) => {
      if (prev.has(anchor)) return prev;
      const next = new Set(prev);
      next.add(anchor);
      return next;
    });
    setTabSubTab(tabId, `schema:${anchor}`);
  };

  const openOtherTable = (
    targetSchema: string,
    targetTable: string,
    anchor: SchemaAnchor = "columns"
  ) => {
    const newTabId = `table:${connectionId}:${database}:${targetSchema}:${targetTable}`;
    const tab: TabInfo = {
      id: newTabId,
      type: "table",
      title: targetTable,
      connectionId,
      connectionColor,
      database,
      schema: targetSchema,
      table: targetTable,
      subTab: `schema:${anchor}`,
    };
    openTab(tab);
  };

  type TabDef = {
    id: SchemaAnchor;
    label: string;
    icon: React.ReactNode;
    count?: number;
    show?: boolean;
  };

  const tabs: TabDef[] = [
    {
      id: "columns",
      label: "Columns",
      icon: <Columns3 size={14} />,
      count: columns.length,
    },
    {
      id: "constraints",
      label: "Constraints",
      icon: <KeyRound size={14} />,
      count: constraintCount,
    },
    {
      id: "indexes",
      label: "Indexes",
      icon: <Boxes size={14} />,
      count: indexes.length,
    },
    {
      id: "fks",
      label: "Foreign Keys",
      icon: <ArrowRightLeft size={14} />,
      count: foreignKeys.length,
    },
    {
      id: "refs",
      label: "References",
      icon: <CornerDownLeft size={14} />,
      count: references.length,
    },
    {
      id: "triggers",
      label: "Triggers",
      icon: <Zap size={14} />,
      count: triggers.length,
    },
    {
      id: "partitions",
      label: "Partitions",
      icon: <GitBranch size={14} />,
      count: partitionCount,
      show: isPartitioned,
    },
    { id: "stats", label: "Statistics", icon: <BarChart3 size={14} /> },
  ];

  const visibleTabs = tabs.filter((t) => t.show !== false);

  return (
    <div className="tv-schema">
      <div className="tv-schema-strip" role="tablist" aria-label="Schema sections">
        {visibleTabs.map((t) => (
          <button
            key={t.id}
            role="tab"
            aria-selected={active === t.id}
            className={`tv-schema-tab ${active === t.id ? "active" : ""}`}
            onClick={() => select(t.id)}
          >
            <span className="tv-schema-tab-icon">{t.icon}</span>
            <span>{t.label}</span>
            {t.count != null && t.count > 0 && (
              <span className="tv-schema-tab-count">{t.count}</span>
            )}
          </button>
        ))}
      </div>

      <div className="tv-schema-body">
        {mounted.has("columns") && (
          <div
            className="tv-schema-panel tv-schema-panel-scroll"
            style={{ display: active === "columns" ? "block" : "none" }}
          >
            {columnsEditMode ? (
              // In-tab Alter Table editor (issue #59). On a
              // successful save we flip back to View mode AND
              // ask `TableView` to refresh its cheap metadata so
              // the read-only Columns panel reflects the new
              // schema immediately. The editor wipes its draft
              // store entry as part of save success — no extra
              // bookkeeping needed here.
              <AlterTableEditor
                connectionId={connectionId}
                database={database}
                schema={schema}
                tableName={table}
                initialColumns={columns}
                onSaved={() => {
                  setColumnsEditMode(false);
                  onAltered?.();
                }}
                onDiscard={() => setColumnsEditMode(false)}
                variant="inline"
              />
            ) : (
              <ColumnsPanel
                columns={columns}
                foreignKeys={foreignKeys}
                onEnterEdit={() => setColumnsEditMode(true)}
              />
            )}
          </div>
        )}

        {mounted.has("constraints") && (
          <div
            className="tv-schema-panel tv-schema-panel-scroll"
            style={{ display: active === "constraints" ? "block" : "none" }}
          >
            <ConstraintsPanel
              table={table}
              columns={columns}
              indexes={indexes}
              foreignKeys={foreignKeys}
            />
          </div>
        )}

        {mounted.has("indexes") && (
          <div
            className="tv-schema-panel tv-schema-panel-scroll"
            style={{ display: active === "indexes" ? "block" : "none" }}
          >
            <IndexesPanel indexes={indexes} />
          </div>
        )}

        {mounted.has("fks") && (
          <div
            className="tv-schema-panel tv-schema-panel-scroll"
            style={{ display: active === "fks" ? "block" : "none" }}
          >
            <ForeignKeysPanel
              foreignKeys={foreignKeys}
              onOpenTarget={(fk) =>
                openOtherTable(schema, fk.referenced_table, "columns")
              }
            />
          </div>
        )}

        {mounted.has("refs") && (
          <div
            className="tv-schema-panel tv-schema-panel-scroll"
            style={{ display: active === "refs" ? "block" : "none" }}
          >
            <ReferencesPanel
              references={references}
              onOpenSource={(r) =>
                openOtherTable(r.referencing_schema, r.referencing_table, "columns")
              }
            />
          </div>
        )}

        {mounted.has("triggers") && (
          <div
            className="tv-schema-panel tv-schema-panel-scroll"
            style={{ display: active === "triggers" ? "block" : "none" }}
          >
            <TriggersPanel triggers={triggers} />
          </div>
        )}

        {mounted.has("partitions") && isPartitioned && (
          <div
            className="tv-schema-panel"
            style={{ display: active === "partitions" ? "flex" : "none" }}
          >
            <PartitionsView
              connectionId={connectionId}
              connectionColor={connectionColor}
              database={database}
              schema={schema}
              parent={table}
              embedded
            />
          </div>
        )}

        {mounted.has("stats") && (
          <div
            className="tv-schema-panel"
            style={{ display: active === "stats" ? "flex" : "none" }}
          >
            <TableStats
              connectionId={connectionId}
              database={database}
              schema={schema}
              table={table}
              embedded
            />
          </div>
        )}
      </div>
    </div>
  );
}
