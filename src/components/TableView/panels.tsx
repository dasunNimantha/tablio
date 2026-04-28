import { useMemo } from "react";
import {
  ColumnInfo,
  IndexInfo,
  ForeignKeyInfo,
  ReferencingTableInfo,
  TriggerInfo,
} from "../../lib/tauri";

/* -----------------------------------------------------------------------
 * Stateless per-domain panels rendered inside TableView.
 *
 * These components own zero data fetching — they receive already-loaded
 * arrays as props. TableView is the single owner of network calls so
 * sub-tab labels can show counts without each panel firing duplicate
 * requests, and so flipping between panels is instant.
 * --------------------------------------------------------------------- */

interface ColumnsPanelProps {
  columns: ColumnInfo[];
  foreignKeys: ForeignKeyInfo[];
}

export function ColumnsPanel({ columns, foreignKeys }: ColumnsPanelProps) {
  const fksByColumn = useMemo(() => {
    const m = new Map<string, ForeignKeyInfo[]>();
    for (const fk of foreignKeys) {
      const list = m.get(fk.column) ?? [];
      list.push(fk);
      m.set(fk.column, list);
    }
    return m;
  }, [foreignKeys]);

  return (
    <table className="tv-table">
      <thead>
        <tr>
          <th style={{ width: 40 }}>#</th>
          <th>Name</th>
          <th>Type</th>
          <th style={{ width: 80 }}>Nullable</th>
          <th>Default</th>
          <th style={{ width: 110 }}>Key</th>
        </tr>
      </thead>
      <tbody>
        {columns.length === 0 ? (
          <tr>
            <td colSpan={6} className="tv-cell-empty">No columns</td>
          </tr>
        ) : (
          columns.map((col, i) => {
            const colFks = fksByColumn.get(col.name) ?? [];
            const fkTitle =
              colFks.length > 0
                ? colFks
                    .map((fk) => `${fk.referenced_table}.${fk.referenced_column}`)
                    .join(" | ")
                : undefined;
            return (
              <tr key={col.name}>
                <td className="tv-cell-muted">{i + 1}</td>
                <td className="tv-cell-name">{col.name}</td>
                <td className="tv-cell-type">{col.data_type}</td>
                <td>{col.is_nullable ? "YES" : "NO"}</td>
                <td className="tv-cell-muted">{col.default_value || "-"}</td>
                <td>
                  <span className="tv-badges">
                    {col.is_primary_key && (
                      <span className="tv-badge tv-badge-pk">PK</span>
                    )}
                    {colFks.length > 0 && (
                      <span className="tv-badge tv-badge-fk" title={fkTitle}>
                        FK
                      </span>
                    )}
                    {col.is_auto_generated && (
                      <span
                        className="tv-badge tv-badge-auto"
                        title="Auto-generated"
                      >
                        AUTO
                      </span>
                    )}
                  </span>
                </td>
              </tr>
            );
          })
        )}
      </tbody>
    </table>
  );
}

interface ConstraintRow {
  kind: "PRIMARY KEY" | "UNIQUE" | "FOREIGN KEY" | "NOT NULL";
  name: string;
  detail: string;
}

interface ConstraintsPanelProps {
  table: string;
  columns: ColumnInfo[];
  indexes: IndexInfo[];
  foreignKeys: ForeignKeyInfo[];
}

export function ConstraintsPanel({
  table,
  columns,
  indexes,
  foreignKeys,
}: ConstraintsPanelProps) {
  // CHECK constraints aren't a first-class catalog query yet — derive
  // what we can from data we already have. We surface the gap with an
  // inline note so users aren't misled into thinking the table has none.
  const constraints = useMemo<ConstraintRow[]>(() => {
    const out: ConstraintRow[] = [];
    const pkCols = columns.filter((c) => c.is_primary_key).map((c) => c.name);
    if (pkCols.length > 0) {
      out.push({
        kind: "PRIMARY KEY",
        name: `${table}_pkey`,
        detail: `(${pkCols.join(", ")})`,
      });
    }
    const pkSig = pkCols.join(",");
    for (const idx of indexes) {
      if (!idx.is_unique) continue;
      // Skip the PK's underlying unique index — already shown above.
      if (idx.columns.join(",") === pkSig) continue;
      out.push({
        kind: "UNIQUE",
        name: idx.name,
        detail: `(${idx.columns.join(", ")})`,
      });
    }
    for (const fk of foreignKeys) {
      out.push({
        kind: "FOREIGN KEY",
        name: fk.name,
        detail: `(${fk.column}) REFERENCES ${fk.referenced_table}(${fk.referenced_column})`,
      });
    }
    const notNullCount = columns.filter((c) => !c.is_nullable).length;
    if (notNullCount > 0) {
      out.push({
        kind: "NOT NULL",
        name: `${notNullCount} column${notNullCount === 1 ? "" : "s"}`,
        detail: columns
          .filter((c) => !c.is_nullable)
          .map((c) => c.name)
          .join(", "),
      });
    }
    return out;
  }, [table, columns, indexes, foreignKeys]);

  return (
    <>
      <table className="tv-table">
        <thead>
          <tr>
            <th style={{ width: 130 }}>Type</th>
            <th>Name</th>
            <th>Definition</th>
          </tr>
        </thead>
        <tbody>
          {constraints.length === 0 ? (
            <tr>
              <td colSpan={3} className="tv-cell-empty">No constraints</td>
            </tr>
          ) : (
            constraints.map((c, i) => (
              <tr key={`${c.kind}-${c.name}-${i}`}>
                <td>
                  <span
                    className={`tv-badge tv-badge-constraint tv-badge-c-${c.kind
                      .toLowerCase()
                      .replace(/\s+/g, "-")}`}
                  >
                    {c.kind}
                  </span>
                </td>
                <td className="tv-cell-name">{c.name}</td>
                <td className="tv-cell-type">{c.detail}</td>
              </tr>
            ))
          )}
        </tbody>
      </table>
      <div className="tv-note">
        CHECK constraints aren't surfaced yet — view DDL to see them.
      </div>
    </>
  );
}

interface IndexesPanelProps {
  indexes: IndexInfo[];
}

export function IndexesPanel({ indexes }: IndexesPanelProps) {
  return (
    <table className="tv-table">
      <thead>
        <tr>
          <th>Name</th>
          <th>Columns</th>
          <th style={{ width: 80 }}>Unique</th>
          <th style={{ width: 100 }}>Type</th>
        </tr>
      </thead>
      <tbody>
        {indexes.length === 0 ? (
          <tr>
            <td colSpan={4} className="tv-cell-empty">No indexes</td>
          </tr>
        ) : (
          indexes.map((idx) => (
            <tr key={idx.name}>
              <td className="tv-cell-name">{idx.name}</td>
              <td className="tv-cell-type">{idx.columns.join(", ")}</td>
              <td>{idx.is_unique ? "YES" : "NO"}</td>
              <td>{idx.index_type}</td>
            </tr>
          ))
        )}
      </tbody>
    </table>
  );
}

interface ForeignKeysPanelProps {
  foreignKeys: ForeignKeyInfo[];
  onOpenTarget: (fk: ForeignKeyInfo) => void;
}

export function ForeignKeysPanel({
  foreignKeys,
  onOpenTarget,
}: ForeignKeysPanelProps) {
  return (
    <table className="tv-table">
      <thead>
        <tr>
          <th>Name</th>
          <th>Column</th>
          <th>References</th>
          <th style={{ width: 110 }}>ON DELETE</th>
          <th style={{ width: 110 }}>ON UPDATE</th>
        </tr>
      </thead>
      <tbody>
        {foreignKeys.length === 0 ? (
          <tr>
            <td colSpan={5} className="tv-cell-empty">No outgoing foreign keys</td>
          </tr>
        ) : (
          foreignKeys.map((fk, i) => (
            <tr key={`${fk.name}-${i}`}>
              <td className="tv-cell-name">{fk.name}</td>
              <td className="tv-cell-type">{fk.column}</td>
              <td>
                <button
                  className="tv-cell-link"
                  onClick={() => onOpenTarget(fk)}
                  title="Open target table"
                >
                  {fk.referenced_table}.{fk.referenced_column}
                </button>
              </td>
              <td>{fk.on_delete}</td>
              <td>{fk.on_update}</td>
            </tr>
          ))
        )}
      </tbody>
    </table>
  );
}

interface ReferencesPanelProps {
  references: ReferencingTableInfo[];
  onOpenSource: (r: ReferencingTableInfo) => void;
}

export function ReferencesPanel({
  references,
  onOpenSource,
}: ReferencesPanelProps) {
  return (
    <table className="tv-table">
      <thead>
        <tr>
          <th>Constraint</th>
          <th>Referencing Table</th>
          <th>Their Column</th>
          <th>Our Column</th>
          <th style={{ width: 110 }}>ON DELETE</th>
          <th style={{ width: 110 }}>ON UPDATE</th>
        </tr>
      </thead>
      <tbody>
        {references.length === 0 ? (
          <tr>
            <td colSpan={6} className="tv-cell-empty">
              Nothing references this table
            </td>
          </tr>
        ) : (
          references.map((r, i) => (
            <tr key={`${r.constraint_name}-${i}`}>
              <td className="tv-cell-name">{r.constraint_name}</td>
              <td>
                <button
                  className="tv-cell-link"
                  onClick={() => onOpenSource(r)}
                  title="Open referencing table"
                >
                  {r.referencing_schema}.{r.referencing_table}
                </button>
              </td>
              <td className="tv-cell-type">{r.referencing_column}</td>
              <td className="tv-cell-type">{r.referenced_column}</td>
              <td>{r.on_delete}</td>
              <td>{r.on_update}</td>
            </tr>
          ))
        )}
      </tbody>
    </table>
  );
}

interface TriggersPanelProps {
  triggers: TriggerInfo[];
}

export function TriggersPanel({ triggers }: TriggersPanelProps) {
  return (
    <table className="tv-table">
      <thead>
        <tr>
          <th>Name</th>
          <th>Timing</th>
          <th>Event</th>
          <th>Table</th>
        </tr>
      </thead>
      <tbody>
        {triggers.length === 0 ? (
          <tr>
            <td colSpan={4} className="tv-cell-empty">No triggers</td>
          </tr>
        ) : (
          triggers.map((t, i) => (
            <tr key={`${t.name}-${i}`}>
              <td className="tv-cell-name">{t.name}</td>
              <td>{t.timing}</td>
              <td>{t.event}</td>
              <td className="tv-cell-type">{t.table_name}</td>
            </tr>
          ))
        )}
      </tbody>
    </table>
  );
}
