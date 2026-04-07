import { useEffect, useMemo, useState } from "react";
import { api, ColumnInfo, IndexInfo, ForeignKeyInfo } from "../../lib/tauri";
import { Loader2 } from "lucide-react";
import "./TableInfo.css";

interface Props {
  connectionId: string;
  database: string;
  schema: string;
  table: string;
}

type SubTab = "columns" | "indexes" | "foreign_keys";

export function TableInfo({ connectionId, database, schema, table }: Props) {
  const [activeTab, setActiveTab] = useState<SubTab>("columns");
  const [columns, setColumns] = useState<ColumnInfo[]>([]);
  const [indexes, setIndexes] = useState<IndexInfo[]>([]);
  const [foreignKeys, setForeignKeys] = useState<ForeignKeyInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      setLoading(true);
      setError(null);
      try {
        const [cols, idxs, fks] = await Promise.all([
          api.listColumns(connectionId, database, schema, table),
          api.listIndexes(connectionId, database, schema, table),
          api.listForeignKeys(connectionId, database, schema, table),
        ]);
        if (cancelled) return;
        setColumns(cols);
        setIndexes(idxs);
        setForeignKeys(fks);
      } catch (e) {
        if (cancelled) return;
        setError(String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    load();
    return () => { cancelled = true; };
  }, [connectionId, database, schema, table]);

  const fksByColumn = useMemo(() => {
    const m = new Map<string, ForeignKeyInfo[]>();
    for (const fk of foreignKeys) {
      const list = m.get(fk.column) ?? [];
      list.push(fk);
      m.set(fk.column, list);
    }
    return m;
  }, [foreignKeys]);

  if (loading) {
    return (
      <div className="table-info-loading">
        <Loader2 size={24} className="spin" />
        <span>Loading structure...</span>
      </div>
    );
  }

  if (error) {
    return <div className="table-info-error">{error}</div>;
  }

  return (
    <div className="table-info">
      <div className="table-info-toolbar">
        <span className="table-info-name">
          {schema}.{table}
        </span>
      </div>
      <div className="table-info-tabs">
        <button
          className={`table-info-tab ${activeTab === "columns" ? "active" : ""}`}
          onClick={() => setActiveTab("columns")}
        >
          Columns ({columns.length})
        </button>
        <button
          className={`table-info-tab ${activeTab === "indexes" ? "active" : ""}`}
          onClick={() => setActiveTab("indexes")}
        >
          Indexes ({indexes.length})
        </button>
        <button
          className={`table-info-tab ${activeTab === "foreign_keys" ? "active" : ""}`}
          onClick={() => setActiveTab("foreign_keys")}
        >
          Foreign Keys ({foreignKeys.length})
        </button>
      </div>
      <div className="table-info-content">
        {activeTab === "columns" && (
          <table className="info-table">
            <thead>
              <tr>
                <th>#</th>
                <th>Name</th>
                <th>Type</th>
                <th>Nullable</th>
                <th>Default</th>
                <th>Key</th>
              </tr>
            </thead>
            <tbody>
              {columns.map((col, i) => {
                const colFks = fksByColumn.get(col.name) ?? [];
                const fkTitle =
                  colFks.length > 0
                    ? colFks
                        .map((fk) => `${fk.referenced_table}.${fk.referenced_column}`)
                        .join(" · ")
                    : undefined;
                return (
                <tr key={col.name}>
                  <td className="info-cell-muted">{i + 1}</td>
                  <td className="info-cell-name">{col.name}</td>
                  <td className="info-cell-type">{col.data_type}</td>
                  <td>{col.is_nullable ? "YES" : "NO"}</td>
                  <td className="info-cell-muted">{col.default_value || "-"}</td>
                  <td>
                    <span className="table-info-key-badges">
                      {col.is_primary_key && <span className="pk-badge">PK</span>}
                      {colFks.length > 0 && (
                        <span className="fk-badge" title={fkTitle}>
                          FK
                        </span>
                      )}
                    </span>
                  </td>
                </tr>
              );
              })}
            </tbody>
          </table>
        )}
        {activeTab === "indexes" && (
          <table className="info-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Columns</th>
                <th>Unique</th>
                <th>Type</th>
              </tr>
            </thead>
            <tbody>
              {indexes.length === 0 ? (
                <tr>
                  <td colSpan={4} className="info-empty">No indexes</td>
                </tr>
              ) : (
                indexes.map((idx) => (
                  <tr key={idx.name}>
                    <td className="info-cell-name">{idx.name}</td>
                    <td className="info-cell-type">{idx.columns.join(", ")}</td>
                    <td>{idx.is_unique ? "YES" : "NO"}</td>
                    <td>{idx.index_type}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        )}
        {activeTab === "foreign_keys" && (
          <table className="info-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Column</th>
                <th>References</th>
                <th>ON DELETE</th>
                <th>ON UPDATE</th>
              </tr>
            </thead>
            <tbody>
              {foreignKeys.length === 0 ? (
                <tr>
                  <td colSpan={5} className="info-empty">No foreign keys</td>
                </tr>
              ) : (
                foreignKeys.map((fk, i) => (
                  <tr key={`${fk.name}-${i}`}>
                    <td className="info-cell-name">{fk.name}</td>
                    <td className="info-cell-type">{fk.column}</td>
                    <td className="info-cell-type">
                      {fk.referenced_table}.{fk.referenced_column}
                    </td>
                    <td>{fk.on_delete}</td>
                    <td>{fk.on_update}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
