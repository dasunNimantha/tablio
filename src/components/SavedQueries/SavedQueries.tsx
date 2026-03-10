import { useEffect, useState } from "react";
import { api, SavedQuery } from "../../lib/tauri";
import { X, Loader2, Trash2 } from "lucide-react";
import "./SavedQueries.css";

interface Props {
  onSelectQuery: (sql: string) => void;
  onClose: () => void;
}

const TRUNCATE_LEN = 80;

export function SavedQueries({ onSelectQuery, onClose }: Props) {
  const [queries, setQueries] = useState<SavedQuery[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const load = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await api.loadSavedQueries();
        setQueries(data);
      } catch (e) {
        setError(String(e));
      } finally {
        setLoading(false);
      }
    };
    load();
  }, []);

  const handleDelete = async (e: React.MouseEvent, queryId: string) => {
    e.stopPropagation();
    try {
      await api.deleteSavedQuery(queryId);
      setQueries((prev) => prev.filter((q) => q.id !== queryId));
    } catch (err) {
      setError(String(err));
    }
  };

  const truncateSql = (sql: string) =>
    sql.length > TRUNCATE_LEN ? sql.slice(0, TRUNCATE_LEN) + "…" : sql;

  const formatTime = (ts: number) =>
    new Date(ts).toLocaleString();

  return (
    <div className="saved-queries-panel">
      <div className="saved-queries-header">
        <span>Saved Queries</span>
        <button className="btn-icon" onClick={onClose}>
          <X size={18} />
        </button>
      </div>
      <div className="saved-queries-content">
        {loading && (
          <div className="saved-queries-loading">
            <Loader2 size={24} className="spin" />
            <span>Loading saved queries...</span>
          </div>
        )}
        {error && (
          <div className="saved-queries-error">{error}</div>
        )}
        {!loading && !error && queries.length === 0 && (
          <div className="saved-queries-empty">No saved queries yet</div>
        )}
        {!loading && !error && queries.length > 0 && (
          <div className="saved-queries-list">
            {queries.map((query) => (
              <div
                key={query.id}
                className="saved-queries-item"
                onClick={() => onSelectQuery(query.sql)}
              >
                <div className="saved-queries-item-main">
                  <span className="saved-queries-name">{query.name}</span>
                  <code className="saved-queries-sql">
                    {truncateSql(query.sql)}
                  </code>
                  <div className="saved-queries-meta">
                    {formatTime(query.updated_at)}
                  </div>
                </div>
                <button
                  className="btn-icon saved-queries-delete"
                  onClick={(e) => handleDelete(e, query.id)}
                  title="Delete"
                >
                  <Trash2 size={14} />
                </button>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
