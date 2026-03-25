import { useEffect, useState, useMemo } from "react";
import { api, ServerConfigEntry } from "../../lib/tauri";
import { Loader2, Search, AlertTriangle, ChevronRight, ChevronDown, Settings } from "lucide-react";

interface Props {
  connectionId: string;
}

export function DashboardConfig({ connectionId }: Props) {
  const [entries, setEntries] = useState<ServerConfigEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [expandedCategories, setExpandedCategories] = useState<Set<string>>(new Set());

  useEffect(() => {
    let cancelled = false;
    const fetchConfig = async () => {
      try {
        const result = await api.getServerConfig({ connection_id: connectionId });
        if (cancelled) return;
        setEntries(result);
        setError(null);
      } catch (e) {
        if (cancelled) return;
        setError(String(e));
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    fetchConfig();
    return () => { cancelled = true; };
  }, [connectionId]);

  const filtered = useMemo(() => {
    if (!search.trim()) return entries;
    const q = search.toLowerCase();
    return entries.filter(
      (e) =>
        e.name.toLowerCase().includes(q) ||
        e.category.toLowerCase().includes(q) ||
        e.description.toLowerCase().includes(q) ||
        e.setting.toLowerCase().includes(q)
    );
  }, [entries, search]);

  const grouped = useMemo(() => {
    const map = new Map<string, ServerConfigEntry[]>();
    for (const entry of filtered) {
      const list = map.get(entry.category) || [];
      list.push(entry);
      map.set(entry.category, list);
    }
    return map;
  }, [filtered]);

  const isSearching = search.trim().length > 0;

  const isExpanded = (cat: string) => {
    if (isSearching) return true;
    return expandedCategories.has(cat);
  };

  const toggleCategory = (cat: string) => {
    if (isSearching) return;
    setExpandedCategories((prev) => {
      const next = new Set(prev);
      if (next.has(cat)) next.delete(cat);
      else next.add(cat);
      return next;
    });
  };

  const expandAll = () => {
    setExpandedCategories(new Set(grouped.keys()));
  };

  const collapseAll = () => {
    setExpandedCategories(new Set());
  };

  const allExpanded = !isSearching && expandedCategories.size === grouped.size && grouped.size > 0;

  const pendingRestartCount = entries.filter((e) => e.pending_restart).length;
  const modifiedCount = entries.filter((e) => e.source && e.source !== "default").length;

  if (loading) {
    return (
      <div className="activity-loading">
        <Loader2 size={24} className="spin" />
        <span>Loading configuration...</span>
      </div>
    );
  }

  return (
    <div className="dashboard-sub-content">
      {error && <div className="activity-error">{error}</div>}

      <div className="activity-sub-toolbar">
        <span className="activity-count">{entries.length} settings</span>
        <span className="activity-count">{grouped.size} categories</span>
        {modifiedCount > 0 && (
          <span className="config-modified-badge">{modifiedCount} modified</span>
        )}
        {pendingRestartCount > 0 && (
          <span className="config-restart-badge">
            <AlertTriangle size={12} />
            {pendingRestartCount} pending restart
          </span>
        )}
        <div style={{ flex: 1 }} />
        {!isSearching && (
          <button className="btn-ghost" onClick={allExpanded ? collapseAll : expandAll}>
            {allExpanded ? "Collapse all" : "Expand all"}
          </button>
        )}
        <div className="config-search">
          <Search size={14} />
          <input
            type="text"
            placeholder="Search settings..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        </div>
      </div>

      <div className="activity-content">
        {grouped.size === 0 ? (
          <div className="dashboard-empty-state">
            <Settings size={32} strokeWidth={1.2} />
            <span>No matching settings</span>
          </div>
        ) : (
          Array.from(grouped.entries()).map(([category, items]) => {
            const expanded = isExpanded(category);
            return (
              <div key={category} className="config-category">
                <div className="config-category-header" onClick={() => toggleCategory(category)}>
                  <span className="config-category-toggle">
                    {expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                  </span>
                  <span className="config-category-name">{category}</span>
                  <span className="config-category-count">{items.length}</span>
                  {items.some((e) => e.pending_restart) && (
                    <AlertTriangle size={12} className="config-category-warn" />
                  )}
                </div>
                {expanded && (
                  <table className="info-table config-table">
                    <thead>
                      <tr>
                        <th>Name</th>
                        <th>Value</th>
                        <th>Unit</th>
                        <th>Context</th>
                        <th>Source</th>
                        <th>Description</th>
                        <th></th>
                      </tr>
                    </thead>
                    <tbody>
                      {items.map((entry) => (
                        <tr key={entry.name} className={entry.pending_restart ? "config-restart-row" : ""}>
                          <td className="config-name">{entry.name}</td>
                          <td className="config-value">{entry.setting}</td>
                          <td className="info-cell-muted">{entry.unit || "-"}</td>
                          <td>
                            <span className="config-context-badge">{entry.context}</span>
                          </td>
                          <td>
                            <span className={`config-source ${entry.source && entry.source !== "default" ? "modified" : ""}`}>
                              {entry.source || "default"}
                            </span>
                          </td>
                          <td className="config-desc" title={entry.description}>
                            {entry.description.substring(0, 80)}
                            {entry.description.length > 80 ? "..." : ""}
                          </td>
                          <td>
                            {entry.pending_restart && (
                              <span className="config-restart-icon" title="Pending restart">
                                <AlertTriangle size={13} />
                              </span>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            );
          })
        )}
      </div>
    </div>
  );
}
