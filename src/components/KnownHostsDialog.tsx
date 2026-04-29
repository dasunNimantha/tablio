import { useCallback, useEffect, useMemo, useState } from "react";
import { createPortal } from "react-dom";
import { Trash2, ShieldCheck, X, AlertCircle, RefreshCw } from "lucide-react";
import { api, type KnownHostEntry } from "../lib/tauri";
import "./KnownHostsDialog.css";

interface KnownHostsDialogProps {
  open: boolean;
  onClose: () => void;
}

/**
 * Modal that lists every entry in `~/.tablio/known_hosts` and lets the
 * user forget individual hosts. Reads happen on every open so the list
 * stays in sync with whatever the SSH layer wrote since the last view.
 *
 * The dialog deliberately leaves entries it can't parse (hashed hosts,
 * `@cert-authority`, etc.) untouched on disk — see `commands::known_hosts`.
 */
export function KnownHostsDialog({ open, onClose }: KnownHostsDialogProps) {
  const [entries, setEntries] = useState<KnownHostEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [pendingForget, setPendingForget] = useState<string | null>(null);
  const [filter, setFilter] = useState("");

  const refresh = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const list = await api.listKnownHosts();
      setEntries(list);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!open) return;
    setFilter("");
    void refresh();
  }, [open, refresh]);

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, onClose]);

  const forget = useCallback(
    async (entry: KnownHostEntry) => {
      const key = entryKey(entry);
      setPendingForget(key);
      setError(null);
      try {
        await api.forgetKnownHost(entry.host, entry.port, entry.fingerprint);
        setEntries((prev) =>
          prev.filter((e) => entryKey(e) !== key),
        );
      } catch (e) {
        setError(e instanceof Error ? e.message : String(e));
      } finally {
        setPendingForget(null);
      }
    },
    [],
  );

  const filtered = useMemo(() => {
    const q = filter.trim().toLowerCase();
    if (!q) return entries;
    return entries.filter(
      (e) =>
        e.host.toLowerCase().includes(q) ||
        e.fingerprint.toLowerCase().includes(q) ||
        e.keyType.toLowerCase().includes(q),
    );
  }, [entries, filter]);

  if (!open) return null;

  return createPortal(
    <div
      className="known-hosts-overlay"
      role="dialog"
      aria-modal="true"
      aria-label="SSH known hosts"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className="known-hosts-dialog">
        <header className="known-hosts-dialog__header">
          <div className="known-hosts-dialog__title">
            <ShieldCheck size={16} />
            <span>SSH known hosts</span>
          </div>
          <button
            type="button"
            className="known-hosts-dialog__close"
            aria-label="Close"
            onClick={onClose}
          >
            <X size={16} />
          </button>
        </header>

        <div className="known-hosts-dialog__toolbar">
          <input
            type="search"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder="Filter by host, key type, or fingerprint…"
            className="known-hosts-dialog__filter"
            aria-label="Filter known hosts"
          />
          <button
            type="button"
            className="btn-secondary known-hosts-dialog__refresh"
            onClick={() => void refresh()}
            disabled={loading}
            aria-label="Refresh"
          >
            <RefreshCw size={14} />
            <span>Refresh</span>
          </button>
        </div>

        {error && (
          <div className="known-hosts-dialog__error" role="alert">
            <AlertCircle size={14} />
            <span>{error}</span>
          </div>
        )}

        <div className="known-hosts-dialog__body">
          {loading && entries.length === 0 ? (
            <div className="known-hosts-dialog__empty">Loading…</div>
          ) : filtered.length === 0 ? (
            <div className="known-hosts-dialog__empty">
              {entries.length === 0
                ? "No SSH hosts have been recorded yet. They appear here the first time you connect through an SSH tunnel."
                : "No entries match your filter."}
            </div>
          ) : (
            <table className="known-hosts-table">
              <thead>
                <tr>
                  <th>Host</th>
                  <th>Port</th>
                  <th>Key type</th>
                  <th>Fingerprint</th>
                  <th aria-label="Actions" />
                </tr>
              </thead>
              <tbody>
                {filtered.map((entry) => {
                  const key = entryKey(entry);
                  const busy = pendingForget === key;
                  return (
                    <tr key={key} data-host={entry.host}>
                      <td className="known-hosts-table__host">{entry.host}</td>
                      <td>{entry.port}</td>
                      <td>{entry.keyType}</td>
                      <td className="known-hosts-table__fp">
                        {entry.fingerprint}
                      </td>
                      <td className="known-hosts-table__actions">
                        <button
                          type="button"
                          className="btn-icon known-hosts-table__forget"
                          aria-label={`Forget ${entry.host}`}
                          title="Forget this host key"
                          disabled={busy}
                          onClick={() => void forget(entry)}
                        >
                          <Trash2 size={14} />
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>

        <footer className="known-hosts-dialog__footer">
          <p className="known-hosts-dialog__hint">
            Forgetting an entry deletes only Tablio's record. The next SSH
            connection re-records the server's current key under
            "trust-on-first-use".
          </p>
          <button type="button" className="btn-primary" onClick={onClose}>
            Close
          </button>
        </footer>
      </div>
    </div>,
    document.body,
  );
}

function entryKey(e: KnownHostEntry): string {
  return `${e.host}|${e.port}|${e.fingerprint}`;
}
