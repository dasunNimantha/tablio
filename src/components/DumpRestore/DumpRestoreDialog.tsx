import { useEffect, useMemo, useRef, useState } from "react";
import { api, DumpRestoreRequest } from "../../lib/tauri";
import { useConnectionStore } from "../../stores/connectionStore";
import { X, Loader2, AlertTriangle, CheckCircle, XCircle, Shield, Search, ChevronDown, ChevronUp } from "lucide-react";
import "./DumpRestoreDialog.css";

interface Props {
  sourceConnectionId: string;
  sourceDatabase: string;
  onClose: () => void;
}

interface TargetOption {
  connectionId: string;
  connectionName: string;
  database: string;
  color: string;
  host: string;
  port: number;
}

function isLocalHost(host: string): boolean {
  return host === "localhost" || host === "127.0.0.1" || host === "::1";
}

type Step = "select" | "confirm" | "running" | "done";

export function DumpRestoreDialog({ sourceConnectionId, sourceDatabase, onClose }: Props) {
  const { connections, activeConnections } = useConnectionStore();
  const [targets, setTargets] = useState<TargetOption[]>([]);
  const [selectedTarget, setSelectedTarget] = useState<string | null>(null);
  const [loadingTargets, setLoadingTargets] = useState(true);
  const [step, setStep] = useState<Step>("select");
  const [result, setResult] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState("");
  const [logs, setLogs] = useState<string[]>([]);
  const [logsOpen, setLogsOpen] = useState(false);
  const logEndRef = useRef<HTMLDivElement>(null);

  const sourceConn = connections.find((c) => c.id === sourceConnectionId);

  useEffect(() => {
    const load = async () => {
      setLoadingTargets(true);
      const opts: TargetOption[] = [];
      const connected = connections.filter((c) => activeConnections.has(c.id));
      for (const conn of connected) {
        try {
          const dbs = await api.listDatabases(conn.id);
          const skip = new Set(["template0", "template1"]);
          for (const db of dbs) {
            if (skip.has(db.name)) continue;
            if (db.name !== sourceDatabase) continue;
            if (conn.id === sourceConnectionId) continue;
            opts.push({
              connectionId: conn.id,
              connectionName: conn.name,
              database: db.name,
              color: conn.color,
              host: conn.host,
              port: conn.port,
            });
          }
        } catch { /* skip */ }
      }
      setTargets(opts);
      setLoadingTargets(false);
    };
    load();
  }, [connections, activeConnections, sourceConnectionId, sourceDatabase]);

  useEffect(() => {
    if (step !== "running" && step !== "done") return;
    let unlisten: (() => void) | undefined;
    const isTauri = !!(window as any).__TAURI_INTERNALS__;
    if (isTauri) {
      import("@tauri-apps/api/event").then(({ listen }) => {
        listen<string>("dump-restore-log", (event) => {
          setLogs((prev) => [...prev, event.payload]);
          setTimeout(() => logEndRef.current?.scrollIntoView({ behavior: "smooth" }), 50);
        }).then((fn) => { unlisten = fn; });
      });
    }
    return () => { unlisten?.(); };
  }, [step]);

  const selectedTargetInfo = selectedTarget
    ? targets.find((t) => `${t.connectionId}:${t.database}` === selectedTarget)
    : null;

  const targetIsRemote = selectedTargetInfo ? !isLocalHost(selectedTargetInfo.host) : false;

  const filteredTargets = useMemo(() => {
    if (!filter) return targets;
    const q = filter.toLowerCase();
    return targets.filter(
      (t) =>
        t.connectionName.toLowerCase().includes(q) ||
        t.database.toLowerCase().includes(q) ||
        t.host.toLowerCase().includes(q)
    );
  }, [targets, filter]);

  const handleRun = async () => {
    if (!selectedTargetInfo) return;
    setStep("running");
    setError(null);
    setResult(null);
    setLogs([]);
    setLogsOpen(true);
    try {
      const request: DumpRestoreRequest = {
        source_connection_id: sourceConnectionId,
        source_database: sourceDatabase,
        target_connection_id: selectedTargetInfo.connectionId,
        target_database: selectedTargetInfo.database,
      };
      const msg = await api.dumpAndRestore(request);
      setResult(msg);
      setStep("done");
    } catch (e) {
      setError(String(e));
      setStep("done");
    }
  };

  const handleBack = () => {
    setStep("select");
    setResult(null);
    setError(null);
  };

  return (
    <div className="dialog-overlay">
      <div className="dialog dr-dialog">
        <div className="dialog-header">
          <h2>Dump &amp; Restore</h2>
          <button className="btn-icon" onClick={onClose} disabled={step === "running"}>
            <X size={18} />
          </button>
        </div>

        <div className="dialog-body">
          {/* ── Step 1: Select target ── */}
          {(step === "select" || step === "confirm") && (
            <>
              {/* Source info - simple read-only row */}
              <div className="form-group">
                <label>Source database</label>
                <div className="dr-readonly-field">
                  <span className="dr-dot" style={{ background: sourceConn?.color || "#6d9eff" }} />
                  <span>{sourceConn?.name}</span>
                  <span className="dr-sep">/</span>
                  <span className="dr-mono">{sourceDatabase}</span>
                  {sourceConn && (
                    <span className="dr-host">{sourceConn.host}:{sourceConn.port}</span>
                  )}
                </div>
              </div>

              {/* Target selection */}
              <div className="form-group">
                <label>Restore to</label>
                {loadingTargets ? (
                  <div className="dr-placeholder">
                    <Loader2 size={16} className="spin" />
                    Loading databases…
                  </div>
                ) : targets.length === 0 ? (
                  <div className="dr-placeholder">
                    No other connected databases available.
                  </div>
                ) : (
                  <div className="dr-target-list">
                    {targets.length > 6 && (
                      <div className="dr-filter">
                        <Search size={14} />
                        <input
                          type="text"
                          placeholder="Filter databases…"
                          value={filter}
                          onChange={(e) => setFilter(e.target.value)}
                        />
                      </div>
                    )}
                    <div className="dr-target-scroll">
                      {filteredTargets.length === 0 ? (
                        <div className="dr-no-match">No matching databases</div>
                      ) : (
                        filteredTargets.map((t) => {
                          const key = `${t.connectionId}:${t.database}`;
                          const active = selectedTarget === key;
                          return (
                            <button
                              key={key}
                              type="button"
                              className={`dr-target-item ${active ? "dr-target-item--active" : ""}`}
                              onClick={() => {
                                setSelectedTarget(key);
                                setStep("select");
                              }}
                              disabled={step === "confirm"}
                            >
                              <span className="dr-dot" style={{ background: t.color }} />
                              <span className="dr-target-name">{t.connectionName}</span>
                              <span className="dr-target-db">{t.database}</span>
                              <span className="dr-target-host">{t.host}:{t.port}</span>
                            </button>
                          );
                        })
                      )}
                    </div>
                  </div>
                )}
              </div>

              {/* Remote target warning */}
              {selectedTargetInfo && targetIsRemote && (
                <div className="dr-banner dr-banner--warn">
                  <Shield size={16} />
                  <div>
                    <strong>Remote target detected.</strong> The target database is on{" "}
                    <strong>{selectedTargetInfo.host}</strong>, not localhost.
                    Make sure this is not a production server.
                  </div>
                </div>
              )}

              {/* Confirm warning */}
              {step === "confirm" && selectedTargetInfo && (
                <div className="dr-banner dr-banner--danger">
                  <AlertTriangle size={16} />
                  <div>
                    <strong>This will overwrite all data</strong> in{" "}
                    <strong>{selectedTargetInfo.connectionName} / {selectedTargetInfo.database}</strong>{" "}
                    with a dump from{" "}
                    <strong>{sourceConn?.name} / {sourceDatabase}</strong>.
                    This cannot be undone.
                  </div>
                </div>
              )}
            </>
          )}

          {/* ── Step 2: Running ── */}
          {step === "running" && (
            <div className="dr-banner dr-banner--info">
              <Loader2 size={18} className="spin" />
              <div>
                <strong>Dumping and restoring…</strong>
                <p className="dr-banner-sub">
                  {sourceConn?.name} / {sourceDatabase} &rarr; {selectedTargetInfo?.connectionName} / {selectedTargetInfo?.database}
                </p>
              </div>
            </div>
          )}

          {/* ── Step 3: Done ── */}
          {step === "done" && result && (
            <div className="dr-banner dr-banner--success">
              <CheckCircle size={18} />
              <div>
                <strong>Restore complete</strong>
                <p className="dr-banner-sub">{result}</p>
              </div>
            </div>
          )}
          {step === "done" && error && (
            <div className="dr-banner dr-banner--error">
              <XCircle size={18} />
              <div>
                <strong>Restore failed</strong>
                <p className="dr-banner-sub">{error}</p>
              </div>
            </div>
          )}

          {/* ── Logs panel ── */}
          {(step === "running" || step === "done") && logs.length > 0 && (
            <div className="dr-logs">
              <button
                type="button"
                className="dr-logs-toggle"
                onClick={() => setLogsOpen((v) => !v)}
              >
                <span>Logs</span>
                {logsOpen ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
              </button>
              {logsOpen && (
                <div className="dr-logs-content">
                  {logs.map((line, i) => (
                    <div key={i} className="dr-log-line">{line}</div>
                  ))}
                  <div ref={logEndRef} />
                </div>
              )}
            </div>
          )}
        </div>

        <div className="dialog-footer dr-footer">
          {step === "select" && (
            <>
              <button className="btn-secondary" onClick={onClose}>Cancel</button>
              <button
                className="btn-primary"
                disabled={!selectedTarget || loadingTargets}
                onClick={() => setStep("confirm")}
              >
                Next
              </button>
            </>
          )}
          {step === "confirm" && (
            <>
              <button className="btn-secondary" onClick={handleBack}>Back</button>
              <button className="dr-btn-danger" onClick={handleRun}>
                Overwrite &amp; Restore
              </button>
            </>
          )}
          {step === "running" && (
            <span className="dr-footer-hint">Please wait…</span>
          )}
          {step === "done" && (
            <>
              {error && <button className="btn-secondary" onClick={handleBack}>Try Again</button>}
              <button className="btn-primary" onClick={onClose}>Done</button>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
