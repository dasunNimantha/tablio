import { useEffect } from "react";
import { createPortal } from "react-dom";
import { ShieldAlert, X } from "lucide-react";
import { useConnectionStore } from "../stores/connectionStore";
import "./HostKeyMismatchPrompt.css";

/**
 * Modal that appears when the SSH bastion presents a host key that
 * doesn't match the one previously recorded in `~/.tablio/known_hosts`.
 *
 * "Forget & retry" forgets the recorded key for that host and asks the
 * connection store to retry the connect (which then re-records the new
 * key under TOFU). Cancel propagates the original mismatch error to the
 * caller.
 *
 * Mount this once near the application root, alongside `PassphrasePrompt`.
 */
export function HostKeyMismatchPrompt() {
  const pending = useConnectionStore((s) => s.pendingHostKeyMismatch);

  useEffect(() => {
    if (!pending) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") pending.resolve(false);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [pending]);

  if (!pending) return null;

  const { info, connectionName } = pending;

  return createPortal(
    <div
      className="host-mismatch-overlay"
      role="dialog"
      aria-modal="true"
      aria-label="SSH host key changed"
    >
      <div className="host-mismatch-prompt">
        <header className="host-mismatch-prompt__header">
          <div className="host-mismatch-prompt__title">
            <ShieldAlert size={16} />
            <span>SSH host key changed</span>
          </div>
          <button
            type="button"
            className="host-mismatch-prompt__close"
            aria-label="Cancel"
            onClick={() => pending.resolve(false)}
          >
            <X size={16} />
          </button>
        </header>

        <div className="host-mismatch-prompt__body">
          <p className="host-mismatch-prompt__lead">
            The SSH server <strong>{info.host}:{info.port}</strong> presented a
            different host key than the one Tablio recorded last time. This can
            happen after a legitimate key rotation, but it's also what an
            active man-in-the-middle attack looks like — only continue if you
            recognise the new fingerprint.
          </p>
          <dl className="host-mismatch-prompt__meta">
            <dt>Connection</dt>
            <dd>{connectionName || "(unnamed)"}</dd>
            <dt>New fingerprint</dt>
            <dd className="host-mismatch-prompt__fp">{info.fingerprint}</dd>
            <dt>Recorded in</dt>
            <dd className="host-mismatch-prompt__path">{info.knownHostsPath}</dd>
          </dl>
        </div>

        <footer className="host-mismatch-prompt__footer">
          <button
            type="button"
            className="btn-secondary"
            onClick={() => pending.resolve(false)}
          >
            Cancel
          </button>
          <button
            type="button"
            className="btn-danger"
            onClick={() => pending.resolve(true)}
          >
            Forget &amp; retry
          </button>
        </footer>
      </div>
    </div>,
    document.body,
  );
}
