import { useEffect, useId, useRef, useState } from "react";
import { AlertTriangle, Loader2 } from "lucide-react";
import { Modal } from "./Modal/Modal";
import "./ConfirmDialog.css";

interface Props {
  title: string;
  message: string;
  confirmLabel?: string;
  danger?: boolean;
  /** May return a promise; the dialog stays open and disabled while it's
   *  pending, and shows the error inline if it rejects. */
  onConfirm: () => void | Promise<unknown>;
  onCancel: () => void;
}

function errorMessage(e: unknown): string {
  if (e instanceof Error) return e.message;
  return typeof e === "string" ? e : String(e);
}

export function ConfirmDialog({
  title,
  message,
  confirmLabel = "Confirm",
  danger = false,
  onConfirm,
  onCancel,
}: Props) {
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  // Synchronous guard: a double click fires twice before React re-renders
  // with `busy` set, so state alone can't stop the second call.
  const runningRef = useRef(false);
  const mountedRef = useRef(true);
  const cancelRef = useRef<HTMLButtonElement>(null);
  const confirmRef = useRef<HTMLButtonElement>(null);
  const titleId = useId();
  const messageId = useId();

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
    };
  }, []);

  const handleConfirm = async () => {
    if (runningRef.current) return;
    runningRef.current = true;
    setError(null);
    try {
      const result = onConfirm();
      if (result && typeof (result as Promise<unknown>).then === "function") {
        setBusy(true);
        await result;
      }
    } catch (e) {
      if (mountedRef.current) setError(errorMessage(e));
    } finally {
      runningRef.current = false;
      if (mountedRef.current) setBusy(false);
    }
  };

  return (
    <Modal
      onClose={onCancel}
      busy={busy}
      role="alertdialog"
      className="confirm-dialog"
      labelledBy={titleId}
      describedBy={messageId}
      // Destructive confirmations start on Cancel so a stray Enter can't
      // drop a table.
      initialFocusRef={danger ? cancelRef : confirmRef}
      testId="confirm-dialog"
    >
      <div className="confirm-dialog-icon" aria-hidden="true">
        <AlertTriangle size={24} />
      </div>
      <h3 id={titleId}>{title}</h3>
      <p id={messageId}>{message}</p>
      {error && (
        <div className="confirm-dialog-error" role="alert">
          {error}
        </div>
      )}
      <div className="confirm-dialog-actions">
        <button ref={cancelRef} className="btn-secondary" onClick={onCancel} disabled={busy}>
          Cancel
        </button>
        <button
          ref={confirmRef}
          className={danger ? "btn-danger" : "btn-primary"}
          onClick={handleConfirm}
          disabled={busy}
          aria-busy={busy || undefined}
        >
          {busy && <Loader2 size={13} className="spin" aria-hidden="true" />}
          {confirmLabel}
        </button>
      </div>
    </Modal>
  );
}
