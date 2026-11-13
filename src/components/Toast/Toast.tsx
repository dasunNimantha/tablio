import { useToastStore, type Toast } from "../../stores/toastStore";
import { CheckCircle2, AlertCircle, Info, X } from "lucide-react";
import "./Toast.css";

const icons = {
  success: <CheckCircle2 size={16} />,
  error: <AlertCircle size={16} />,
  info: <Info size={16} />,
};

function ToastItem({ toast }: { toast: Toast }) {
  const removeToast = useToastStore((s) => s.removeToast);
  const pauseToast = useToastStore((s) => s.pauseToast);
  const resumeToast = useToastStore((s) => s.resumeToast);
  const isError = toast.type === "error";
  return (
    <div
      className={`toast toast-${toast.type}`}
      // Errors interrupt; confirmations are announced politely.
      role={isError ? "alert" : "status"}
      aria-live={isError ? "assertive" : "polite"}
      aria-atomic="true"
      data-testid={`toast-${toast.type}`}
      onMouseEnter={() => pauseToast(toast.id)}
      onMouseLeave={() => resumeToast(toast.id)}
      onFocus={() => pauseToast(toast.id)}
      onBlur={(e) => {
        if (!e.currentTarget.contains(e.relatedTarget as Node | null)) resumeToast(toast.id);
      }}
    >
      <span className="toast-icon" aria-hidden="true">
        {icons[toast.type]}
      </span>
      <span className="toast-message">{toast.message}</span>
      <button
        type="button"
        className="toast-close"
        aria-label="Dismiss notification"
        title="Dismiss"
        onClick={() => removeToast(toast.id)}
      >
        <X size={14} aria-hidden="true" />
      </button>
    </div>
  );
}

export function ToastContainer() {
  const toasts = useToastStore((s) => s.toasts);

  // The container stays mounted (even when empty) so screen readers have
  // already registered the live region before the first toast arrives.
  return (
    <div className="toast-container">
      {toasts.map((t) => (
        <ToastItem key={t.id} toast={t} />
      ))}
    </div>
  );
}
