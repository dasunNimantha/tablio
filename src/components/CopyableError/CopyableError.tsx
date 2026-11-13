import { useState } from "react";
import { Check, Copy } from "lucide-react";
import "./CopyableError.css";

/**
 * Multi-line error output (pg_dump / psql stderr and the like): keeps
 * line breaks, scrolls instead of growing the dialog, and has a Copy
 * button so the full text can be pasted into a bug report.
 */
export function CopyableError({ message }: { message: string }) {
  const [copied, setCopied] = useState(false);
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(message);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      setCopied(false);
    }
  };
  return (
    <div className="copyable-error">
      <pre className="copyable-error__text" tabIndex={0} aria-label="Error details">
        {message}
      </pre>
      <button type="button" className="btn-ghost copyable-error__copy" onClick={copy}>
        {copied ? <Check size={12} aria-hidden="true" /> : <Copy size={12} aria-hidden="true" />}
        {copied ? "Copied" : "Copy error"}
      </button>
    </div>
  );
}
