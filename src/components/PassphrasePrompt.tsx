import { useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { Key, X } from "lucide-react";
import { useConnectionStore } from "../stores/connectionStore";
import "./PassphrasePrompt.css";

/**
 * Modal that appears when a connection requires its SSH key passphrase
 * at connect time (because the user enabled "Ask when connecting" on
 * the connection's SSH section). Subscribes to the connection store and
 * resolves the pending request when the user submits or cancels.
 *
 * Mount this once near the application root.
 */
export function PassphrasePrompt() {
  const pending = useConnectionStore((s) => s.pendingPassphrasePrompt);
  const [value, setValue] = useState("");
  const inputRef = useRef<HTMLInputElement | null>(null);

  // Reset the field whenever a new prompt is shown.
  useEffect(() => {
    if (pending) {
      setValue("");
      // Defer focus to next tick so the input exists in the DOM.
      requestAnimationFrame(() => inputRef.current?.focus());
    }
  }, [pending]);

  // Close on Escape.
  useEffect(() => {
    if (!pending) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        pending.reject(new Error("Passphrase prompt cancelled"));
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [pending]);

  if (!pending) return null;

  const submit = (e?: React.FormEvent) => {
    e?.preventDefault();
    pending.resolve(value);
  };

  const cancel = () => {
    pending.reject(new Error("Passphrase prompt cancelled"));
  };

  return createPortal(
    <div className="passphrase-prompt-overlay" role="dialog" aria-modal="true">
      <form className="passphrase-prompt" onSubmit={submit}>
        <div className="passphrase-prompt__header">
          <div className="passphrase-prompt__title">
            <Key size={16} />
            <span>SSH key passphrase</span>
          </div>
          <button
            type="button"
            className="passphrase-prompt__close"
            onClick={cancel}
            aria-label="Cancel"
          >
            <X size={16} />
          </button>
        </div>
        <div className="passphrase-prompt__body">
          <p className="passphrase-prompt__lead">
            Enter the passphrase for this connection's SSH identity file.
          </p>
          <dl className="passphrase-prompt__meta">
            <dt>Connection</dt>
            <dd>{pending.connectionName || "(unnamed)"}</dd>
            <dt>Identity file</dt>
            <dd className="passphrase-prompt__path">{pending.keyPath || "(unspecified)"}</dd>
          </dl>
          <input
            ref={inputRef}
            className="passphrase-prompt__input"
            type="password"
            value={value}
            onChange={(e) => setValue(e.target.value)}
            placeholder="Passphrase (leave blank if the key is unencrypted)"
            autoComplete="off"
          />
        </div>
        <div className="passphrase-prompt__footer">
          <button type="button" className="btn-secondary" onClick={cancel}>
            Cancel
          </button>
          <button type="submit" className="btn-primary">
            Continue
          </button>
        </div>
      </form>
    </div>,
    document.body,
  );
}
