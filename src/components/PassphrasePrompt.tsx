import { useEffect, useId, useRef, useState } from "react";
import { Modal } from "./Modal/Modal";
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

  const titleId = useId();

  // Reset the field whenever a new prompt is shown.
  useEffect(() => {
    if (pending) setValue("");
  }, [pending]);

  if (!pending) return null;

  const submit = (e?: React.FormEvent) => {
    e?.preventDefault();
    pending.resolve(value);
  };

  const cancel = () => {
    pending.reject(new Error("Passphrase prompt cancelled"));
  };

  const isPassword = pending.kind === "password";

  return (
    <Modal
      portal
      as="form"
      onSubmit={submit}
      onClose={cancel}
      closeOnOverlayClick={false}
      overlayClassName="passphrase-prompt-overlay"
      className="passphrase-prompt"
      labelledBy={titleId}
      initialFocusRef={inputRef}
    >
        <div className="passphrase-prompt__header">
          <div className="passphrase-prompt__title" id={titleId}>
            <Key size={16} aria-hidden="true" />
            <span>{isPassword ? "SSH password" : "SSH key passphrase"}</span>
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
            {isPassword
              ? "Enter the SSH password for this connection's tunnel. It is not saved."
              : "Enter the passphrase for this connection's SSH identity file."}
          </p>
          <dl className="passphrase-prompt__meta">
            <dt>Connection</dt>
            <dd>{pending.connectionName || "(unnamed)"}</dd>
            {isPassword ? (
              <>
                <dt>SSH login</dt>
                <dd className="passphrase-prompt__path">{pending.sshTarget || "(unspecified)"}</dd>
              </>
            ) : (
              <>
                <dt>Identity file</dt>
                <dd className="passphrase-prompt__path">{pending.keyPath || "(unspecified)"}</dd>
              </>
            )}
          </dl>
          <input
            ref={inputRef}
            className="passphrase-prompt__input"
            type="password"
            value={value}
            onChange={(e) => setValue(e.target.value)}
            placeholder={isPassword ? "Password" : "Passphrase (leave blank if the key is unencrypted)"}
            aria-label={isPassword ? "SSH password" : "Key passphrase"}
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
    </Modal>
  );
}
