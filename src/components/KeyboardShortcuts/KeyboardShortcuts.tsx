import { useId } from "react";
import { X, Keyboard } from "lucide-react";
import { Modal } from "../Modal/Modal";
import { SHORTCUT_GROUPS, displayKey } from "./shortcuts";
import "./KeyboardShortcuts.css";

interface Props {
  onClose: () => void;
}

const isMac = typeof navigator !== "undefined" && /Mac/i.test(navigator.platform || navigator.userAgent);

export function KeyboardShortcuts({ onClose }: Props) {
  const titleId = useId();
  return (
    <Modal onClose={onClose} className="dialog keyboard-shortcuts-dialog" labelledBy={titleId}>
      <div className="dialog-header">
        <h2 id={titleId}>
          <Keyboard size={18} aria-hidden="true" />
          Keyboard Shortcuts
        </h2>
        <button className="btn-icon" onClick={onClose} aria-label="Close" title="Close">
          <X size={18} />
        </button>
      </div>

      <div className="dialog-body keyboard-shortcuts-body">
        {SHORTCUT_GROUPS.map((group) => (
          <table className="keyboard-shortcuts-table" key={group.title}>
            <caption className="keyboard-shortcuts-group">{group.title}</caption>
            <tbody>
              {group.shortcuts.map(({ keys, description }) => (
                <tr key={`${group.title}:${description}`}>
                  <td className="keyboard-shortcuts-keys">
                    {keys.map((key, j) => (
                      <kbd key={j} className="keyboard-key">
                        {displayKey(key, isMac)}
                      </kbd>
                    ))}
                  </td>
                  <td className="keyboard-shortcuts-desc">{description}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ))}
      </div>
    </Modal>
  );
}
