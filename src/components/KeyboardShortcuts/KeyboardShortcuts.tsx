import { useEffect } from "react";
import { X, Keyboard } from "lucide-react";
import "./KeyboardShortcuts.css";

interface Props {
  onClose: () => void;
}

const shortcuts = [
  { keys: ["Ctrl", "Enter"], description: "Execute query" },
  { keys: ["Ctrl", "S"], description: "Save changes" },
  { keys: ["Ctrl", "N"], description: "New query tab" },
  { keys: ["Ctrl", "W"], description: "Close tab" },
  { keys: ["Ctrl", "Shift", "F"], description: "Format SQL" },
  { keys: ["Ctrl", "F"], description: "Find in editor" },
  { keys: ["Ctrl", "/"], description: "Toggle comment" },
  { keys: ["Escape"], description: "Close dialog" },
];

export function KeyboardShortcuts({ onClose }: Props) {
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        onClose();
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [onClose]);

  return (
    <div className="dialog-overlay" onClick={onClose}>
      <div
        className="dialog keyboard-shortcuts-dialog"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="dialog-header">
          <h2>
            <Keyboard size={18} />
            Keyboard Shortcuts
          </h2>
          <button className="btn-icon" onClick={onClose}>
            <X size={18} />
          </button>
        </div>

        <div className="dialog-body keyboard-shortcuts-body">
          <table className="keyboard-shortcuts-table">
            <tbody>
              {shortcuts.map(({ keys, description }, i) => (
                <tr key={i}>
                  <td className="keyboard-shortcuts-keys">
                    {keys.map((key, j) => (
                      <span key={j} className="keyboard-key">
                        {key}
                      </span>
                    ))}
                  </td>
                  <td className="keyboard-shortcuts-desc">{description}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
