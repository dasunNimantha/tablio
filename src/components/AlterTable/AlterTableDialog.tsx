import { useCallback, useId, useState } from "react";
import { X } from "lucide-react";
import { Modal } from "../Modal/Modal";
import { AlterTableEditor } from "./AlterTableEditor";
import "./AlterTableDialog.css";

/**
 * Modal wrapper for the Alter Table editor.
 *
 * After issue #59 the editor body lives in {@link AlterTableEditor};
 * this component is now just the dialog chrome (overlay + header)
 * plus a passthrough to the editor with `variant="modal"`.
 *
 * The modal entry is preserved for backwards compatibility with the
 * sidebar's right-click → Alter Table context-menu action; the new
 * inline editor on the Schema view mounts the same `AlterTableEditor`
 * directly with `variant="inline"`.
 */

interface Props {
  connectionId: string;
  database: string;
  schema: string;
  tableName: string;
  onClose: () => void;
  onAltered: () => void;
}

export function AlterTableDialog({
  connectionId,
  database,
  schema,
  tableName,
  onClose,
  onAltered,
}: Props) {
  // Save in the editor calls both `onAltered` (refresh sidebar) and
  // closes the dialog. The editor itself wipes the persisted draft;
  // we don't need to do anything else here.
  const handleSaved = () => {
    onAltered();
    onClose();
  };

  const titleId = useId();
  const [state, setState] = useState({ dirty: false, busy: false });
  const handleStateChange = useCallback(
    (next: { dirty: boolean; busy: boolean }) =>
      setState((prev) =>
        prev.dirty === next.dirty && prev.busy === next.busy ? prev : next,
      ),
    [],
  );

  return (
    <Modal
      onClose={onClose}
      className="dialog alter-table-dialog"
      labelledBy={titleId}
      // Pending edits survive closing (they're kept as a per-table
      // draft), so only a save in flight blocks Escape / overlay clicks.
      busy={state.busy}
    >
      <div className="dialog-header">
        <h2 id={titleId}>Alter Table</h2>
        <button className="btn-icon" onClick={onClose} aria-label="Close" title="Close">
          <X size={18} />
        </button>
      </div>
      <AlterTableEditor
        connectionId={connectionId}
        database={database}
        schema={schema}
        tableName={tableName}
        onSaved={handleSaved}
        onDiscard={onClose}
        variant="modal"
        onStateChange={handleStateChange}
      />
    </Modal>
  );
}
