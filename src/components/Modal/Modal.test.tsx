import { describe, it, expect, vi, afterEach } from "vitest";
import { render, screen, fireEvent, cleanup } from "@testing-library/react";
import { useState } from "react";
import { Modal } from "./Modal";
import { ConfirmDialog } from "../ConfirmDialog";

afterEach(cleanup);

function Harness({ busy = false, onClose = vi.fn() }: { busy?: boolean; onClose?: () => void }) {
  const [open, setOpen] = useState(false);
  return (
    <>
      <button onClick={() => setOpen(true)}>Open</button>
      {open && (
        <Modal
          busy={busy}
          labelledBy="t"
          onClose={() => {
            onClose();
            setOpen(false);
          }}
        >
          <h2 id="t">Title</h2>
          <button className="btn-icon" aria-label="Close">x</button>
          <input aria-label="Name" />
          <button className="btn-primary">Save</button>
        </Modal>
      )}
    </>
  );
}

describe("Modal", () => {
  it("is a labelled modal dialog that focuses the first field", () => {
    render(<Harness />);
    fireEvent.click(screen.getByText("Open"));
    const dialog = screen.getByRole("dialog", { name: "Title" });
    expect(dialog).toHaveAttribute("aria-modal", "true");
    expect(document.activeElement).toBe(screen.getByLabelText("Name"));
  });

  it("closes on Escape and returns focus to the opener", () => {
    const onClose = vi.fn();
    render(<Harness onClose={onClose} />);
    const opener = screen.getByText("Open");
    opener.focus();
    fireEvent.click(opener);
    fireEvent.keyDown(window, { key: "Escape" });
    expect(onClose).toHaveBeenCalledTimes(1);
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(document.activeElement).toBe(opener);
  });

  it("ignores Escape and backdrop clicks while busy", () => {
    const onClose = vi.fn();
    render(<Harness busy onClose={onClose} />);
    fireEvent.click(screen.getByText("Open"));
    fireEvent.keyDown(window, { key: "Escape" });
    const overlay = document.querySelector(".dialog-overlay")!;
    fireEvent.mouseDown(overlay);
    fireEvent.click(overlay);
    expect(onClose).not.toHaveBeenCalled();
  });

  it("closes on a backdrop click but not on a click inside", () => {
    const onClose = vi.fn();
    render(<Harness onClose={onClose} />);
    fireEvent.click(screen.getByText("Open"));
    const name = screen.getByLabelText("Name");
    fireEvent.mouseDown(name);
    fireEvent.click(name);
    expect(onClose).not.toHaveBeenCalled();
    const overlay = document.querySelector(".dialog-overlay")!;
    fireEvent.mouseDown(overlay);
    fireEvent.click(overlay);
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("wraps Tab and Shift+Tab inside the dialog", () => {
    render(<Harness />);
    fireEvent.click(screen.getByText("Open"));
    const save = screen.getByText("Save");
    const close = screen.getByLabelText("Close");
    save.focus();
    fireEvent.keyDown(window, { key: "Tab" });
    expect(document.activeElement).toBe(close);
    fireEvent.keyDown(window, { key: "Tab", shiftKey: true });
    expect(document.activeElement).toBe(save);
  });

  it("only the topmost of nested modals reacts to Escape", () => {
    const outer = vi.fn();
    const inner = vi.fn();
    render(
      <Modal onClose={outer} label="Outer">
        <input aria-label="outer field" />
        <Modal onClose={inner} label="Inner">
          <button>ok</button>
        </Modal>
      </Modal>,
    );
    fireEvent.keyDown(window, { key: "Escape" });
    expect(inner).toHaveBeenCalledTimes(1);
    expect(outer).not.toHaveBeenCalled();
  });
});

describe("ConfirmDialog", () => {
  it("runs the action once on a double click and disables both buttons while pending", async () => {
    let resolve!: () => void;
    const onConfirm = vi.fn(() => new Promise<void>((r) => (resolve = r)));
    render(<ConfirmDialog title="Drop" message="Sure?" confirmLabel="Drop" danger onConfirm={onConfirm} onCancel={vi.fn()} />);
    const drop = screen.getByRole("button", { name: "Drop" });
    fireEvent.click(drop);
    fireEvent.click(drop);
    expect(onConfirm).toHaveBeenCalledTimes(1);
    expect(await screen.findByRole("button", { name: "Drop" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Cancel" })).toBeDisabled();
    resolve();
  });

  it("stays open and shows the error when the action rejects", async () => {
    const onCancel = vi.fn();
    render(
      <ConfirmDialog
        title="Truncate"
        message="Sure?"
        danger
        onConfirm={() => Promise.reject(new Error("permission denied for table users"))}
        onCancel={onCancel}
      />,
    );
    fireEvent.click(screen.getByRole("button", { name: "Confirm" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("permission denied for table users");
    expect(screen.getByRole("alertdialog", { name: "Truncate" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Confirm" })).not.toBeDisabled();
  });

  it("starts on Cancel for destructive confirmations and closes on Escape", () => {
    const onCancel = vi.fn();
    render(<ConfirmDialog title="Drop" message="m" danger onConfirm={vi.fn()} onCancel={onCancel} />);
    expect(document.activeElement).toBe(screen.getByRole("button", { name: "Cancel" }));
    fireEvent.keyDown(window, { key: "Escape" });
    expect(onCancel).toHaveBeenCalled();
  });
});
