import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, screen, fireEvent, cleanup, act } from "@testing-library/react";
import { PassphrasePrompt } from "./PassphrasePrompt";
import {
  useConnectionStore,
  type PendingPassphrasePrompt,
} from "../stores/connectionStore";

function setPending(partial: Partial<PendingPassphrasePrompt>) {
  const resolve = vi.fn();
  const reject = vi.fn();
  const pending: PendingPassphrasePrompt = {
    connectionName: "Test DB",
    keyPath: "/keys/id_ed25519",
    resolve,
    reject,
    ...partial,
  };
  useConnectionStore.setState({ pendingPassphrasePrompt: pending });
  return { resolve, reject, pending };
}

describe("PassphrasePrompt", () => {
  beforeEach(() => {
    useConnectionStore.setState({ pendingPassphrasePrompt: null });
  });

  afterEach(() => {
    cleanup();
    useConnectionStore.setState({ pendingPassphrasePrompt: null });
    vi.restoreAllMocks();
  });

  it("renders nothing while no prompt is pending", () => {
    const { container } = render(<PassphrasePrompt />);
    expect(container).toBeEmptyDOMElement();
  });

  it("surfaces the connection name and identity file when pending", () => {
    setPending({
      connectionName: "Prod replica",
      keyPath: "/etc/ssh/prod.pem",
    });
    render(<PassphrasePrompt />);
    expect(screen.getByText("Prod replica")).toBeInTheDocument();
    expect(screen.getByText("/etc/ssh/prod.pem")).toBeInTheDocument();
  });

  it("falls back to friendly placeholders when name / key path are blank", () => {
    setPending({ connectionName: "", keyPath: "" });
    render(<PassphrasePrompt />);
    expect(screen.getByText("(unnamed)")).toBeInTheDocument();
    expect(screen.getByText("(unspecified)")).toBeInTheDocument();
  });

  it("submitting the form resolves with the entered passphrase", () => {
    const { resolve } = setPending({});
    render(<PassphrasePrompt />);
    const input = screen.getByPlaceholderText(/Passphrase/i);
    fireEvent.change(input, { target: { value: "hunter2" } });
    fireEvent.submit(input.closest("form")!);
    expect(resolve).toHaveBeenCalledExactlyOnceWith("hunter2");
  });

  it("submitting an empty passphrase still resolves (unencrypted key path)", () => {
    const { resolve } = setPending({});
    render(<PassphrasePrompt />);
    fireEvent.submit(
      screen.getByPlaceholderText(/Passphrase/i).closest("form")!,
    );
    expect(resolve).toHaveBeenCalledExactlyOnceWith("");
  });

  it("Cancel button rejects with a cancellation error and never resolves", () => {
    const { resolve, reject } = setPending({});
    render(<PassphrasePrompt />);
    const buttons = screen.getAllByRole("button", { name: /Cancel/i });
    const footerCancel = buttons.find((b) =>
      b.classList.contains("btn-secondary"),
    );
    expect(footerCancel).toBeDefined();
    fireEvent.click(footerCancel!);
    expect(reject).toHaveBeenCalledTimes(1);
    expect(reject.mock.calls[0][0]).toBeInstanceOf(Error);
    expect((reject.mock.calls[0][0] as Error).message).toMatch(/cancel/i);
    expect(resolve).not.toHaveBeenCalled();
  });

  it("X close icon also rejects", () => {
    const { reject } = setPending({});
    render(<PassphrasePrompt />);
    const xClose = screen
      .getAllByRole("button", { name: /Cancel/i })
      .find((b) => b.classList.contains("passphrase-prompt__close"));
    expect(xClose).toBeDefined();
    fireEvent.click(xClose!);
    expect(reject).toHaveBeenCalledTimes(1);
  });

  it("Escape key rejects with a cancellation error", () => {
    const { reject } = setPending({});
    render(<PassphrasePrompt />);
    fireEvent.keyDown(window, { key: "Escape" });
    expect(reject).toHaveBeenCalledTimes(1);
    expect((reject.mock.calls[0][0] as Error).message).toMatch(/cancel/i);
  });

  it("typing into a stale prompt is reset when a new prompt is raised", () => {
    const first = setPending({ connectionName: "First" });
    render(<PassphrasePrompt />);
    const input = screen.getByPlaceholderText(/Passphrase/i) as HTMLInputElement;
    fireEvent.change(input, { target: { value: "leaked-from-prev" } });
    expect(input.value).toBe("leaked-from-prev");

    // Resolve the first request and raise a fresh one. The input must be
    // cleared so we never leak the previous passphrase into the next
    // connection's submission.
    act(() => first.resolve(""));
    act(() => {
      setPending({ connectionName: "Second" });
    });

    const inputAfter = screen.getByPlaceholderText(/Passphrase/i) as HTMLInputElement;
    expect(inputAfter.value).toBe("");
  });
});
