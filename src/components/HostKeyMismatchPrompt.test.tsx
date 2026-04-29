import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, screen, fireEvent, cleanup } from "@testing-library/react";
import { HostKeyMismatchPrompt } from "./HostKeyMismatchPrompt";
import {
  useConnectionStore,
  type PendingHostKeyMismatchPrompt,
} from "../stores/connectionStore";

function setPending(
  partial: Partial<PendingHostKeyMismatchPrompt> = {},
): { resolve: ReturnType<typeof vi.fn>; pending: PendingHostKeyMismatchPrompt } {
  const resolve = vi.fn();
  const pending: PendingHostKeyMismatchPrompt = {
    connectionName: "Prod replica",
    info: {
      host: "bastion.example",
      port: 2222,
      fingerprint: "SHA256:abc123",
      knownHostsPath: "/home/u/.tablio/known_hosts",
    },
    resolve,
    ...partial,
  };
  useConnectionStore.setState({ pendingHostKeyMismatch: pending });
  return { resolve, pending };
}

describe("HostKeyMismatchPrompt", () => {
  beforeEach(() => {
    useConnectionStore.setState({ pendingHostKeyMismatch: null });
  });

  afterEach(() => {
    cleanup();
    useConnectionStore.setState({ pendingHostKeyMismatch: null });
    vi.restoreAllMocks();
  });

  it("renders nothing while no prompt is pending", () => {
    const { container } = render(<HostKeyMismatchPrompt />);
    expect(container).toBeEmptyDOMElement();
  });

  it("surfaces host:port, fingerprint, key path, and the connection name", () => {
    setPending({});
    render(<HostKeyMismatchPrompt />);
    // Host and port are rendered together inside the lead paragraph.
    expect(
      screen.getByText(
        (_, node) => node?.textContent === "bastion.example:2222",
      ),
    ).toBeInTheDocument();
    expect(screen.getByText("SHA256:abc123")).toBeInTheDocument();
    expect(screen.getByText("/home/u/.tablio/known_hosts")).toBeInTheDocument();
    expect(screen.getByText("Prod replica")).toBeInTheDocument();
  });

  it("falls back to (unnamed) when the connection has no name", () => {
    setPending({ connectionName: "" });
    render(<HostKeyMismatchPrompt />);
    expect(screen.getByText("(unnamed)")).toBeInTheDocument();
  });

  it("Forget & retry resolves with true exactly once", () => {
    const { resolve } = setPending({});
    render(<HostKeyMismatchPrompt />);
    fireEvent.click(screen.getByRole("button", { name: /Forget & retry/i }));
    expect(resolve).toHaveBeenCalledExactlyOnceWith(true);
  });

  it("Cancel resolves with false", () => {
    const { resolve } = setPending({});
    render(<HostKeyMismatchPrompt />);
    const footerCancel = screen
      .getAllByRole("button", { name: /Cancel/i })
      .find((b) => b.classList.contains("btn-secondary"));
    expect(footerCancel).toBeDefined();
    fireEvent.click(footerCancel!);
    expect(resolve).toHaveBeenCalledExactlyOnceWith(false);
  });

  it("X close icon resolves with false", () => {
    const { resolve } = setPending({});
    render(<HostKeyMismatchPrompt />);
    const xClose = screen
      .getAllByRole("button", { name: /Cancel/i })
      .find((b) => b.classList.contains("host-mismatch-prompt__close"));
    expect(xClose).toBeDefined();
    fireEvent.click(xClose!);
    expect(resolve).toHaveBeenCalledExactlyOnceWith(false);
  });

  it("Escape key resolves with false (default-deny)", () => {
    const { resolve } = setPending({});
    render(<HostKeyMismatchPrompt />);
    fireEvent.keyDown(window, { key: "Escape" });
    expect(resolve).toHaveBeenCalledExactlyOnceWith(false);
  });

  it("ignores other keys", () => {
    const { resolve } = setPending({});
    render(<HostKeyMismatchPrompt />);
    fireEvent.keyDown(window, { key: "Enter" });
    fireEvent.keyDown(window, { key: " " });
    expect(resolve).not.toHaveBeenCalled();
  });
});
