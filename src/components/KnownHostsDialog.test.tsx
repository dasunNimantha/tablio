import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, screen, fireEvent, waitFor, cleanup } from "@testing-library/react";
import { KnownHostsDialog } from "./KnownHostsDialog";
import { api, type KnownHostEntry } from "../lib/tauri";

const sampleEntries: KnownHostEntry[] = [
  {
    host: "bastion.example.com",
    port: 22,
    keyType: "ssh-ed25519",
    fingerprint: "SHA256:aaa",
  },
  {
    host: "10.0.0.5",
    port: 2222,
    keyType: "ssh-rsa",
    fingerprint: "SHA256:bbb",
  },
];

describe("KnownHostsDialog", () => {
  beforeEach(() => {
    vi.spyOn(api, "listKnownHosts").mockResolvedValue([...sampleEntries]);
    vi.spyOn(api, "forgetKnownHost").mockResolvedValue(undefined);
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  it("renders nothing when closed", () => {
    const { container } = render(
      <KnownHostsDialog open={false} onClose={vi.fn()} />,
    );
    expect(container).toBeEmptyDOMElement();
    expect(api.listKnownHosts).not.toHaveBeenCalled();
  });

  it("loads and lists entries when opened", async () => {
    render(<KnownHostsDialog open={true} onClose={vi.fn()} />);
    await waitFor(() => {
      expect(screen.getByText("bastion.example.com")).toBeInTheDocument();
    });
    expect(screen.getByText("10.0.0.5")).toBeInTheDocument();
    expect(screen.getByText("ssh-ed25519")).toBeInTheDocument();
    expect(screen.getByText("ssh-rsa")).toBeInTheDocument();
    expect(screen.getByText("2222")).toBeInTheDocument();
  });

  it("filters entries case-insensitively across host, key type, and fingerprint", async () => {
    render(<KnownHostsDialog open={true} onClose={vi.fn()} />);
    await waitFor(() => screen.getByText("bastion.example.com"));

    const filter = screen.getByLabelText(/Filter known hosts/i);
    fireEvent.change(filter, { target: { value: "RSA" } });

    expect(screen.queryByText("bastion.example.com")).not.toBeInTheDocument();
    expect(screen.getByText("10.0.0.5")).toBeInTheDocument();

    fireEvent.change(filter, { target: { value: "sha256:aaa" } });
    expect(screen.queryByText("10.0.0.5")).not.toBeInTheDocument();
    expect(screen.getByText("bastion.example.com")).toBeInTheDocument();

    fireEvent.change(filter, { target: { value: "nope" } });
    expect(screen.getByText(/No entries match your filter/i)).toBeInTheDocument();
  });

  it("forgets the matching entry and removes it from the list", async () => {
    render(<KnownHostsDialog open={true} onClose={vi.fn()} />);
    await waitFor(() => screen.getByText("bastion.example.com"));

    const forgetButton = screen.getByRole("button", {
      name: /Forget bastion\.example\.com/i,
    });
    fireEvent.click(forgetButton);

    await waitFor(() => {
      expect(screen.queryByText("bastion.example.com")).not.toBeInTheDocument();
    });
    expect(api.forgetKnownHost).toHaveBeenCalledWith(
      "bastion.example.com",
      22,
      "SHA256:aaa",
    );
    expect(screen.getByText("10.0.0.5")).toBeInTheDocument();
  });

  it("surfaces backend errors instead of crashing", async () => {
    (api.listKnownHosts as ReturnType<typeof vi.fn>).mockRejectedValueOnce(
      new Error("disk full"),
    );
    render(<KnownHostsDialog open={true} onClose={vi.fn()} />);
    await waitFor(() => {
      expect(screen.getByRole("alert")).toHaveTextContent("disk full");
    });
  });

  it("shows the empty state when there are no recorded hosts", async () => {
    (api.listKnownHosts as ReturnType<typeof vi.fn>).mockResolvedValueOnce([]);
    render(<KnownHostsDialog open={true} onClose={vi.fn()} />);
    await waitFor(() => {
      expect(
        screen.getByText(/No SSH hosts have been recorded yet/i),
      ).toBeInTheDocument();
    });
  });

  it("calls onClose when Escape is pressed and when the Close button is clicked", async () => {
    const onClose = vi.fn();
    render(<KnownHostsDialog open={true} onClose={onClose} />);
    await waitFor(() => screen.getByText("bastion.example.com"));

    fireEvent.keyDown(window, { key: "Escape" });
    expect(onClose).toHaveBeenCalledTimes(1);

    const closeButtons = screen.getAllByRole("button", { name: /Close/i });
    const footerClose = closeButtons.find((b) =>
      b.classList.contains("btn-primary"),
    );
    expect(footerClose).toBeDefined();
    fireEvent.click(footerClose!);
    expect(onClose).toHaveBeenCalledTimes(2);
  });

  it("refreshes the list on demand", async () => {
    render(<KnownHostsDialog open={true} onClose={vi.fn()} />);
    await waitFor(() => screen.getByText("bastion.example.com"));
    expect(api.listKnownHosts).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByRole("button", { name: /Refresh/i }));
    await waitFor(() => {
      expect(api.listKnownHosts).toHaveBeenCalledTimes(2);
    });
  });
});
