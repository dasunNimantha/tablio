import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, screen, fireEvent, cleanup } from "@testing-library/react";
import { BackupRestoreDialog } from "./BackupRestoreDialog";
import { api } from "../../lib/tauri";
import { useConnectionStore } from "../../stores/connectionStore";

vi.mock("@tauri-apps/plugin-dialog", () => ({ save: vi.fn(), open: vi.fn() }));

function schemaVersion(id: string): number {
  return useConnectionStore.getState().schemaVersions[id] ?? 0;
}

async function startRestore(path: string) {
  render(<BackupRestoreDialog connectionId="c1" database="main" onClose={vi.fn()} />);
  fireEvent.click(screen.getByRole("button", { name: /^Restore$/ }));
  fireEvent.change(screen.getByPlaceholderText("Select a backup file..."), {
    target: { value: path },
  });
  fireEvent.click(screen.getByRole("button", { name: /start restore/i }));
}

describe("BackupRestoreDialog restore", () => {
  beforeEach(() => {
    useConnectionStore.setState({ schemaVersions: {} });
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  it("refreshes the connection's schema after a successful restore", async () => {
    vi.spyOn(api, "restoreDatabase").mockResolvedValue("Restore completed");
    await startRestore("/tmp/backup.db");
    expect(await screen.findByText("Restore complete")).toBeInTheDocument();
    expect(schemaVersion("c1")).toBe(1);
  });

  it("refreshes the schema after a failed restore too (it may have applied part of the dump)", async () => {
    vi.spyOn(api, "restoreDatabase").mockRejectedValue("psql restore failed");
    await startRestore("/tmp/bad.sql");
    expect(await screen.findByText("Restore failed")).toBeInTheDocument();
    expect(schemaVersion("c1")).toBe(1);
  });
});
