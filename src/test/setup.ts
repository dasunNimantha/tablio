import "@testing-library/jest-dom";
import { vi } from "vitest";

vi.mock("../lib/tauri", () => ({
  api: {
    fetchRows: vi.fn(),
    applyChanges: vi.fn(),
    executeQuery: vi.fn(),
    exportTableToFile: vi.fn(),
    exportQueryResultToFile: vi.fn(),
    listColumns: vi.fn(),
  },
}));
