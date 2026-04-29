import "@testing-library/jest-dom";
import { vi } from "vitest";

// Stub the Tauri bridge module. We keep the real exports for type
// helpers (interfaces, parseSshHostKeyMismatch, prefixes, etc.) so
// individual tests can opt into mocking specific `api.*` methods via
// `vi.spyOn(api, "...")` instead of having to redeclare every single
// command up front. Only the shape that *all* relative-importers
// previously relied on is replaced wholesale.
vi.mock("../lib/tauri", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../lib/tauri")>();
  return {
    ...actual,
    api: {
      ...actual.api,
      fetchRows: vi.fn(),
      applyChanges: vi.fn(),
      executeQuery: vi.fn(),
      exportTableToFile: vi.fn(),
      exportQueryResultToFile: vi.fn(),
      listColumns: vi.fn(),
    },
  };
});
