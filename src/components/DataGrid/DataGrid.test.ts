import { describe, it, expect } from "vitest";

function encodePkKey(values: unknown[]): string {
  return JSON.stringify(values);
}

function decodePkKey(
  key: string,
  pkCols: { name: string }[]
): [string, unknown][] | null {
  let pkValues: unknown[];
  try {
    pkValues = JSON.parse(key) as unknown[];
  } catch {
    return null;
  }
  if (pkCols.length === 0 || pkValues.length !== pkCols.length) return null;
  return pkCols.map((c, i) => [c.name, pkValues[i]]);
}

function shouldSuppressKeyboard(
  editing: boolean,
  key: string,
  ctrlKey: boolean,
  metaKey: boolean
): boolean {
  if (!editing) {
    if (
      key === "Delete" ||
      key === "Backspace" ||
      key === "Enter" ||
      key === "F2" ||
      (key.length === 1 && !ctrlKey && !metaKey)
    ) {
      return true;
    }
  }
  return false;
}

describe("DataGrid delete key encoding", () => {
  it("round-trips simple values", () => {
    const values = [1, "hello"];
    const key = encodePkKey(values);
    const pkCols = [{ name: "id" }, { name: "name" }];
    const decoded = decodePkKey(key, pkCols);
    expect(decoded).toEqual([
      ["id", 1],
      ["name", "hello"],
    ]);
  });

  it("round-trips values containing colon", () => {
    const values = ["a:b", "c:d:e"];
    const key = encodePkKey(values);
    const pkCols = [{ name: "x" }, { name: "y" }];
    const decoded = decodePkKey(key, pkCols);
    expect(decoded).toEqual([
      ["x", "a:b"],
      ["y", "c:d:e"],
    ]);
  });

  it("round-trips null and number", () => {
    const values = [null, 42];
    const key = encodePkKey(values);
    const decoded = decodePkKey(key, [{ name: "a" }, { name: "b" }]);
    expect(decoded).toEqual([
      ["a", null],
      ["b", 42],
    ]);
  });

  it("returns null for invalid JSON", () => {
    expect(decodePkKey("not json", [{ name: "id" }])).toBeNull();
  });

  it("returns null when key length does not match pk columns", () => {
    const key = encodePkKey([1]);
    expect(decodePkKey(key, [{ name: "id" }, { name: "id2" }])).toBeNull();
  });

  it("returns null for empty pk columns", () => {
    expect(decodePkKey("[1]", [])).toBeNull();
  });

  it("round-trips boolean and empty string", () => {
    const values = [true, "", false];
    const key = encodePkKey(values);
    const decoded = decodePkKey(key, [{ name: "a" }, { name: "b" }, { name: "c" }]);
    expect(decoded).toEqual([
      ["a", true],
      ["b", ""],
      ["c", false],
    ]);
  });
});

describe("DataGrid suppressKeyboardEvent", () => {
  describe("when not editing", () => {
    it("suppresses printable character keys", () => {
      expect(shouldSuppressKeyboard(false, "a", false, false)).toBe(true);
      expect(shouldSuppressKeyboard(false, "z", false, false)).toBe(true);
      expect(shouldSuppressKeyboard(false, "1", false, false)).toBe(true);
      expect(shouldSuppressKeyboard(false, " ", false, false)).toBe(true);
    });

    it("suppresses Delete key", () => {
      expect(shouldSuppressKeyboard(false, "Delete", false, false)).toBe(true);
    });

    it("suppresses Backspace key", () => {
      expect(shouldSuppressKeyboard(false, "Backspace", false, false)).toBe(true);
    });

    it("suppresses Enter key", () => {
      expect(shouldSuppressKeyboard(false, "Enter", false, false)).toBe(true);
    });

    it("suppresses F2 key", () => {
      expect(shouldSuppressKeyboard(false, "F2", false, false)).toBe(true);
    });

    it("does NOT suppress Ctrl+C (copy shortcut)", () => {
      expect(shouldSuppressKeyboard(false, "c", true, false)).toBe(false);
    });

    it("does NOT suppress Cmd+V (paste shortcut on Mac)", () => {
      expect(shouldSuppressKeyboard(false, "v", false, true)).toBe(false);
    });

    it("does NOT suppress Ctrl+A (select all)", () => {
      expect(shouldSuppressKeyboard(false, "a", true, false)).toBe(false);
    });

    it("does NOT suppress multi-character keys like Tab, Escape, Arrow", () => {
      expect(shouldSuppressKeyboard(false, "Tab", false, false)).toBe(false);
      expect(shouldSuppressKeyboard(false, "Escape", false, false)).toBe(false);
      expect(shouldSuppressKeyboard(false, "ArrowDown", false, false)).toBe(false);
      expect(shouldSuppressKeyboard(false, "ArrowUp", false, false)).toBe(false);
      expect(shouldSuppressKeyboard(false, "ArrowLeft", false, false)).toBe(false);
      expect(shouldSuppressKeyboard(false, "ArrowRight", false, false)).toBe(false);
    });

    it("does NOT suppress F1, F3-F12 (only F2 is suppressed)", () => {
      expect(shouldSuppressKeyboard(false, "F1", false, false)).toBe(false);
      expect(shouldSuppressKeyboard(false, "F3", false, false)).toBe(false);
      expect(shouldSuppressKeyboard(false, "F12", false, false)).toBe(false);
    });
  });

  describe("when already editing", () => {
    it("never suppresses any key", () => {
      expect(shouldSuppressKeyboard(true, "a", false, false)).toBe(false);
      expect(shouldSuppressKeyboard(true, "Delete", false, false)).toBe(false);
      expect(shouldSuppressKeyboard(true, "Backspace", false, false)).toBe(false);
      expect(shouldSuppressKeyboard(true, "Enter", false, false)).toBe(false);
      expect(shouldSuppressKeyboard(true, "F2", false, false)).toBe(false);
      expect(shouldSuppressKeyboard(true, " ", false, false)).toBe(false);
    });
  });
});
