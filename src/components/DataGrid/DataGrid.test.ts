import { describe, it, expect } from "vitest";

/**
 * DataGrid encodes delete keys as JSON.stringify(pkValues) and decodes with JSON.parse.
 * These tests verify the encoding is safe for any PK value (e.g. containing colons).
 */
describe("DataGrid delete key encoding", () => {
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
});
