import { describe, it, expect } from "vitest";

function computeEditCount(
  editingRows: unknown[][],
  originalRows: unknown[][]
): number {
  let count = 0;
  for (let r = 0; r < editingRows.length; r++) {
    const orig = originalRows[r];
    if (!orig) continue;
    for (let c = 0; c < editingRows[r].length; c++) {
      const ov = orig[c],
        nv = editingRows[r][c];
      if (ov !== nv && !(ov === null && nv === null)) count++;
    }
  }
  return count;
}

function escapeCsvValue(v: unknown): string {
  if (v === null || v === undefined) return "";
  const s = typeof v === "object" ? JSON.stringify(v) : String(v);
  if (s.includes(",") || s.includes('"') || s.includes("\n")) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

function buildRowData(
  editingRows: unknown[][],
  columns: string[]
): Record<string, unknown>[] {
  return editingRows.map((row, i) => {
    const obj: Record<string, unknown> = { __rowIdx: i };
    columns.forEach((col, colIdx) => {
      obj[col] = row[colIdx];
    });
    return obj;
  });
}

function formatCellValue(value: unknown): string {
  if (value === null || value === undefined) return "NULL";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

describe("ResultTable edit count", () => {
  it("returns 0 when no edits", () => {
    const rows = [[1, "a"], [2, "b"]];
    expect(computeEditCount(rows, rows)).toBe(0);
  });

  it("detects single cell change", () => {
    const original = [[1, "a"], [2, "b"]];
    const edited = [[1, "changed"], [2, "b"]];
    expect(computeEditCount(edited, original)).toBe(1);
  });

  it("detects multiple cell changes", () => {
    const original = [[1, "a", true], [2, "b", false]];
    const edited = [[1, "x", true], [2, "b", true]];
    expect(computeEditCount(edited, original)).toBe(2);
  });

  it("treats null-to-null as no change", () => {
    const original = [[null, "a"]];
    const edited = [[null, "a"]];
    expect(computeEditCount(edited, original)).toBe(0);
  });

  it("detects null-to-value change", () => {
    const original = [[null]];
    const edited = [["value"]];
    expect(computeEditCount(edited, original)).toBe(1);
  });

  it("detects value-to-null change", () => {
    const original = [["value"]];
    const edited = [[null]];
    expect(computeEditCount(edited, original)).toBe(1);
  });

  it("handles empty rows", () => {
    expect(computeEditCount([], [])).toBe(0);
  });

  it("handles rows with different lengths safely", () => {
    const original = [[1, 2, 3]];
    const edited = [[1, 2, 3]];
    expect(computeEditCount(edited, original)).toBe(0);
  });
});

describe("ResultTable CSV escape", () => {
  it("returns empty string for null", () => {
    expect(escapeCsvValue(null)).toBe("");
  });

  it("returns empty string for undefined", () => {
    expect(escapeCsvValue(undefined)).toBe("");
  });

  it("returns plain string for simple values", () => {
    expect(escapeCsvValue("hello")).toBe("hello");
    expect(escapeCsvValue(42)).toBe("42");
    expect(escapeCsvValue(true)).toBe("true");
  });

  it("wraps strings containing commas in quotes", () => {
    expect(escapeCsvValue("a,b")).toBe('"a,b"');
  });

  it("wraps strings containing newlines in quotes", () => {
    expect(escapeCsvValue("line1\nline2")).toBe('"line1\nline2"');
  });

  it("escapes double quotes by doubling them", () => {
    expect(escapeCsvValue('say "hello"')).toBe('"say ""hello"""');
  });

  it("serializes objects as JSON", () => {
    expect(escapeCsvValue({ key: "val" })).toBe('"{""key"":""val""}"');
  });

  it("serializes arrays as JSON", () => {
    expect(escapeCsvValue([1, 2, 3])).toBe('"[1,2,3]"');
  });
});

describe("ResultTable row data builder", () => {
  it("converts rows to objects with __rowIdx", () => {
    const rows = [["Alice", 30], ["Bob", 25]];
    const cols = ["name", "age"];
    const result = buildRowData(rows, cols);
    expect(result).toEqual([
      { __rowIdx: 0, name: "Alice", age: 30 },
      { __rowIdx: 1, name: "Bob", age: 25 },
    ]);
  });

  it("handles empty rows", () => {
    expect(buildRowData([], ["a", "b"])).toEqual([]);
  });

  it("handles null values", () => {
    const rows = [[null, "test"]];
    const cols = ["x", "y"];
    const result = buildRowData(rows, cols);
    expect(result).toEqual([{ __rowIdx: 0, x: null, y: "test" }]);
  });
});

describe("ResultTable cell value formatter", () => {
  it("returns NULL for null", () => {
    expect(formatCellValue(null)).toBe("NULL");
  });

  it("returns NULL for undefined", () => {
    expect(formatCellValue(undefined)).toBe("NULL");
  });

  it("returns string representation for numbers", () => {
    expect(formatCellValue(42)).toBe("42");
    expect(formatCellValue(3.14)).toBe("3.14");
  });

  it("returns string as-is", () => {
    expect(formatCellValue("hello")).toBe("hello");
  });

  it("returns JSON for objects", () => {
    expect(formatCellValue({ a: 1 })).toBe('{"a":1}');
  });

  it("returns JSON for arrays", () => {
    expect(formatCellValue([1, 2])).toBe("[1,2]");
  });

  it("handles boolean values", () => {
    expect(formatCellValue(true)).toBe("true");
    expect(formatCellValue(false)).toBe("false");
  });

  it("handles empty string", () => {
    expect(formatCellValue("")).toBe("");
  });
});
