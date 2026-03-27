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

// --- Search navigation logic (extracted from ResultTable component) ---

interface SearchMatch {
  rowIndex: number;
  colId: string;
}

function buildSearchMatches(
  searchQuery: string,
  editingRows: unknown[][],
  columns: string[]
): SearchMatch[] {
  if (!searchQuery) return [];
  const q = searchQuery.toLowerCase();
  const matches: SearchMatch[] = [];
  for (let r = 0; r < editingRows.length; r++) {
    for (let c = 0; c < editingRows[r].length; c++) {
      const val = editingRows[r][c];
      const str =
        val === null || val === undefined
          ? "null"
          : typeof val === "object"
            ? JSON.stringify(val)
            : String(val);
      if (str.toLowerCase().includes(q) && columns[c]) {
        matches.push({ rowIndex: r, colId: columns[c] });
      }
    }
  }
  return matches;
}

function computeNextIdx(current: number, total: number): number {
  if (total === 0) return -1;
  return current + 1 >= total ? 0 : current + 1;
}

function computePrevIdx(current: number, total: number): number {
  if (total === 0) return -1;
  return current - 1 < 0 ? total - 1 : current - 1;
}

function isCellSearchCurrent(
  currentMatch: SearchMatch | null,
  rowIndex: number,
  colField: string | undefined
): boolean {
  if (!currentMatch) return false;
  return rowIndex === currentMatch.rowIndex && colField === currentMatch.colId;
}

function formatSearchCounter(matchIdx: number, matchCount: number): string {
  if (matchCount === 0) return "No results";
  return `${matchIdx + 1} / ${matchCount}`;
}

describe("ResultTable search match building", () => {
  const cols = ["id", "name", "email"];

  it("returns empty array for empty query", () => {
    const rows = [[1, "Alice", "alice@test.com"]];
    expect(buildSearchMatches("", rows, cols)).toEqual([]);
  });

  it("returns empty array for no rows", () => {
    expect(buildSearchMatches("test", [], cols)).toEqual([]);
  });

  it("finds single match", () => {
    const rows = [[1, "Alice", "bob@test.com"]];
    const matches = buildSearchMatches("Alice", rows, cols);
    expect(matches).toEqual([{ rowIndex: 0, colId: "name" }]);
  });

  it("finds matches across multiple columns and rows", () => {
    const rows = [
      [1, "alice", "alice@test.com"],
      [2, "bob", "bob@test.com"],
    ];
    const matches = buildSearchMatches("alice", rows, cols);
    expect(matches).toEqual([
      { rowIndex: 0, colId: "name" },
      { rowIndex: 0, colId: "email" },
    ]);
  });

  it("is case-insensitive", () => {
    const rows = [[1, "ALICE", "alice@test.com"]];
    const matches = buildSearchMatches("alice", rows, cols);
    expect(matches.length).toBe(2);
  });

  it("matches null values when searching for 'null'", () => {
    const rows = [[1, null, "test"]];
    const matches = buildSearchMatches("null", rows, cols);
    expect(matches).toEqual([{ rowIndex: 0, colId: "name" }]);
  });

  it("matches numeric values as strings", () => {
    const rows = [[42, "test", "test"]];
    const matches = buildSearchMatches("42", rows, cols);
    expect(matches).toEqual([{ rowIndex: 0, colId: "id" }]);
  });

  it("matches object values via JSON.stringify", () => {
    const rows = [[1, { key: "value" }, "test"]];
    const matches = buildSearchMatches("value", rows, cols);
    expect(matches).toEqual([{ rowIndex: 0, colId: "name" }]);
  });

  it("returns no matches for non-existent query", () => {
    const rows = [[1, "Alice", "alice@test.com"]];
    expect(buildSearchMatches("zzz", rows, cols)).toEqual([]);
  });

  it("skips cells beyond columns array length", () => {
    const shortCols = ["id"];
    const rows = [[1, "Alice", "alice@test.com"]];
    const matches = buildSearchMatches("alice", rows, shortCols);
    expect(matches).toEqual([]);
  });
});

describe("ResultTable search navigation index", () => {
  it("next wraps from last to first", () => {
    expect(computeNextIdx(4, 5)).toBe(0);
  });

  it("next advances normally", () => {
    expect(computeNextIdx(1, 5)).toBe(2);
  });

  it("next returns -1 for empty", () => {
    expect(computeNextIdx(0, 0)).toBe(-1);
  });

  it("prev wraps from first to last", () => {
    expect(computePrevIdx(0, 5)).toBe(4);
  });

  it("prev goes back normally", () => {
    expect(computePrevIdx(3, 5)).toBe(2);
  });

  it("prev returns -1 for empty", () => {
    expect(computePrevIdx(0, 0)).toBe(-1);
  });

  it("single match: next stays at 0", () => {
    expect(computeNextIdx(0, 1)).toBe(0);
  });

  it("single match: prev stays at 0", () => {
    expect(computePrevIdx(0, 1)).toBe(0);
  });
});

describe("ResultTable cell search current highlight", () => {
  it("returns false when no current match", () => {
    expect(isCellSearchCurrent(null, 0, "name")).toBe(false);
  });

  it("returns true for exact match", () => {
    expect(isCellSearchCurrent({ rowIndex: 1, colId: "name" }, 1, "name")).toBe(true);
  });

  it("returns false for wrong row", () => {
    expect(isCellSearchCurrent({ rowIndex: 1, colId: "name" }, 2, "name")).toBe(false);
  });

  it("returns false for wrong column", () => {
    expect(isCellSearchCurrent({ rowIndex: 1, colId: "name" }, 1, "email")).toBe(false);
  });

  it("returns false for undefined colField", () => {
    expect(isCellSearchCurrent({ rowIndex: 0, colId: "id" }, 0, undefined)).toBe(false);
  });
});

describe("ResultTable search counter display", () => {
  it("shows 'No results' for zero matches", () => {
    expect(formatSearchCounter(-1, 0)).toBe("No results");
  });

  it("shows correct position for first match", () => {
    expect(formatSearchCounter(0, 3)).toBe("1 / 3");
  });

  it("shows correct position for last match", () => {
    expect(formatSearchCounter(2, 3)).toBe("3 / 3");
  });
});

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
