import { describe, it, expect } from "vitest";
import { parseSimpleSelect } from "./ResultTable";

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

describe("parseSimpleSelect", () => {
  it("parses simple SELECT * FROM table with empty schema", () => {
    expect(parseSimpleSelect("SELECT * FROM users")).toEqual({ schema: "", table: "users" });
  });

  it("parses SELECT with schema.table", () => {
    expect(parseSimpleSelect("SELECT * FROM myschema.users")).toEqual({ schema: "myschema", table: "users" });
  });

  it('parses SELECT with quoted "schema"."table"', () => {
    expect(parseSimpleSelect('SELECT * FROM "my_schema"."my_table"')).toEqual({ schema: "my_schema", table: "my_table" });
  });

  it('parses SELECT with quoted "table" only — schema is empty', () => {
    expect(parseSimpleSelect('SELECT * FROM "my_table"')).toEqual({ schema: "", table: "my_table" });
  });

  it("parses SELECT with columns listed", () => {
    expect(parseSimpleSelect("SELECT id, name FROM customers WHERE id > 5")).toEqual({ schema: "", table: "customers" });
  });

  it("parses case-insensitive FROM", () => {
    expect(parseSimpleSelect("select * from Products")).toEqual({ schema: "", table: "Products" });
  });

  it("returns null for JOIN query", () => {
    expect(parseSimpleSelect("SELECT * FROM users JOIN orders ON users.id = orders.user_id")).toBeNull();
  });

  it("returns null for UNION query", () => {
    expect(parseSimpleSelect("SELECT * FROM users UNION SELECT * FROM admins")).toBeNull();
  });

  it("returns null for INSERT statement", () => {
    expect(parseSimpleSelect("INSERT INTO users (name) VALUES ('test')")).toBeNull();
  });

  it("returns null for UPDATE statement", () => {
    expect(parseSimpleSelect("UPDATE users SET name = 'test'")).toBeNull();
  });

  it("returns null for DELETE statement", () => {
    expect(parseSimpleSelect("DELETE FROM users WHERE id = 1")).toBeNull();
  });

  it("returns null for CTE (WITH clause)", () => {
    expect(parseSimpleSelect("WITH cte AS (SELECT * FROM users) SELECT * FROM cte")).toBeNull();
  });

  it("returns null for multiple FROM tables", () => {
    expect(parseSimpleSelect("SELECT * FROM users, orders")).toBeNull();
  });

  it("returns null for INTERSECT query", () => {
    expect(parseSimpleSelect("SELECT id FROM a INTERSECT SELECT id FROM b")).toBeNull();
  });

  it("returns null for EXCEPT query", () => {
    expect(parseSimpleSelect("SELECT id FROM a EXCEPT SELECT id FROM b")).toBeNull();
  });

  it("handles trailing whitespace", () => {
    expect(parseSimpleSelect("  SELECT * FROM users  ")).toEqual({ schema: "", table: "users" });
  });

  it("handles WHERE, ORDER BY, LIMIT clauses", () => {
    expect(parseSimpleSelect("SELECT * FROM users WHERE active = true ORDER BY id LIMIT 100")).toEqual({ schema: "", table: "users" });
  });

  it("returns null for empty string", () => {
    expect(parseSimpleSelect("")).toBeNull();
  });
});

// --- isUpdatable logic (editable vs read-only detection) ---

interface ColumnMeta {
  name: string;
  is_primary_key: boolean;
}

interface SourceTable {
  schema: string;
  table: string;
}

function computeIsUpdatable(
  sourceTable: SourceTable | null,
  tableColumns: ColumnMeta[] | null,
  resultColumns: string[],
): boolean {
  if (!sourceTable) return false;
  if (!tableColumns) return false;
  const pkColumns = tableColumns.filter((c) => c.is_primary_key);
  if (pkColumns.length === 0) return false;
  return pkColumns.every((pk) => resultColumns.includes(pk.name));
}

function computeReadOnlyReason(
  sourceTable: SourceTable | null,
  tableColumns: ColumnMeta[] | null,
  resultColumns: string[],
): string {
  if (!sourceTable) return "Complex query — no single source table detected";
  if (!tableColumns) return "Column metadata not loaded";
  const pkColumns = tableColumns.filter((c) => c.is_primary_key);
  if (pkColumns.length === 0) return "Table has no primary key";
  const missingPks = pkColumns.filter((pk) => !resultColumns.includes(pk.name));
  if (missingPks.length > 0) {
    return `PK column${missingPks.length > 1 ? "s" : ""} (${missingPks.map((c) => c.name).join(", ")}) not in result`;
  }
  return "";
}

describe("ResultTable isUpdatable logic", () => {
  const tableWithPk: ColumnMeta[] = [
    { name: "id", is_primary_key: true },
    { name: "name", is_primary_key: false },
    { name: "email", is_primary_key: false },
  ];

  const tableWithCompositePk: ColumnMeta[] = [
    { name: "user_id", is_primary_key: true },
    { name: "role_id", is_primary_key: true },
    { name: "assigned_at", is_primary_key: false },
  ];

  const tableWithoutPk: ColumnMeta[] = [
    { name: "log_message", is_primary_key: false },
    { name: "created_at", is_primary_key: false },
  ];

  const source: SourceTable = { schema: "public", table: "users" };

  it("is editable when source table has PK and all PKs in result", () => {
    expect(computeIsUpdatable(source, tableWithPk, ["id", "name", "email"])).toBe(true);
  });

  it("is editable when result has PKs plus extra columns", () => {
    expect(computeIsUpdatable(source, tableWithPk, ["id", "name", "email", "extra"])).toBe(true);
  });

  it("is editable with subset of columns as long as PK is present", () => {
    expect(computeIsUpdatable(source, tableWithPk, ["id", "name"])).toBe(true);
  });

  it("is read-only when PK column is missing from result", () => {
    expect(computeIsUpdatable(source, tableWithPk, ["name", "email"])).toBe(false);
  });

  it("is read-only when no source table (complex query)", () => {
    expect(computeIsUpdatable(null, tableWithPk, ["id", "name"])).toBe(false);
  });

  it("is read-only when table has no primary key", () => {
    expect(computeIsUpdatable(source, tableWithoutPk, ["log_message", "created_at"])).toBe(false);
  });

  it("is read-only when table columns not loaded yet", () => {
    expect(computeIsUpdatable(source, null, ["id", "name"])).toBe(false);
  });

  it("handles composite PK — editable when all PK columns present", () => {
    expect(computeIsUpdatable(source, tableWithCompositePk, ["user_id", "role_id", "assigned_at"])).toBe(true);
  });

  it("handles composite PK — read-only when one PK column missing", () => {
    expect(computeIsUpdatable(source, tableWithCompositePk, ["user_id", "assigned_at"])).toBe(false);
  });

  it("handles composite PK — read-only when all PK columns missing", () => {
    expect(computeIsUpdatable(source, tableWithCompositePk, ["assigned_at"])).toBe(false);
  });

  it("handles empty result columns", () => {
    expect(computeIsUpdatable(source, tableWithPk, [])).toBe(false);
  });

  it("handles empty table columns", () => {
    expect(computeIsUpdatable(source, [], ["id"])).toBe(false);
  });
});

describe("ResultTable read-only reason", () => {
  const tableWithPk: ColumnMeta[] = [
    { name: "id", is_primary_key: true },
    { name: "name", is_primary_key: false },
  ];

  const tableWithCompositePk: ColumnMeta[] = [
    { name: "user_id", is_primary_key: true },
    { name: "role_id", is_primary_key: true },
  ];

  const source: SourceTable = { schema: "public", table: "users" };

  it("returns complex query reason when no source table", () => {
    expect(computeReadOnlyReason(null, tableWithPk, ["id"])).toContain("no single source table");
  });

  it("returns metadata not loaded reason", () => {
    expect(computeReadOnlyReason(source, null, ["id"])).toContain("metadata not loaded");
  });

  it("returns no primary key reason", () => {
    const noPk: ColumnMeta[] = [{ name: "x", is_primary_key: false }];
    expect(computeReadOnlyReason(source, noPk, ["x"])).toContain("no primary key");
  });

  it("returns missing PK column reason for single PK", () => {
    const reason = computeReadOnlyReason(source, tableWithPk, ["name"]);
    expect(reason).toContain("id");
    expect(reason).toContain("not in result");
  });

  it("returns missing PK columns reason for composite PK", () => {
    const reason = computeReadOnlyReason(source, tableWithCompositePk, ["name"]);
    expect(reason).toContain("user_id");
    expect(reason).toContain("role_id");
    expect(reason).toContain("not in result");
  });

  it("returns empty string when all conditions met (editable)", () => {
    expect(computeReadOnlyReason(source, tableWithPk, ["id", "name"])).toBe("");
  });
});

// --- Read-only mode behavior ---

function shouldShowEditButtons(isUpdatable: boolean): {
  showAddRow: boolean;
  showTestData: boolean;
  showDeleteSelected: boolean;
  showSaveToDb: boolean;
} {
  return {
    showAddRow: isUpdatable,
    showTestData: isUpdatable,
    showDeleteSelected: isUpdatable,
    showSaveToDb: isUpdatable,
  };
}

function shouldCellBeEditable(isUpdatable: boolean, colName: string, _pkColumns: string[]): boolean {
  if (!isUpdatable) return false;
  return true;
}

function shouldHandleKeyboardShortcut(
  isUpdatable: boolean,
  key: string,
  ctrlKey: boolean,
): boolean {
  if (!isUpdatable) return false;
  if ((key === "Delete" || key === "Backspace")) return true;
  if (ctrlKey && key.toLowerCase() === "a") return true;
  return false;
}

describe("ResultTable read-only mode — button visibility", () => {
  it("hides all edit buttons when read-only", () => {
    const result = shouldShowEditButtons(false);
    expect(result.showAddRow).toBe(false);
    expect(result.showTestData).toBe(false);
    expect(result.showDeleteSelected).toBe(false);
    expect(result.showSaveToDb).toBe(false);
  });

  it("shows all edit buttons when editable", () => {
    const result = shouldShowEditButtons(true);
    expect(result.showAddRow).toBe(true);
    expect(result.showTestData).toBe(true);
    expect(result.showDeleteSelected).toBe(true);
    expect(result.showSaveToDb).toBe(true);
  });
});

describe("ResultTable read-only mode — cell editability", () => {
  it("cells are not editable when read-only", () => {
    expect(shouldCellBeEditable(false, "name", ["id"])).toBe(false);
  });

  it("cells are editable when updatable", () => {
    expect(shouldCellBeEditable(true, "name", ["id"])).toBe(true);
  });

  it("PK cells are editable when updatable", () => {
    expect(shouldCellBeEditable(true, "id", ["id"])).toBe(true);
  });
});

describe("ResultTable read-only mode — keyboard shortcuts", () => {
  it("ignores Delete key when read-only", () => {
    expect(shouldHandleKeyboardShortcut(false, "Delete", false)).toBe(false);
  });

  it("ignores Backspace key when read-only", () => {
    expect(shouldHandleKeyboardShortcut(false, "Backspace", false)).toBe(false);
  });

  it("ignores Ctrl+A when read-only", () => {
    expect(shouldHandleKeyboardShortcut(false, "a", true)).toBe(false);
  });

  it("handles Delete key when editable", () => {
    expect(shouldHandleKeyboardShortcut(true, "Delete", false)).toBe(true);
  });

  it("handles Backspace key when editable", () => {
    expect(shouldHandleKeyboardShortcut(true, "Backspace", false)).toBe(true);
  });

  it("handles Ctrl+A when editable", () => {
    expect(shouldHandleKeyboardShortcut(true, "a", true)).toBe(true);
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
