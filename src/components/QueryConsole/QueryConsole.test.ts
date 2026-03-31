import { describe, it, expect } from "vitest";

interface HistoryEntry {
  sql: string;
  timestamp: number;
  executionTimeMs: number;
  rowCount: number;
  error?: string;
}

function sortHistoryWithPins(
  history: HistoryEntry[],
  pinnedQueries: Set<number>
): { entry: HistoryEntry; idx: number }[] {
  const indexed = history.map((entry, idx) => ({ entry, idx }));
  return indexed.sort((a, b) => {
    const ap = pinnedQueries.has(a.idx) ? 0 : 1;
    const bp = pinnedQueries.has(b.idx) ? 0 : 1;
    return ap - bp || a.idx - b.idx;
  });
}

function togglePin(pinned: Set<number>, idx: number): Set<number> {
  const next = new Set(pinned);
  next.has(idx) ? next.delete(idx) : next.add(idx);
  return next;
}

const SQL_KEYWORDS = [
  "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "IS", "NULL",
  "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE", "CREATE",
  "TABLE", "ALTER", "DROP", "INDEX", "JOIN", "LEFT", "RIGHT", "INNER",
  "OUTER", "ON", "AS", "ORDER", "BY", "GROUP", "HAVING", "LIMIT",
  "OFFSET", "DISTINCT", "COUNT", "SUM", "AVG", "MIN", "MAX", "CASE",
  "WHEN", "THEN", "ELSE", "END", "EXISTS", "BETWEEN", "LIKE", "ILIKE",
  "UNION", "ALL", "WITH", "RETURNING", "CASCADE", "TRUNCATE", "EXPLAIN",
  "ANALYZE", "COALESCE", "CAST", "EXTRACT", "LATERAL", "CROSS", "FULL",
  "NATURAL", "USING", "EXCEPT", "INTERSECT", "FETCH", "FIRST", "NEXT",
  "ROWS", "ONLY", "ASC", "DESC", "NULLS", "LAST", "SCHEMA", "GRANT",
  "REVOKE", "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "UNIQUE",
  "CHECK", "DEFAULT", "CONSTRAINT", "BEGIN", "COMMIT", "ROLLBACK",
  "BOOLEAN", "INTEGER", "BIGINT", "SMALLINT", "TEXT", "VARCHAR",
  "TIMESTAMP", "DATE", "TIME", "NUMERIC", "DECIMAL", "SERIAL",
  "BIGSERIAL", "UUID", "JSONB", "JSON", "ARRAY", "INTERVAL",
];

function parseReferencedTables(sql: string): string[] {
  const tables = new Set<string>();
  for (const m of sql.matchAll(/\b(?:FROM|JOIN|UPDATE|INTO)\s+(\w+)/gi)) {
    tables.add(m[1]);
  }
  return [...tables];
}

function detectDotContext(textBefore: string): string | null {
  const match = textBefore.match(/(\w+)\.\w*$/);
  return match ? match[1] : null;
}

describe("QueryConsole history sorting with pins", () => {
  const entries: HistoryEntry[] = [
    { sql: "SELECT 1", timestamp: 1000, executionTimeMs: 10, rowCount: 1 },
    { sql: "SELECT 2", timestamp: 2000, executionTimeMs: 20, rowCount: 2 },
    { sql: "SELECT 3", timestamp: 3000, executionTimeMs: 30, rowCount: 3 },
    { sql: "SELECT 4", timestamp: 4000, executionTimeMs: 40, rowCount: 4 },
  ];

  it("returns entries in original order when nothing is pinned", () => {
    const sorted = sortHistoryWithPins(entries, new Set());
    expect(sorted.map((s) => s.idx)).toEqual([0, 1, 2, 3]);
  });

  it("moves pinned entries to the top", () => {
    const sorted = sortHistoryWithPins(entries, new Set([2]));
    expect(sorted[0].idx).toBe(2);
    expect(sorted[0].entry.sql).toBe("SELECT 3");
  });

  it("preserves order among pinned entries", () => {
    const sorted = sortHistoryWithPins(entries, new Set([1, 3]));
    expect(sorted[0].idx).toBe(1);
    expect(sorted[1].idx).toBe(3);
  });

  it("preserves order among unpinned entries", () => {
    const sorted = sortHistoryWithPins(entries, new Set([2]));
    const unpinned = sorted.filter((s) => s.idx !== 2);
    expect(unpinned.map((s) => s.idx)).toEqual([0, 1, 3]);
  });

  it("handles all entries pinned", () => {
    const sorted = sortHistoryWithPins(entries, new Set([0, 1, 2, 3]));
    expect(sorted.map((s) => s.idx)).toEqual([0, 1, 2, 3]);
  });

  it("handles empty history", () => {
    const sorted = sortHistoryWithPins([], new Set());
    expect(sorted).toEqual([]);
  });

  it("handles single entry pinned", () => {
    const single = [entries[0]];
    const sorted = sortHistoryWithPins(single, new Set([0]));
    expect(sorted).toHaveLength(1);
    expect(sorted[0].idx).toBe(0);
  });
});

describe("QueryConsole pin toggling", () => {
  it("adds a pin", () => {
    const result = togglePin(new Set(), 2);
    expect(result.has(2)).toBe(true);
    expect(result.size).toBe(1);
  });

  it("removes a pin", () => {
    const result = togglePin(new Set([2]), 2);
    expect(result.has(2)).toBe(false);
    expect(result.size).toBe(0);
  });

  it("preserves other pins when adding", () => {
    const result = togglePin(new Set([1, 3]), 2);
    expect(result.has(1)).toBe(true);
    expect(result.has(2)).toBe(true);
    expect(result.has(3)).toBe(true);
  });

  it("preserves other pins when removing", () => {
    const result = togglePin(new Set([1, 2, 3]), 2);
    expect(result.has(1)).toBe(true);
    expect(result.has(2)).toBe(false);
    expect(result.has(3)).toBe(true);
  });
});

describe("SQL keywords list", () => {
  it("contains essential keywords", () => {
    const essentials = [
      "SELECT", "FROM", "WHERE", "INSERT", "UPDATE", "DELETE",
      "JOIN", "ORDER", "GROUP", "HAVING", "LIMIT", "CREATE", "ALTER", "DROP",
    ];
    for (const kw of essentials) {
      expect(SQL_KEYWORDS).toContain(kw);
    }
  });

  it("has no duplicates", () => {
    expect(new Set(SQL_KEYWORDS).size).toBe(SQL_KEYWORDS.length);
  });

  it("all keywords are uppercase", () => {
    for (const kw of SQL_KEYWORDS) {
      expect(kw).toBe(kw.toUpperCase());
    }
  });

  it("contains PostgreSQL-specific keywords", () => {
    expect(SQL_KEYWORDS).toContain("ILIKE");
    expect(SQL_KEYWORDS).toContain("RETURNING");
    expect(SQL_KEYWORDS).toContain("JSONB");
    expect(SQL_KEYWORDS).toContain("SERIAL");
    expect(SQL_KEYWORDS).toContain("BIGSERIAL");
  });

  it("contains data type keywords", () => {
    const types = ["BOOLEAN", "INTEGER", "BIGINT", "TEXT", "VARCHAR", "TIMESTAMP", "UUID", "JSON", "JSONB"];
    for (const t of types) {
      expect(SQL_KEYWORDS).toContain(t);
    }
  });
});

describe("SQL autocomplete context parsing", () => {
  it("extracts table from FROM clause", () => {
    expect(parseReferencedTables("SELECT * FROM users")).toEqual(["users"]);
  });

  it("extracts table from JOIN clause", () => {
    expect(parseReferencedTables("SELECT * FROM users JOIN orders ON users.id = orders.user_id")).toEqual(["users", "orders"]);
  });

  it("extracts table from UPDATE clause", () => {
    expect(parseReferencedTables("UPDATE users SET name = 'test'")).toEqual(["users"]);
  });

  it("extracts table from INSERT INTO clause", () => {
    expect(parseReferencedTables("INSERT INTO users VALUES (1, 'test')")).toEqual(["users"]);
  });

  it("extracts multiple tables from complex query", () => {
    const sql = "SELECT * FROM users JOIN orders ON users.id = orders.uid JOIN products ON orders.pid = products.id";
    const tables = parseReferencedTables(sql);
    expect(tables).toContain("users");
    expect(tables).toContain("orders");
    expect(tables).toContain("products");
  });

  it("returns empty for no tables", () => {
    expect(parseReferencedTables("SELECT 1")).toEqual([]);
  });

  it("is case insensitive", () => {
    expect(parseReferencedTables("select * from Users")).toEqual(["Users"]);
  });

  it("deduplicates tables", () => {
    const tables = parseReferencedTables("SELECT * FROM users JOIN users ON users.id = users.id");
    expect(tables).toEqual(["users"]);
  });
});

// --- Query history deduplication logic ---

function addToHistory(
  history: HistoryEntry[],
  sql: string,
  executionTimeMs: number,
  rowCount: number,
  error?: string,
): HistoryEntry[] {
  const trimmed = sql.trim();
  const filtered = history.filter((h) => h.sql.trim() !== trimmed);
  const newEntry: HistoryEntry = {
    sql,
    timestamp: Date.now(),
    executionTimeMs,
    rowCount,
    error,
  };
  return [newEntry, ...filtered.slice(0, 99)];
}

describe("QueryConsole history deduplication", () => {
  it("adds new query to empty history", () => {
    const result = addToHistory([], "SELECT 1", 10, 1);
    expect(result).toHaveLength(1);
    expect(result[0].sql).toBe("SELECT 1");
  });

  it("adds different query to existing history", () => {
    const existing: HistoryEntry[] = [
      { sql: "SELECT 1", timestamp: 1000, executionTimeMs: 10, rowCount: 1 },
    ];
    const result = addToHistory(existing, "SELECT 2", 20, 2);
    expect(result).toHaveLength(2);
    expect(result[0].sql).toBe("SELECT 2");
    expect(result[1].sql).toBe("SELECT 1");
  });

  it("deduplicates same query — moves to top with updated stats", () => {
    const existing: HistoryEntry[] = [
      { sql: "SELECT 1", timestamp: 1000, executionTimeMs: 10, rowCount: 1 },
      { sql: "SELECT 2", timestamp: 2000, executionTimeMs: 20, rowCount: 2 },
    ];
    const result = addToHistory(existing, "SELECT 1", 50, 5);
    expect(result).toHaveLength(2);
    expect(result[0].sql).toBe("SELECT 1");
    expect(result[0].executionTimeMs).toBe(50);
    expect(result[0].rowCount).toBe(5);
    expect(result[1].sql).toBe("SELECT 2");
  });

  it("deduplicates with whitespace differences", () => {
    const existing: HistoryEntry[] = [
      { sql: "  SELECT 1  ", timestamp: 1000, executionTimeMs: 10, rowCount: 1 },
    ];
    const result = addToHistory(existing, "SELECT 1", 20, 1);
    expect(result).toHaveLength(1);
    expect(result[0].sql).toBe("SELECT 1");
  });

  it("does not deduplicate case-different queries", () => {
    const existing: HistoryEntry[] = [
      { sql: "SELECT 1", timestamp: 1000, executionTimeMs: 10, rowCount: 1 },
    ];
    const result = addToHistory(existing, "select 1", 20, 1);
    expect(result).toHaveLength(2);
  });

  it("caps history at 100 entries", () => {
    const existing: HistoryEntry[] = Array.from({ length: 105 }, (_, i) => ({
      sql: `SELECT ${i}`,
      timestamp: i * 1000,
      executionTimeMs: 10,
      rowCount: 1,
    }));
    const result = addToHistory(existing, "SELECT new", 5, 1);
    expect(result).toHaveLength(100);
    expect(result[0].sql).toBe("SELECT new");
  });

  it("preserves error entries from different queries", () => {
    const existing: HistoryEntry[] = [
      { sql: "SELECT bad", timestamp: 1000, executionTimeMs: 0, rowCount: 0, error: "syntax error" },
    ];
    const result = addToHistory(existing, "SELECT good", 10, 1);
    expect(result).toHaveLength(2);
    expect(result[1].error).toBe("syntax error");
  });

  it("replaces error entry when same query succeeds", () => {
    const existing: HistoryEntry[] = [
      { sql: "SELECT * FROM users", timestamp: 1000, executionTimeMs: 0, rowCount: 0, error: "table not found" },
    ];
    const result = addToHistory(existing, "SELECT * FROM users", 30, 10);
    expect(result).toHaveLength(1);
    expect(result[0].error).toBeUndefined();
    expect(result[0].rowCount).toBe(10);
  });

  it("replaces success entry when same query fails", () => {
    const existing: HistoryEntry[] = [
      { sql: "SELECT * FROM users", timestamp: 1000, executionTimeMs: 30, rowCount: 10 },
    ];
    const result = addToHistory(existing, "SELECT * FROM users", 0, 0, "connection lost");
    expect(result).toHaveLength(1);
    expect(result[0].error).toBe("connection lost");
  });

  it("handles rapid re-execution without duplicating", () => {
    let history: HistoryEntry[] = [];
    for (let i = 0; i < 10; i++) {
      history = addToHistory(history, "SELECT * FROM orders", i * 5, i);
    }
    expect(history).toHaveLength(1);
    expect(history[0].rowCount).toBe(9);
  });

  it("maintains order: newest first", () => {
    let history: HistoryEntry[] = [];
    history = addToHistory(history, "SELECT 1", 10, 1);
    history = addToHistory(history, "SELECT 2", 20, 2);
    history = addToHistory(history, "SELECT 3", 30, 3);
    expect(history[0].sql).toBe("SELECT 3");
    expect(history[1].sql).toBe("SELECT 2");
    expect(history[2].sql).toBe("SELECT 1");
  });
});

describe("SQL autocomplete dot context detection", () => {
  it("detects table name before dot", () => {
    expect(detectDotContext("SELECT users.")).toBe("users");
  });

  it("detects table name with partial column", () => {
    expect(detectDotContext("SELECT users.na")).toBe("users");
  });

  it("returns null when no dot", () => {
    expect(detectDotContext("SELECT ")).toBeNull();
  });

  it("returns null for empty string", () => {
    expect(detectDotContext("")).toBeNull();
  });

  it("handles schema-qualified names", () => {
    expect(detectDotContext("SELECT t.")).toBe("t");
  });

  it("detects in WHERE clause", () => {
    expect(detectDotContext("WHERE users.")).toBe("users");
  });
});

// ---------------------------------------------------------------------------
// SQL validation marker positioning helpers
// ---------------------------------------------------------------------------

interface ValidationError {
  message: string;
  position: number | null;
}

function computeLineCol(
  sql: string,
  byteOffset: number
): { line: number; col: number } {
  const text = sql.slice(0, byteOffset);
  const lines = text.split("\n");
  return { line: lines.length, col: lines[lines.length - 1].length + 1 };
}

function computeMarkerRange(
  sql: string,
  error: ValidationError
): { startLine: number; startCol: number; endLine: number; endCol: number } {
  if (error.position != null && error.position > 0) {
    const offset = error.position - 1;
    const { line, col } = computeLineCol(sql, offset);
    const currentLineStart = sql.lastIndexOf("\n", offset - 1) + 1;
    const currentLineEnd = sql.indexOf("\n", offset);
    const lineText = sql.slice(
      currentLineStart,
      currentLineEnd === -1 ? sql.length : currentLineEnd
    );
    const wordMatch = lineText.slice(col - 1).match(/^\w+/);
    const endCol = wordMatch ? col + wordMatch[0].length : col + 1;
    return { startLine: line, startCol: col, endLine: line, endCol };
  }
  const lines = sql.split("\n");
  return {
    startLine: 1,
    startCol: 1,
    endLine: lines.length,
    endCol: lines[lines.length - 1].length + 1,
  };
}

function shouldSkipValidation(sql: string, executing: boolean): boolean {
  if (executing) return true;
  if (!sql.trim()) return true;
  return false;
}

describe("SQL validation marker positioning", () => {
  it("position at start of single-line query", () => {
    const result = computeMarkerRange("SELCT 1", { message: "syntax error", position: 1 });
    expect(result.startLine).toBe(1);
    expect(result.startCol).toBe(1);
    expect(result.endCol).toBe(6);
  });

  it("position in middle of single-line query", () => {
    const result = computeMarkerRange("SELECT * FORM users", { message: "syntax error", position: 10 });
    expect(result.startLine).toBe(1);
    expect(result.startCol).toBe(10);
    expect(result.endCol).toBe(14);
  });

  it("position on second line of multi-line query", () => {
    const sql = "SELECT *\nFORM users";
    const result = computeMarkerRange(sql, { message: "syntax error", position: 10 });
    expect(result.startLine).toBe(2);
    expect(result.startCol).toBe(1);
    expect(result.endCol).toBe(5);
  });

  it("no position underlines entire query", () => {
    const sql = "SELECT * FROM bad_table";
    const result = computeMarkerRange(sql, { message: "table not found", position: null });
    expect(result.startLine).toBe(1);
    expect(result.startCol).toBe(1);
    expect(result.endLine).toBe(1);
    expect(result.endCol).toBe(sql.length + 1);
  });

  it("no position on multi-line query spans all lines", () => {
    const sql = "SELECT *\nFROM users\nWHERE id = 1";
    const result = computeMarkerRange(sql, { message: "error", position: null });
    expect(result.startLine).toBe(1);
    expect(result.endLine).toBe(3);
  });

  it("position at end of line", () => {
    const sql = "SELECT 1 +";
    const result = computeMarkerRange(sql, { message: "expected expression", position: 11 });
    expect(result.startLine).toBe(1);
    expect(result.startCol).toBe(11);
  });

  it("position zero treated as no position", () => {
    const sql = "bad query";
    const result = computeMarkerRange(sql, { message: "error", position: 0 });
    expect(result.startLine).toBe(1);
    expect(result.startCol).toBe(1);
    expect(result.endLine).toBe(1);
  });

  it("handles position beyond string length gracefully", () => {
    const sql = "SELECT 1";
    const result = computeMarkerRange(sql, { message: "error", position: 100 });
    expect(result.startLine).toBeGreaterThanOrEqual(1);
  });
});

describe("SQL validation skip logic", () => {
  it("skips when executing", () => {
    expect(shouldSkipValidation("SELECT 1", true)).toBe(true);
  });

  it("skips for empty SQL", () => {
    expect(shouldSkipValidation("", false)).toBe(true);
  });

  it("skips for whitespace-only SQL", () => {
    expect(shouldSkipValidation("   \n  ", false)).toBe(true);
  });

  it("does not skip for valid SQL when not executing", () => {
    expect(shouldSkipValidation("SELECT 1", false)).toBe(false);
  });

  it("does not skip for partial SQL", () => {
    expect(shouldSkipValidation("SEL", false)).toBe(false);
  });
});

describe("SQL validation line/column computation", () => {
  it("single line offset 0", () => {
    expect(computeLineCol("SELECT 1", 0)).toEqual({ line: 1, col: 1 });
  });

  it("single line offset 7", () => {
    expect(computeLineCol("SELECT 1", 7)).toEqual({ line: 1, col: 8 });
  });

  it("second line start", () => {
    expect(computeLineCol("SELECT\n1", 7)).toEqual({ line: 2, col: 1 });
  });

  it("second line middle", () => {
    expect(computeLineCol("SELECT\nFROM users", 12)).toEqual({ line: 2, col: 6 });
  });

  it("third line", () => {
    expect(computeLineCol("A\nB\nC", 4)).toEqual({ line: 3, col: 1 });
  });

  it("empty string", () => {
    expect(computeLineCol("", 0)).toEqual({ line: 1, col: 1 });
  });
});
