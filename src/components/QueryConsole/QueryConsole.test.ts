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
