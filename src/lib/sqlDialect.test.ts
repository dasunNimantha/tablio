import { describe, it, expect } from "vitest";
import {
  boolLiteral,
  caseInsensitiveLike,
  paginationClause,
  quoteIdent,
  quoteQualified,
  supportsIlike,
  type DbType,
} from "./sqlDialect";

describe("quoteIdent", () => {
  it('uses double quotes for postgres / cockroachdb / sqlite / cassandra', () => {
    for (const t of ["postgres", "cockroachdb", "sqlite", "cassandra"] as DbType[]) {
      expect(quoteIdent(t, "users")).toBe('"users"');
    }
  });

  it("uses backticks for mysql / mariadb / tidb", () => {
    for (const t of ["mysql", "mariadb", "tidb"] as DbType[]) {
      expect(quoteIdent(t, "users")).toBe("`users`");
    }
  });

  it("uses square brackets for mssql", () => {
    expect(quoteIdent("mssql", "users")).toBe("[users]");
  });

  it("falls back to double quotes when dbType is undefined", () => {
    expect(quoteIdent(undefined, "users")).toBe('"users"');
  });

  it("doubles the delimiter inside the identifier per dialect", () => {
    // Postgres-style: " is doubled.
    expect(quoteIdent("postgres", 'col"name')).toBe('"col""name"');
    // MySQL-style: ` is doubled.
    expect(quoteIdent("mysql", "col`name")).toBe("`col``name`");
    // MSSQL-style: ] is doubled, [ is left alone (only ] terminates).
    expect(quoteIdent("mssql", "col]name")).toBe("[col]]name]");
    expect(quoteIdent("mssql", "col[name")).toBe("[col[name]");
  });

  it("handles empty identifier (still quotes it)", () => {
    expect(quoteIdent("postgres", "")).toBe('""');
    expect(quoteIdent("mysql", "")).toBe("``");
    expect(quoteIdent("mssql", "")).toBe("[]");
  });

  it("handles identifier containing only the delimiter character", () => {
    // PG: `"` doubled → `""`, wrapped → `""""` (4 chars).
    expect(quoteIdent("postgres", '"')).toBe('""""');
    // MySQL: ` doubled → ``, wrapped → ```` (4 chars).
    expect(quoteIdent("mysql", "`")).toBe("````");
    // MSSQL: ] doubled → ]], wrapped → []]] (4 chars).
    expect(quoteIdent("mssql", "]")).toBe("[]]]");
  });
});

describe("quoteQualified", () => {
  it("emits schema.table for dialects with schemas", () => {
    expect(quoteQualified("postgres", "public", "users")).toBe('"public"."users"');
    expect(quoteQualified("mysql", "app", "users")).toBe("`app`.`users`");
    expect(quoteQualified("mssql", "dbo", "users")).toBe("[dbo].[users]");
  });

  it("drops schema for sqlite (no schema concept)", () => {
    expect(quoteQualified("sqlite", "main", "users")).toBe('"users"');
  });

  it("drops schema when schema is empty / null / undefined", () => {
    expect(quoteQualified("postgres", "", "users")).toBe('"users"');
    expect(quoteQualified("postgres", null, "users")).toBe('"users"');
    expect(quoteQualified("postgres", undefined, "users")).toBe('"users"');
  });

  it("escapes delimiters in both schema and table parts", () => {
    expect(quoteQualified("postgres", 'wei"rd', "tab")).toBe('"wei""rd"."tab"');
    expect(quoteQualified("mysql", "we`ird", "tab")).toBe("`we``ird`.`tab`");
  });
});

describe("supportsIlike", () => {
  it("returns true only for postgres-family dialects", () => {
    expect(supportsIlike("postgres")).toBe(true);
    expect(supportsIlike("cockroachdb")).toBe(true);
    expect(supportsIlike("mysql")).toBe(false);
    expect(supportsIlike("mariadb")).toBe(false);
    expect(supportsIlike("tidb")).toBe(false);
    expect(supportsIlike("sqlite")).toBe(false);
    expect(supportsIlike("mssql")).toBe(false);
    expect(supportsIlike("cassandra")).toBe(false);
    expect(supportsIlike(undefined)).toBe(false);
  });
});

describe("caseInsensitiveLike", () => {
  it("uses native ILIKE on PG / CRDB", () => {
    expect(caseInsensitiveLike("postgres", '"name"', "'%alice%'")).toBe(
      `"name" ILIKE '%alice%'`,
    );
    expect(caseInsensitiveLike("cockroachdb", '"name"', "'%alice%'")).toBe(
      `"name" ILIKE '%alice%'`,
    );
  });

  it("falls back to LOWER(...) LIKE LOWER(...) on engines without ILIKE", () => {
    expect(caseInsensitiveLike("mysql", "`name`", "'%alice%'")).toBe(
      "LOWER(`name`) LIKE LOWER('%alice%')",
    );
    expect(caseInsensitiveLike("mssql", "[name]", "'%alice%'")).toBe(
      "LOWER([name]) LIKE LOWER('%alice%')",
    );
    expect(caseInsensitiveLike("sqlite", '"name"', "'%alice%'")).toBe(
      `LOWER("name") LIKE LOWER('%alice%')`,
    );
  });
});

describe("paginationClause", () => {
  it("emits LIMIT / OFFSET on PG / MySQL / SQLite / CRDB / Cassandra", () => {
    for (const t of ["postgres", "mysql", "mariadb", "tidb", "sqlite", "cockroachdb", "cassandra"] as DbType[]) {
      expect(paginationClause(t, 50, 100)).toBe("LIMIT 50 OFFSET 100");
    }
  });

  it("emits OFFSET ... ROWS FETCH NEXT ... ROWS ONLY on MSSQL", () => {
    expect(paginationClause("mssql", 50, 100)).toBe(
      "OFFSET 100 ROWS FETCH NEXT 50 ROWS ONLY",
    );
  });

  it("clamps negative values to zero", () => {
    expect(paginationClause("postgres", -5, -10)).toBe("LIMIT 0 OFFSET 0");
    expect(paginationClause("mssql", -5, -10)).toBe(
      "OFFSET 0 ROWS FETCH NEXT 0 ROWS ONLY",
    );
  });

  it("floors fractional values to integers", () => {
    expect(paginationClause("postgres", 50.7, 100.3)).toBe("LIMIT 50 OFFSET 100");
  });

  it("treats NaN / non-finite inputs as zero", () => {
    expect(paginationClause("postgres", NaN, NaN)).toBe("LIMIT 0 OFFSET 0");
    expect(paginationClause("postgres", Infinity, -Infinity)).toBe(
      "LIMIT 0 OFFSET 0",
    );
  });
});

describe("boolLiteral", () => {
  it("uses TRUE / FALSE for most dialects", () => {
    for (const t of ["postgres", "cockroachdb", "sqlite", "mssql"] as DbType[]) {
      expect(boolLiteral(t, true)).toBe("TRUE");
      expect(boolLiteral(t, false)).toBe("FALSE");
    }
  });

  it("uses 1 / 0 for MySQL family", () => {
    for (const t of ["mysql", "mariadb", "tidb"] as DbType[]) {
      expect(boolLiteral(t, true)).toBe("1");
      expect(boolLiteral(t, false)).toBe("0");
    }
  });

  it("uses lowercase true / false for cassandra", () => {
    expect(boolLiteral("cassandra", true)).toBe("true");
    expect(boolLiteral("cassandra", false)).toBe("false");
  });
});
