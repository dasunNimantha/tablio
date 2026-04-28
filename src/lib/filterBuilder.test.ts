import { describe, it, expect } from "vitest";
import { buildWhereClause, type FilterCondition } from "./filterBuilder";
import type { ColumnInfo } from "./tauri";

const col = (name: string, dataType: string, isPk = false): ColumnInfo => ({
  name,
  data_type: dataType,
  is_nullable: true,
  is_primary_key: isPk,
  default_value: null,
  ordinal_position: 0,
  is_auto_generated: false,
});

const cond = (
  overrides: Partial<FilterCondition> & { column: string }
): FilterCondition => ({
  id: "1",
  column: overrides.column,
  operator: overrides.operator ?? "=",
  value: overrides.value ?? "",
  join: overrides.join ?? "AND",
});

describe("buildWhereClause", () => {
  const columns: ColumnInfo[] = [
    col("id", "integer", true),
    col("name", "character varying"),
    col("amount", "numeric"),
  ];

  it("returns null when no conditions", () => {
    expect(buildWhereClause([], columns)).toBeNull();
  });

  it("returns null when condition has no column", () => {
    expect(
      buildWhereClause([cond({ column: "", value: "x" })], columns)
    ).toBeNull();
  });

  it("returns null when value operator but empty value", () => {
    expect(
      buildWhereClause([cond({ column: "name", operator: "=", value: "" })], columns)
    ).toBeNull();
  });

  it("builds single equality with string literal", () => {
    expect(
      buildWhereClause(
        [cond({ column: "name", operator: "=", value: "Alice" })],
        columns
      )
    ).toBe(`"name" = 'Alice'`);
  });

  it("escapes single quotes in string value", () => {
    expect(
      buildWhereClause(
        [cond({ column: "name", operator: "=", value: "O'Brien" })],
        columns
      )
    ).toBe(`"name" = 'O''Brien'`);
  });

  it("escapes double quotes in column name", () => {
    expect(
      buildWhereClause(
        [cond({ column: 'col"umn', operator: "=", value: "x" })],
        columns
      )
    ).toBe(`"col""umn" = 'x'`);
  });

  it("builds numeric comparison without quotes", () => {
    expect(
      buildWhereClause(
        [cond({ column: "id", operator: ">", value: "10" })],
        columns
      )
    ).toBe(`"id" > 10`);
  });

  it("builds numeric for numeric data_type", () => {
    expect(
      buildWhereClause(
        [cond({ column: "amount", operator: ">=", value: "100.5" })],
        columns
      )
    ).toBe(`"amount" >= 100.5`);
  });

  it("builds string literal when numeric column has non-numeric value", () => {
    expect(
      buildWhereClause(
        [cond({ column: "id", operator: "=", value: "abc" })],
        columns
      )
    ).toBe(`"id" = 'abc'`);
  });

  it("builds IS NULL", () => {
    expect(
      buildWhereClause(
        [cond({ column: "name", operator: "IS NULL", value: "" })],
        columns
      )
    ).toBe(`"name" IS NULL`);
  });

  it("builds IS NOT NULL", () => {
    expect(
      buildWhereClause(
        [cond({ column: "name", operator: "IS NOT NULL", value: "" })],
        columns
      )
    ).toBe(`"name" IS NOT NULL`);
  });

  it("builds LIKE with escaped quotes", () => {
    expect(
      buildWhereClause(
        [cond({ column: "name", operator: "LIKE", value: "%'%" })],
        columns
      )
    ).toBe(`"name" LIKE '%''%'`);
  });

  it("builds two conditions with AND", () => {
    expect(
      buildWhereClause(
        [
          cond({ column: "id", operator: "=", value: "1", join: "AND" }),
          cond({ column: "name", operator: "=", value: "a", join: "AND" }),
        ],
        columns
      )
    ).toBe(`"id" = 1 AND "name" = 'a'`);
  });

  it("builds two conditions with OR", () => {
    expect(
      buildWhereClause(
        [
          cond({ column: "id", operator: "=", value: "1", join: "OR" }),
          cond({ column: "id", operator: "=", value: "2", join: "OR" }),
        ],
        columns
      )
    ).toBe(`"id" = 1 OR "id" = 2`);
  });

  it("groups mixed AND then OR with parentheses", () => {
    const result = buildWhereClause(
      [
        cond({ column: "id", operator: "=", value: "1", join: "AND" }),
        cond({ column: "name", operator: "=", value: "a", join: "AND" }),
        cond({ column: "amount", operator: ">", value: "0", join: "OR" }),
      ],
      columns
    );
    expect(result).toBe(`("id" = 1 AND "name" = 'a') OR "amount" > 0`);
  });

  it("groups OR then AND with parentheses", () => {
    // valid[1].join = OR connects first two; valid[2].join = AND connects second group
    const result = buildWhereClause(
      [
        cond({ column: "id", operator: "=", value: "1", join: "OR" }),
        cond({ column: "id", operator: "=", value: "2", join: "OR" }),
        cond({ column: "name", operator: "=", value: "x", join: "AND" }),
      ],
      columns
    );
    expect(result).toBe(`("id" = 1 OR "id" = 2) AND "name" = 'x'`);
  });

  it("filters out invalid conditions", () => {
    expect(
      buildWhereClause(
        [
          cond({ column: "name", operator: "=", value: "" }),
          cond({ column: "id", operator: "=", value: "1" }),
        ],
        columns
      )
    ).toBe(`"id" = 1`);
  });

  describe("value validation edge cases", () => {
    it("treats whitespace-only value as empty (numeric column)", () => {
      // Repro: typing 4 spaces into a numeric column previously slipped past
      // the truthiness check and emitted `"id" =     ` (no right operand)
      // -- a guaranteed DB syntax error. Now it's rejected at validation
      // time so no SQL is emitted at all.
      expect(
        buildWhereClause(
          [cond({ column: "id", operator: "=", value: "    " })],
          columns
        )
      ).toBeNull();
    });

    it("treats whitespace-only value as empty (string column)", () => {
      // Same bug class as above for non-numeric columns: previously emitted
      // `"name" = '   '` which silently matched whitespace-padded strings,
      // almost certainly not what the user wanted.
      expect(
        buildWhereClause(
          [cond({ column: "name", operator: "=", value: "   " })],
          columns
        )
      ).toBeNull();
    });

    it("treats whitespace-only value as empty (LIKE)", () => {
      expect(
        buildWhereClause(
          [cond({ column: "name", operator: "LIKE", value: "  " })],
          columns
        )
      ).toBeNull();
    });

    it("trims surrounding whitespace from the value", () => {
      // After trim the value is still meaningful, so the condition stays.
      // We embed the trimmed value (not the raw string with padding) so
      // the SQL is canonical.
      expect(
        buildWhereClause(
          [cond({ column: "name", operator: "=", value: "  Alice  " })],
          columns
        )
      ).toBe(`"name" = 'Alice'`);
    });

    it("trims surrounding whitespace from numeric value", () => {
      expect(
        buildWhereClause(
          [cond({ column: "id", operator: "=", value: " 42 " })],
          columns
        )
      ).toBe(`"id" = 42`);
    });

    it("rejects Infinity on numeric column (falls through to quoted)", () => {
      // `Number("Infinity")` is not NaN, so the previous `!isNaN(...)` check
      // accepted it and shipped `"id" = Infinity` -- works on Postgres
      // float columns, errors on integer columns and on most other engines.
      // Now it falls through to the quoted-string branch so the database
      // produces a typed error if the value really is meaningless for the
      // column.
      expect(
        buildWhereClause(
          [cond({ column: "id", operator: "=", value: "Infinity" })],
          columns
        )
      ).toBe(`"id" = 'Infinity'`);
    });

    it("rejects -Infinity on numeric column", () => {
      expect(
        buildWhereClause(
          [cond({ column: "id", operator: "=", value: "-Infinity" })],
          columns
        )
      ).toBe(`"id" = '-Infinity'`);
    });

    it("rejects NaN on numeric column", () => {
      expect(
        buildWhereClause(
          [cond({ column: "id", operator: "=", value: "NaN" })],
          columns
        )
      ).toBe(`"id" = 'NaN'`);
    });

    it("rejects hex literal on numeric column", () => {
      // MySQL parses `0x10` as 16, Postgres rejects it outright, MSSQL
      // treats `0x10` as a varbinary literal. Embedding the user's raw
      // string causes silent dialect divergence; quoting it instead lets
      // the database surface a single, obvious error.
      expect(
        buildWhereClause(
          [cond({ column: "id", operator: "=", value: "0x10" })],
          columns
        )
      ).toBe(`"id" = '0x10'`);
    });

    it("rejects octal-style literal on numeric column", () => {
      expect(
        buildWhereClause(
          [cond({ column: "id", operator: "=", value: "0o17" })],
          columns
        )
      ).toBe(`"id" = '0o17'`);
    });

    it("rejects binary-style literal on numeric column", () => {
      expect(
        buildWhereClause(
          [cond({ column: "id", operator: "=", value: "0b101" })],
          columns
        )
      ).toBe(`"id" = '0b101'`);
    });

    it("accepts negative numeric value", () => {
      expect(
        buildWhereClause(
          [cond({ column: "id", operator: ">", value: "-5" })],
          columns
        )
      ).toBe(`"id" > -5`);
    });

    it("accepts decimal numeric value", () => {
      expect(
        buildWhereClause(
          [cond({ column: "amount", operator: "=", value: "3.14" })],
          columns
        )
      ).toBe(`"amount" = 3.14`);
    });

    it("accepts scientific-notation numeric value", () => {
      expect(
        buildWhereClause(
          [cond({ column: "amount", operator: "<", value: "1.5e10" })],
          columns
        )
      ).toBe(`"amount" < 1.5e10`);
    });

    it("rejects values with extra characters that Number() would still parse", () => {
      // `Number("5,000")` is NaN so this would have been quoted already,
      // but pinning it here to lock the contract.
      expect(
        buildWhereClause(
          [cond({ column: "amount", operator: "=", value: "5,000" })],
          columns
        )
      ).toBe(`"amount" = '5,000'`);
    });

    it("preserves NUMERIC precision by not round-tripping through Number()", () => {
      // 17-digit decimal -- if we ever switched to embedding
      // `String(Number(value))` instead of the trimmed user input, this
      // would lose the trailing digit to float64 imprecision. The current
      // implementation embeds the trimmed string verbatim once it matches
      // the decimal-literal pattern, so exact precision is preserved for
      // NUMERIC / DECIMAL columns.
      expect(
        buildWhereClause(
          [cond({ column: "amount", operator: "=", value: "12345678901234567.89" })],
          columns
        )
      ).toBe(`"amount" = 12345678901234567.89`);
    });

    it("does not number-coerce when the value has internal whitespace", () => {
      expect(
        buildWhereClause(
          [cond({ column: "id", operator: "=", value: "1 OR 1" })],
          columns
        )
      ).toBe(`"id" = '1 OR 1'`);
    });

    it("escapes single quotes after trimming", () => {
      expect(
        buildWhereClause(
          [cond({ column: "name", operator: "=", value: "  O'Brien  " })],
          columns
        )
      ).toBe(`"name" = 'O''Brien'`);
    });
  });

  describe("dialect-aware identifier quoting", () => {
    it("defaults to double quotes when dbType is omitted", () => {
      expect(
        buildWhereClause(
          [cond({ column: "name", operator: "=", value: "x" })],
          columns
        )
      ).toBe(`"name" = 'x'`);
    });

    it("uses backticks for mysql/mariadb/tidb", () => {
      for (const t of ["mysql", "mariadb", "tidb"] as const) {
        expect(
          buildWhereClause(
            [cond({ column: "name", operator: "=", value: "x" })],
            columns,
            t
          )
        ).toBe("`name` = 'x'");
      }
    });

    it("uses square brackets for mssql", () => {
      expect(
        buildWhereClause(
          [cond({ column: "name", operator: "=", value: "x" })],
          columns,
          "mssql"
        )
      ).toBe(`[name] = 'x'`);
    });

    it("escapes the dialect delimiter inside the column name", () => {
      // Repro: previously hardcoded `"` doubling, so a column literally
      // named `col"evil` got quoted correctly on PG but produced
      // backtick-mismatched garbage on MySQL.
      expect(
        buildWhereClause(
          [cond({ column: "col`evil", operator: "=", value: "x" })],
          [col("col`evil", "text")],
          "mysql"
        )
      ).toBe("`col``evil` = 'x'");
    });
  });

  describe("dialect-aware ILIKE rewrite", () => {
    it("emits native ILIKE on postgres / cockroachdb", () => {
      for (const t of ["postgres", "cockroachdb"] as const) {
        expect(
          buildWhereClause(
            [cond({ column: "name", operator: "ILIKE", value: "%alice%" })],
            columns,
            t
          )
        ).toBe(`"name" ILIKE '%alice%'`);
      }
    });

    it("rewrites ILIKE to LOWER(...) LIKE LOWER(...) on engines without ILIKE", () => {
      // Repro: previously the FilterBar offered ILIKE for every
      // connection but emitted the literal `col ILIKE 'x'` against
      // every dialect, which errors on MySQL/MSSQL/SQLite/Cassandra.
      // Now the operator stays meaningful: case-insensitive intent is
      // honored via a portable rewrite.
      expect(
        buildWhereClause(
          [cond({ column: "name", operator: "ILIKE", value: "%alice%" })],
          columns,
          "mysql"
        )
      ).toBe("LOWER(`name`) LIKE LOWER('%alice%')");

      expect(
        buildWhereClause(
          [cond({ column: "name", operator: "ILIKE", value: "%alice%" })],
          columns,
          "mssql"
        )
      ).toBe(`LOWER([name]) LIKE LOWER('%alice%')`);

      expect(
        buildWhereClause(
          [cond({ column: "name", operator: "ILIKE", value: "%alice%" })],
          columns,
          "sqlite"
        )
      ).toBe(`LOWER("name") LIKE LOWER('%alice%')`);

      expect(
        buildWhereClause(
          [cond({ column: "name", operator: "ILIKE", value: "%alice%" })],
          columns,
          "cassandra"
        )
      ).toBe(`LOWER("name") LIKE LOWER('%alice%')`);
    });

    it("LIKE stays untouched on every dialect (no rewrite)", () => {
      // LIKE is case-sensitive by spec everywhere; we only rewrite
      // ILIKE. Pin that we don't accidentally LOWER() LIKE wraps a
      // plain LIKE comparison.
      for (const t of ["postgres", "mysql", "mssql", "sqlite", "cassandra"] as const) {
        const out = buildWhereClause(
          [cond({ column: "name", operator: "LIKE", value: "%alice%" })],
          columns,
          t
        );
        expect(out).toContain("LIKE '%alice%'");
        expect(out).not.toContain("LOWER(");
      }
    });

    it("escapes single quotes inside ILIKE rewrite value", () => {
      expect(
        buildWhereClause(
          [cond({ column: "name", operator: "ILIKE", value: "%O'B%" })],
          columns,
          "mysql"
        )
      ).toBe("LOWER(`name`) LIKE LOWER('%O''B%')");
    });

    it("trims whitespace around the ILIKE value before rewriting", () => {
      expect(
        buildWhereClause(
          [cond({ column: "name", operator: "ILIKE", value: "  alice  " })],
          columns,
          "mysql"
        )
      ).toBe("LOWER(`name`) LIKE LOWER('alice')");
    });
  });
});
