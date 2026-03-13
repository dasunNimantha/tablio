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
});
