import { describe, it, expect } from "vitest";

/**
 * Mirrors the column-name filter inside `ColumnsPanel` (and the
 * identical one in `AlterTableDialog`). We test the pure
 * decision-making rather than the rendered DOM so the assertions
 * survive cosmetic UI churn — the React tree's class names and
 * styles change often, but the filtering contract should not.
 *
 * Contract:
 *   - Empty / whitespace-only query → return the input unchanged
 *   - Case-insensitive substring match on column name
 *   - Preserve input order
 *   - Match against `name` only (data type, nullability, etc.
 *     are out of scope per issue #60)
 */
function filterColumnsByName<T extends { name: string }>(
  columns: T[],
  query: string,
): T[] {
  const q = query.trim().toLowerCase();
  if (!q) return columns;
  return columns.filter((c) => c.name.toLowerCase().includes(q));
}

interface Col {
  name: string;
  data_type: string;
}

const sampleColumns: Col[] = [
  { name: "id", data_type: "uuid" },
  { name: "user_email", data_type: "text" },
  { name: "user_name", data_type: "text" },
  { name: "created_at", data_type: "timestamp" },
  { name: "EMAIL_VERIFIED", data_type: "boolean" },
];

describe("Column-name filter (issue #60)", () => {
  it("returns the input array unchanged when the query is empty", () => {
    expect(filterColumnsByName(sampleColumns, "")).toBe(sampleColumns);
  });

  it("returns the input array unchanged when the query is whitespace only", () => {
    expect(filterColumnsByName(sampleColumns, "   ")).toBe(sampleColumns);
  });

  it("matches a single column exactly", () => {
    const out = filterColumnsByName(sampleColumns, "created_at");
    expect(out.map((c) => c.name)).toEqual(["created_at"]);
  });

  it("matches a substring of the column name", () => {
    const out = filterColumnsByName(sampleColumns, "user");
    expect(out.map((c) => c.name)).toEqual(["user_email", "user_name"]);
  });

  it("matches case-insensitively", () => {
    const out = filterColumnsByName(sampleColumns, "EMAIL");
    expect(out.map((c) => c.name).sort()).toEqual([
      "EMAIL_VERIFIED",
      "user_email",
    ]);
  });

  it("preserves input order", () => {
    const out = filterColumnsByName(sampleColumns, "_");
    expect(out.map((c) => c.name)).toEqual([
      "user_email",
      "user_name",
      "created_at",
      "EMAIL_VERIFIED",
    ]);
  });

  it("returns an empty array when no column matches", () => {
    expect(filterColumnsByName(sampleColumns, "nonexistent")).toEqual([]);
  });

  it("trims surrounding whitespace before matching", () => {
    // Users will paste column names with stray whitespace; the
    // filter should treat "  user  " the same as "user".
    const out = filterColumnsByName(sampleColumns, "  user  ");
    expect(out.map((c) => c.name)).toEqual(["user_email", "user_name"]);
  });

  it("does NOT match data_type (#60 explicitly out of scope)", () => {
    // Per the issue's out-of-scope list, type-based filtering is
    // deferred. A query that happens to be a type name should
    // only match columns whose NAME contains that substring.
    const out = filterColumnsByName(sampleColumns, "boolean");
    expect(out).toEqual([]);
  });

  it("handles an empty columns array", () => {
    expect(filterColumnsByName([], "anything")).toEqual([]);
  });

  it("is robust to special regex characters (treats them as literals)", () => {
    // The implementation uses `includes`, not regex, so special
    // characters like `.` or `*` are matched literally. A user
    // searching for a column literally named "user.id" should
    // find it without escaping anything.
    const cols: Col[] = [
      { name: "user.id", data_type: "text" },
      { name: "user_id", data_type: "uuid" },
    ];
    expect(filterColumnsByName(cols, "user.id").map((c) => c.name)).toEqual([
      "user.id",
    ]);
  });
});
