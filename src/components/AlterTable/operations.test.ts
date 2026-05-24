import { describe, it, expect } from "vitest";
import {
  applyOperations,
  generatePreviewSql,
  reorderOperations,
  PG_TYPES,
  type PendingNewColumn,
} from "./operations";
import type { AlterTableOperation, ColumnInfo } from "../../lib/tauri";

function col(
  name: string,
  data_type: string,
  is_nullable = true,
  default_value: string | null = null,
  is_primary_key = false,
): ColumnInfo {
  return {
    name,
    data_type,
    is_nullable,
    default_value,
    is_primary_key,
    is_auto_generated: false,
    ordinal_position: 1,
  };
}

describe("applyOperations", () => {
  it("returns each column unchanged when there are no operations", () => {
    const cols = [col("id", "integer"), col("name", "text")];
    const eff = applyOperations(cols, []);
    expect(eff.get("id")).toEqual({
      name: "id",
      type: "integer",
      nullable: true,
      default: null,
    });
    expect(eff.get("name")).toEqual({
      name: "name",
      type: "text",
      nullable: true,
      default: null,
    });
  });

  it("applies a rename_column to the effective name", () => {
    const eff = applyOperations(
      [col("first_name", "text")],
      [{ op: "rename_column", old_name: "first_name", new_name: "given_name" }],
    );
    expect(eff.get("first_name")?.name).toBe("given_name");
  });

  it("applies a change_type", () => {
    const eff = applyOperations(
      [col("age", "integer")],
      [{ op: "change_type", column_name: "age", new_type: "bigint" }],
    );
    expect(eff.get("age")?.type).toBe("bigint");
  });

  it("applies set_nullable in both directions", () => {
    const eff = applyOperations(
      [col("a", "text", true), col("b", "text", false)],
      [
        { op: "set_nullable", column_name: "a", nullable: false },
        { op: "set_nullable", column_name: "b", nullable: true },
      ],
    );
    expect(eff.get("a")?.nullable).toBe(false);
    expect(eff.get("b")?.nullable).toBe(true);
  });

  it("applies set_default with a new value", () => {
    const eff = applyOperations(
      [col("created_at", "timestamptz")],
      [
        {
          op: "set_default",
          column_name: "created_at",
          default_value: "now()",
        },
      ],
    );
    expect(eff.get("created_at")?.default).toBe("now()");
  });

  it("applies set_default with null = drop default", () => {
    const eff = applyOperations(
      [col("status", "text", true, "'pending'")],
      [{ op: "set_default", column_name: "status", default_value: null }],
    );
    expect(eff.get("status")?.default).toBeNull();
  });

  it("combines rename + change_type + set_nullable on the same column", () => {
    // The dialog routinely queues several ops against the same column
    // (rename, then re-type, then flip nullability). All three should
    // fold into one effective row.
    const eff = applyOperations(
      [col("legacy_id", "integer", true)],
      [
        { op: "rename_column", old_name: "legacy_id", new_name: "id" },
        { op: "change_type", column_name: "legacy_id", new_type: "bigint" },
        { op: "set_nullable", column_name: "legacy_id", nullable: false },
      ],
    );
    expect(eff.get("legacy_id")).toEqual({
      name: "id",
      type: "bigint",
      nullable: false,
      default: null,
    });
  });

  it("keeps the dropped column in the result map so the editor still renders it (the dropped badge handler reads it)", () => {
    const eff = applyOperations(
      [col("obsolete", "text")],
      [{ op: "drop_column", column_name: "obsolete" }],
    );
    // The row is still present — the "Dropped" overlay is rendered
    // by the editor based on a sibling drop-tracking set, not by
    // the absence of the row here.
    expect(eff.has("obsolete")).toBe(true);
  });

  it("ignores ops that target a column that doesn't exist", () => {
    const eff = applyOperations(
      [col("a", "text")],
      [{ op: "rename_column", old_name: "nope", new_name: "noooo" }],
    );
    expect(eff.get("a")?.name).toBe("a"); // unchanged
  });
});

describe("generatePreviewSql", () => {
  const SCHEMA = "public";
  const TABLE = "users";

  it("returns the no-op comment when there are no changes", () => {
    expect(generatePreviewSql(SCHEMA, TABLE, null, [], [])).toBe("-- No changes");
  });

  it("emits ALTER TABLE … RENAME TO when the table is renamed and qualifies subsequent ops against the new name", () => {
    const sql = generatePreviewSql(SCHEMA, TABLE, "people", [
      { op: "drop_column", column_name: "stale" },
    ], []);
    expect(sql).toContain(`ALTER TABLE "public"."users" RENAME TO "people";`);
    // Subsequent ops are qualified against the *new* name.
    expect(sql).toContain(`ALTER TABLE "public"."people" DROP COLUMN "stale";`);
  });

  it("emits ADD COLUMN with default + NOT NULL", () => {
    const sql = generatePreviewSql(
      SCHEMA,
      TABLE,
      null,
      [
        {
          op: "add_column",
          column: {
            name: "created_at",
            data_type: "timestamptz",
            is_nullable: false,
            is_primary_key: false,
            default_value: "now()",
          },
        },
      ],
      [],
    );
    expect(sql).toBe(
      `ALTER TABLE "public"."users" ADD COLUMN "created_at" timestamptz NOT NULL DEFAULT now();`,
    );
  });

  it("emits DROP COLUMN", () => {
    const sql = generatePreviewSql(SCHEMA, TABLE, null, [
      { op: "drop_column", column_name: "legacy" },
    ], []);
    expect(sql).toBe(`ALTER TABLE "public"."users" DROP COLUMN "legacy";`);
  });

  it("emits RENAME COLUMN", () => {
    const sql = generatePreviewSql(SCHEMA, TABLE, null, [
      { op: "rename_column", old_name: "first_name", new_name: "given_name" },
    ], []);
    expect(sql).toBe(
      `ALTER TABLE "public"."users" RENAME COLUMN "first_name" TO "given_name";`,
    );
  });

  it("emits ALTER COLUMN … TYPE", () => {
    const sql = generatePreviewSql(SCHEMA, TABLE, null, [
      { op: "change_type", column_name: "age", new_type: "bigint" },
    ], []);
    expect(sql).toBe(
      `ALTER TABLE "public"."users" ALTER COLUMN "age" TYPE bigint;`,
    );
  });

  it("emits SET / DROP NOT NULL for set_nullable", () => {
    expect(
      generatePreviewSql(SCHEMA, TABLE, null, [
        { op: "set_nullable", column_name: "email", nullable: false },
      ], []),
    ).toContain(`ALTER COLUMN "email" SET NOT NULL`);
    expect(
      generatePreviewSql(SCHEMA, TABLE, null, [
        { op: "set_nullable", column_name: "email", nullable: true },
      ], []),
    ).toContain(`ALTER COLUMN "email" DROP NOT NULL`);
  });

  it("emits SET / DROP DEFAULT for set_default", () => {
    expect(
      generatePreviewSql(SCHEMA, TABLE, null, [
        { op: "set_default", column_name: "status", default_value: "'new'" },
      ], []),
    ).toContain(`ALTER COLUMN "status" SET DEFAULT 'new'`);
    expect(
      generatePreviewSql(SCHEMA, TABLE, null, [
        { op: "set_default", column_name: "status", default_value: null },
      ], []),
    ).toContain(`ALTER COLUMN "status" DROP DEFAULT`);
  });

  it("appends pending new columns as ADD COLUMNs", () => {
    const pending: PendingNewColumn[] = [
      { name: "tag", data_type: "text", is_nullable: true, default_value: "" },
    ];
    const sql = generatePreviewSql(SCHEMA, TABLE, null, [], pending);
    expect(sql).toBe(`ALTER TABLE "public"."users" ADD COLUMN "tag" text;`);
  });

  it("skips pending new columns whose name is blank (user hasn't typed yet)", () => {
    const pending: PendingNewColumn[] = [
      { name: "", data_type: "text", is_nullable: true, default_value: "" },
    ];
    expect(generatePreviewSql(SCHEMA, TABLE, null, [], pending)).toBe(
      "-- No changes",
    );
  });
});

describe("reorderOperations", () => {
  // Why the ordering matters: the server applies ops in array order,
  // and a `rename_column` followed by a `change_type` that uses the
  // OLD name would fail server-side. Same shape with `rename_table`.
  // We push renames to the front so subsequent qualifier resolution
  // works against the post-rename identifiers.

  it("keeps the input order when there are no renames", () => {
    const ops: AlterTableOperation[] = [
      { op: "drop_column", column_name: "a" },
      { op: "change_type", column_name: "b", new_type: "bigint" },
    ];
    expect(reorderOperations(ops)).toEqual(ops);
  });

  it("puts rename_table first when present", () => {
    const ops: AlterTableOperation[] = [
      { op: "drop_column", column_name: "a" },
      { op: "rename_table", new_name: "renamed" },
    ];
    const out = reorderOperations(ops);
    expect(out[0]).toEqual({ op: "rename_table", new_name: "renamed" });
  });

  it("puts rename_column ops after rename_table but before field-targeted ops", () => {
    const ops: AlterTableOperation[] = [
      { op: "change_type", column_name: "x", new_type: "text" },
      { op: "rename_column", old_name: "x", new_name: "y" },
      { op: "rename_table", new_name: "tbl2" },
    ];
    const out = reorderOperations(ops);
    expect(out[0].op).toBe("rename_table");
    expect(out[1].op).toBe("rename_column");
    expect(out[2].op).toBe("change_type");
  });

  it("preserves relative order within a category", () => {
    const ops: AlterTableOperation[] = [
      { op: "rename_column", old_name: "a", new_name: "alpha" },
      { op: "rename_column", old_name: "b", new_name: "beta" },
      { op: "drop_column", column_name: "c" },
      { op: "change_type", column_name: "d", new_type: "text" },
    ];
    const out = reorderOperations(ops);
    expect(out.map((o) => o.op)).toEqual([
      "rename_column",
      "rename_column",
      "drop_column",
      "change_type",
    ]);
    // and within the rename_column bucket, original order is kept:
    expect((out[0] as { old_name: string }).old_name).toBe("a");
    expect((out[1] as { old_name: string }).old_name).toBe("b");
  });
});

describe("PG_TYPES", () => {
  it("contains the canonical set used across the app", () => {
    // Sanity: a small spot check so a typo in the curated list
    // doesn't slip through. Not a comprehensive Postgres-type
    // inventory.
    expect(PG_TYPES).toContain("integer");
    expect(PG_TYPES).toContain("text");
    expect(PG_TYPES).toContain("jsonb");
    expect(PG_TYPES).toContain("uuid");
    expect(PG_TYPES).toContain("timestamptz");
  });

  it("is non-empty", () => {
    expect(PG_TYPES.length).toBeGreaterThan(10);
  });
});
