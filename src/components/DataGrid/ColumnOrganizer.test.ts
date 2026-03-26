import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  applyColumnSettings,
  loadColumnSettings,
  saveColumnSettings,
  type ColumnSettings,
} from "./ColumnOrganizer";
import type { ColumnInfo } from "../../lib/tauri";

const col = (name: string, dataType: string, isPk = false): ColumnInfo => ({
  name,
  data_type: dataType,
  is_nullable: true,
  is_primary_key: isPk,
  default_value: null,
  ordinal_position: 0,
  is_auto_generated: false,
});

describe("applyColumnSettings", () => {
  const columns: ColumnInfo[] = [
    col("id", "integer", true),
    col("a", "text"),
    col("b", "text"),
    col("c", "text"),
  ];

  it("returns all indices when settings null", () => {
    const r = applyColumnSettings(columns, null);
    expect(r.visibleIndices).toEqual([0, 1, 2, 3]);
  });

  it("returns all indices when order empty and hidden empty", () => {
    const r = applyColumnSettings(columns, { order: [], hidden: new Set() });
    expect(r.visibleIndices).toEqual([0, 1, 2, 3]);
  });

  it("puts PK columns first then ordered non-PK", () => {
    const r = applyColumnSettings(columns, {
      order: ["id", "c", "b", "a"],
      hidden: new Set(),
    });
    expect(r.visibleIndices).toEqual([0, 3, 2, 1]);
  });

  it("excludes hidden columns", () => {
    const r = applyColumnSettings(columns, {
      order: ["id", "a", "b", "c"],
      hidden: new Set(["b"]),
    });
    expect(r.visibleIndices).toEqual([0, 1, 3]);
  });

  it("appends columns not in order and not hidden", () => {
    const cols: ColumnInfo[] = [
      col("id", "int", true),
      col("x", "text"),
      col("y", "text"),
    ];
    const r = applyColumnSettings(cols, {
      order: ["id", "y"],
      hidden: new Set(),
    });
    expect(r.visibleIndices).toEqual([0, 2, 1]);
  });

  it("handles settings with stale column names", () => {
    const r = applyColumnSettings(columns, {
      order: ["id", "old_col", "a"],
      hidden: new Set(["old_col", "b"]),
    });
    expect(r.visibleIndices).toEqual([0, 1, 3]);
  });
});

describe("loadColumnSettings / saveColumnSettings", () => {
  const key = "tablio-cols:conn1:db:schema:table";

  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    localStorage.clear();
  });

  it("returns null when nothing stored", () => {
    expect(loadColumnSettings("conn1", "db", "schema", "table")).toBeNull();
  });

  it("round-trips order and hidden", () => {
    const settings: ColumnSettings = {
      order: ["id", "a", "b"],
      hidden: new Set(["b"]),
    };
    saveColumnSettings("conn1", "db", "schema", "table", settings);
    const loaded = loadColumnSettings("conn1", "db", "schema", "table");
    expect(loaded).not.toBeNull();
    expect(loaded!.order).toEqual(["id", "a", "b"]);
    expect(Array.from(loaded!.hidden)).toEqual(["b"]);
  });

  it("uses correct storage key", () => {
    saveColumnSettings("c1", "d1", "s1", "t1", { order: ["x"], hidden: new Set() });
    expect(localStorage.getItem("tablio-cols:c1:d1:s1:t1")).not.toBeNull();
    expect(loadColumnSettings("c2", "d2", "s2", "t2")).toBeNull();
  });
});
