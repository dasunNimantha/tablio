import { describe, it, expect } from "vitest";
import { pendingInsertToNewRow, type PendingInsert } from "./pendingInsert";

describe("pendingInsertToNewRow", () => {
  const cols = ["id", "name", "note", "created_at"];

  it("omits untouched columns so their DEFAULT applies", () => {
    const ins: PendingInsert = {
      tempId: 1,
      values: [null, "Ada", null, null],
      touched: [false, true, false, false],
    };
    expect(pendingInsertToNewRow(ins, cols)).toEqual({ values: [["name", "Ada"]] });
  });

  it("sends an explicit NULL chosen by the user", () => {
    const ins: PendingInsert = {
      tempId: 2,
      values: [null, "Ada", null, null],
      touched: [false, true, true, false],
    };
    expect(pendingInsertToNewRow(ins, cols)).toEqual({ values: [["name", "Ada"], ["note", null]] });
  });

  it("keeps empty strings and exact numeric strings unchanged", () => {
    const ins: PendingInsert = {
      tempId: 3,
      values: ["9223372036854775807", "", null, null],
      touched: [true, true, false, false],
    };
    expect(pendingInsertToNewRow(ins, cols)).toEqual({
      values: [["id", "9223372036854775807"], ["name", ""]],
    });
  });

  it("uses the values stored on the insert, independent of the visible page", () => {
    const a: PendingInsert = { tempId: 4, values: ["1", "first", null, null], touched: [true, true, false, false] };
    const b: PendingInsert = { tempId: 5, values: ["2", "second", null, null], touched: [true, true, false, false] };
    expect([a, b].map((ins) => pendingInsertToNewRow(ins, cols))).toEqual([
      { values: [["id", "1"], ["name", "first"]] },
      { values: [["id", "2"], ["name", "second"]] },
    ]);
  });
});
