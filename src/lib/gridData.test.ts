import { describe, it, expect } from "vitest";
import {
  cellEditText,
  colField,
  colIndexFromId,
  restoreLineEndings,
  rowToObject,
  shouldSuppressGridKey,
  uniqueColumnKeys,
  usesLargeTextEditor,
} from "./gridData";

describe("colField / colIndexFromId", () => {
  it("addresses columns by position, so dotted names are safe", () => {
    expect(colField(0)).toBe("c0");
    expect(colField(12)).toBe("c12");
    expect(colField(3)).not.toContain(".");
  });

  it("round-trips", () => {
    for (const i of [0, 1, 7, 42]) expect(colIndexFromId(colField(i))).toBe(i);
  });

  it("returns -1 for non data-column ids", () => {
    expect(colIndexFromId(undefined)).toBe(-1);
    expect(colIndexFromId("")).toBe(-1);
    expect(colIndexFromId("meta.source")).toBe(-1);
    expect(colIndexFromId("0")).toBe(-1);
    expect(colIndexFromId("c1x")).toBe(-1);
  });
});

describe("cellEditText", () => {
  it("keeps raw JSON text from JSON/JSONB columns as-is", () => {
    expect(cellEditText('{"a": 1,  "b":[1,2]}')).toBe('{"a": 1,  "b":[1,2]}');
  });

  it("stringifies structured values instead of [object Object]", () => {
    expect(cellEditText({ a: 1 })).toBe('{"a":1}');
    expect(cellEditText([1, "x"])).toBe('[1,"x"]');
  });

  it("keeps NULL distinct", () => {
    expect(cellEditText(null)).toBeNull();
    expect(cellEditText(undefined)).toBeNull();
  });

  it("stringifies scalars", () => {
    expect(cellEditText(0)).toBe("0");
    expect(cellEditText(false)).toBe("false");
    expect(cellEditText("")).toBe("");
  });

  it("keeps newlines", () => {
    expect(cellEditText("a\r\nb\nc")).toBe("a\r\nb\nc");
  });
});

describe("restoreLineEndings", () => {
  it("restores CRLF when the original used it", () => {
    expect(restoreLineEndings("one\r\ntwo", "one\ntwo\nthree")).toBe("one\r\ntwo\r\nthree");
  });

  it("leaves LF text alone", () => {
    expect(restoreLineEndings("one\ntwo", "one\ntwo!")).toBe("one\ntwo!");
  });

  it("leaves edited text that has its own CR alone", () => {
    expect(restoreLineEndings("a\r\nb", "a\rb")).toBe("a\rb");
  });

  it("ignores non-string originals", () => {
    expect(restoreLineEndings(null, "a\nb")).toBe("a\nb");
    expect(restoreLineEndings(5, "a\nb")).toBe("a\nb");
  });
});

describe("usesLargeTextEditor", () => {
  it("uses the multi-line editor for json, text and long values", () => {
    expect(usesLargeTextEditor("jsonb", "{}")).toBe(true);
    expect(usesLargeTextEditor("json", null)).toBe(true);
    expect(usesLargeTextEditor("text", "short")).toBe(true);
    expect(usesLargeTextEditor("longtext", null)).toBe(true);
    expect(usesLargeTextEditor("varchar(255)", "line 1\nline 2")).toBe(true);
    expect(usesLargeTextEditor("varchar(255)", "x".repeat(200))).toBe(true);
    expect(usesLargeTextEditor("map<text, int>", { a: 1 })).toBe(true);
  });

  it("keeps the single-line editor for short scalar values", () => {
    expect(usesLargeTextEditor("varchar(50)", "admin")).toBe(false);
    expect(usesLargeTextEditor("integer", 5)).toBe(false);
    expect(usesLargeTextEditor("boolean", true)).toBe(false);
    expect(usesLargeTextEditor("timestamptz", null)).toBe(false);
  });
});

describe("shouldSuppressGridKey", () => {
  const base = { editing: false, hasRowSelection: false };

  it("lets Enter, F2 and typing start editing", () => {
    for (const key of ["Enter", "F2", "a", "Z", "5", " "]) {
      expect(shouldSuppressGridKey({ ...base, key })).toBe(false);
    }
  });

  it("lets Delete/Backspace act on the focused cell when no rows are selected", () => {
    expect(shouldSuppressGridKey({ ...base, key: "Delete" })).toBe(false);
    expect(shouldSuppressGridKey({ ...base, key: "Backspace" })).toBe(false);
  });

  it("reserves Delete/Backspace for row deletion while rows are selected", () => {
    expect(shouldSuppressGridKey({ ...base, hasRowSelection: true, key: "Delete" })).toBe(true);
    expect(shouldSuppressGridKey({ ...base, hasRowSelection: true, key: "Backspace" })).toBe(true);
    expect(shouldSuppressGridKey({ ...base, hasRowSelection: true, key: "Enter" })).toBe(false);
  });

  it("never suppresses keys inside an open editor", () => {
    for (const key of ["Delete", "Backspace", "Enter", "a"]) {
      expect(shouldSuppressGridKey({ editing: true, hasRowSelection: true, key })).toBe(false);
    }
  });
});

describe("uniqueColumnKeys / rowToObject", () => {
  it("keeps unique names", () => {
    expect(uniqueColumnKeys(["id", "name"])).toEqual(["id", "name"]);
  });

  it("suffixes duplicates", () => {
    expect(uniqueColumnKeys(["id", "name", "id", "id"])).toEqual(["id", "name", "id_2", "id_3"]);
  });

  it("avoids colliding with a real column named like a suffix", () => {
    expect(uniqueColumnKeys(["id", "id", "id_2"])).toEqual(["id", "id_3", "id_2"]);
  });

  it("keeps every value of a row with duplicate columns", () => {
    expect(rowToObject(["id", "id"], [1, 2])).toEqual({ id: 1, id_2: 2 });
  });
});
