import { describe, it, expect } from "vitest";
import {
  compareCellValues,
  compareNumeric,
  editorInputToValue,
  isIntegerType,
  isTextLike,
  parseCellInput,
  valueCategory,
} from "./cellValues";

describe("valueCategory", () => {
  it("recognises integer types precisely", () => {
    for (const t of ["int", "INT(11)", "int2", "int4", "int8", "integer", "smallint", "bigint",
      "tinyint", "mediumint", "serial", "bigserial", "smallserial", "int unsigned", "bigint(20) unsigned"]) {
      expect(isIntegerType(t)).toBe(true);
    }
  });

  it("does not treat interval, point or other int-containing names as integers", () => {
    for (const t of ["interval", "point", "tinytext", "international", "int4range", "integer[]"]) {
      expect(isIntegerType(t)).toBe(false);
    }
    expect(valueCategory("interval")).toBe("text");
    expect(valueCategory("point")).toBe("text");
  });

  it("classifies exact and float numerics", () => {
    expect(valueCategory("numeric(38,10)")).toBe("exact");
    expect(valueCategory("DECIMAL(10, 2)")).toBe("exact");
    expect(valueCategory("double precision")).toBe("float");
    expect(valueCategory("real")).toBe("float");
  });

  it("treats character types as text-like and others not", () => {
    expect(isTextLike("varchar(20)")).toBe(true);
    expect(isTextLike("text")).toBe(true);
    expect(isTextLike("integer")).toBe(false);
    expect(isTextLike("timestamp with time zone")).toBe(false);
  });
});

describe("parseCellInput", () => {
  it("returns numbers for safe integers", () => {
    expect(parseCellInput("42", "integer")).toEqual({ ok: true, value: 42 });
    expect(parseCellInput(" -7 ", "bigint")).toEqual({ ok: true, value: -7 });
  });

  it("keeps integers beyond the safe range as exact strings", () => {
    expect(parseCellInput("9223372036854775807", "bigint")).toEqual({ ok: true, value: "9223372036854775807" });
    expect(parseCellInput("-9007199254740993", "int8")).toEqual({ ok: true, value: "-9007199254740993" });
    expect(parseCellInput("9007199254740991", "bigint")).toEqual({ ok: true, value: 9007199254740991 });
  });

  it("rejects non-numeric integer input instead of truncating", () => {
    expect(parseCellInput("12abc", "integer").ok).toBe(false);
    expect(parseCellInput("1.5", "integer").ok).toBe(false);
    expect(parseCellInput("", "integer").ok).toBe(false);
  });

  it("keeps exact decimals that don't round-trip as strings", () => {
    expect(parseCellInput("12345678901234567890.1234567891", "numeric(38,10)"))
      .toEqual({ ok: true, value: "12345678901234567890.1234567891" });
    expect(parseCellInput("1.10", "decimal(10,2)")).toEqual({ ok: true, value: "1.10" });
    expect(parseCellInput("12.5", "numeric")).toEqual({ ok: true, value: 12.5 });
    expect(parseCellInput("1.2.3", "numeric").ok).toBe(false);
  });

  it("parses floats and allows special values", () => {
    expect(parseCellInput("3.25", "double precision")).toEqual({ ok: true, value: 3.25 });
    expect(parseCellInput("NaN", "real")).toEqual({ ok: true, value: "NaN" });
    expect(parseCellInput("abc", "float").ok).toBe(false);
  });

  it("parses booleans strictly", () => {
    expect(parseCellInput("true", "boolean")).toEqual({ ok: true, value: true });
    expect(parseCellInput("0", "bool")).toEqual({ ok: true, value: false });
    expect(parseCellInput("maybe", "boolean").ok).toBe(false);
  });

  it("leaves interval, point and text values untouched", () => {
    expect(parseCellInput("1 day", "interval")).toEqual({ ok: true, value: "1 day" });
    expect(parseCellInput("(1,2)", "point")).toEqual({ ok: true, value: "(1,2)" });
    expect(parseCellInput("  padded  ", "text")).toEqual({ ok: true, value: "  padded  " });
  });
});

describe("editorInputToValue", () => {
  const text = { data_type: "varchar(20)", is_nullable: true };
  const int = { data_type: "integer", is_nullable: true };
  const intNotNull = { data_type: "integer", is_nullable: false };

  it("keeps an empty string for text columns instead of turning it into NULL", () => {
    expect(editorInputToValue("", text)).toEqual({ ok: true, value: "" });
  });

  it("keeps the literal strings null / Null for text columns", () => {
    expect(editorInputToValue("Null", text)).toEqual({ ok: true, value: "Null" });
    expect(editorInputToValue("null", text)).toEqual({ ok: true, value: "null" });
  });

  it("does not trim text", () => {
    expect(editorInputToValue("  a ", text)).toEqual({ ok: true, value: "  a " });
  });

  it("treats empty input as NULL for nullable non-text columns", () => {
    expect(editorInputToValue("", int)).toEqual({ ok: true, value: null });
    expect(editorInputToValue("NULL", int)).toEqual({ ok: true, value: null });
  });

  it("rejects empty input for NOT NULL non-text columns", () => {
    expect(editorInputToValue("", intNotNull).ok).toBe(false);
  });

  it("sends edited JSON back as its exact text", () => {
    const json = { data_type: "jsonb", is_nullable: true };
    expect(editorInputToValue('{ "b": 1, "a": [1, 2] }', json))
      .toEqual({ ok: true, value: '{ "b": 1, "a": [1, 2] }' });
    expect(editorInputToValue("[1,\n 2]", json)).toEqual({ ok: true, value: "[1,\n 2]" });
    expect(editorInputToValue("42", json)).toEqual({ ok: true, value: "42" });
  });

  it("keeps the JSON null literal distinct from SQL NULL in JSON columns", () => {
    const json = { data_type: "jsonb", is_nullable: true };
    expect(editorInputToValue("null", json)).toEqual({ ok: true, value: "null" });
    expect(editorInputToValue(" NULL ", json).ok).toBe(false); // not valid JSON
    expect(editorInputToValue("", json)).toEqual({ ok: true, value: null });
    expect(editorInputToValue(null, json)).toEqual({ ok: true, value: null });
    // Other non-text types still read the word as SQL NULL.
    expect(editorInputToValue("null", { data_type: "integer", is_nullable: true })).toEqual({ ok: true, value: null });
  });

  it("accepts non-finite float text as sent by the backend", () => {
    const f = { data_type: "double precision", is_nullable: true };
    for (const v of ["NaN", "Infinity", "-Infinity"]) expect(editorInputToValue(v, f)).toEqual({ ok: true, value: v });
  });

  it("rejects invalid JSON and never stores [object Object]", () => {
    const json = { data_type: "json", is_nullable: true };
    expect(editorInputToValue("[object Object]", json).ok).toBe(false);
    expect(editorInputToValue("{a:1}", json)).toEqual({ ok: false, error: "Invalid JSON" });
    expect(editorInputToValue("", json)).toEqual({ ok: true, value: null });
  });

  it("rejects invalid numbers and keeps big integers exact", () => {
    expect(editorInputToValue("12abc", int).ok).toBe(false);
    expect(editorInputToValue("123456789012345678901", { data_type: "numeric", is_nullable: true }))
      .toEqual({ ok: true, value: "123456789012345678901" });
  });
});

describe("compareNumeric / compareCellValues", () => {
  it("compares big integer strings exactly", () => {
    expect(compareNumeric("9223372036854775807", "9223372036854775806")).toBe(1);
    expect(compareNumeric("-9223372036854775807", "5")).toBe(-1);
    expect(compareNumeric(5, "9007199254740993")).toBe(-1);
  });

  it("compares decimal strings exactly", () => {
    expect(compareNumeric("1.10", "1.1")).toBe(0);
    expect(compareNumeric("0.12345678901234567891", "0.12345678901234567890")).toBe(1);
    expect(compareNumeric("-1.5", "-1.25")).toBe(-1);
  });

  it("sorts mixed bigint numbers and strings numerically", () => {
    const vals: unknown[] = ["9007199254740993", 10, 2, null];
    vals.sort((a, b) => compareCellValues(a, b));
    expect(vals).toEqual([null, 2, 10, "9007199254740993"]);
  });

  it("only compares two numeric strings numerically for numeric columns", () => {
    expect(compareCellValues("10", "9")).toBe(-1);
    expect(compareCellValues("10", "9", true)).toBe(1);
  });
});
