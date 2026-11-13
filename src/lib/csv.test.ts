import { describe, it, expect } from "vitest";
import { detectCsvDelimiter, normalizeHeaders, parseCsv, parseCsvRecords } from "./csv";

describe("parseCsvRecords", () => {
  it("keeps the column after a quoted field containing the delimiter", () => {
    const { headers, rows } = parseCsv('id,username,email\n1,"Smith, John",john@example.com');
    expect(headers).toEqual(["id", "username", "email"]);
    expect(rows).toEqual([["1", "Smith, John", "john@example.com"]]);
  });

  it("keeps quoted line breaks inside one field", () => {
    const rows = parseCsvRecords('a,b\n1,"line one\nline two"\n2,x\n');
    expect(rows).toEqual([
      ["a", "b"],
      ["1", "line one\nline two"],
      ["2", "x"],
    ]);
  });

  it("unescapes doubled quotes", () => {
    expect(parseCsvRecords('"say ""hi""",b')).toEqual([['say "hi"', "b"]]);
  });

  it("handles CRLF and lone CR line endings", () => {
    expect(parseCsvRecords("a,b\r\n1,2\r3,4")).toEqual([
      ["a", "b"],
      ["1", "2"],
      ["3", "4"],
    ]);
  });

  it("strips a UTF-8 BOM", () => {
    expect(parseCsvRecords("﻿id,name\n1,x")[0]).toEqual(["id", "name"]);
  });

  it("supports a custom delimiter", () => {
    expect(parseCsvRecords('a;b;c\n1;"x;y";3', ";")).toEqual([
      ["a", "b", "c"],
      ["1", "x;y", "3"],
    ]);
    expect(parseCsvRecords("a\tb\n1\t2", "\t")).toEqual([
      ["a", "b"],
      ["1", "2"],
    ]);
  });

  it("keeps empty fields, including a trailing one", () => {
    expect(parseCsvRecords("a,,c,\n")).toEqual([["a", "", "c", ""]]);
  });

  it("skips blank lines but keeps a quoted empty value", () => {
    expect(parseCsvRecords('a\n\n""\n\nb\n')).toEqual([["a"], [""], ["b"]]);
  });

  it("trims unquoted fields but keeps whitespace inside quotes", () => {
    expect(parseCsvRecords(' a , " b " ,c')).toEqual([["a", " b ", "c"]]);
  });

  it("rejects an unterminated quoted field", () => {
    expect(() => parseCsvRecords('a,"unterminated\n1,2')).toThrow(/unbalanced/);
  });
});

describe("detectCsvDelimiter", () => {
  it("picks the delimiter used in the header line, ignoring quoted text", () => {
    expect(detectCsvDelimiter("a;b;c\n1;2;3")).toBe(";");
    expect(detectCsvDelimiter('"x,y,z";b;c\n')).toBe(";");
    expect(detectCsvDelimiter("a\tb\n")).toBe("\t");
    expect(detectCsvDelimiter("single\n")).toBe(",");
  });
});

describe("normalizeHeaders", () => {
  it("names blank headers and de-duplicates repeats case-insensitively", () => {
    expect(normalizeHeaders(["id", "", "Name", "name", "id", " "])).toEqual([
      "id",
      "column_2",
      "Name",
      "name_2",
      "id_2",
      "column_6",
    ]);
  });
});
