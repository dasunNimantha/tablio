import { describe, it, expect } from "vitest";
import {
  parseSubTab,
  serializeSubTab,
  SCHEMA_ANCHORS,
  type SchemaAnchor,
} from "./subTab";

describe("parseSubTab — empty / unknown input falls through to Data mode", () => {
  it("returns Data for null", () => {
    expect(parseSubTab(null)).toEqual({ mode: "data" });
  });

  it("returns Data for undefined", () => {
    expect(parseSubTab(undefined)).toEqual({ mode: "data" });
  });

  it("returns Data for an empty string", () => {
    expect(parseSubTab("")).toEqual({ mode: "data" });
  });

  it("returns Data for a string we don't recognise at all", () => {
    expect(parseSubTab("nonsense")).toEqual({ mode: "data" });
  });
});

describe("parseSubTab — explicit modes", () => {
  it("'data' parses as Data mode", () => {
    expect(parseSubTab("data")).toEqual({ mode: "data" });
  });

  it("'schema' parses as Schema mode with no anchor", () => {
    expect(parseSubTab("schema")).toEqual({ mode: "schema" });
  });
});

describe("parseSubTab — schema:<anchor> deep links", () => {
  it.each(SCHEMA_ANCHORS)(
    "parses 'schema:%s' as schema mode with that anchor",
    (anchor) => {
      expect(parseSubTab(`schema:${anchor}`)).toEqual({
        mode: "schema",
        anchor,
      });
    }
  );

  it("strips an unknown anchor but stays in schema mode", () => {
    // We don't want the user to land on Data when their old tab said
    // "schema:foo" — schema with no anchor is the safer default.
    expect(parseSubTab("schema:bogus-section")).toEqual({ mode: "schema" });
  });
});

describe("parseSubTab — legacy bare-anchor decay", () => {
  it.each(SCHEMA_ANCHORS)(
    "old persisted tab '%s' (no schema: prefix) decays to schema:%s",
    (anchor) => {
      expect(parseSubTab(anchor)).toEqual({
        mode: "schema",
        anchor,
      });
    }
  );
});

describe("serializeSubTab — round trips with parseSubTab", () => {
  it("serialises Data mode as 'data'", () => {
    expect(serializeSubTab("data")).toBe("data");
    expect(serializeSubTab("data", "columns")).toBe("data");
  });

  it("serialises bare Schema mode as 'schema'", () => {
    expect(serializeSubTab("schema")).toBe("schema");
  });

  it.each(SCHEMA_ANCHORS)(
    "serialises Schema + %s as 'schema:%s'",
    (anchor) => {
      expect(serializeSubTab("schema", anchor)).toBe(`schema:${anchor}`);
    }
  );

  it("round trips data, schema, and every schema:<anchor>", () => {
    const cases: Array<[string, ReturnType<typeof parseSubTab>]> = [
      ["data", { mode: "data" }],
      ["schema", { mode: "schema" }],
      ...SCHEMA_ANCHORS.map<[string, ReturnType<typeof parseSubTab>]>(
        (a: SchemaAnchor) => [`schema:${a}`, { mode: "schema", anchor: a }]
      ),
    ];
    for (const [serialized, parsed] of cases) {
      expect(parseSubTab(serialized)).toEqual(parsed);
      expect(serializeSubTab(parsed.mode, parsed.anchor)).toBe(serialized);
    }
  });
});
