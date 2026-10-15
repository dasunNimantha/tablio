import { describe, it, expect } from "vitest";
import { structureSignature } from "./schemaSync";

describe("structureSignature", () => {
  const table = (name: string, extra: Record<string, unknown> = {}) => ({
    id: `c:db:public:tables:${name}`,
    type: "table",
    label: name,
    ...extra,
  });

  it("ignores row counts, sizes and labels", () => {
    const before = [table("users", { rowCount: 10, sizeBytes: 100 })];
    const after = [table("users", { rowCount: 999, sizeBytes: 5000, label: "Users" })];
    expect(structureSignature(after)).toBe(structureSignature(before));
  });

  it("detects added, dropped and renamed objects", () => {
    const base = structureSignature([table("users")]);
    expect(structureSignature([table("users"), table("orders")])).not.toBe(base);
    expect(structureSignature([])).not.toBe(base);
    expect(structureSignature([table("accounts")])).not.toBe(base);
  });

  it("detects a table turning into a view or becoming partitioned", () => {
    const base = structureSignature([table("users")]);
    expect(structureSignature([{ ...table("users"), type: "view" }])).not.toBe(base);
    expect(structureSignature([table("users", { partitionStrategy: "range" })])).not.toBe(base);
  });

  it("includes preloaded partition children", () => {
    const parent = (parts: string[]) =>
      table("events", {
        partitionStrategy: "range",
        preloadedChildren: parts.map((p) => table(`events:${p}`)),
      });
    expect(structureSignature([parent(["p1", "p2"])])).not.toBe(
      structureSignature([parent(["p1"])])
    );
  });
});
