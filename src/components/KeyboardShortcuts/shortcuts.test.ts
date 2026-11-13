import { describe, it, expect } from "vitest";
import { SHORTCUT_GROUPS, displayKey } from "./shortcuts";

const all = SHORTCUT_GROUPS.flatMap((g) => g.shortcuts);
const has = (keys: string[]) => all.some((s) => s.keys.join("+") === keys.join("+"));

describe("keyboard shortcuts panel", () => {
  it("describes Ctrl+S as saving the query (there is no generic save)", () => {
    expect(all.find((s) => s.keys.join("+") === "Ctrl+S")?.description).toBe("Save query");
  });

  it("lists the window shortcuts App.tsx handles", () => {
    for (const keys of [["Ctrl", "?"], ["Ctrl", "="], ["Ctrl", "-"], ["Ctrl", "0"]]) {
      expect(has(keys)).toBe(true);
    }
  });

  it("keeps the tab shortcuts", () => {
    expect(has(["Ctrl", "N"])).toBe(true);
    expect(has(["Ctrl", "W"])).toBe(true);
    expect(has(["Ctrl", "Shift", "F"])).toBe(true);
  });

  it("has no duplicate entries within a group", () => {
    for (const g of SHORTCUT_GROUPS) {
      const ids = g.shortcuts.map((s) => s.keys.join("+"));
      expect(new Set(ids).size).toBe(ids.length);
    }
  });

  it("shows Ctrl as ⌘ on macOS only", () => {
    expect(displayKey("Ctrl", true)).toBe("⌘");
    expect(displayKey("Ctrl", false)).toBe("Ctrl");
    expect(displayKey("Enter", true)).toBe("Enter");
  });
});
