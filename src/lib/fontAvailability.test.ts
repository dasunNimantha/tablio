import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import {
  isFontAvailable,
  detectAvailableFonts,
  UI_FONT_CANDIDATES,
  EDITOR_FONT_CANDIDATES,
  SYSTEM_DEFAULT_SENTINEL,
  type FontOption,
} from "./fontAvailability";

/**
 * Hook `document.fonts.check` so we can drive deterministic
 * availability results from tests. Returns a setter that lets each
 * test decide which families are "installed".
 */
function withFontsCheck(checker: (font: string) => boolean) {
  const original = (
    document as Document & { fonts?: { check?: (f: string) => boolean } }
  ).fonts;
  Object.defineProperty(document, "fonts", {
    value: { check: vi.fn(checker) },
    configurable: true,
  });
  return () => {
    Object.defineProperty(document, "fonts", {
      value: original,
      configurable: true,
    });
  };
}

describe("isFontAvailable", () => {
  let restore: (() => void) | null = null;

  afterEach(() => {
    restore?.();
    restore = null;
  });

  it("returns true for the empty sentinel (always offered)", () => {
    expect(isFontAvailable(SYSTEM_DEFAULT_SENTINEL)).toBe(true);
  });

  it("passes the family quoted in 12px form to document.fonts.check", () => {
    const checked: string[] = [];
    restore = withFontsCheck((font) => {
      checked.push(font);
      return true;
    });
    isFontAvailable("Cascadia Code");
    // Multi-word families must be quoted or the CSS parser would
    // treat each word as a separate token and the lookup would
    // silently miss.
    expect(checked).toEqual([`12px "Cascadia Code"`]);
  });

  it("returns the value document.fonts.check returns", () => {
    restore = withFontsCheck(() => false);
    expect(isFontAvailable("NonExistentFont")).toBe(false);
  });

  it("returns false (rather than throwing) when document.fonts is absent", () => {
    restore = withFontsCheck(() => true);
    // Remove the fonts API entirely to mimic older JSDOM / a stripped
    // environment.
    Object.defineProperty(document, "fonts", {
      value: undefined,
      configurable: true,
    });
    expect(isFontAvailable("Inter")).toBe(false);
  });

  it("returns false (rather than propagating) if check throws", () => {
    restore = withFontsCheck(() => {
      throw new Error("boom");
    });
    expect(isFontAvailable("Inter")).toBe(false);
  });
});

describe("detectAvailableFonts", () => {
  let restore: (() => void) | null = null;

  afterEach(() => {
    restore?.();
    restore = null;
  });

  it("keeps every bundled font even when the OS reports them as missing", () => {
    // Bundled fonts have @font-face declarations in global.css so
    // they're always available inside the app regardless of what
    // the OS-level font enumeration thinks.
    restore = withFontsCheck(() => false);
    const out = detectAvailableFonts(EDITOR_FONT_CANDIDATES);
    expect(out.find((o) => o.family === "JetBrains Mono")).toBeDefined();
    expect(out.find((o) => o.family === "Fira Code")).toBeDefined();
  });

  it("always keeps the System default sentinel", () => {
    restore = withFontsCheck(() => false);
    const out = detectAvailableFonts(UI_FONT_CANDIDATES);
    expect(
      out.find((o) => o.family === SYSTEM_DEFAULT_SENTINEL),
    ).toBeDefined();
  });

  it("filters non-bundled fonts that the system doesn't have", () => {
    // Only allow Inter and "Cascadia Code" through.
    restore = withFontsCheck((font) =>
      font.includes('"Inter"') || font.includes('"Cascadia Code"'),
    );
    const out = detectAvailableFonts(UI_FONT_CANDIDATES);
    expect(out.find((o) => o.family === "Inter")).toBeDefined();
    expect(out.find((o) => o.family === "Roboto")).toBeUndefined();
    expect(out.find((o) => o.family === "Segoe UI")).toBeUndefined();
  });

  it("returns the result as an array (not a frozen ReadonlyArray)", () => {
    restore = withFontsCheck(() => true);
    const out = detectAvailableFonts(UI_FONT_CANDIDATES);
    // The consumer pushes a "Custom..." option onto the result in
    // some UI variants — make sure the helper hands back a writable
    // array, not the original readonly literal.
    expect(Array.isArray(out)).toBe(true);
    expect(() => out.push({ label: "X", family: "X" })).not.toThrow();
  });

  it("preserves the candidate order so the user-facing list stays predictable", () => {
    restore = withFontsCheck(() => true);
    const out = detectAvailableFonts(UI_FONT_CANDIDATES);
    expect(out[0].family).toBe(SYSTEM_DEFAULT_SENTINEL);
    expect(out[1].family).toBe("Fira Sans");
  });
});

describe("candidate lists", () => {
  it("UI list starts with the System default sentinel", () => {
    expect(UI_FONT_CANDIDATES[0].family).toBe(SYSTEM_DEFAULT_SENTINEL);
    expect(UI_FONT_CANDIDATES[0].label).toMatch(/system default/i);
  });

  it("editor list starts with the System monospace sentinel", () => {
    expect(EDITOR_FONT_CANDIDATES[0].family).toBe(SYSTEM_DEFAULT_SENTINEL);
    expect(EDITOR_FONT_CANDIDATES[0].label).toMatch(/system monospace/i);
  });

  it("includes every bundled font as a candidate", () => {
    const bundledUi = UI_FONT_CANDIDATES.filter((o) => o.bundled);
    expect(bundledUi.map((o) => o.family)).toEqual(["Fira Sans"]);
    const bundledEditor = EDITOR_FONT_CANDIDATES.filter((o) => o.bundled);
    expect(bundledEditor.map((o) => o.family).sort()).toEqual(
      ["Fira Code", "JetBrains Mono"],
    );
  });

  it("uses unique family values within each list", () => {
    const uiFamilies = UI_FONT_CANDIDATES.map((o) => o.family);
    expect(new Set(uiFamilies).size).toBe(uiFamilies.length);
    const editorFamilies = EDITOR_FONT_CANDIDATES.map((o) => o.family);
    expect(new Set(editorFamilies).size).toBe(editorFamilies.length);
  });

  // Sanity: the types here let me feed any ReadonlyArray<FontOption>
  // to detectAvailableFonts.
  it("type-checks against detectAvailableFonts", () => {
    const arbitrary: ReadonlyArray<FontOption> = [
      { label: "X", family: "X" },
    ];
    expect(() => detectAvailableFonts(arbitrary)).not.toThrow();
  });
});
