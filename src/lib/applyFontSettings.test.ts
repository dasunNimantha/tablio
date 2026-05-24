import { describe, it, expect, beforeEach } from "vitest";
import { applyFontSettings } from "./applyFontSettings";
import {
  DEFAULT_USER_SETTINGS,
  type UserSettings,
} from "../stores/userSettingsStore";

function makeRoot(): HTMLElement {
  // Use a fresh detached element per test so prior writes from
  // sibling tests can't leak. We only care about the .style
  // surface; nothing in applyFontSettings inspects the DOM tree.
  return document.createElement("div");
}

function settings(over: Partial<UserSettings> = {}): UserSettings {
  return { ...DEFAULT_USER_SETTINGS, ...over };
}

describe("applyFontSettings", () => {
  let root: HTMLElement;
  beforeEach(() => {
    root = makeRoot();
  });

  describe("font sizes are always written", () => {
    it("writes the default px values for an unedited install", () => {
      applyFontSettings(DEFAULT_USER_SETTINGS, root);
      expect(root.style.getPropertyValue("--ui-font-size")).toBe("13px");
      expect(root.style.getPropertyValue("--editor-font-size")).toBe("14px");
    });

    it("writes user-chosen px values", () => {
      applyFontSettings(settings({ uiFontSize: 16, editorFontSize: 18 }), root);
      expect(root.style.getPropertyValue("--ui-font-size")).toBe("16px");
      expect(root.style.getPropertyValue("--editor-font-size")).toBe("18px");
    });
  });

  describe("family overrides", () => {
    it("does NOT set --font-sans / --font-mono when both are null (defaults)", () => {
      // Letting the :root declaration win means there's no override
      // string to maintain in sync; the value lives in global.css.
      applyFontSettings(DEFAULT_USER_SETTINGS, root);
      expect(root.style.getPropertyValue("--font-sans")).toBe("");
      expect(root.style.getPropertyValue("--font-mono")).toBe("");
    });

    it("prepends the user's UI family to the bundled fallback chain", () => {
      applyFontSettings(settings({ uiFontFamily: "Inter" }), root);
      const v = root.style.getPropertyValue("--font-sans");
      expect(v).toContain('"Inter"');
      // Bundled Fira Sans must remain as the next fallback so an
      // unavailable user font still has a known-good replacement.
      expect(v).toContain('"Fira Sans"');
      expect(v).toContain("sans-serif");
    });

    it("prepends the user's editor family to the bundled monospace chain", () => {
      applyFontSettings(settings({ editorFontFamily: "Cascadia Code" }), root);
      const v = root.style.getPropertyValue("--font-mono");
      expect(v).toContain('"Cascadia Code"');
      expect(v).toContain('"JetBrains Mono"');
      expect(v).toContain("monospace");
    });

    it("clears a previously-set override when the family flips back to null", () => {
      // Reset-to-defaults path: a prior apply wrote a value, the
      // next apply (with `null`) must remove the inline override so
      // the :root variable wins again.
      applyFontSettings(settings({ uiFontFamily: "Inter" }), root);
      expect(root.style.getPropertyValue("--font-sans")).not.toBe("");
      applyFontSettings(settings({ uiFontFamily: null }), root);
      expect(root.style.getPropertyValue("--font-sans")).toBe("");
    });

    it("only touches the family that's customized", () => {
      applyFontSettings(settings({ uiFontFamily: "Inter" }), root);
      // UI was set, mono should stay at "no override".
      expect(root.style.getPropertyValue("--font-sans")).not.toBe("");
      expect(root.style.getPropertyValue("--font-mono")).toBe("");
    });
  });

  describe("defaults to documentElement when no root is passed", () => {
    it("writes the CSS vars to <html> in the absence of an explicit root", () => {
      // Clean any drift from prior tests.
      document.documentElement.style.removeProperty("--font-sans");
      document.documentElement.style.removeProperty("--font-mono");
      document.documentElement.style.removeProperty("--ui-font-size");
      document.documentElement.style.removeProperty("--editor-font-size");

      applyFontSettings(settings({ uiFontFamily: "Inter", uiFontSize: 15 }));

      expect(
        document.documentElement.style.getPropertyValue("--font-sans"),
      ).toContain('"Inter"');
      expect(
        document.documentElement.style.getPropertyValue("--ui-font-size"),
      ).toBe("15px");
    });
  });
});
