import { describe, it, expect, beforeEach } from "vitest";
import {
  useUserSettingsStore,
  hydrateUserSettings,
  DEFAULT_USER_SETTINGS,
  UI_FONT_SIZE_MIN,
  UI_FONT_SIZE_MAX,
  EDITOR_FONT_SIZE_MIN,
  EDITOR_FONT_SIZE_MAX,
} from "./userSettingsStore";

describe("userSettingsStore", () => {
  beforeEach(() => {
    // Reset the in-memory store + cleared localStorage so persisted
    // state from a previous test never leaks into the next one.
    useUserSettingsStore.setState({ settings: { ...DEFAULT_USER_SETTINGS } });
    localStorage.clear();
  });

  describe("defaults", () => {
    it("starts with `null` font families so the bundled CSS stack wins", () => {
      const { settings } = useUserSettingsStore.getState();
      expect(settings.uiFontFamily).toBeNull();
      expect(settings.editorFontFamily).toBeNull();
    });

    it("ships sensible default font sizes", () => {
      const { settings } = useUserSettingsStore.getState();
      expect(settings.uiFontSize).toBe(13);
      expect(settings.editorFontSize).toBe(14);
    });
  });

  describe("setUiFontFamily / setEditorFontFamily", () => {
    it("stores the chosen family", () => {
      useUserSettingsStore.getState().setUiFontFamily("Inter");
      expect(useUserSettingsStore.getState().settings.uiFontFamily).toBe("Inter");
      useUserSettingsStore.getState().setEditorFontFamily("Cascadia Code");
      expect(useUserSettingsStore.getState().settings.editorFontFamily).toBe(
        "Cascadia Code",
      );
    });

    it("normalizes empty strings to null", () => {
      // An empty family from the dialog's free-form recovery path
      // must not end up storing `""` — the App-effect would set the
      // CSS variable to an invalid value.
      useUserSettingsStore.getState().setUiFontFamily("Inter");
      useUserSettingsStore.getState().setUiFontFamily("");
      expect(useUserSettingsStore.getState().settings.uiFontFamily).toBeNull();
      useUserSettingsStore.getState().setUiFontFamily("   ");
      expect(useUserSettingsStore.getState().settings.uiFontFamily).toBeNull();
    });

    it("does NOT touch the other family", () => {
      useUserSettingsStore.getState().setUiFontFamily("Inter");
      useUserSettingsStore.getState().setEditorFontFamily("JetBrains Mono");
      const s = useUserSettingsStore.getState().settings;
      expect(s.uiFontFamily).toBe("Inter");
      expect(s.editorFontFamily).toBe("JetBrains Mono");
    });
  });

  describe("setUiFontSize / setEditorFontSize clamping", () => {
    it("rounds + clamps UI size to the supported range", () => {
      useUserSettingsStore.getState().setUiFontSize(13.4);
      expect(useUserSettingsStore.getState().settings.uiFontSize).toBe(13);
      useUserSettingsStore.getState().setUiFontSize(13.6);
      expect(useUserSettingsStore.getState().settings.uiFontSize).toBe(14);
      useUserSettingsStore.getState().setUiFontSize(2);
      expect(useUserSettingsStore.getState().settings.uiFontSize).toBe(
        UI_FONT_SIZE_MIN,
      );
      useUserSettingsStore.getState().setUiFontSize(99);
      expect(useUserSettingsStore.getState().settings.uiFontSize).toBe(
        UI_FONT_SIZE_MAX,
      );
    });

    it("rounds + clamps editor size to the supported range", () => {
      useUserSettingsStore.getState().setEditorFontSize(11.2);
      expect(useUserSettingsStore.getState().settings.editorFontSize).toBe(11);
      useUserSettingsStore.getState().setEditorFontSize(0);
      expect(useUserSettingsStore.getState().settings.editorFontSize).toBe(
        EDITOR_FONT_SIZE_MIN,
      );
      useUserSettingsStore.getState().setEditorFontSize(100);
      expect(useUserSettingsStore.getState().settings.editorFontSize).toBe(
        EDITOR_FONT_SIZE_MAX,
      );
    });

    it("falls back to the default when given NaN / Infinity", () => {
      useUserSettingsStore.getState().setUiFontSize(NaN);
      expect(useUserSettingsStore.getState().settings.uiFontSize).toBe(
        DEFAULT_USER_SETTINGS.uiFontSize,
      );
      useUserSettingsStore.getState().setEditorFontSize(Infinity);
      // Infinity isn't finite → goes to default, then default of 14
      // is already in range so it stays 14.
      expect(useUserSettingsStore.getState().settings.editorFontSize).toBe(
        DEFAULT_USER_SETTINGS.editorFontSize,
      );
    });
  });

  describe("resetToDefaults", () => {
    it("clears every customization", () => {
      const s = useUserSettingsStore.getState();
      s.setUiFontFamily("Inter");
      s.setUiFontSize(17);
      s.setEditorFontFamily("Cascadia Code");
      s.setEditorFontSize(20);
      s.resetToDefaults();
      expect(useUserSettingsStore.getState().settings).toEqual(
        DEFAULT_USER_SETTINGS,
      );
    });
  });

  describe("persistence — hydrateUserSettings", () => {
    it("rehydrates a valid persisted blob", () => {
      // Pre-seed localStorage with a hand-crafted blob — mirrors the
      // "user opens the app for the second time" path. We call
      // hydrateUserSettings directly because vitest caches modules
      // across tests so we can't simply re-import the singleton.
      localStorage.setItem(
        "tablio-user-settings",
        JSON.stringify({
          uiFontFamily: "Inter",
          uiFontSize: 15,
          editorFontFamily: "JetBrains Mono",
          editorFontSize: 18,
        }),
      );
      expect(hydrateUserSettings()).toEqual({
        uiFontFamily: "Inter",
        uiFontSize: 15,
        editorFontFamily: "JetBrains Mono",
        editorFontSize: 18,
      });
    });

    it("returns defaults when localStorage is empty", () => {
      expect(hydrateUserSettings()).toEqual(DEFAULT_USER_SETTINGS);
    });

    it("ignores a corrupt blob and falls back to defaults", () => {
      localStorage.setItem("tablio-user-settings", "{not json");
      expect(hydrateUserSettings()).toEqual(DEFAULT_USER_SETTINGS);
    });

    it("ignores fields of the wrong type instead of crashing", () => {
      localStorage.setItem(
        "tablio-user-settings",
        JSON.stringify({
          uiFontFamily: 42, // wrong type → fallback null
          uiFontSize: "thirteen", // wrong type → fallback default
          editorFontFamily: null,
          editorFontSize: 16,
        }),
      );
      const out = hydrateUserSettings();
      expect(out.uiFontFamily).toBeNull();
      expect(out.uiFontSize).toBe(DEFAULT_USER_SETTINGS.uiFontSize);
      expect(out.editorFontFamily).toBeNull();
      expect(out.editorFontSize).toBe(16);
    });

    it("clamps persisted sizes that are outside the supported range", () => {
      // A user manually edited their localStorage to set absurd
      // values, or an old version of the app stored a wider range.
      // The hydrator must bring them back into bounds rather than
      // pass them through.
      localStorage.setItem(
        "tablio-user-settings",
        JSON.stringify({
          uiFontFamily: null,
          uiFontSize: 200,
          editorFontFamily: null,
          editorFontSize: 1,
        }),
      );
      const out = hydrateUserSettings();
      expect(out.uiFontSize).toBe(UI_FONT_SIZE_MAX);
      expect(out.editorFontSize).toBe(EDITOR_FONT_SIZE_MIN);
    });
  });
});
