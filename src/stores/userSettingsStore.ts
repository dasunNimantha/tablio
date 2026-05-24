import { create } from "zustand";

/**
 * Persistent user preferences (issue #62).
 *
 * Currently scoped to font family + size for both the UI shell and
 * the Monaco editor surface, but designed so future prefs (e.g.
 * tab width, vim mode, autosave interval) can slot in without
 * refactoring the storage shape.
 *
 * Family fields use `null` to mean "no override — fall back to the
 * bundled CSS variable stack" so the initial state is indistinguishable
 * from a brand-new install. The Preferences dialog renders this as
 * the "System default" / "System monospace" sentinel option.
 *
 * Persisted to `localStorage` (not session) so settings survive
 * across app restarts — matches the zoom / theme / sidebar-width
 * preferences already in `App.tsx`. Debounced write to keep
 * keystrokes cheap.
 */

export interface UserSettings {
  /** `null` means use the bundled `--font-sans` stack. */
  uiFontFamily: string | null;
  /** UI text size in px. Clamped to [12, 18]. */
  uiFontSize: number;
  /** `null` means use the bundled `--font-mono` stack. */
  editorFontFamily: string | null;
  /** Monaco + result-grid font size in px. Clamped to [11, 22]. */
  editorFontSize: number;
}

export const DEFAULT_USER_SETTINGS: UserSettings = {
  uiFontFamily: null,
  uiFontSize: 13,
  editorFontFamily: null,
  editorFontSize: 14,
};

/**
 * Allowed UI font-size range. Below 12 the sidebar tree node labels
 * start clipping their icons; above 18 the toolbars and tab strip
 * start wrapping. Tested by eye on a 1080p viewport.
 */
export const UI_FONT_SIZE_MIN = 12;
export const UI_FONT_SIZE_MAX = 18;

/**
 * Allowed editor font-size range. Wider than the UI range because
 * Monaco handles small + large gracefully and SQL editing benefits
 * from a wider acceptable band (mobile-style 22 for low-vision use,
 * down to 11 for dense-screen power users).
 */
export const EDITOR_FONT_SIZE_MIN = 11;
export const EDITOR_FONT_SIZE_MAX = 22;

interface State {
  settings: UserSettings;
  setUiFontFamily: (v: string | null) => void;
  setUiFontSize: (v: number) => void;
  setEditorFontFamily: (v: string | null) => void;
  setEditorFontSize: (v: number) => void;
  resetToDefaults: () => void;
}

const STORAGE_KEY = "tablio-user-settings";

function clampUi(v: number): number {
  if (!Number.isFinite(v)) return DEFAULT_USER_SETTINGS.uiFontSize;
  return Math.min(UI_FONT_SIZE_MAX, Math.max(UI_FONT_SIZE_MIN, Math.round(v)));
}

function clampEditor(v: number): number {
  if (!Number.isFinite(v)) return DEFAULT_USER_SETTINGS.editorFontSize;
  return Math.min(
    EDITOR_FONT_SIZE_MAX,
    Math.max(EDITOR_FONT_SIZE_MIN, Math.round(v)),
  );
}

/**
 * Read + validate persisted settings from `localStorage`. Exported
 * so tests can exercise the hydrate path without having to reload
 * the whole module (vitest caches modules across tests).
 */
export function hydrateUserSettings(): UserSettings {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return DEFAULT_USER_SETTINGS;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return DEFAULT_USER_SETTINGS;
    // Defensive: validate every field so a corrupt blob doesn't
    // crash the editor or set absurd sizes.
    return {
      uiFontFamily:
        typeof parsed.uiFontFamily === "string" || parsed.uiFontFamily === null
          ? parsed.uiFontFamily
          : DEFAULT_USER_SETTINGS.uiFontFamily,
      uiFontSize:
        typeof parsed.uiFontSize === "number"
          ? clampUi(parsed.uiFontSize)
          : DEFAULT_USER_SETTINGS.uiFontSize,
      editorFontFamily:
        typeof parsed.editorFontFamily === "string" ||
        parsed.editorFontFamily === null
          ? parsed.editorFontFamily
          : DEFAULT_USER_SETTINGS.editorFontFamily,
      editorFontSize:
        typeof parsed.editorFontSize === "number"
          ? clampEditor(parsed.editorFontSize)
          : DEFAULT_USER_SETTINGS.editorFontSize,
    };
  } catch {
    return DEFAULT_USER_SETTINGS;
  }
}

let _persistTimer: ReturnType<typeof setTimeout> | null = null;
function persist(settings: UserSettings) {
  if (_persistTimer) clearTimeout(_persistTimer);
  _persistTimer = setTimeout(() => {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(settings));
    } catch {
      // localStorage can throw on quota / private-browsing mode —
      // swallow so the in-memory store still works for the session.
    }
  }, 300);
}

/** Trim → null for empty strings so the store doesn't store "" as
 *  a meaningful family value. */
function normalizeFamily(v: string | null): string | null {
  if (v === null) return null;
  const trimmed = v.trim();
  return trimmed.length === 0 ? null : trimmed;
}

export const useUserSettingsStore = create<State>((set) => ({
  settings: hydrateUserSettings(),

  setUiFontFamily: (v) => {
    set((s) => {
      const next: UserSettings = {
        ...s.settings,
        uiFontFamily: normalizeFamily(v),
      };
      persist(next);
      return { settings: next };
    });
  },

  setUiFontSize: (v) => {
    set((s) => {
      const next: UserSettings = { ...s.settings, uiFontSize: clampUi(v) };
      persist(next);
      return { settings: next };
    });
  },

  setEditorFontFamily: (v) => {
    set((s) => {
      const next: UserSettings = {
        ...s.settings,
        editorFontFamily: normalizeFamily(v),
      };
      persist(next);
      return { settings: next };
    });
  },

  setEditorFontSize: (v) => {
    set((s) => {
      const next: UserSettings = {
        ...s.settings,
        editorFontSize: clampEditor(v),
      };
      persist(next);
      return { settings: next };
    });
  },

  resetToDefaults: () => {
    set(() => {
      persist(DEFAULT_USER_SETTINGS);
      return { settings: { ...DEFAULT_USER_SETTINGS } };
    });
  },
}));
