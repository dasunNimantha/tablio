/**
 * Curated lists of UI / editor fonts plus a runtime check that
 * filters them down to whatever the user's OS actually has
 * installed (issue #62).
 *
 * Why curated rather than free-form: enumerating every font on the
 * system isn't possible from a web context (the proposed
 * `navigator.fonts` API still isn't shipped on every Tauri-target
 * platform), and free-text invites broken values. A short, hand-
 * picked list of common UI sans + popular monospaced families
 * plus a system-default sentinel covers ~95% of what users would
 * pick anyway.
 *
 * Bundled fonts (Fira Sans, JetBrains Mono, Fira Code) always
 * stay in the dropdown even if `document.fonts.check` returns
 * false for some reason — their @font-face declarations live in
 * `src/styles/global.css` so they're guaranteed to be available
 * inside the app regardless of OS state.
 */

export interface FontOption {
  /** Human-readable label shown in the dropdown. */
  label: string;
  /**
   * Family token written into the CSS `font-family` property.
   * `""` is the sentinel that maps to `null` in the store
   * (i.e. "use the bundled default stack").
   */
  family: string;
  /** Bundled = always available; never filtered out. */
  bundled?: boolean;
}

/**
 * Sentinel value used for "no override" / "use defaults". The
 * Preferences dialog renders this as the first option in each
 * dropdown; selecting it persists `null` in the store.
 */
export const SYSTEM_DEFAULT_SENTINEL = "";

export const UI_FONT_CANDIDATES: ReadonlyArray<FontOption> = [
  { label: "System default", family: SYSTEM_DEFAULT_SENTINEL },
  { label: "Fira Sans (bundled)", family: "Fira Sans", bundled: true },
  { label: "Inter", family: "Inter" },
  { label: "Roboto", family: "Roboto" },
  { label: "Segoe UI", family: "Segoe UI" },
  { label: "SF Pro Text", family: "SF Pro Text" },
  { label: "Helvetica Neue", family: "Helvetica Neue" },
  { label: "Arial", family: "Arial" },
  { label: "Ubuntu", family: "Ubuntu" },
  { label: "DejaVu Sans", family: "DejaVu Sans" },
  { label: "Open Sans", family: "Open Sans" },
  { label: "Noto Sans", family: "Noto Sans" },
];

export const EDITOR_FONT_CANDIDATES: ReadonlyArray<FontOption> = [
  { label: "System monospace", family: SYSTEM_DEFAULT_SENTINEL },
  {
    label: "JetBrains Mono (bundled)",
    family: "JetBrains Mono",
    bundled: true,
  },
  { label: "Fira Code (bundled)", family: "Fira Code", bundled: true },
  { label: "Cascadia Code", family: "Cascadia Code" },
  { label: "Cascadia Mono", family: "Cascadia Mono" },
  { label: "Consolas", family: "Consolas" },
  { label: "SF Mono", family: "SF Mono" },
  { label: "Menlo", family: "Menlo" },
  { label: "Monaco", family: "Monaco" },
  { label: "Source Code Pro", family: "Source Code Pro" },
  { label: "Roboto Mono", family: "Roboto Mono" },
  { label: "DejaVu Sans Mono", family: "DejaVu Sans Mono" },
  { label: "Ubuntu Mono", family: "Ubuntu Mono" },
  { label: "IBM Plex Mono", family: "IBM Plex Mono" },
];

/**
 * Probe whether a single font family is available via the CSS
 * Font Loading API. Returns `false` on environments without
 * `document.fonts.check` (e.g. older JSDOM in unit tests).
 *
 * Family names are wrapped in double quotes because CSS expects
 * quoted multi-word family identifiers; passing `Cascadia Code`
 * without quotes makes the API parse it as two separate tokens.
 */
export function isFontAvailable(family: string): boolean {
  if (!family) return true; // sentinel — always offered
  try {
    const fonts = (document as Document & {
      fonts?: { check: (font: string) => boolean };
    }).fonts;
    if (!fonts || typeof fonts.check !== "function") return false;
    return fonts.check(`12px "${family}"`);
  } catch {
    return false;
  }
}

/**
 * Filter a candidate list to the subset actually available on the
 * host system. Bundled fonts pass through unconditionally because
 * @font-face guarantees their presence inside the Tablio renderer.
 * The sentinel "System default" option always passes through too.
 */
export function detectAvailableFonts(
  candidates: ReadonlyArray<FontOption>,
): FontOption[] {
  return candidates.filter((opt) => {
    if (opt.bundled) return true;
    if (opt.family === SYSTEM_DEFAULT_SENTINEL) return true;
    return isFontAvailable(opt.family);
  });
}
