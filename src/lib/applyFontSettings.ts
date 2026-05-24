import type { UserSettings } from "../stores/userSettingsStore";

/**
 * Write the user's font preferences to CSS custom properties on the
 * given root element (issue #62). The whole UI reads from those
 * variables — `var(--font-sans)`, `var(--font-mono)`,
 * `var(--ui-font-size)`, `var(--editor-font-size)` — so a single
 * pass here updates the entire app live, no remount needed.
 *
 * Family handling: when the user picks a non-default family we
 * *prepend* it to the existing bundled fallback chain rather than
 * replacing it outright. Two reasons:
 *
 * 1. If the chosen font becomes unavailable later (uninstalled on
 *    Linux, fallback after an OS update), the bundled stack still
 *    has a sensible monospace / sans to land on.
 * 2. Glyphs missing from the user's font (e.g. an emoji column
 *    title with a font that has no emoji) still resolve through
 *    the fallback chain instead of rendering as tofu.
 *
 * When the user picks the "System default" sentinel (`null` in the
 * store), we remove the CSS variable override so the value declared
 * in `:root` of `global.css` wins again — no flash, no string
 * surgery.
 */

/** Bundled UI fallback chain — must mirror `:root --font-sans`
 *  in `src/styles/global.css` so the apply path produces the same
 *  cascade that the no-override case does. */
const UI_FALLBACK_CHAIN =
  `-apple-system, BlinkMacSystemFont, "Segoe UI", "Helvetica Neue", Arial, sans-serif`;

/** Bundled monospace fallback chain — mirrors `:root --font-mono`. */
const MONO_FALLBACK_CHAIN =
  `"JetBrains Mono", "SF Mono", "Fira Code", ui-monospace, Menlo, Monaco, "Cascadia Mono", monospace`;

export function applyFontSettings(
  settings: UserSettings,
  root: HTMLElement = document.documentElement,
): void {
  // UI family — set the variable when the user has a custom pick,
  // clear it otherwise so the :root declaration wins.
  if (settings.uiFontFamily) {
    root.style.setProperty(
      "--font-sans",
      `"${settings.uiFontFamily}", "Fira Sans", ${UI_FALLBACK_CHAIN}`,
    );
  } else {
    root.style.removeProperty("--font-sans");
  }

  // Editor family — same pattern for monospace.
  if (settings.editorFontFamily) {
    root.style.setProperty(
      "--font-mono",
      `"${settings.editorFontFamily}", ${MONO_FALLBACK_CHAIN}`,
    );
  } else {
    root.style.removeProperty("--font-mono");
  }

  // Sizes are unconditional — the variables always carry a px value,
  // either the user's pick or the default seeded by the store.
  root.style.setProperty("--ui-font-size", `${settings.uiFontSize}px`);
  root.style.setProperty(
    "--editor-font-size",
    `${settings.editorFontSize}px`,
  );
}
