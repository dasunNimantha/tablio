import type { Monaco } from "@monaco-editor/react";
import { cssColorToMonacoHex, selectionBackgroundFromAccent } from "./monacoThemeColor";

export function getCssVar(name: string, fallback: string): string {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim() || fallback;
}

export function isLightTheme(): boolean {
  return document.documentElement.getAttribute("data-theme") === "light";
}

const DARK_RULES = [
  { token: "string.sql", foreground: "98c379" },
  { token: "string", foreground: "98c379" },
  { token: "keyword", foreground: "6daaef" },
  { token: "number", foreground: "d19a66" },
  { token: "comment", foreground: "6a737d", fontStyle: "italic" },
  { token: "operator", foreground: "c8ccd4" },
];

const LIGHT_RULES = [
  { token: "string.sql", foreground: "50a14f" },
  { token: "string", foreground: "50a14f" },
  { token: "keyword", foreground: "4078f2" },
  { token: "number", foreground: "986801" },
  { token: "comment", foreground: "a0a1a7", fontStyle: "italic" },
  { token: "operator", foreground: "383a42" },
];

function resolveThemeColors(light: boolean): Record<string, string> {
  const bg = cssColorToMonacoHex(getCssVar("--bg-primary", "#1e1e1e"), "#1e1e1e");
  const bgSurface = cssColorToMonacoHex(getCssVar("--bg-surface", "#252526"), "#252526");
  const textPrimary = cssColorToMonacoHex(getCssVar("--text-primary", "#d4d4d4"), "#d4d4d4");
  const textMuted = cssColorToMonacoHex(getCssVar("--text-muted", "#6e6e7c"), "#6e6e7c");
  const accent = cssColorToMonacoHex(getCssVar("--accent", "#6398ff"), "#6398ff");
  const border = cssColorToMonacoHex(
    getCssVar("--border", "rgba(255,255,255,0.08)"),
    light ? "rgba(0,0,0,0.12)" : "rgba(255,255,255,0.08)",
  );
  const bgHover = cssColorToMonacoHex(
    getCssVar("--bg-hover", "rgba(255,255,255,0.07)"),
    light ? "rgba(0,0,0,0.04)" : "rgba(255,255,255,0.07)",
  );
  const accentMuted = cssColorToMonacoHex(
    getCssVar("--accent-muted", "rgba(99,152,255,0.14)"),
    "rgba(99,152,255,0.14)",
  );

  return {
    "editor.background": bg,
    "editor.foreground": textPrimary,
    "editorLineNumber.foreground": textMuted,
    "editor.selectionBackground": selectionBackgroundFromAccent(getCssVar("--accent", "#6398ff"), "#6398ff"),
    "editorWidget.background": bgSurface,
    "editorWidget.border": border,
    "editorSuggestWidget.background": bgSurface,
    "editorSuggestWidget.border": border,
    "editorSuggestWidget.foreground": textPrimary,
    "editorSuggestWidget.highlightForeground": accent,
    "editorSuggestWidget.selectedBackground": accentMuted,
    "editorSuggestWidget.selectedForeground": textPrimary,
    "editorSuggestWidget.focusHighlightForeground": accent,
    "editorHoverWidget.background": bgSurface,
    "editorHoverWidget.border": border,
    "editorHoverWidget.foreground": textPrimary,
    "editorHoverWidget.statusBarBackground": bgSurface,
    "editorMarkerNavigation.background": bgSurface,
    "editorMarkerNavigationError.background": bgSurface,
    "editorMarkerNavigationError.headerBackground": bgSurface,
    "editorMarkerNavigationWarning.background": bgSurface,
    "editorMarkerNavigationWarning.headerBackground": bgSurface,
    "editorMarkerNavigationInfo.background": bgSurface,
    "editorMarkerNavigationInfo.headerBackground": bgSurface,
    "list.hoverBackground": bgHover,
    "list.hoverForeground": textPrimary,
    "list.focusBackground": accentMuted,
    "list.focusForeground": textPrimary,
    "list.highlightForeground": accent,
  };
}

/**
 * Register or update the tablio-dark / tablio-light Monaco themes and return
 * the name that matches the current app theme.
 */
export function syncMonacoTheme(monaco: Monaco, version: number): string {
  const light = isLightTheme();
  const colors = resolveThemeColors(light);
  const darkName = `tablio-dark-${version}`;
  const lightName = `tablio-light-${version}`;

  monaco.editor.defineTheme(darkName, {
    base: "vs-dark", inherit: true, rules: DARK_RULES, colors,
  });
  monaco.editor.defineTheme(lightName, {
    base: "vs", inherit: true, rules: LIGHT_RULES, colors,
  });

  return light ? lightName : darkName;
}
