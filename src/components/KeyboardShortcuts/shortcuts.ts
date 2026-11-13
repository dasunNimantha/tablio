export interface Shortcut {
  keys: string[];
  description: string;
}

export interface ShortcutGroup {
  title: string;
  shortcuts: Shortcut[];
}

/**
 * Every shortcut listed here must be wired up somewhere:
 * - Query editor: Monaco actions in QueryConsole (Ctrl+Enter, Ctrl+S,
 *   Escape) plus Monaco's built-in Ctrl+F / Ctrl+/.
 * - Tabs: Ctrl+N / Ctrl+W / Ctrl+Shift+F (tab bar and query console).
 * - Window: App.tsx (Ctrl+?, zoom keys).
 */
export const SHORTCUT_GROUPS: ShortcutGroup[] = [
  {
    title: "Query editor",
    shortcuts: [
      { keys: ["Ctrl", "Enter"], description: "Run query" },
      { keys: ["Esc"], description: "Stop the running query" },
      { keys: ["Ctrl", "S"], description: "Save query" },
      { keys: ["Ctrl", "Shift", "F"], description: "Format SQL" },
      { keys: ["Ctrl", "F"], description: "Find in editor" },
      { keys: ["Ctrl", "/"], description: "Toggle comment" },
    ],
  },
  {
    title: "Tabs",
    shortcuts: [
      { keys: ["Ctrl", "N"], description: "New query tab" },
      { keys: ["Ctrl", "W"], description: "Close tab" },
    ],
  },
  {
    title: "Explorer",
    shortcuts: [
      { keys: ["↑", "↓"], description: "Move between items" },
      { keys: ["→", "←"], description: "Expand / collapse" },
      { keys: ["Enter"], description: "Open table or connect" },
      { keys: ["Shift", "F10"], description: "Open context menu" },
      { keys: ["F2"], description: "Rename folder" },
    ],
  },
  {
    title: "Window",
    shortcuts: [
      { keys: ["Ctrl", "?"], description: "Show keyboard shortcuts" },
      { keys: ["Ctrl", "="], description: "Zoom in" },
      { keys: ["Ctrl", "-"], description: "Zoom out" },
      { keys: ["Ctrl", "0"], description: "Reset zoom" },
      { keys: ["Esc"], description: "Close dialog or menu" },
    ],
  },
];

/** On macOS the Ctrl modifier is shown as ⌘ (the handlers accept both). */
export function displayKey(key: string, isMac: boolean): string {
  return isMac && key === "Ctrl" ? "⌘" : key;
}
