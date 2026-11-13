/**
 * Helpers shared by the table grid (DataGrid) and the query result grid
 * (ResultTable): column ids, the text a cell shows and edits, editor
 * choice, keyboard handling and row export.
 */

import { valueCategory } from "./cellValues";

/**
 * ag-grid column id / row-object key for the column at `index`.
 *
 * Grids address columns by position, never by name: ag-grid treats a
 * `field` containing "." as a nested path (so `"meta.source"` or an
 * auto-named `price*1.1` read as undefined), and query results may
 * repeat a name (`SELECT a.id, b.id`).
 */
export function colField(index: number): string {
  return `c${index}`;
}

/** Inverse of {@link colField}; -1 for ids that aren't data columns. */
export function colIndexFromId(colId: string | null | undefined): number {
  if (!colId) return -1;
  const m = /^c(\d+)$/.exec(colId);
  return m ? Number(m[1]) : -1;
}

/**
 * The text a cell displays and its editor starts with.
 *
 * Strings are shown as-is, which includes JSON/JSONB columns: the
 * backend sends them as their raw JSON text. Structured values that
 * still arrive as objects/arrays (Cassandra maps, UDTs, tuples) are
 * shown as JSON. NULL stays NULL so callers can render it distinctly.
 */
export function cellEditText(v: unknown): string | null {
  if (v === null || v === undefined) return null;
  if (typeof v === "string") return v;
  if (typeof v === "object") {
    try {
      return JSON.stringify(v);
    } catch {
      return String(v);
    }
  }
  return String(v);
}

/**
 * Textarea values always use "\n" line breaks, so editing a value that
 * used "\r\n" would silently rewrite every line ending. When the
 * original used CRLF and the edited text has no "\r" of its own, put
 * the CRLF line endings back.
 */
export function restoreLineEndings(original: unknown, edited: string): string {
  if (typeof original !== "string" || !original.includes("\r\n")) return edited;
  if (edited.includes("\r")) return edited;
  return edited.replace(/\n/g, "\r\n");
}

const LONG_TEXT_TYPES = /^(text|tinytext|mediumtext|longtext|ntext|citext|clob|nclob|xml)$/;

/** Values longer than this are edited in the multi-line editor. */
export const LARGE_TEXT_THRESHOLD = 80;

/**
 * Whether a cell should be edited in the multi-line popup editor
 * instead of ag-grid's single-line input (which drops newlines).
 */
export function usesLargeTextEditor(dataType: string | null | undefined, value: unknown): boolean {
  if (valueCategory(dataType) === "json") return true;
  const t = (dataType ?? "").toLowerCase().replace(/\(.*?\)/g, "").trim();
  if (LONG_TEXT_TYPES.test(t)) return true;
  if (value !== null && typeof value === "object") return true;
  if (typeof value === "string") {
    return value.includes("\n") || value.includes("\r") || value.length > LARGE_TEXT_THRESHOLD;
  }
  return false;
}

/** Editor params for ag-grid's large text editor (default maxLength is 200). */
export const LARGE_TEXT_EDITOR_PARAMS = { maxLength: 10_000_000, rows: 10, cols: 60 };

export interface GridKeyInfo {
  editing: boolean;
  key: string;
  /** True when one or more rows are multi-selected. */
  hasRowSelection: boolean;
}

/**
 * ag-grid `suppressKeyboardEvent`: which keys the grid must ignore.
 *
 * Enter / F2 / typing start editing the focused cell as usual. Delete
 * and Backspace are left to the grid's own row-delete shortcut only
 * while rows are selected; otherwise they clear / start editing the
 * focused cell as ag-grid does by default.
 */
export function shouldSuppressGridKey({ editing, key, hasRowSelection }: GridKeyInfo): boolean {
  if (editing) return false;
  return (key === "Delete" || key === "Backspace") && hasRowSelection;
}

/**
 * Unique keys for a list of column names, for exporting a row as an
 * object: repeated names get a numeric suffix (`id`, `id_2`, ...) that
 * doesn't collide with another real column.
 */
export function uniqueColumnKeys(names: string[]): string[] {
  const taken = new Set(names);
  const used = new Set<string>();
  return names.map((name) => {
    if (!used.has(name)) {
      used.add(name);
      return name;
    }
    let n = 2;
    while (used.has(`${name}_${n}`) || taken.has(`${name}_${n}`)) n++;
    const key = `${name}_${n}`;
    used.add(key);
    return key;
  });
}

/** A row as a plain object keyed by (disambiguated) column names. */
export function rowToObject(names: string[], row: unknown[]): Record<string, unknown> {
  const keys = uniqueColumnKeys(names);
  const obj: Record<string, unknown> = {};
  keys.forEach((k, i) => { obj[k] = row[i]; });
  return obj;
}
