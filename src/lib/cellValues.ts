/**
 * Shared parsing / comparison helpers for editable cell values.
 *
 * The backend sends integers outside JavaScript's safe range and exact
 * NUMERIC/DECIMAL values that don't survive an f64 round-trip as
 * strings holding the exact decimal text. Everything here keeps those
 * values as strings so they are displayed, compared and written back
 * without rounding.
 */

export type ValueCategory =
  | "integer"
  | "exact"
  | "float"
  | "boolean"
  | "json"
  | "uuid"
  | "date"
  | "time"
  | "timestamp"
  | "inet"
  | "text";

const INTEGER_TYPES = new Set([
  "int", "int1", "int2", "int3", "int4", "int8", "integer", "smallint",
  "bigint", "tinyint", "mediumint", "serial", "serial2", "serial4",
  "serial8", "smallserial", "bigserial", "varint", "counter",
]);
const EXACT_TYPES = new Set(["numeric", "decimal", "dec", "fixed"]);
const FLOAT_TYPES = new Set([
  "real", "float", "float4", "float8", "double", "double precision",
]);

/** Strip size/precision arguments and MySQL sign modifiers. */
function baseType(dataType: string | null | undefined): string {
  return (dataType ?? "")
    .toLowerCase()
    .replace(/\(.*?\)/g, " ")
    .replace(/\b(unsigned|signed|zerofill)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function valueCategory(dataType: string | null | undefined): ValueCategory {
  const t = baseType(dataType);
  if (INTEGER_TYPES.has(t)) return "integer";
  if (EXACT_TYPES.has(t)) return "exact";
  if (FLOAT_TYPES.has(t)) return "float";
  if (t === "bool" || t === "boolean") return "boolean";
  if (t === "json" || t === "jsonb") return "json";
  if (t === "uuid" || t === "uniqueidentifier") return "uuid";
  if (t === "date") return "date";
  if (/^time( with(out)? time zone)?$/.test(t) || t === "timetz") return "time";
  if (/^(timestamp|timestamptz|datetime|datetime2|smalldatetime|datetimeoffset)\b/.test(t)) return "timestamp";
  if (t === "inet" || t === "cidr") return "inet";
  return "text";
}

export function isIntegerType(dataType: string | null | undefined): boolean {
  return valueCategory(dataType) === "integer";
}

export function isNumericType(dataType: string | null | undefined): boolean {
  const c = valueCategory(dataType);
  return c === "integer" || c === "exact" || c === "float";
}

/**
 * Text-like columns are the ones where an empty string is a legitimate
 * value distinct from NULL. For every other category "" can't be
 * stored, so clearing the cell means NULL.
 */
export function isTextLike(dataType: string | null | undefined): boolean {
  return valueCategory(dataType) === "text";
}

const INTEGER_RE = /^[-+]?\d+$/;
const DECIMAL_RE = /^[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?$/;
const FLOAT_SPECIAL_RE = /^[-+]?(?:inf|infinity|nan)$/i;

export type ParseResult = { ok: true; value: unknown } | { ok: false; error: string };

/**
 * Convert raw editor text into the value sent to the backend.
 *
 * - Integers inside the safe range become numbers; larger ones stay as
 *   their exact decimal string.
 * - Exact decimals become numbers only when that is lossless, otherwise
 *   the validated text is kept.
 * - Non-numeric input for numeric/boolean columns is rejected instead
 *   of being silently truncated (`parseInt("12abc")` used to give 12).
 * - Every other type is returned exactly as typed (no trimming).
 */
export function parseCellInput(raw: string, dataType: string | null | undefined): ParseResult {
  const cat = valueCategory(dataType);
  const s = raw.trim();
  switch (cat) {
    case "integer": {
      if (!INTEGER_RE.test(s)) return { ok: false, error: `Expected integer, got "${raw}"` };
      const big = BigInt(s);
      if (big <= BigInt(Number.MAX_SAFE_INTEGER) && big >= BigInt(Number.MIN_SAFE_INTEGER)) {
        return { ok: true, value: Number(big) };
      }
      return { ok: true, value: big.toString() };
    }
    case "exact": {
      if (!DECIMAL_RE.test(s)) return { ok: false, error: `Expected number, got "${raw}"` };
      const n = Number(s);
      if (Number.isFinite(n) && String(n) === s) return { ok: true, value: n };
      return { ok: true, value: s.startsWith("+") ? s.slice(1) : s };
    }
    case "float": {
      if (FLOAT_SPECIAL_RE.test(s)) return { ok: true, value: s };
      if (!DECIMAL_RE.test(s)) return { ok: false, error: `Expected number, got "${raw}"` };
      return { ok: true, value: Number(s) };
    }
    case "boolean": {
      const lower = s.toLowerCase();
      if (["true", "t", "1", "yes", "y"].includes(lower)) return { ok: true, value: true };
      if (["false", "f", "0", "no", "n"].includes(lower)) return { ok: true, value: false };
      return { ok: false, error: "Expected boolean (true/false/t/f/1/0)" };
    }
    default:
      return { ok: true, value: raw };
  }
}

export interface EditableColumn {
  data_type: string;
  is_nullable: boolean;
}

/**
 * Convert what a grid cell editor produced into a value to store.
 *
 * An empty string stays an empty string for text-like columns (so ''
 * and NULL remain distinct); for every other type it can't be stored
 * and means NULL when the column allows it. Setting NULL on a text
 * column is an explicit action (the "Set NULL" menu item), never a
 * side effect of clearing the cell.
 */
export function editorInputToValue(raw: unknown, col: EditableColumn): ParseResult {
  if (raw === null || raw === undefined) {
    return col.is_nullable ? { ok: true, value: null } : { ok: false, error: "This column is NOT NULL — value required" };
  }
  const s = typeof raw === "string" ? raw : String(raw);
  if (!isTextLike(col.data_type)) {
    const t = s.trim();
    if (t === "" || t.toLowerCase() === "null") {
      return col.is_nullable ? { ok: true, value: null } : { ok: false, error: "This column is NOT NULL — value required" };
    }
  }
  return parseCellInput(s, col.data_type);
}

/** True for JS numbers and strings that hold a plain decimal number. */
export function isNumericLike(v: unknown): boolean {
  if (typeof v === "number") return Number.isFinite(v);
  if (typeof v === "bigint") return true;
  return typeof v === "string" && DECIMAL_RE.test(v.trim());
}

function splitDecimal(s: string): { neg: boolean; int: string; frac: string } {
  let t = s.trim();
  let neg = false;
  if (t.startsWith("-")) { neg = true; t = t.slice(1); } else if (t.startsWith("+")) t = t.slice(1);
  const [i = "", f = ""] = t.split(".");
  const int = i.replace(/^0+/, "");
  const frac = f.replace(/0+$/, "");
  if (int === "" && frac === "") neg = false;
  return { neg, int, frac };
}

function compareMagnitude(a: { int: string; frac: string }, b: { int: string; frac: string }): number {
  if (a.int.length !== b.int.length) return a.int.length < b.int.length ? -1 : 1;
  if (a.int !== b.int) return a.int < b.int ? -1 : 1;
  const len = Math.max(a.frac.length, b.frac.length);
  const fa = a.frac.padEnd(len, "0");
  const fb = b.frac.padEnd(len, "0");
  if (fa === fb) return 0;
  return fa < fb ? -1 : 1;
}

/** Exact comparison of two numeric values (numbers or decimal strings). */
export function compareNumeric(a: unknown, b: unknown): number {
  if (typeof a === "number" && typeof b === "number") return a === b ? 0 : a < b ? -1 : 1;
  const sa = String(a);
  const sb = String(b);
  if (/e/i.test(sa) || /e/i.test(sb)) {
    const na = Number(sa), nb = Number(sb);
    return na === nb ? 0 : na < nb ? -1 : 1;
  }
  const da = splitDecimal(sa);
  const db = splitDecimal(sb);
  if (da.neg !== db.neg) return da.neg ? -1 : 1;
  const m = compareMagnitude(da, db);
  return da.neg ? -m : m;
}

/**
 * Comparator for client-side sorting. NULLs sort first. Numbers and
 * numeric strings compare numerically when either side is a JS number
 * (a bigint column mixes both) or when the column is known to be
 * numeric; everything else compares as text.
 */
export function compareCellValues(a: unknown, b: unknown, numericColumn = false): number {
  const an = a === null || a === undefined;
  const bn = b === null || b === undefined;
  if (an || bn) return an && bn ? 0 : an ? -1 : 1;
  if (isNumericLike(a) && isNumericLike(b) &&
      (numericColumn || typeof a === "number" || typeof b === "number")) {
    return compareNumeric(a, b);
  }
  if (typeof a === "boolean" && typeof b === "boolean") return a === b ? 0 : a ? 1 : -1;
  const sa = typeof a === "object" ? JSON.stringify(a) : String(a);
  const sb = typeof b === "object" ? JSON.stringify(b) : String(b);
  return sa < sb ? -1 : sa > sb ? 1 : 0;
}

/** Text used for cell display / search. */
export function cellToText(v: unknown): string {
  if (v === null || v === undefined) return "null";
  return typeof v === "object" ? JSON.stringify(v) : String(v);
}
