export interface SourceTable {
  schema: string;
  table: string;
}

type Token =
  | { kind: "word"; value: string; upper: string }
  | { kind: "quoted"; value: string }
  | { kind: "string" }
  | { kind: "number" }
  | { kind: "punct"; value: string };

/** Tokenise SQL, dropping comments. Returns null on unterminated input. */
function tokenize(sql: string): Token[] | null {
  const out: Token[] = [];
  let i = 0;
  const n = sql.length;
  while (i < n) {
    const ch = sql[i];
    if (/\s/.test(ch)) { i++; continue; }
    if (ch === "-" && sql[i + 1] === "-") {
      while (i < n && sql[i] !== "\n") i++;
      continue;
    }
    if (ch === "#") {
      while (i < n && sql[i] !== "\n") i++;
      continue;
    }
    if (ch === "/" && sql[i + 1] === "*") {
      const end = sql.indexOf("*/", i + 2);
      if (end === -1) return null;
      i = end + 2;
      continue;
    }
    if (ch === "'") {
      i++;
      for (;;) {
        if (i >= n) return null;
        if (sql[i] === "\\") { i += 2; continue; }
        if (sql[i] === "'") {
          if (sql[i + 1] === "'") { i += 2; continue; }
          i++;
          break;
        }
        i++;
      }
      out.push({ kind: "string" });
      continue;
    }
    if (ch === '"' || ch === "`" || ch === "[") {
      const close = ch === "[" ? "]" : ch;
      let j = i + 1;
      let value = "";
      for (;;) {
        if (j >= n) return null;
        if (sql[j] === close) {
          if (sql[j + 1] === close) { value += close; j += 2; continue; }
          j++;
          break;
        }
        value += sql[j];
        j++;
      }
      out.push({ kind: "quoted", value });
      i = j;
      continue;
    }
    if (/[A-Za-z_\u0080-￿]/.test(ch)) {
      let j = i + 1;
      while (j < n && /[A-Za-z0-9_$\u0080-￿]/.test(sql[j])) j++;
      const value = sql.slice(i, j);
      out.push({ kind: "word", value, upper: value.toUpperCase() });
      i = j;
      continue;
    }
    if (/[0-9]/.test(ch) || (ch === "." && /[0-9]/.test(sql[i + 1] ?? ""))) {
      let j = i + 1;
      while (j < n && /[0-9.eE]/.test(sql[j])) j++;
      out.push({ kind: "number" });
      i = j;
      continue;
    }
    out.push({ kind: "punct", value: ch });
    i++;
  }
  return out;
}

const isWord = (t: Token | undefined, upper: string) =>
  !!t && t.kind === "word" && t.upper === upper;
const isPunct = (t: Token | undefined, value: string) =>
  !!t && t.kind === "punct" && t.value === value;

const RESERVED = new Set([
  "SELECT", "FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET",
  "FETCH", "FOR", "UNION", "INTERSECT", "EXCEPT", "JOIN", "INNER", "LEFT",
  "RIGHT", "FULL", "CROSS", "NATURAL", "OUTER", "ON", "USING", "AS",
  "DISTINCT", "ALL", "TOP", "ONLY", "LATERAL", "INTO", "WINDOW", "VALUES",
  "TABLESAMPLE", "WITH", "AND", "OR", "NOT", "NULL", "CASE", "WHEN", "THEN",
  "ELSE", "END", "IS", "IN", "LIKE", "BETWEEN", "EXISTS", "ANY", "SOME",
  "OVER", "PARTITION", "QUALIFY", "STRAIGHT_JOIN", "SQL_CALC_FOUND_ROWS",
]);

/** Clauses allowed to follow the table reference. */
const TAIL_START = new Set(["WHERE", "ORDER", "LIMIT", "OFFSET", "FETCH", "FOR"]);
/** Top-level keywords that make the result not map 1:1 onto table rows. */
const TAIL_FORBIDDEN = new Set([
  "GROUP", "HAVING", "UNION", "INTERSECT", "EXCEPT", "JOIN", "WINDOW", "INTO",
  "QUALIFY", "FROM",
]);

function identName(t: Token | undefined): string | null {
  if (!t) return null;
  if (t.kind === "quoted") return t.value;
  if (t.kind === "word" && !RESERVED.has(t.upper)) return t.value;
  return null;
}

/**
 * Extract `schema.table` from a single, simple SELECT whose result rows
 * map one-to-one onto rows of that table and whose result columns are
 * plain table columns:
 *
 *   SELECT <* | col, t.col, ...> FROM <table> [[AS] alias]
 *     [WHERE ...] [ORDER BY ...] [LIMIT/OFFSET/FETCH ...] [;]
 *
 * Anything else — multiple statements, expressions / subqueries /
 * functions / literals / aliases in the select list, DISTINCT/TOP,
 * joins or comma joins, GROUP BY / HAVING, set operations, CTEs —
 * returns null so the result is shown read-only. Subqueries inside
 * WHERE are allowed because they only filter rows.
 */
export function parseSimpleSelect(sql: string): SourceTable | null {
  const toks = tokenize(sql);
  if (!toks || toks.length === 0) return null;

  // Exactly one statement: allow a single trailing semicolon only.
  let end = toks.length;
  while (end > 0 && isPunct(toks[end - 1], ";")) end--;
  if (toks.length - end > 1) return null;
  for (let k = 0; k < end; k++) if (isPunct(toks[k], ";")) return null;
  const t = toks.slice(0, end);

  if (!isWord(t[0], "SELECT")) return null;
  let i = 1;

  // Select list up to FROM: only `*`, `col`, `q.col`, `q.*`, optionally
  // `col AS col` (an alias equal to the column name is harmless).
  const qualifiers: string[] = [];
  let sawItem = false;
  for (;;) {
    const tok = t[i];
    if (!tok) return null;
    if (isPunct(tok, "*")) {
      i++;
    } else {
      const first = identName(tok);
      if (first === null) return null;
      i++;
      let colName = first;
      if (isPunct(t[i], ".")) {
        qualifiers.push(first);
        i++;
        if (isPunct(t[i], "*")) {
          i++;
          colName = "*";
        } else {
          const second = identName(t[i]);
          if (second === null) return null;
          if (isPunct(t[i + 1], ".")) return null; // schema.table.col
          colName = second;
          i++;
        }
      }
      if (isWord(t[i], "AS") || (t[i] && identName(t[i]) !== null)) {
        if (isWord(t[i], "AS")) i++;
        const alias = identName(t[i]);
        if (alias === null || colName === "*" || alias.toLowerCase() !== colName.toLowerCase()) return null;
        i++;
      }
    }
    sawItem = true;
    if (isPunct(t[i], ",")) { i++; continue; }
    break;
  }
  if (!sawItem || !isWord(t[i], "FROM")) return null;
  i++;

  // Table reference: table | schema.table (quoted or bare).
  const a = identName(t[i]);
  if (a === null) return null;
  i++;
  let schema = "";
  let table = a;
  if (isPunct(t[i], ".")) {
    const b = identName(t[i + 1]);
    if (b === null) return null;
    schema = a;
    table = b;
    i += 2;
    if (isPunct(t[i], ".")) return null; // db.schema.table
  }

  // Optional alias.
  let alias: string | null = null;
  if (isWord(t[i], "AS")) {
    alias = identName(t[i + 1]);
    if (alias === null) return null;
    i += 2;
  } else if (t[i] && identName(t[i]) !== null) {
    alias = identName(t[i]);
    i++;
  }

  // Column qualifiers must refer to this table.
  for (const q of qualifiers) {
    const ql = q.toLowerCase();
    if (ql !== table.toLowerCase() && (alias === null || ql !== alias.toLowerCase())) return null;
  }

  // What follows the table must be the end or an allowed clause.
  if (i < t.length) {
    const next = t[i];
    if (next.kind !== "word" || !TAIL_START.has(next.upper)) return null;
  }

  // No forbidden keyword at parenthesis depth 0 in the tail.
  let depth = 0;
  for (let k = i; k < t.length; k++) {
    const tok = t[k];
    if (isPunct(tok, "(")) depth++;
    else if (isPunct(tok, ")")) { depth--; if (depth < 0) return null; }
    else if (depth === 0 && tok.kind === "word" && TAIL_FORBIDDEN.has(tok.upper)) return null;
  }
  if (depth !== 0) return null;

  return { schema, table };
}
