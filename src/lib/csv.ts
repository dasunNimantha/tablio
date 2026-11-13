/**
 * RFC 4180 CSV parsing for the import dialog.
 *
 * The parser walks the whole text character by character so quoted fields
 * may contain the delimiter, doubled quotes (`""`) and line breaks. It also
 * accepts CRLF / LF / CR line endings, strips a leading UTF-8 BOM, and
 * takes a custom delimiter (`;`, tab, `|`, …).
 *
 * Unquoted fields are trimmed (the dialog always did this and spreadsheets
 * often write `a, b`); quoted fields are kept verbatim. Lines that are
 * completely empty are skipped.
 */
export interface CsvParseResult {
  headers: string[];
  rows: string[][];
}

export const CSV_DELIMITERS = [",", ";", "\t", "|"] as const;

export function parseCsvRecords(text: string, delimiter = ","): string[][] {
  if (delimiter.length !== 1 || delimiter === '"' || delimiter === "\n" || delimiter === "\r") {
    throw new Error("Delimiter must be a single character other than a quote or line break.");
  }
  let src = text;
  if (src.charCodeAt(0) === 0xfeff) src = src.slice(1);

  const records: string[][] = [];
  let record: string[] = [];
  let field = "";
  let quoted = false; // current field started with a quote
  let inQuotes = false; // currently inside the quoted section
  let fieldStarted = false;

  const endField = () => {
    record.push(quoted ? field : field.trim());
    field = "";
    quoted = false;
    fieldStarted = false;
  };
  const endRecord = () => {
    endField();
    // Skip blank lines (a record that is one empty, unquoted field).
    if (!(record.length === 1 && record[0] === "" && !quotedEmptyRecord)) records.push(record);
    record = [];
    quotedEmptyRecord = false;
  };
  let quotedEmptyRecord = false;

  for (let i = 0; i < src.length; i++) {
    const ch = src[i];
    if (inQuotes) {
      if (ch === '"') {
        if (src[i + 1] === '"') {
          field += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        field += ch;
      }
      continue;
    }
    if (ch === '"' && !fieldStarted) {
      // Opening quote (leading whitespace before it is ignored).
      quoted = true;
      inQuotes = true;
      fieldStarted = true;
      field = "";
      if (record.length === 0) quotedEmptyRecord = true;
      continue;
    }
    if (ch === delimiter) {
      endField();
      quotedEmptyRecord = false;
      continue;
    }
    if (ch === "\r" || ch === "\n") {
      if (ch === "\r" && src[i + 1] === "\n") i++;
      endRecord();
      continue;
    }
    if (quoted) {
      // Whitespace between a closing quote and the delimiter is padding;
      // other stray text (`"a"b`) is kept rather than silently dropped.
      if (ch !== " " && ch !== "\t") field += ch;
      continue;
    }
    if (ch === " " || ch === "\t") {
      field += ch;
      continue;
    }
    fieldStarted = true;
    quotedEmptyRecord = false;
    field += ch;
  }
  if (inQuotes) {
    throw new Error("The file ends inside a quoted field (unbalanced \").");
  }
  if (field !== "" || fieldStarted || quoted || record.length > 0) endRecord();
  return records;
}

/** Picks the most likely delimiter from the first record (outside quotes). */
export function detectCsvDelimiter(text: string): string {
  const counts = new Map<string, number>(CSV_DELIMITERS.map((d) => [d, 0]));
  let inQuotes = false;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (ch === '"') {
      if (inQuotes && text[i + 1] === '"') {
        i++;
        continue;
      }
      inQuotes = !inQuotes;
      continue;
    }
    if (!inQuotes && (ch === "\n" || ch === "\r")) break;
    if (!inQuotes && counts.has(ch)) counts.set(ch, counts.get(ch)! + 1);
  }
  let best = ",";
  let bestCount = 0;
  for (const [d, n] of counts) {
    if (n > bestCount) {
      best = d;
      bestCount = n;
    }
  }
  return best;
}

/**
 * Makes source column names usable as unique keys: blank headers become
 * `column_N` (1-based position) and repeats get a `_2`, `_3`, … suffix.
 */
export function normalizeHeaders(headers: string[]): string[] {
  const used = new Set<string>();
  return headers.map((raw, i) => {
    const base = raw.trim() === "" ? `column_${i + 1}` : raw;
    let name = base;
    let n = 2;
    while (used.has(name.toLowerCase())) name = `${base}_${n++}`;
    used.add(name.toLowerCase());
    return name;
  });
}

export function parseCsv(text: string, delimiter?: string): CsvParseResult {
  const records = parseCsvRecords(text, delimiter ?? detectCsvDelimiter(text));
  if (records.length === 0) return { headers: [], rows: [] };
  const [first, ...rows] = records;
  return { headers: normalizeHeaders(first), rows };
}
