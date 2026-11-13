use anyhow::Result;
use rust_xlsxwriter::{Format, Workbook};
use serde_json::Value;

/// Render rows as CSV (RFC 4180, `\n` line ends).
///
/// * `NULL` is an empty field and an empty string is `""`, so the two
///   stay distinguishable (most importers read an unquoted empty field
///   as NULL / missing).
/// * Text that a spreadsheet would run as a formula (starting with `=`,
///   `+`, `-`, `@`, tab or carriage return) is prefixed with `'`, the
///   usual CSV-injection guard: opening an export in Excel / Sheets must
///   not run `=HYPERLINK(...)` or `@SUM(...)` from the data. Plain
///   numbers such as `-12.5` (drivers send exact decimals as strings)
///   are left alone. Column names get the same treatment.
pub fn to_csv(columns: &[String], rows: &[Vec<Value>]) -> String {
    let mut out = String::new();

    out.push_str(
        &columns
            .iter()
            .map(|c| csv_text_field(c))
            .collect::<Vec<_>>()
            .join(","),
    );
    out.push('\n');

    for row in rows {
        let line: Vec<String> = row
            .iter()
            .map(|v| match v {
                Value::Null => String::new(),
                Value::String(s) => csv_text_field(s),
                Value::Bool(b) => b.to_string(),
                Value::Number(n) => n.to_string(),
                _ => csv_text_field(&v.to_string()),
            })
            .collect();
        out.push_str(&line.join(","));
        out.push('\n');
    }
    out
}

/// A text value as a CSV field: formula-neutralised, quoted when needed,
/// and `""` when empty.
fn csv_text_field(s: &str) -> String {
    if s.is_empty() {
        return "\"\"".to_string();
    }
    escape_csv(&neutralize_formula(s))
}

/// Prefix `s` with `'` when a spreadsheet would evaluate it as a formula.
fn neutralize_formula(s: &str) -> std::borrow::Cow<'_, str> {
    let risky = s
        .chars()
        .next()
        .is_some_and(|c| matches!(c, '=' | '+' | '-' | '@' | '\t' | '\r'));
    if risky && !is_numeric_literal(s) {
        std::borrow::Cow::Owned(format!("'{s}"))
    } else {
        std::borrow::Cow::Borrowed(s)
    }
}

/// Render rows as a JSON array of objects.
///
/// Keys follow the result's column order (not sorted), and a column
/// name that repeats (`SELECT a.id, b.id ...`) gets a `_2`, `_3`, ...
/// suffix instead of overwriting the earlier value.
pub fn to_json(columns: &[String], rows: &[Vec<Value>]) -> String {
    if rows.is_empty() {
        return "[]".to_string();
    }
    let keys: Vec<String> = unique_keys(columns)
        .iter()
        .map(|k| serde_json::to_string(k).unwrap_or_else(|_| "\"\"".to_string()))
        .collect();
    let mut out = String::from("[\n");
    for (ri, row) in rows.iter().enumerate() {
        out.push_str("  {");
        let mut first = true;
        for (key, val) in keys.iter().zip(row.iter()) {
            out.push_str(if first { "\n" } else { ",\n" });
            first = false;
            let rendered = serde_json::to_string_pretty(val)
                .unwrap_or_else(|_| "null".to_string())
                .replace('\n', "\n    ");
            out.push_str("    ");
            out.push_str(key);
            out.push_str(": ");
            out.push_str(&rendered);
        }
        out.push_str(if first { "}" } else { "\n  }" });
        out.push_str(if ri + 1 < rows.len() { ",\n" } else { "\n" });
    }
    out.push(']');
    out
}

/// Column names made unique: a repeat becomes `name_2`, `name_3`, ...
/// (skipping suffixes that are themselves column names).
fn unique_keys(columns: &[String]) -> Vec<String> {
    let all: std::collections::HashSet<&str> = columns.iter().map(String::as_str).collect();
    let mut used: std::collections::HashSet<String> = std::collections::HashSet::new();
    columns
        .iter()
        .map(|c| {
            if used.insert(c.clone()) {
                return c.clone();
            }
            let mut n = 2;
            loop {
                let candidate = format!("{c}_{n}");
                if !all.contains(candidate.as_str()) && used.insert(candidate.clone()) {
                    return candidate;
                }
                n += 1;
            }
        })
        .collect()
}

/// SQL flavour an `INSERT` export is written for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SqlExportDialect {
    /// `"ident"` quoting, standard string literals. Used when the target
    /// engine is unknown (query results).
    #[default]
    Standard,
    Postgres,
    /// MySQL / MariaDB / TiDB: `` `ident` `` and backslash-escaped strings.
    MySql,
    Sqlite,
    /// SQL Server: `[ident]`, `N'...'` strings.
    Mssql,
    Cassandra,
}

impl SqlExportDialect {
    pub fn for_db(db_type: &crate::models::DbType) -> Self {
        use crate::models::DbType;
        match db_type {
            DbType::Postgres | DbType::Cockroachdb => Self::Postgres,
            DbType::Mysql | DbType::Mariadb | DbType::Tidb => Self::MySql,
            DbType::Sqlite => Self::Sqlite,
            DbType::Mssql => Self::Mssql,
            DbType::Cassandra => Self::Cassandra,
        }
    }

    fn ident(self, name: &str) -> String {
        match self {
            Self::MySql => format!("`{}`", name.replace('`', "``")),
            Self::Mssql => format!("[{}]", name.replace(']', "]]")),
            _ => format!("\"{}\"", name.replace('"', "\"\"")),
        }
    }

    fn string(self, s: &str) -> String {
        match self {
            // Default sql_mode treats `\` as an escape character.
            Self::MySql => format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''")),
            Self::Mssql => format!("N'{}'", s.replace('\'', "''")),
            _ => format!("'{}'", s.replace('\'', "''")),
        }
    }

    /// Literal for binary data given as lowercase / uppercase hex digits.
    fn binary(self, hex: &str) -> String {
        match self {
            Self::Postgres => format!("'\\x{hex}'::bytea"),
            Self::Mssql | Self::Cassandra => format!("0x{hex}"),
            Self::Standard | Self::MySql | Self::Sqlite => format!("X'{hex}'"),
        }
    }
}

/// How a column's values are written in an `INSERT` export.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SqlColumnKind {
    #[default]
    Other,
    /// Numeric strings (big integers, exact decimals) are written bare.
    Numeric,
    /// Values the drivers render as hex text (`0x..`, `X'..'`, `\x..`)
    /// are written as the dialect's binary literal.
    Binary,
}

impl SqlColumnKind {
    pub fn for_type(data_type: &str) -> Self {
        if is_binary_type(data_type) {
            Self::Binary
        } else if is_numeric_type(data_type) {
            Self::Numeric
        } else {
            Self::Other
        }
    }
}

/// Render rows as `INSERT INTO ... VALUES (...);` statements.
///
/// `schema` is optional: when provided the output emits a fully
/// qualified `"schema"."table"` reference so the dump can be replayed
/// in a database that doesn't have a matching `search_path` /
/// `current_schema`.
///
/// This is the dialect-neutral form (`"…"` identifiers, standard string
/// literals) used for query results, whose target engine and column
/// types are unknown; table exports use [`to_sql_inserts_for`].
pub fn to_sql_inserts(
    schema: Option<&str>,
    table_name: &str,
    columns: &[String],
    rows: &[Vec<Value>],
) -> String {
    to_sql_inserts_for(
        SqlExportDialect::Standard,
        schema,
        table_name,
        columns,
        &[],
        rows,
    )
}

/// True for column types whose values can be written as bare SQL numeric
/// literals.
pub fn is_numeric_type(data_type: &str) -> bool {
    let t = data_type.to_ascii_lowercase();
    // `interval`/`point` contain "int" but aren't numbers.
    if t.contains("interval") || t.contains("point") {
        return false;
    }
    [
        "int", "serial", "numeric", "decimal", "real", "double", "float", "number", "dec(",
    ]
    .iter()
    .any(|k| t.contains(k))
        || t == "dec"
}

/// True for binary column types (`bytea`, `blob`, `varbinary(16)`, ...).
pub fn is_binary_type(data_type: &str) -> bool {
    let lower = data_type.to_ascii_lowercase();
    let base = lower.split('(').next().unwrap_or("").trim();
    matches!(
        base,
        "bytea"
            | "blob"
            | "tinyblob"
            | "mediumblob"
            | "longblob"
            | "binary"
            | "varbinary"
            | "image"
    )
}

/// The hex digits of a binary value as the drivers render it (`0xdead`,
/// `X'dead'`, `\xdead`), or `None` when `s` isn't in one of those forms.
fn binary_hex(s: &str) -> Option<&str> {
    let hex = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("\\x"))
        .or_else(|| {
            s.strip_prefix("X'")
                .or_else(|| s.strip_prefix("x'"))
                .and_then(|r| r.strip_suffix('\''))
        })?;
    (hex.len() % 2 == 0 && hex.bytes().all(|b| b.is_ascii_hexdigit())).then_some(hex)
}

/// A plain decimal literal (`-12`, `3.50`, `1e10`), safe to emit unquoted.
fn is_numeric_literal(s: &str) -> bool {
    crate::util::sql::numeric_literal(s).is_some_and(|t| t.len() == s.len())
}

/// Like [`to_sql_inserts`], with per-column `numeric` flags. Drivers send
/// integers beyond 2^53 and exact decimals as JSON strings to avoid
/// precision loss; for numeric columns those are emitted unquoted so the
/// literal keeps its exact value and type. Anything else stays quoted.
pub fn to_sql_inserts_typed(
    schema: Option<&str>,
    table_name: &str,
    columns: &[String],
    numeric: &[bool],
    rows: &[Vec<Value>],
) -> String {
    let kinds: Vec<SqlColumnKind> = numeric
        .iter()
        .map(|n| {
            if *n {
                SqlColumnKind::Numeric
            } else {
                SqlColumnKind::Other
            }
        })
        .collect();
    to_sql_inserts_for(
        SqlExportDialect::Standard,
        schema,
        table_name,
        columns,
        &kinds,
        rows,
    )
}

/// `INSERT` statements in `dialect`, with per-column value handling
/// (`kinds`; missing entries are [`SqlColumnKind::Other`]).
pub fn to_sql_inserts_for(
    dialect: SqlExportDialect,
    schema: Option<&str>,
    table_name: &str,
    columns: &[String],
    kinds: &[SqlColumnKind],
    rows: &[Vec<Value>],
) -> String {
    let mut out = String::new();
    let cols_str = columns
        .iter()
        .map(|c| dialect.ident(c))
        .collect::<Vec<_>>()
        .join(", ");

    let qualified = match schema {
        Some(s) if !s.is_empty() => format!("{}.{}", dialect.ident(s), dialect.ident(table_name)),
        _ => dialect.ident(table_name),
    };

    for row in rows {
        let vals: Vec<String> = row
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let kind = kinds.get(i).copied().unwrap_or_default();
                match v {
                    Value::Null => "NULL".to_string(),
                    Value::Bool(b) => b.to_string(),
                    Value::Number(n) => n.to_string(),
                    Value::String(s) if kind == SqlColumnKind::Numeric && is_numeric_literal(s) => {
                        s.clone()
                    }
                    Value::String(s) if kind == SqlColumnKind::Binary => match binary_hex(s) {
                        Some(hex) => dialect.binary(hex),
                        None => dialect.string(s),
                    },
                    Value::String(s) => dialect.string(s),
                    _ => dialect.string(&v.to_string()),
                }
            })
            .collect();

        out.push_str(&format!(
            "INSERT INTO {} ({}) VALUES ({});\n",
            qualified,
            cols_str,
            vals.join(", ")
        ));
    }
    out
}

/// Excel sheet-name rules: max 31 chars, may not contain `: \ / ? * [ ]`.
/// An empty/whitespace-only input falls back to `"Result"` so we never
/// emit a workbook with an unnamed sheet.
pub fn sanitize_sheet_name(raw: &str) -> String {
    let cleaned: String = raw
        .chars()
        .filter(|c| !matches!(c, ':' | '\\' | '/' | '?' | '*' | '[' | ']'))
        .collect();
    let trimmed = cleaned.trim();
    if trimmed.is_empty() {
        return "Result".to_string();
    }
    // Truncate by char count, not bytes, to avoid splitting multi-byte UTF-8.
    let truncated: String = trimmed.chars().take(31).collect();
    truncated
}

/// Render rows as an `.xlsx` workbook, returning the serialized bytes.
///
/// Type fidelity (the headline ask in #69 / #70):
/// - `Value::Number` writes as a numeric cell (not stringified), except
///   integers beyond ±2^53, which Excel's doubles can't hold exactly and
///   are written as text
/// - `Value::Bool` writes as a boolean cell
/// - `Value::Null` writes nothing (blank cell)
/// - `Value::String` that parses as ISO-8601 date / datetime writes as
///   an Excel datetime with `yyyy-mm-dd hh:mm:ss` formatting. Values with
///   a UTC offset (`timestamptz`) are converted to UTC and formatted with
///   a literal `UTC` suffix. Any other string, including numeric strings
///   that drivers use for big integers and exact decimals, stays text so
///   no digits are lost
/// - `Value::Array` / `Value::Object` fall back to their JSON
///   representation since Excel has no native equivalent
///
/// Calls `autofit()` once at the end so column widths track the longest
/// rendered value without a per-cell pass.
///
/// Excel cells hold at most 32,767 characters. Longer text is cut to fit
/// and ends with [`XLSX_TRUNCATION_MARKER`] instead of failing the whole
/// export; how many cells were cut is returned in [`XlsxExport`].
pub fn to_xlsx(columns: &[String], rows: &[Vec<Value>], sheet_name: &str) -> Result<XlsxExport> {
    let mut workbook = Workbook::new();
    let safe_name = sanitize_sheet_name(sheet_name);
    let datetime_fmt = Format::new().set_num_format("yyyy-mm-dd hh:mm:ss");
    let datetime_utc_fmt = Format::new().set_num_format("yyyy-mm-dd hh:mm:ss \"UTC\"");
    let header_fmt = Format::new().set_bold();
    let mut truncated_cells = 0usize;

    let worksheet = workbook.add_worksheet();
    worksheet.set_name(&safe_name)?;

    for (col_idx, col) in columns.iter().enumerate() {
        let text = fit_xlsx_text(col, &mut truncated_cells);
        worksheet.write_string_with_format(0, col_idx as u16, text.as_ref(), &header_fmt)?;
    }

    for (row_idx, row) in rows.iter().enumerate() {
        let excel_row = (row_idx + 1) as u32;
        for (col_idx, val) in row.iter().enumerate() {
            write_typed_cell(
                worksheet,
                excel_row,
                col_idx as u16,
                val,
                &datetime_fmt,
                &datetime_utc_fmt,
                &mut truncated_cells,
            )?;
        }
    }

    worksheet.autofit();
    let bytes = workbook.save_to_buffer()?;
    Ok(XlsxExport {
        bytes,
        truncated_cells,
    })
}

/// A rendered workbook.
#[derive(Debug)]
pub struct XlsxExport {
    pub bytes: Vec<u8>,
    /// Cells whose text exceeded Excel's limit and was cut.
    pub truncated_cells: usize,
}

impl XlsxExport {
    /// User-facing warning about truncated cells, if any.
    pub fn warning(&self) -> Option<String> {
        (self.truncated_cells > 0).then(|| {
            format!(
                "{} cell(s) held more than {} characters, Excel's limit, and were cut short \
                 (marked \"{}\"). Export as CSV or JSON to keep the full text.",
                self.truncated_cells,
                XLSX_MAX_CELL_CHARS,
                XLSX_TRUNCATION_MARKER.trim()
            )
        })
    }
}

/// Excel's per-cell text limit, in UTF-16 code units.
pub const XLSX_MAX_CELL_CHARS: usize = 32_767;
/// Appended to text cut to fit [`XLSX_MAX_CELL_CHARS`].
pub const XLSX_TRUNCATION_MARKER: &str = " [truncated]";

/// `s`, cut to fit an Excel cell (with the marker) when it is too long.
fn fit_xlsx_text<'a>(s: &'a str, truncated: &mut usize) -> std::borrow::Cow<'a, str> {
    if s.encode_utf16().count() <= XLSX_MAX_CELL_CHARS {
        return std::borrow::Cow::Borrowed(s);
    }
    *truncated += 1;
    let budget = XLSX_MAX_CELL_CHARS - XLSX_TRUNCATION_MARKER.encode_utf16().count();
    let mut used = 0;
    let mut end = 0;
    for (idx, ch) in s.char_indices() {
        if used + ch.len_utf16() > budget {
            break;
        }
        used += ch.len_utf16();
        end = idx + ch.len_utf8();
    }
    std::borrow::Cow::Owned(format!("{}{}", &s[..end], XLSX_TRUNCATION_MARKER))
}

/// Largest integer magnitude Excel (an IEEE double) represents exactly.
const MAX_EXACT_F64_INT: u64 = 1 << 53;

fn write_typed_cell(
    worksheet: &mut rust_xlsxwriter::Worksheet,
    row: u32,
    col: u16,
    val: &Value,
    datetime_fmt: &Format,
    datetime_utc_fmt: &Format,
    truncated: &mut usize,
) -> Result<()> {
    match val {
        Value::Null => {
            worksheet.write_blank(row, col, &Format::default())?;
        }
        Value::Bool(b) => {
            worksheet.write_boolean(row, col, *b)?;
        }
        Value::Number(n) => {
            // Excel stores every number as f64. Integers beyond 2^53 would
            // be silently rounded, so they are written as text instead.
            if let Some(i) = n.as_i64() {
                if i.unsigned_abs() <= MAX_EXACT_F64_INT {
                    worksheet.write_number(row, col, i as f64)?;
                } else {
                    worksheet.write_string(row, col, i.to_string())?;
                }
            } else if let Some(u) = n.as_u64() {
                if u <= MAX_EXACT_F64_INT {
                    worksheet.write_number(row, col, u as f64)?;
                } else {
                    worksheet.write_string(row, col, u.to_string())?;
                }
            } else if let Some(f) = n.as_f64() {
                worksheet.write_number(row, col, f)?;
            } else {
                worksheet.write_string(row, col, n.to_string())?;
            }
        }
        Value::String(s) => match parse_iso_datetime(s) {
            // Numeric strings (big integers, exact decimals) fall through
            // to a text cell, which keeps every digit.
            Some(IsoDateTime::Naive(dt)) => {
                worksheet.write_datetime_with_format(row, col, dt, datetime_fmt)?;
            }
            Some(IsoDateTime::Utc(dt)) => {
                worksheet.write_datetime_with_format(row, col, dt, datetime_utc_fmt)?;
            }
            None => {
                worksheet.write_string(row, col, fit_xlsx_text(s, truncated).as_ref())?;
            }
        },
        // No native Excel cell type for arrays / objects; fall back to
        // the JSON representation so the data isn't silently dropped.
        Value::Array(_) | Value::Object(_) => {
            let text = val.to_string();
            worksheet.write_string(row, col, fit_xlsx_text(&text, truncated).as_ref())?;
        }
    }
    Ok(())
}

#[derive(Debug, PartialEq)]
enum IsoDateTime {
    /// No offset in the source value; written as-is.
    Naive(chrono::NaiveDateTime),
    /// The source carried a UTC offset (e.g. `timestamptz`). Excel has no
    /// offset-aware type, so the instant is converted to UTC and the cell
    /// format shows a literal "UTC" suffix so the shift is visible.
    Utc(chrono::NaiveDateTime),
}

/// Best-effort ISO-8601 parse covering the shapes our drivers emit for
/// `timestamp` / `date` / `time` values.
fn parse_iso_datetime(s: &str) -> Option<IsoDateTime> {
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

    for fmt in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
    ] {
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
            return Some(IsoDateTime::Naive(dt));
        }
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(IsoDateTime::Utc(dt.naive_utc()));
    }
    // `2024-01-02 03:04:05+02` / `+02:00` as printed by PostgreSQL.
    for fmt in ["%Y-%m-%d %H:%M:%S%.f%#z", "%Y-%m-%d %H:%M:%S%#z"] {
        if let Ok(dt) = chrono::DateTime::parse_from_str(s, fmt) {
            return Some(IsoDateTime::Utc(dt.naive_utc()));
        }
    }
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Some(IsoDateTime::Naive(
            d.and_time(NaiveTime::from_hms_opt(0, 0, 0)?),
        ));
    }
    None
}

fn escape_csv(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_basic() {
        let cols = ["a".to_string(), "b".to_string()];
        let rows = vec![
            vec![Value::Number(1i64.into()), Value::String("x".into())],
            vec![Value::Null, Value::String("with,comma".into())],
        ];
        let out = to_csv(&cols, &rows);
        assert!(out.contains("a,b"));
        assert!(out.contains("1,x"));
        assert!(out.contains(r#""with,comma""#));
    }

    #[test]
    fn csv_empty_rows() {
        let cols = ["id".to_string()];
        let out = to_csv(&cols, &[]);
        assert_eq!(out, "id\n");
    }

    #[test]
    fn csv_empty_columns_and_rows() {
        let out = to_csv(&[], &[]);
        assert_eq!(out, "\n");
    }

    #[test]
    fn csv_quotes_in_value() {
        let cols = ["v".to_string()];
        let rows = vec![vec![Value::String("say \"hello\"".into())]];
        let out = to_csv(&cols, &rows);
        assert!(out.contains(r#""say ""hello"""#));
    }

    #[test]
    fn csv_newline_in_value() {
        let cols = ["v".to_string()];
        let rows = vec![vec![Value::String("line1\nline2".into())]];
        let out = to_csv(&cols, &rows);
        assert!(out.contains(r#""line1"#));
    }

    #[test]
    fn csv_boolean_value() {
        let cols = ["flag".to_string()];
        let rows = vec![vec![Value::Bool(true)], vec![Value::Bool(false)]];
        let out = to_csv(&cols, &rows);
        assert!(out.contains("true"));
        assert!(out.contains("false"));
    }

    #[test]
    fn csv_array_value() {
        let cols = ["arr".to_string()];
        let rows = vec![vec![Value::Array(vec![Value::Number(1i64.into())])]];
        let out = to_csv(&cols, &rows);
        assert!(out.contains("[1]"));
    }

    #[test]
    fn csv_null_renders_empty() {
        let cols = ["a".to_string(), "b".to_string()];
        let rows = vec![vec![Value::Null, Value::String("x".into())]];
        let out = to_csv(&cols, &rows);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[1], ",x");
    }

    #[test]
    fn json_basic() {
        let cols = ["id".to_string()];
        let rows = vec![vec![Value::Number(1i64.into())]];
        let out = to_json(&cols, &rows);
        assert!(out.contains("\"id\""));
        assert!(out.contains("1"));
    }

    #[test]
    fn json_empty_rows() {
        let cols = ["id".to_string()];
        let out = to_json(&cols, &[]);
        assert_eq!(out.trim(), "[]");
    }

    #[test]
    fn json_multiple_columns() {
        let cols = ["a".to_string(), "b".to_string()];
        let rows = vec![vec![
            Value::Number(1i64.into()),
            Value::String("hello".into()),
        ]];
        let out = to_json(&cols, &rows);
        let parsed: Vec<serde_json::Map<String, Value>> = serde_json::from_str(&out).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0]["a"], Value::Number(1i64.into()));
        assert_eq!(parsed[0]["b"], Value::String("hello".into()));
    }

    #[test]
    fn json_null_and_bool() {
        let cols = ["x".to_string(), "y".to_string()];
        let rows = vec![vec![Value::Null, Value::Bool(false)]];
        let out = to_json(&cols, &rows);
        let parsed: Vec<serde_json::Map<String, Value>> = serde_json::from_str(&out).unwrap();
        assert_eq!(parsed[0]["x"], Value::Null);
        assert_eq!(parsed[0]["y"], Value::Bool(false));
    }

    #[test]
    fn sql_inserts_basic() {
        let cols = ["name".to_string()];
        let rows = vec![vec![Value::String("O'Brien".into())]];
        let out = to_sql_inserts(None, "users", &cols, &rows);
        assert!(out.contains("INSERT INTO \"users\""));
        assert!(out.contains("'O''Brien'"));
    }

    #[test]
    fn sql_inserts_with_schema_qualifies_target() {
        // Regression test: previously the schema was dropped on the
        // floor, producing `INSERT INTO "events"` for a table named
        // `analytics.events`. That file would re-import into the wrong
        // table (or fail) when restored against a multi-schema database.
        let cols = ["id".to_string()];
        let rows = vec![vec![Value::Number(1i64.into())]];
        let out = to_sql_inserts(Some("analytics"), "events", &cols, &rows);
        assert!(
            out.contains(r#"INSERT INTO "analytics"."events""#),
            "expected schema-qualified target, got: {out}"
        );
    }

    #[test]
    fn sql_inserts_with_empty_schema_falls_back_to_unqualified() {
        let cols = ["id".to_string()];
        let rows = vec![vec![Value::Number(1i64.into())]];
        let out = to_sql_inserts(Some(""), "t", &cols, &rows);
        assert!(out.contains(r#"INSERT INTO "t""#));
        assert!(!out.contains("\"\".\"t\""));
    }

    #[test]
    fn sql_inserts_with_quoted_schema_escapes_correctly() {
        let cols = ["v".to_string()];
        let rows = vec![vec![Value::Number(1i64.into())]];
        let out = to_sql_inserts(Some(r#"weird"schema"#), "t", &cols, &rows);
        assert!(out.contains(r#""weird""schema"."t""#));
    }

    #[test]
    fn sql_inserts_empty_rows() {
        let cols = ["id".to_string()];
        let out = to_sql_inserts(None, "t", &cols, &[]);
        assert_eq!(out, "");
    }

    #[test]
    fn sql_inserts_null_and_number() {
        let cols = ["a".to_string(), "b".to_string()];
        let rows = vec![vec![Value::Null, Value::Number(42i64.into())]];
        let out = to_sql_inserts(None, "t", &cols, &rows);
        assert!(out.contains("NULL, 42"));
    }

    #[test]
    fn sql_inserts_bool() {
        let cols = ["flag".to_string()];
        let rows = vec![vec![Value::Bool(true)]];
        let out = to_sql_inserts(None, "t", &cols, &rows);
        assert!(out.contains("true"));
    }

    #[test]
    fn sql_inserts_table_name_with_quote() {
        let cols = ["v".to_string()];
        let rows = vec![vec![Value::Number(1i64.into())]];
        let out = to_sql_inserts(None, r#"my"table"#, &cols, &rows);
        assert!(out.contains(r#""my""table""#));
    }

    #[test]
    fn sql_inserts_column_name_with_quote() {
        let cols = [r#"col"umn"#.to_string()];
        let rows = vec![vec![Value::Number(1i64.into())]];
        let out = to_sql_inserts(None, "t", &cols, &rows);
        assert!(out.contains(r#""col""umn""#));
    }

    #[test]
    fn sql_inserts_multiple_rows() {
        let cols = ["id".to_string()];
        let rows = vec![
            vec![Value::Number(1i64.into())],
            vec![Value::Number(2i64.into())],
            vec![Value::Number(3i64.into())],
        ];
        let out = to_sql_inserts(None, "t", &cols, &rows);
        assert_eq!(out.matches("INSERT INTO").count(), 3);
    }

    #[test]
    fn escape_csv_no_special() {
        assert_eq!(escape_csv("hello"), "hello");
    }

    #[test]
    fn escape_csv_with_comma() {
        assert_eq!(escape_csv("a,b"), r#""a,b""#);
    }

    #[test]
    fn escape_csv_with_double_quote() {
        assert_eq!(escape_csv(r#"say "hi""#), r#""say ""hi""""#);
    }

    #[test]
    fn escape_csv_with_newline() {
        assert_eq!(escape_csv("a\nb"), "\"a\nb\"");
    }

    #[test]
    fn escape_csv_with_carriage_return() {
        assert_eq!(escape_csv("a\rb"), "\"a\rb\"");
    }

    #[test]
    fn escape_csv_empty_string() {
        assert_eq!(escape_csv(""), "");
    }

    #[test]
    fn escape_csv_all_specials_combined() {
        let input = "a,b\n\"c\r";
        let escaped = escape_csv(input);
        assert!(escaped.starts_with('"'));
        assert!(escaped.ends_with('"'));
    }

    #[test]
    fn csv_object_value() {
        let cols = ["data".to_string()];
        let rows = vec![vec![Value::Object(serde_json::Map::from_iter(vec![(
            "key".to_string(),
            Value::String("val".into()),
        )]))]];
        let out = to_csv(&cols, &rows);
        assert!(out.contains("key"));
        assert!(out.contains("val"));
    }

    #[test]
    fn json_array_value() {
        let cols = ["arr".to_string()];
        let rows = vec![vec![Value::Array(vec![
            Value::Number(1i64.into()),
            Value::Number(2i64.into()),
        ])]];
        let out = to_json(&cols, &rows);
        let parsed: Vec<serde_json::Map<String, Value>> = serde_json::from_str(&out).unwrap();
        assert_eq!(
            parsed[0]["arr"],
            Value::Array(vec![Value::Number(1i64.into()), Value::Number(2i64.into())])
        );
    }

    #[test]
    fn json_multiple_rows() {
        let cols = ["id".to_string(), "name".to_string()];
        let rows = vec![
            vec![Value::Number(1i64.into()), Value::String("Alice".into())],
            vec![Value::Number(2i64.into()), Value::String("Bob".into())],
        ];
        let out = to_json(&cols, &rows);
        let parsed: Vec<serde_json::Map<String, Value>> = serde_json::from_str(&out).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0]["name"], Value::String("Alice".into()));
        assert_eq!(parsed[1]["name"], Value::String("Bob".into()));
    }

    #[test]
    fn sql_inserts_array_value() {
        let cols = ["arr".to_string()];
        let rows = vec![vec![Value::Array(vec![Value::Number(1i64.into())])]];
        let out = to_sql_inserts(None, "t", &cols, &rows);
        assert!(out.contains("[1]"));
        assert!(out.contains("'"));
    }

    #[test]
    fn sql_inserts_object_value() {
        let cols = ["data".to_string()];
        let rows = vec![vec![Value::Object(serde_json::Map::from_iter(vec![(
            "k".to_string(),
            Value::String("v".into()),
        )]))]];
        let out = to_sql_inserts(None, "t", &cols, &rows);
        assert!(out.contains("INSERT INTO"));
    }

    #[test]
    fn csv_number_types() {
        let cols = ["int".to_string(), "float".to_string()];
        let n = serde_json::Number::from_f64(7.25).unwrap();
        let rows = vec![vec![Value::Number(42i64.into()), Value::Number(n)]];
        let out = to_csv(&cols, &rows);
        assert!(out.contains("42"));
        assert!(out.contains("7.25"));
    }

    // ── xlsx ──────────────────────────────────────────────────────────

    #[test]
    fn xlsx_sheet_name_sanitization_strips_forbidden_chars() {
        assert_eq!(sanitize_sheet_name("a:b\\c/d?e*f[g]h"), "abcdefgh");
    }

    #[test]
    fn xlsx_sheet_name_sanitization_truncates_to_31_chars() {
        let raw = "a".repeat(64);
        let out = sanitize_sheet_name(&raw);
        assert_eq!(out.chars().count(), 31);
    }

    #[test]
    fn xlsx_sheet_name_sanitization_truncates_multibyte_safely() {
        // 40 codepoints, each 2 bytes when encoded as UTF-8. A naive
        // byte-truncate to 31 would split one of these characters and
        // produce invalid UTF-8; we truncate by char count instead.
        let raw = "ñ".repeat(40);
        let out = sanitize_sheet_name(&raw);
        assert_eq!(out.chars().count(), 31);
    }

    #[test]
    fn xlsx_sheet_name_falls_back_to_result_when_empty() {
        assert_eq!(sanitize_sheet_name(""), "Result");
        assert_eq!(sanitize_sheet_name("   "), "Result");
        assert_eq!(sanitize_sheet_name("[]/\\"), "Result");
    }

    #[test]
    fn xlsx_round_trip_preserves_types() {
        // Single source of truth for the headline acceptance criterion
        // in #69 / #70: numbers stay numbers, booleans stay booleans,
        // dates stay dates, nulls stay blank when the file is reopened.
        use calamine::{Data, Reader};

        let cols = vec![
            "i".to_string(),
            "f".to_string(),
            "b".to_string(),
            "s".to_string(),
            "d".to_string(),
            "n".to_string(),
        ];
        let rows = vec![vec![
            Value::Number(42i64.into()),
            Value::Number(serde_json::Number::from_f64(7.25).unwrap()),
            Value::Bool(true),
            Value::String("hello".into()),
            Value::String("2024-05-01T12:30:00".into()),
            Value::Null,
        ]];
        let bytes = to_xlsx(&cols, &rows, "users").expect("to_xlsx").bytes;

        let cursor = std::io::Cursor::new(bytes);
        let mut wb = calamine::open_workbook_auto_from_rs(cursor).expect("open workbook");
        let names = wb.sheet_names();
        assert_eq!(names, vec!["users".to_string()]);
        let range = wb.worksheet_range("users").expect("range");

        // Header row (formatted bold, but still read as Strings).
        let headers: Vec<&Data> = range.rows().next().unwrap().iter().collect();
        assert!(matches!(headers[0], Data::String(s) if s == "i"));
        assert!(matches!(headers[5], Data::String(s) if s == "n"));

        let data_row: Vec<&Data> = range.rows().nth(1).unwrap().iter().collect();
        assert!(
            matches!(data_row[0], Data::Float(f) if (*f - 42.0).abs() < f64::EPSILON
                || matches!(data_row[0], Data::Int(42))),
            "integer cell should survive as numeric, got {:?}",
            data_row[0]
        );
        assert!(
            matches!(data_row[1], Data::Float(f) if (*f - 7.25).abs() < 1e-9),
            "float cell should survive as numeric, got {:?}",
            data_row[1]
        );
        assert!(
            matches!(data_row[2], Data::Bool(true)),
            "bool cell should survive as boolean, got {:?}",
            data_row[2]
        );
        assert!(
            matches!(data_row[3], Data::String(s) if s == "hello"),
            "string cell stays string, got {:?}",
            data_row[3]
        );
        assert!(
            matches!(data_row[4], Data::DateTime(_)),
            "ISO datetime string should be written as a datetime cell, got {:?}",
            data_row[4]
        );
        assert!(
            matches!(data_row[5], Data::Empty),
            "Value::Null should write as a blank cell, got {:?}",
            data_row[5]
        );
    }

    #[test]
    fn xlsx_uses_sanitized_sheet_name_in_output() {
        use calamine::Reader;

        let bytes = to_xlsx(
            &["a".to_string()],
            &[vec![Value::Number(1i64.into())]],
            "schema:table",
        )
        .expect("to_xlsx")
        .bytes;
        let wb = calamine::open_workbook_auto_from_rs(std::io::Cursor::new(bytes))
            .expect("open workbook");
        // The forbidden `:` is stripped; the sheet name must be the
        // sanitized version so the workbook is loadable.
        assert_eq!(wb.sheet_names(), vec!["schematable".to_string()]);
    }

    #[test]
    fn xlsx_empty_rows_emits_only_header() {
        use calamine::Reader;

        let bytes = to_xlsx(&["id".to_string()], &[], "t")
            .expect("to_xlsx")
            .bytes;
        let mut wb = calamine::open_workbook_auto_from_rs(std::io::Cursor::new(bytes))
            .expect("open workbook");
        let range = wb.worksheet_range("t").expect("range");
        // Only the header row, no data rows.
        assert_eq!(range.rows().count(), 1);
    }

    #[test]
    fn xlsx_array_and_object_fall_back_to_json() {
        use calamine::{Data, Reader};

        let cols = vec!["arr".to_string(), "obj".to_string()];
        let rows = vec![vec![
            Value::Array(vec![Value::Number(1i64.into()), Value::Number(2i64.into())]),
            Value::Object(serde_json::Map::from_iter(vec![(
                "k".to_string(),
                Value::String("v".into()),
            )])),
        ]];
        let bytes = to_xlsx(&cols, &rows, "t").expect("to_xlsx").bytes;
        let mut wb = calamine::open_workbook_auto_from_rs(std::io::Cursor::new(bytes))
            .expect("open workbook");
        let range = wb.worksheet_range("t").expect("range");
        let row: Vec<&Data> = range.rows().nth(1).unwrap().iter().collect();
        assert!(matches!(row[0], Data::String(s) if s.contains('[') && s.contains('1')));
        assert!(matches!(row[1], Data::String(s) if s.contains("\"k\"") && s.contains("\"v\"")));
    }

    #[test]
    fn parse_iso_datetime_accepts_common_shapes() {
        assert!(parse_iso_datetime("2024-05-01T12:30:00").is_some());
        assert!(parse_iso_datetime("2024-05-01T12:30:00.123").is_some());
        assert!(parse_iso_datetime("2024-05-01 12:30:00").is_some());
        assert!(parse_iso_datetime("2024-05-01T12:30:00Z").is_some());
        assert!(parse_iso_datetime("2024-05-01").is_some());
        assert!(parse_iso_datetime("not a date").is_none());
        assert!(parse_iso_datetime("hello").is_none());
    }

    #[test]
    fn parse_iso_datetime_marks_offset_values_as_utc() {
        use chrono::NaiveDate;
        let expected = NaiveDate::from_ymd_opt(2024, 5, 1)
            .unwrap()
            .and_hms_opt(10, 30, 0)
            .unwrap();
        assert_eq!(
            parse_iso_datetime("2024-05-01T12:30:00+02:00"),
            Some(IsoDateTime::Utc(expected))
        );
        assert_eq!(
            parse_iso_datetime("2024-05-01 12:30:00+02"),
            Some(IsoDateTime::Utc(expected))
        );
        assert_eq!(
            parse_iso_datetime("2024-05-01 10:30:00"),
            Some(IsoDateTime::Naive(expected))
        );
        assert_eq!(parse_iso_datetime("12345678901234567890"), None);
    }

    #[test]
    fn xlsx_keeps_big_integers_and_numeric_strings_exact() {
        use calamine::{Data, Reader};
        let cols = vec!["big".to_string(), "small".to_string(), "dec".to_string()];
        let rows = vec![vec![
            Value::Number(9_007_199_254_740_993i64.into()),
            Value::Number(9_007_199_254_740_992i64.into()),
            Value::String("12345678901234567890.123456789".into()),
        ]];
        let bytes = to_xlsx(&cols, &rows, "t").unwrap().bytes;
        let mut wb = calamine::open_workbook_auto_from_rs(std::io::Cursor::new(bytes)).unwrap();
        let range = wb.worksheet_range("t").unwrap();
        let row: Vec<&Data> = range.rows().nth(1).unwrap().iter().collect();
        assert!(
            matches!(row[0], Data::String(s) if s == "9007199254740993"),
            "{:?}",
            row[0]
        );
        assert!(
            matches!(row[1], Data::Float(_) | Data::Int(_)),
            "2^53 itself is exact and stays numeric: {:?}",
            row[1]
        );
        assert!(
            matches!(row[2], Data::String(s) if s == "12345678901234567890.123456789"),
            "{:?}",
            row[2]
        );
    }

    #[test]
    fn numeric_literal_detection() {
        for ok in ["0", "-12", "+3", "3.50", ".5", "5.", "1e10", "-2.5E-3"] {
            assert!(is_numeric_literal(ok), "{ok}");
        }
        for bad in [
            "", "-", ".", "1e", "1,000", "0x10", "NaN", "Infinity", "1 ", "1;DROP",
        ] {
            assert!(!is_numeric_literal(bad), "{bad}");
        }
    }

    #[test]
    fn numeric_type_detection() {
        for t in [
            "integer",
            "BIGINT",
            "numeric(20,2)",
            "decimal",
            "double precision",
            "real",
            "int8",
            "NUMBER(10)",
            "bigserial",
        ] {
            assert!(is_numeric_type(t), "{t}");
        }
        for t in [
            "text",
            "interval",
            "point",
            "varchar(10)",
            "timestamptz",
            "money",
        ] {
            assert!(!is_numeric_type(t), "{t}");
        }
    }

    #[test]
    fn sql_inserts_unquote_numeric_strings_only_for_numeric_columns() {
        let cols = vec!["id".to_string(), "code".to_string(), "amount".to_string()];
        let rows = vec![vec![
            Value::String("9007199254740993".into()),
            Value::String("00123".into()),
            Value::String("not-a-number".into()),
        ]];
        let out = to_sql_inserts_typed(None, "t", &cols, &[true, false, true], &rows);
        assert!(
            out.contains("VALUES (9007199254740993, '00123', 'not-a-number');"),
            "{out}"
        );
        // Untyped variant keeps quoting every string.
        let out = to_sql_inserts(None, "t", &cols, &rows);
        assert!(out.contains("VALUES ('9007199254740993', '00123', 'not-a-number');"));
    }

    #[test]
    fn csv_neutralizes_formulas() {
        let cols = ["=cmd".to_string()];
        let rows = vec![
            vec![Value::String("=HYPERLINK(\"http://x\",\"y\")".into())],
            vec![Value::String("@SUM(A1)".into())],
            vec![Value::String("+1+2".into())],
            vec![Value::String("-2+3".into())],
            vec![Value::String("\tx".into())],
            vec![Value::String("-12.5".into())],
            vec![Value::String("a=b".into())],
        ];
        let out = to_csv(&cols, &rows);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[0], "'=cmd");
        assert_eq!(lines[1], "\"'=HYPERLINK(\"\"http://x\"\",\"\"y\"\")\"");
        assert_eq!(lines[2], "'@SUM(A1)");
        assert_eq!(lines[3], "'+1+2");
        assert_eq!(lines[4], "'-2+3");
        assert_eq!(lines[5], "'\tx");
        // Plain numbers and text with `=` later on are untouched.
        assert_eq!(lines[6], "-12.5");
        assert_eq!(lines[7], "a=b");
    }

    #[test]
    fn csv_distinguishes_null_from_empty_string() {
        let cols = ["a".to_string(), "b".to_string()];
        let rows = vec![vec![Value::Null, Value::String(String::new())]];
        assert_eq!(to_csv(&cols, &rows), "a,b\n,\"\"\n");
    }

    #[test]
    fn json_keeps_column_order_and_duplicates() {
        let cols = [
            "z".to_string(),
            "id".to_string(),
            "a".to_string(),
            "id".to_string(),
        ];
        let rows = vec![vec![
            Value::Number(1.into()),
            Value::Number(2.into()),
            serde_json::json!({"k": [1, 2]}),
            Value::Number(3.into()),
        ]];
        let out = to_json(&cols, &rows);
        let z = out.find("\"z\"").unwrap();
        let id = out.find("\"id\"").unwrap();
        let a = out.find("\"a\"").unwrap();
        let id2 = out.find("\"id_2\"").unwrap();
        assert!(z < id && id < a && a < id2, "{out}");
        let parsed: Value = serde_json::from_str(&out).expect("valid JSON");
        assert_eq!(parsed[0]["id"], serde_json::json!(2));
        assert_eq!(parsed[0]["id_2"], serde_json::json!(3));
        assert_eq!(parsed[0]["a"]["k"][1], serde_json::json!(2));
        assert_eq!(to_json(&cols, &[]), "[]");
    }

    #[test]
    fn json_duplicate_suffix_skips_existing_names() {
        let cols = ["id".to_string(), "id_2".to_string(), "id".to_string()];
        assert_eq!(unique_keys(&cols), vec!["id", "id_2", "id_3"]);
    }

    #[test]
    fn xlsx_truncates_overlong_text_and_counts_it() {
        use calamine::Reader;
        let long = "é".repeat(40_000);
        let cols = ["v".to_string()];
        let rows = vec![
            vec![Value::String(long.clone())],
            vec![Value::String("short".into())],
        ];
        let out = to_xlsx(&cols, &rows, "t").expect("long text must not fail the export");
        assert_eq!(out.truncated_cells, 1);
        assert!(out.warning().unwrap().contains("1 cell"));
        let mut wb = calamine::open_workbook_auto_from_rs(std::io::Cursor::new(out.bytes)).unwrap();
        let range = wb.worksheet_range("t").unwrap();
        let cell = range.get((1, 0)).unwrap().to_string();
        assert!(cell.ends_with(XLSX_TRUNCATION_MARKER));
        assert_eq!(cell.encode_utf16().count(), XLSX_MAX_CELL_CHARS);
        assert!(to_xlsx(&cols, &rows[1..], "t").unwrap().warning().is_none());
    }

    #[test]
    fn sql_export_uses_dialect_quoting_and_binary_literals() {
        let cols = ["id".to_string(), "b".to_string(), "s".to_string()];
        let kinds = [
            SqlColumnKind::Numeric,
            SqlColumnKind::Binary,
            SqlColumnKind::Other,
        ];
        let row = |bin: &str| {
            vec![vec![
                Value::String("12345678901234567890".into()),
                Value::String(bin.into()),
                Value::String("it's \\ ok".into()),
            ]]
        };
        let my = to_sql_inserts_for(
            SqlExportDialect::MySql,
            Some("db"),
            "t",
            &cols,
            &kinds,
            &row("0xdead"),
        );
        assert_eq!(
            my,
            "INSERT INTO `db`.`t` (`id`, `b`, `s`) VALUES (12345678901234567890, X'dead', 'it''s \\\\ ok');\n"
        );
        let pg = to_sql_inserts_for(
            SqlExportDialect::Postgres,
            Some("public"),
            "t",
            &cols,
            &kinds,
            &row("\\xdead"),
        );
        assert!(pg.contains("'\\xdead'::bytea"), "{pg}");
        assert!(pg.contains("\"public\".\"t\""), "{pg}");
        let lite = to_sql_inserts_for(
            SqlExportDialect::Sqlite,
            None,
            "t",
            &cols,
            &kinds,
            &row("X'dead'"),
        );
        assert!(lite.contains(", X'dead', "), "{lite}");
        let ms = to_sql_inserts_for(
            SqlExportDialect::Mssql,
            Some("dbo"),
            "t",
            &cols,
            &kinds,
            &row("0xDEAD"),
        );
        assert!(
            ms.starts_with("INSERT INTO [dbo].[t] ([id], [b], [s])"),
            "{ms}"
        );
        assert!(ms.contains(", 0xDEAD, N'it''s \\ ok'"), "{ms}");
        // A binary column value that isn't hex stays a quoted string.
        let odd = to_sql_inserts_for(
            SqlExportDialect::MySql,
            None,
            "t",
            &cols,
            &kinds,
            &row("zz"),
        );
        assert!(odd.contains(", 'zz', "), "{odd}");
    }

    #[test]
    fn binary_type_names() {
        for t in [
            "bytea",
            "BLOB",
            "longblob",
            "varbinary(16)",
            "binary(4)",
            "image",
        ] {
            assert!(is_binary_type(t), "{t}");
        }
        for t in ["text", "varchar(10)", "bit", "int"] {
            assert!(!is_binary_type(t), "{t}");
        }
    }
}
