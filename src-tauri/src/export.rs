use anyhow::Result;
use rust_xlsxwriter::{Format, Workbook};
use serde_json::Value;

pub fn to_csv(columns: &[String], rows: &[Vec<Value>]) -> String {
    let mut out = String::new();

    out.push_str(
        &columns
            .iter()
            .map(|c| escape_csv(c))
            .collect::<Vec<_>>()
            .join(","),
    );
    out.push('\n');

    for row in rows {
        let line: Vec<String> = row
            .iter()
            .map(|v| match v {
                Value::Null => String::new(),
                Value::String(s) => escape_csv(s),
                Value::Bool(b) => b.to_string(),
                Value::Number(n) => n.to_string(),
                _ => escape_csv(&v.to_string()),
            })
            .collect();
        out.push_str(&line.join(","));
        out.push('\n');
    }
    out
}

pub fn to_json(columns: &[String], rows: &[Vec<Value>]) -> String {
    let objects: Vec<serde_json::Map<String, Value>> = rows
        .iter()
        .map(|row| {
            columns
                .iter()
                .zip(row.iter())
                .map(|(col, val)| (col.clone(), val.clone()))
                .collect()
        })
        .collect();

    serde_json::to_string_pretty(&objects).unwrap_or_else(|_| "[]".to_string())
}

/// Render rows as `INSERT INTO ... VALUES (...);` statements.
///
/// `schema` is optional: when provided the output emits a fully
/// qualified `"schema"."table"` reference so the dump can be replayed
/// in a database that doesn't have a matching `search_path` /
/// `current_schema`. Previously this argument was missing entirely and
/// every export silently dropped to `INSERT INTO "table"`, which round-
/// tripped fine on single-schema databases (SQLite, MySQL when
/// `database == schema`) but broke restores in PostgreSQL multi-schema
/// setups.
///
/// The output uses standard `"…"` identifier quoting (PG / SQLite /
/// MSSQL with `QUOTED_IDENTIFIER ON` / Cassandra all accept this).
/// MySQL with default `sql_mode` would need backticks — the dialect
/// mismatch is tracked separately; this fix at least makes
/// schema-qualified targets correct on the dialects that already work.
pub fn to_sql_inserts(
    schema: Option<&str>,
    table_name: &str,
    columns: &[String],
    rows: &[Vec<Value>],
) -> String {
    let mut out = String::new();
    let cols_str = columns
        .iter()
        .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");

    let qualified = match schema {
        Some(s) if !s.is_empty() => format!(
            "\"{}\".\"{}\"",
            s.replace('"', "\"\""),
            table_name.replace('"', "\"\"")
        ),
        _ => format!("\"{}\"", table_name.replace('"', "\"\"")),
    };

    for row in rows {
        let vals: Vec<String> = row
            .iter()
            .map(|v| match v {
                Value::Null => "NULL".to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Number(n) => n.to_string(),
                Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                _ => format!("'{}'", v.to_string().replace('\'', "''")),
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
/// - `Value::Number` writes as a numeric cell (not stringified)
/// - `Value::Bool` writes as a boolean cell
/// - `Value::Null` writes nothing (blank cell)
/// - `Value::String` that parses as ISO-8601 date / datetime writes as
///   an Excel datetime with `yyyy-mm-dd hh:mm:ss` formatting; otherwise
///   plain string
/// - `Value::Array` / `Value::Object` fall back to their JSON
///   representation since Excel has no native equivalent
///
/// Calls `autofit()` once at the end so column widths track the longest
/// rendered value without a per-cell pass.
pub fn to_xlsx(columns: &[String], rows: &[Vec<Value>], sheet_name: &str) -> Result<Vec<u8>> {
    let mut workbook = Workbook::new();
    let safe_name = sanitize_sheet_name(sheet_name);
    let datetime_fmt = Format::new().set_num_format("yyyy-mm-dd hh:mm:ss");
    let header_fmt = Format::new().set_bold();

    let worksheet = workbook.add_worksheet();
    worksheet.set_name(&safe_name)?;

    for (col_idx, col) in columns.iter().enumerate() {
        worksheet.write_string_with_format(0, col_idx as u16, col, &header_fmt)?;
    }

    for (row_idx, row) in rows.iter().enumerate() {
        let excel_row = (row_idx + 1) as u32;
        for (col_idx, val) in row.iter().enumerate() {
            write_typed_cell(worksheet, excel_row, col_idx as u16, val, &datetime_fmt)?;
        }
    }

    worksheet.autofit();
    let bytes = workbook.save_to_buffer()?;
    Ok(bytes)
}

fn write_typed_cell(
    worksheet: &mut rust_xlsxwriter::Worksheet,
    row: u32,
    col: u16,
    val: &Value,
    datetime_fmt: &Format,
) -> Result<()> {
    match val {
        Value::Null => {
            worksheet.write_blank(row, col, &Format::default())?;
        }
        Value::Bool(b) => {
            worksheet.write_boolean(row, col, *b)?;
        }
        Value::Number(n) => {
            // Prefer i64 / u64 over f64 to keep integers as integers in
            // the workbook (Excel still stores them as f64 internally,
            // but the cell type stays "number" either way).
            if let Some(i) = n.as_i64() {
                worksheet.write_number(row, col, i as f64)?;
            } else if let Some(u) = n.as_u64() {
                worksheet.write_number(row, col, u as f64)?;
            } else if let Some(f) = n.as_f64() {
                worksheet.write_number(row, col, f)?;
            } else {
                worksheet.write_string(row, col, n.to_string())?;
            }
        }
        Value::String(s) => {
            if let Some(dt) = parse_iso_datetime(s) {
                worksheet.write_datetime_with_format(row, col, dt, datetime_fmt)?;
            } else {
                worksheet.write_string(row, col, s.as_str())?;
            }
        }
        // No native Excel cell type for arrays / objects; fall back to
        // the JSON representation so the data isn't silently dropped.
        Value::Array(_) | Value::Object(_) => {
            worksheet.write_string(row, col, val.to_string())?;
        }
    }
    Ok(())
}

/// Best-effort ISO-8601 parse covering the shapes our drivers emit for
/// `timestamp` / `date` / `time` values.
fn parse_iso_datetime(s: &str) -> Option<chrono::NaiveDateTime> {
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
        return Some(dt);
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Some(dt);
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        return Some(dt);
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(dt);
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.naive_utc());
    }
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Some(d.and_time(NaiveTime::from_hms_opt(0, 0, 0)?));
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
        let n = serde_json::Number::from_f64(3.14).unwrap();
        let rows = vec![vec![Value::Number(42i64.into()), Value::Number(n)]];
        let out = to_csv(&cols, &rows);
        assert!(out.contains("42"));
        assert!(out.contains("3.14"));
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
            Value::Number(serde_json::Number::from_f64(3.14).unwrap()),
            Value::Bool(true),
            Value::String("hello".into()),
            Value::String("2024-05-01T12:30:00".into()),
            Value::Null,
        ]];
        let bytes = to_xlsx(&cols, &rows, "users").expect("to_xlsx");

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
            matches!(data_row[1], Data::Float(f) if (*f - 3.14).abs() < 1e-9),
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
        .expect("to_xlsx");
        let wb = calamine::open_workbook_auto_from_rs(std::io::Cursor::new(bytes))
            .expect("open workbook");
        // The forbidden `:` is stripped; the sheet name must be the
        // sanitized version so the workbook is loadable.
        assert_eq!(wb.sheet_names(), vec!["schematable".to_string()]);
    }

    #[test]
    fn xlsx_empty_rows_emits_only_header() {
        use calamine::Reader;

        let bytes = to_xlsx(&["id".to_string()], &[], "t").expect("to_xlsx");
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
        let bytes = to_xlsx(&cols, &rows, "t").expect("to_xlsx");
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
}
