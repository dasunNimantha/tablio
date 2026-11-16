//! Parse `.xlsx` / `.xls` workbooks for the import dialog.
//!
//! The flow is two-stage so the dialog can render a sheet picker
//! cheaply: `parse_xlsx_workbook` returns metadata + a small preview
//! of the first sheet, and `parse_xlsx_sheet` returns the full row
//! payload once the user has chosen which sheet to import.
//!
//! Cell-type fidelity is the headline win over CSV: numbers stay as
//! `Value::Number`, booleans as `Value::Bool`, dates as ISO-8601
//! strings, and empties as `Value::Null`. The existing `import_data`
//! command (`pg_import_data` and friends) already accepts typed
//! `serde_json::Value` rows, so XLSX imports get type-faithful inserts
//! for free.

use calamine::{Data, Reader};
use serde::Serialize;
use serde_json::Value;
use std::io::Cursor;

/// Hard cap on the byte payload accepted from the renderer. The bytes
/// arrive as a JSON number array (~4x the file size) and the zipped XML
/// inflates several times while parsing, so a large workbook costs a
/// multiple of its size in memory. 25 MB of xlsx is already over a
/// million cells. The import dialog checks the same limit
/// (`MAX_XLSX_BYTES`) before reading the file.
const MAX_BYTES: usize = 25 * 1024 * 1024;

const PREVIEW_ROWS: usize = 50;

/// How many error-cell locations are listed individually; the total is
/// always reported in `error_cell_count`.
const MAX_LISTED_ERROR_CELLS: usize = 20;

#[derive(Debug, Serialize)]
pub struct XlsxWorkbookPreview {
    pub sheet_names: Vec<String>,
    pub default_sheet: String,
    pub sheet: XlsxSheetPreview,
    /// Identifies the uploaded workbook for `parse_xlsx_sheet`, so the
    /// file is sent (and the default sheet parsed) only once.
    pub token: String,
}

#[derive(Debug, Serialize)]
pub struct XlsxSheetPreview {
    pub name: String,
    pub headers: Vec<String>,
    pub preview_rows: Vec<Vec<Value>>,
    pub total_data_rows: usize,
    pub inferred_types: Vec<&'static str>,
    /// Data cells holding an Excel error (`#N/A`, `#DIV/0!`, ...). They are
    /// imported as NULL; this lists the first few as `"B5: #N/A"`.
    pub error_cells: Vec<String>,
    pub error_cell_count: usize,
}

/// Full payload for a single sheet — returned to the dialog after the
/// user picks which sheet to import. Headers and `inferred_types`
/// match `XlsxSheetPreview` so the renderer can display the same
/// type hint badges either way.
#[derive(Debug, Serialize)]
pub struct XlsxSheetPayload {
    pub name: String,
    pub headers: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub inferred_types: Vec<&'static str>,
    /// See [`XlsxSheetPreview::error_cells`].
    pub error_cells: Vec<String>,
    pub error_cell_count: usize,
}

/// The last workbook the import dialog opened: its bytes, and the default
/// sheet already parsed for the preview. One slot; opening another file
/// replaces it.
struct CachedWorkbook {
    token: String,
    bytes: std::sync::Arc<Vec<u8>>,
    parsed: Option<(String, Sheet)>,
}

static CACHE: std::sync::Mutex<Option<CachedWorkbook>> = std::sync::Mutex::new(None);

/// The file's bytes: sent as a raw IPC body (a `Uint8Array`), or as a
/// JSON `{ "bytes": [...] }` body from older callers.
fn request_bytes(request: &tauri::ipc::Request<'_>) -> Result<Vec<u8>, String> {
    match request.body() {
        tauri::ipc::InvokeBody::Raw(bytes) => Ok(bytes.clone()),
        // A plain array when the webview falls back from the raw IPC
        // channel to postMessage; `{ "bytes": [...] }` from older callers.
        tauri::ipc::InvokeBody::Json(v) => {
            let bytes = if v.is_array() {
                v.clone()
            } else {
                v["bytes"].clone()
            };
            serde_json::from_value(bytes).map_err(|e| format!("Invalid workbook payload: {e}"))
        }
    }
}

/// Parse a workbook for the import preview.
///
/// The bytes arrive as a raw IPC body instead of a JSON number array
/// (~3.5x the file size, sent twice before), parsing runs off the async
/// runtime, and the parsed default sheet is kept so `parse_xlsx_sheet`
/// doesn't receive or parse the file again.
#[tauri::command]
pub async fn parse_xlsx_workbook(
    request: tauri::ipc::Request<'_>,
) -> Result<XlsxWorkbookPreview, String> {
    let bytes = request_bytes(&request)?;
    tauri::async_runtime::spawn_blocking(move || {
        let (mut preview, sheet) = preview_with_sheet(&bytes)?;
        let token = uuid::Uuid::new_v4().simple().to_string();
        preview.token = token.clone();
        let cached = CachedWorkbook {
            token,
            bytes: std::sync::Arc::new(bytes),
            parsed: Some((preview.default_sheet.clone(), sheet)),
        };
        *CACHE.lock().unwrap_or_else(|e| e.into_inner()) = Some(cached);
        Ok::<_, anyhow::Error>(preview)
    })
    .await
    .map_err(|e| e.to_string())?
    .map_err(|e| e.to_string())
}

/// The full payload for one sheet of the workbook `token` refers to.
#[tauri::command]
pub async fn parse_xlsx_sheet(
    token: String,
    sheet_name: String,
) -> Result<XlsxSheetPayload, String> {
    tauri::async_runtime::spawn_blocking(move || {
        let bytes = {
            let mut cache = CACHE.lock().unwrap_or_else(|e| e.into_inner());
            let cached = cache.as_mut().filter(|c| c.token == token).ok_or_else(|| {
                anyhow::anyhow!("The workbook is no longer loaded; choose the file again.")
            })?;
            if cached
                .parsed
                .as_ref()
                .is_some_and(|(name, _)| *name == sheet_name)
            {
                let (_, sheet) = cached.parsed.take().expect("checked above");
                return Ok(payload_from_sheet(&sheet_name, sheet));
            }
            cached.bytes.clone()
        };
        parse_sheet_impl(&bytes, &sheet_name)
    })
    .await
    .map_err(|e| e.to_string())?
    .map_err(|e: anyhow::Error| e.to_string())
}

pub fn parse_workbook_impl(bytes: &[u8]) -> anyhow::Result<XlsxWorkbookPreview> {
    preview_with_sheet(bytes).map(|(preview, _)| preview)
}

/// The preview of the workbook's default sheet, plus that sheet fully
/// parsed (kept for the import itself).
fn preview_with_sheet(bytes: &[u8]) -> anyhow::Result<(XlsxWorkbookPreview, Sheet)> {
    check_size(bytes)?;
    let mut wb = calamine::open_workbook_auto_from_rs(Cursor::new(bytes))
        .map_err(|e| anyhow::anyhow!("Could not open workbook: {e}"))?;

    let sheet_names: Vec<String> = wb.sheet_names();
    if sheet_names.is_empty() {
        anyhow::bail!("Workbook has no sheets");
    }
    let default_sheet = sheet_names[0].clone();
    let sheet = read_sheet(&mut wb, &default_sheet)?;
    let preview = XlsxSheetPreview {
        name: default_sheet.clone(),
        headers: sheet.headers.clone(),
        preview_rows: sheet.body.iter().take(PREVIEW_ROWS).cloned().collect(),
        total_data_rows: sheet.body.len(),
        inferred_types: sheet.inferred_types.clone(),
        error_cells: sheet.error_cells.clone(),
        error_cell_count: sheet.error_cell_count,
    };
    Ok((
        XlsxWorkbookPreview {
            sheet_names,
            default_sheet,
            sheet: preview,
            token: String::new(),
        },
        sheet,
    ))
}

fn check_size(bytes: &[u8]) -> anyhow::Result<()> {
    if bytes.len() > MAX_BYTES {
        anyhow::bail!(
            "File is too large: {} MB (limit is {} MB)",
            bytes.len() / 1024 / 1024,
            MAX_BYTES / 1024 / 1024
        );
    }
    Ok(())
}

fn payload_from_sheet(sheet_name: &str, sheet: Sheet) -> XlsxSheetPayload {
    XlsxSheetPayload {
        name: sheet_name.to_string(),
        headers: sheet.headers,
        rows: sheet.body,
        inferred_types: sheet.inferred_types,
        error_cells: sheet.error_cells,
        error_cell_count: sheet.error_cell_count,
    }
}

pub fn parse_sheet_impl(bytes: &[u8], sheet_name: &str) -> anyhow::Result<XlsxSheetPayload> {
    check_size(bytes)?;
    let mut wb = calamine::open_workbook_auto_from_rs(Cursor::new(bytes))
        .map_err(|e| anyhow::anyhow!("Could not open workbook: {e}"))?;
    let sheet = read_sheet(&mut wb, sheet_name)?;
    Ok(payload_from_sheet(sheet_name, sheet))
}

struct Sheet {
    headers: Vec<String>,
    body: Vec<Vec<Value>>,
    inferred_types: Vec<&'static str>,
    error_cells: Vec<String>,
    error_cell_count: usize,
}

impl Sheet {
    fn empty() -> Self {
        Sheet {
            headers: Vec::new(),
            body: Vec::new(),
            inferred_types: Vec::new(),
            error_cells: Vec::new(),
            error_cell_count: 0,
        }
    }
}

/// Spreadsheet-style column letters: 0 -> A, 25 -> Z, 26 -> AA.
fn column_letters(mut col: usize) -> String {
    let mut out = Vec::new();
    loop {
        out.push(b'A' + (col % 26) as u8);
        if col < 26 {
            break;
        }
        col = col / 26 - 1;
    }
    out.reverse();
    String::from_utf8(out).unwrap_or_default()
}

/// Read a sheet from the workbook.
///
/// Trailing empty rows / columns are trimmed so blank padding at the
/// end of an analyst's spreadsheet doesn't get re-imported as NULLs.
fn read_sheet<RS, R>(wb: &mut R, sheet_name: &str) -> anyhow::Result<Sheet>
where
    RS: std::io::Read + std::io::Seek,
    R: Reader<RS>,
    R::Error: std::fmt::Display,
{
    let range = wb
        .worksheet_range(sheet_name)
        .map_err(|e| anyhow::anyhow!("Sheet '{sheet_name}' not found: {e}"))?;
    Ok(sheet_from_range(&range))
}

fn sheet_from_range(range: &calamine::Range<Data>) -> Sheet {
    let raw_rows: Vec<Vec<&Data>> = range.rows().map(|r| r.iter().collect()).collect();
    if raw_rows.is_empty() {
        return Sheet::empty();
    }
    // Absolute (0-based) position of the range's top-left cell, used to
    // report error cells with the addresses Excel shows.
    let (first_row, first_col) = range.start().unwrap_or((0, 0));

    // Compute the rightmost non-empty column across all rows so we can
    // truncate trailing-empty padding without losing real data in
    // shorter rows.
    let last_used_col = raw_rows
        .iter()
        .map(|row| {
            row.iter()
                .rposition(|cell| !matches!(cell, Data::Empty))
                .map(|i| i + 1)
                .unwrap_or(0)
        })
        .max()
        .unwrap_or(0);

    if last_used_col == 0 {
        return Sheet::empty();
    }

    let header_row = &raw_rows[0];
    let headers: Vec<String> = (0..last_used_col)
        .map(|i| {
            header_row
                .get(i)
                .map(|c| header_to_string(c, i))
                .unwrap_or_else(|| default_header_name(i))
        })
        .collect();

    let mut error_cells = Vec::new();
    let mut error_cell_count = 0usize;
    let body_raw: Vec<Vec<Value>> = raw_rows[1..]
        .iter()
        .enumerate()
        .map(|(r, row)| {
            (0..last_used_col)
                .map(|i| match row.get(i) {
                    Some(Data::Error(err)) => {
                        // Excel error values have no SQL equivalent; they
                        // import as NULL but are reported to the caller.
                        error_cell_count += 1;
                        if error_cells.len() < MAX_LISTED_ERROR_CELLS {
                            error_cells.push(format!(
                                "{}{}: {}",
                                column_letters(first_col as usize + i),
                                first_row as usize + r + 2,
                                err
                            ));
                        }
                        Value::Null
                    }
                    Some(c) => cell_to_value(c),
                    None => Value::Null,
                })
                .collect::<Vec<_>>()
        })
        .collect();

    // Drop trailing fully-empty rows.
    let last_used_row = body_raw
        .iter()
        .rposition(|row| row.iter().any(|v| !matches!(v, Value::Null)))
        .map(|i| i + 1)
        .unwrap_or(0);

    let body: Vec<Vec<Value>> = body_raw.into_iter().take(last_used_row).collect();
    let inferred_types = infer_column_types(&body, last_used_col);
    Sheet {
        headers,
        body,
        inferred_types,
        error_cells,
        error_cell_count,
    }
}

fn header_to_string(cell: &Data, col_idx: usize) -> String {
    match cell {
        Data::Empty => default_header_name(col_idx),
        Data::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                default_header_name(col_idx)
            } else {
                trimmed.to_string()
            }
        }
        Data::Int(i) => i.to_string(),
        Data::Float(f) => f.to_string(),
        Data::Bool(b) => b.to_string(),
        Data::DateTime(dt) => dt.to_string(),
        Data::DateTimeIso(s) | Data::DurationIso(s) => s.clone(),
        Data::Error(_) => default_header_name(col_idx),
    }
}

fn default_header_name(col_idx: usize) -> String {
    format!("Column{}", col_idx + 1)
}

/// Map a `calamine::Data` cell into a typed JSON value. The frontend
/// hands these straight to `import_data`, which accepts
/// `Vec<Vec<serde_json::Value>>`; types ride through unchanged.
fn cell_to_value(cell: &Data) -> Value {
    match cell {
        Data::Empty => Value::Null,
        Data::String(s) => Value::String(s.clone()),
        Data::Int(i) => Value::Number((*i).into()),
        Data::Float(f) => serde_json::Number::from_f64(*f)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(f.to_string())),
        Data::Bool(b) => Value::Bool(*b),
        Data::DateTime(dt) => {
            // Excel stores datetimes as floats; calamine exposes a
            // helper that converts to chrono. Fall back to the raw
            // value if conversion fails (e.g. out-of-range serial).
            if let Some(naive) = dt.as_datetime() {
                Value::String(naive.format("%Y-%m-%dT%H:%M:%S").to_string())
            } else {
                Value::Number(
                    serde_json::Number::from_f64(dt.as_f64())
                        .unwrap_or_else(|| serde_json::Number::from(0)),
                )
            }
        }
        Data::DateTimeIso(s) | Data::DurationIso(s) => Value::String(s.clone()),
        // Reported separately by `sheet_from_range`.
        Data::Error(_) => Value::Null,
    }
}

fn type_label(v: &Value) -> Option<&'static str> {
    match v {
        Value::Null => None,
        Value::Number(_) => Some("number"),
        Value::Bool(_) => Some("bool"),
        Value::String(s) => {
            if looks_like_iso_datetime(s) {
                Some("date")
            } else {
                Some("string")
            }
        }
        Value::Array(_) | Value::Object(_) => Some("string"),
    }
}

fn looks_like_iso_datetime(s: &str) -> bool {
    // Cheap "is this an ISO-ish datetime" probe just for the inferred
    // column-type hint shown in the UI. We don't need full RFC3339
    // accuracy — anything starting with `YYYY-MM-DD` is good enough
    // to label the column "date".
    s.len() >= 10
        && s.as_bytes().get(4) == Some(&b'-')
        && s.as_bytes().get(7) == Some(&b'-')
        && s.chars().take(4).all(|c| c.is_ascii_digit())
}

fn infer_column_types(body: &[Vec<Value>], cols: usize) -> Vec<&'static str> {
    let mut out = vec!["string"; cols];
    for (col, slot) in out.iter_mut().enumerate().take(cols) {
        let mut seen: Option<&'static str> = None;
        let mut mixed = false;
        for row in body {
            if let Some(label) = row.get(col).and_then(type_label) {
                match seen {
                    None => seen = Some(label),
                    Some(prev) if prev != label => {
                        mixed = true;
                        break;
                    }
                    _ => {}
                }
            }
        }
        *slot = if mixed {
            "mixed"
        } else {
            seen.unwrap_or("string")
        };
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::export::to_xlsx;

    fn make_workbook(cols: &[&str], rows: Vec<Vec<Value>>) -> Vec<u8> {
        let columns: Vec<String> = cols.iter().map(|s| s.to_string()).collect();
        to_xlsx(&columns, &rows, "Sheet1")
            .expect("xlsx encode")
            .bytes
    }

    #[test]
    fn workbook_preview_lists_sheets_and_picks_first_as_default() {
        let bytes = make_workbook(
            &["a", "b"],
            vec![vec![Value::Number(1i64.into()), Value::String("x".into())]],
        );
        let preview = parse_workbook_impl(&bytes).expect("preview");
        assert_eq!(preview.sheet_names, vec!["Sheet1"]);
        assert_eq!(preview.default_sheet, "Sheet1");
        assert_eq!(preview.sheet.name, "Sheet1");
        assert_eq!(preview.sheet.headers, vec!["a", "b"]);
        assert_eq!(preview.sheet.preview_rows.len(), 1);
        assert_eq!(preview.sheet.total_data_rows, 1);
    }

    #[test]
    fn workbook_preview_caps_rows_to_preview_window() {
        let rows: Vec<Vec<Value>> = (0..120)
            .map(|i| vec![Value::Number((i as i64).into())])
            .collect();
        let bytes = make_workbook(&["n"], rows);
        let preview = parse_workbook_impl(&bytes).expect("preview");
        assert_eq!(preview.sheet.preview_rows.len(), PREVIEW_ROWS);
        // The full count is reported separately so the dialog can
        // surface "120 rows · previewing first 50" copy.
        assert_eq!(preview.sheet.total_data_rows, 120);
    }

    #[test]
    fn parse_sheet_returns_full_typed_payload() {
        let bytes = make_workbook(
            &["i", "b", "n"],
            vec![
                vec![Value::Number(7i64.into()), Value::Bool(true), Value::Null],
                vec![
                    Value::Number(8i64.into()),
                    Value::Bool(false),
                    Value::String("x".into()),
                ],
            ],
        );
        let payload = parse_sheet_impl(&bytes, "Sheet1").expect("sheet");
        assert_eq!(payload.name, "Sheet1");
        assert_eq!(payload.headers, vec!["i", "b", "n"]);
        assert_eq!(payload.rows.len(), 2);
        assert!(matches!(&payload.rows[0][0], Value::Number(_)));
        assert!(matches!(&payload.rows[0][1], Value::Bool(true)));
        assert!(matches!(&payload.rows[0][2], Value::Null));
        assert!(matches!(&payload.rows[1][2], Value::String(s) if s == "x"));
        // Type inference reflects body data, not headers.
        assert_eq!(payload.inferred_types, vec!["number", "bool", "string"]);
    }

    #[test]
    fn parse_sheet_unknown_sheet_returns_error() {
        let bytes = make_workbook(&["a"], vec![vec![Value::Number(1i64.into())]]);
        let err = parse_sheet_impl(&bytes, "Nope").unwrap_err().to_string();
        assert!(
            err.contains("Nope"),
            "expected sheet name in error, got: {err}"
        );
    }

    #[test]
    fn error_cells_are_reported_with_addresses() {
        use calamine::{CellErrorType, Range};
        let mut range: Range<Data> = Range::new((0, 0), (3, 1));
        range.set_value((0, 0), Data::String("a".into()));
        range.set_value((0, 1), Data::String("b".into()));
        range.set_value((1, 0), Data::Int(1));
        range.set_value((1, 1), Data::Error(CellErrorType::NA));
        range.set_value((2, 0), Data::Error(CellErrorType::Div0));
        range.set_value((2, 1), Data::String("ok".into()));
        range.set_value((3, 0), Data::Int(3));
        let sheet = sheet_from_range(&range);
        assert_eq!(sheet.body.len(), 3);
        assert_eq!(sheet.body[0][1], Value::Null);
        assert_eq!(sheet.error_cell_count, 2);
        assert_eq!(sheet.error_cells, vec!["B2: #N/A", "A3: #DIV/0!"]);
    }

    #[test]
    fn column_letters_cover_multi_letter_columns() {
        assert_eq!(column_letters(0), "A");
        assert_eq!(column_letters(25), "Z");
        assert_eq!(column_letters(26), "AA");
        assert_eq!(column_letters(27), "AB");
        assert_eq!(column_letters(701), "ZZ");
        assert_eq!(column_letters(702), "AAA");
    }

    #[test]
    fn payload_size_cap_rejects_oversize_input() {
        // Force the cap branch — we don't allocate 110MB, we just lie
        // about the slice length via a synthetic `Vec` slightly above
        // MAX_BYTES. That's what `bytes.len()` checks.
        let bytes = vec![0u8; MAX_BYTES + 1];
        let err = parse_workbook_impl(&bytes).unwrap_err().to_string();
        assert!(err.contains("too large"), "expected size error, got: {err}");
        let err = parse_sheet_impl(&bytes, "Sheet1").unwrap_err().to_string();
        assert!(err.contains("too large"));
    }

    #[test]
    fn invalid_bytes_return_a_friendly_error() {
        let err = parse_workbook_impl(b"not a workbook")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("Could not open"),
            "expected friendly error, got: {err}"
        );
    }

    #[test]
    fn empty_trailing_rows_are_truncated() {
        // Build a workbook with two data rows followed by an explicit
        // all-null row. Round-tripping through to_xlsx + read_sheet
        // should emit exactly two body rows.
        let bytes = make_workbook(
            &["a"],
            vec![
                vec![Value::Number(1i64.into())],
                vec![Value::Number(2i64.into())],
                vec![Value::Null],
                vec![Value::Null],
            ],
        );
        let preview = parse_workbook_impl(&bytes).expect("preview");
        assert_eq!(preview.sheet.total_data_rows, 2);
        assert_eq!(preview.sheet.preview_rows.len(), 2);
    }

    #[test]
    fn inferred_types_flags_mixed_columns() {
        let bytes = make_workbook(
            &["c"],
            vec![
                vec![Value::Number(1i64.into())],
                vec![Value::String("two".into())],
            ],
        );
        let preview = parse_workbook_impl(&bytes).expect("preview");
        assert_eq!(preview.sheet.inferred_types, vec!["mixed"]);
    }

    #[test]
    fn inferred_types_uses_string_for_empty_column() {
        let bytes = make_workbook(&["c"], vec![vec![Value::Null], vec![Value::Null]]);
        let preview = parse_workbook_impl(&bytes).expect("preview");
        assert_eq!(preview.sheet.inferred_types, vec!["string"]);
    }

    #[test]
    fn inferred_types_recognizes_dates() {
        let bytes = make_workbook(
            &["d"],
            vec![
                vec![Value::String("2024-05-01T12:30:00".into())],
                vec![Value::String("2024-06-01T09:00:00".into())],
            ],
        );
        let preview = parse_workbook_impl(&bytes).expect("preview");
        // After round-tripping through xlsx, dates come back as
        // DateTime cells which read_sheet maps to ISO-8601 strings;
        // the inferred-type heuristic flags them as `date`.
        assert_eq!(preview.sheet.inferred_types, vec!["date"]);
    }

    #[tokio::test]
    async fn sheet_payload_comes_from_the_cached_workbook() {
        let bytes = make_workbook(
            &["id", "name"],
            (0..120)
                .map(|i| vec![serde_json::json!(i), serde_json::json!(format!("n{i}"))])
                .collect(),
        );
        let expected = parse_sheet_impl(&bytes, "Sheet1").unwrap();
        let (preview, sheet) = preview_with_sheet(&bytes).unwrap();
        assert_eq!(preview.sheet.total_data_rows, 120);
        assert_eq!(preview.sheet.preview_rows.len(), PREVIEW_ROWS);
        *CACHE.lock().unwrap() = Some(CachedWorkbook {
            token: "t1".into(),
            bytes: std::sync::Arc::new(bytes),
            parsed: Some(("Sheet1".into(), sheet)),
        });

        // The parsed default sheet is handed over as-is...
        let payload = parse_xlsx_sheet("t1".into(), "Sheet1".into())
            .await
            .unwrap();
        assert_eq!(payload.rows, expected.rows);
        assert_eq!(payload.headers, expected.headers);
        assert!(CACHE.lock().unwrap().as_ref().unwrap().parsed.is_none());
        // ...and asking again re-parses from the cached bytes.
        let again = parse_xlsx_sheet("t1".into(), "Sheet1".into())
            .await
            .unwrap();
        assert_eq!(again.rows, expected.rows);

        let err = parse_xlsx_sheet("stale".into(), "Sheet1".into())
            .await
            .unwrap_err();
        assert!(err.contains("no longer loaded"), "{err}");
    }
}
