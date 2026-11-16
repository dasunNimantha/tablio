//! Shared checks for the streaming exports, run against each engine.
#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;
use tablio_lib::commands::export::{
    export_query_to_path, export_table_to_path, export_table_to_string,
};
use tablio_lib::commands::xlsx_import::parse_sheet_impl;
use tablio_lib::db::DatabaseDriver;
use tablio_lib::export::{
    sanitize_sheet_name, to_csv, to_json, to_sql_inserts, to_sql_inserts_for, to_xlsx,
    SqlColumnKind, SqlExportDialect,
};
use tablio_lib::models::*;

/// Text values with every kind of character an export has to escape.
pub const TRICKY_TEXTS: &[&str] = &[
    "",
    "a,b",
    "say \"hi\"",
    "line1\nline2",
    "日本語 ñ 🎉",
    "=HYPERLINK(\"http://x\",\"y\")",
    "-12.5",
    "@SUM(A1)",
    "+1+2",
    "O''Brien",
    "plain",
];

/// `INSERT` value lists for an `(id, txt, b, n, f)` table: `binary`
/// renders a binary literal from hex digits.
pub fn tricky_value_rows(count: usize, binary: impl Fn(&str) -> String) -> Vec<String> {
    (0..count)
        .map(|i| {
            let txt = if i % 7 == 3 {
                "NULL".to_string()
            } else {
                format!("'{}'", TRICKY_TEXTS[i % TRICKY_TEXTS.len()])
            };
            let b = match i % 4 {
                0 => "NULL".to_string(),
                1 => binary("deadbeef"),
                2 => binary("00ff"),
                _ => binary(""),
            };
            let n = if i % 5 == 0 {
                "NULL".to_string()
            } else {
                format!("{}.{:02}", i as i64 * 1_000_003 - 7, i % 100)
            };
            let f = if i % 6 == 0 {
                "NULL".to_string()
            } else {
                format!("{}", i as f64 / 4.0)
            };
            format!("({}, {txt}, {b}, {n}, {f})", i + 1)
        })
        .collect()
}

fn request(database: &str, schema: &str, table: &str, format: &str) -> ExportRequest {
    ExportRequest {
        connection_id: "test".into(),
        database: database.into(),
        schema: schema.into(),
        table: table.into(),
        format: format.into(),
        filter: None,
    }
}

/// Only the given files are left in `dir` (no `.part` leftovers).
fn assert_dir_holds(dir: &Path, expected: &[&str]) {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    let mut expected: Vec<String> = expected.iter().map(|s| s.to_string()).collect();
    expected.sort();
    assert_eq!(names, expected);
}

/// Export `table` in every format with tiny pages (so it spans many of
/// them) and compare with the in-memory path: one `fetch_rows` of the
/// whole table in primary-key order, rendered by `to_csv` / `to_json` /
/// `to_sql_inserts_for` / `to_xlsx`.
pub async fn assert_streamed_table_export_matches(
    driver: Arc<dyn DatabaseDriver>,
    db_type: DbType,
    database: &str,
    schema: &str,
    table: &str,
    expected_rows: usize,
) {
    let columns = driver.list_columns(database, schema, table).await.unwrap();
    let pk_sort: Vec<SortSpec> = columns
        .iter()
        .filter(|c| c.is_primary_key)
        .map(|c| SortSpec {
            column: c.name.clone(),
            direction: SortDirection::Asc,
        })
        .collect();
    assert!(!pk_sort.is_empty(), "the test table needs a primary key");
    let all = driver
        .fetch_rows(database, schema, table, 0, 1_000_000, pk_sort, None)
        .await
        .unwrap();
    assert_eq!(all.rows.len(), expected_rows);
    let names: Vec<String> = all.columns.iter().map(|c| c.name.clone()).collect();
    let kinds: Vec<SqlColumnKind> = all
        .columns
        .iter()
        .map(|c| SqlColumnKind::for_type(&c.data_type))
        .collect();

    let dir = tempfile::tempdir().unwrap();
    for format in ["csv", "json", "sql"] {
        let req = request(database, schema, table, format);
        let path = dir.path().join(format!("out.{format}"));
        let (rows, warning) = export_table_to_path(&driver, &db_type, &req, path.clone(), 7)
            .await
            .unwrap();
        assert_eq!(rows, expected_rows as u64);
        assert!(warning.is_none());
        let expected = match format {
            "csv" => to_csv(&names, &all.rows),
            "json" => to_json(&names, &all.rows),
            _ => to_sql_inserts_for(
                SqlExportDialect::for_db(&db_type),
                Some(schema),
                table,
                &names,
                &kinds,
                &all.rows,
            ),
        };
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            expected,
            "{format} file export differs"
        );
        assert_eq!(
            export_table_to_string(&driver, &db_type, &req, 7)
                .await
                .unwrap(),
            expected,
            "{format} string export differs"
        );
    }

    let req = request(database, schema, table, "xlsx");
    let path = dir.path().join("out.xlsx");
    export_table_to_path(&driver, &db_type, &req, path.clone(), 7)
        .await
        .unwrap();
    let sheet = sanitize_sheet_name(table);
    let streamed = parse_sheet_impl(&std::fs::read(&path).unwrap(), &sheet).unwrap();
    let expected_bytes = to_xlsx(&names, &all.rows, table).unwrap().bytes;
    let expected = parse_sheet_impl(&expected_bytes, &sheet).unwrap();
    assert_eq!(streamed.headers, expected.headers);
    assert_eq!(streamed.rows, expected.rows);

    // A failed export (bad filter) leaves an existing file untouched.
    std::fs::write(dir.path().join("out.csv"), "keep me").unwrap();
    let mut bad = request(database, schema, table, "csv");
    bad.filter = Some("no_such_column = 1".into());
    assert!(
        export_table_to_path(&driver, &db_type, &bad, dir.path().join("out.csv"), 7)
            .await
            .is_err()
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("out.csv")).unwrap(),
        "keep me"
    );
    assert_dir_holds(dir.path(), &["out.csv", "out.json", "out.sql", "out.xlsx"]);
}

/// Full export of a query returning 60,000 rows (more than the
/// console keeps), plus an exact comparison for a small query against
/// what the console shows for it.
pub async fn assert_full_query_export(
    driver: Arc<dyn DatabaseDriver>,
    database: &str,
    sixty_thousand_rows_sql: &str,
    small_sql: &str,
) {
    assert_full_query_export_of(driver, database, sixty_thousand_rows_sql, 60_000, small_sql).await
}

/// [`assert_full_query_export`] for a big query returning `big_rows`.
pub async fn assert_full_query_export_of(
    driver: Arc<dyn DatabaseDriver>,
    database: &str,
    sixty_thousand_rows_sql: &str,
    big_rows: u64,
    small_sql: &str,
) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("all.csv");
    let (rows, _) = export_query_to_path(
        &driver,
        database,
        sixty_thousand_rows_sql,
        "csv",
        "q",
        path.clone(),
    )
    .await
    .unwrap();
    assert_eq!(rows, big_rows);
    let csv = std::fs::read_to_string(&path).unwrap();
    assert_eq!(csv.lines().count() as u64, big_rows + 1);

    let path = dir.path().join("all.xlsx");
    let (rows, _) = export_query_to_path(
        &driver,
        database,
        sixty_thousand_rows_sql,
        "xlsx",
        "q",
        path.clone(),
    )
    .await
    .unwrap();
    assert_eq!(rows, big_rows);
    let sheet = parse_sheet_impl(&std::fs::read(&path).unwrap(), "q").unwrap();
    assert_eq!(sheet.rows.len() as u64, big_rows);

    let shown = driver.execute_query(database, small_sql).await.unwrap();
    assert!(shown.total_rows.is_none());
    for format in ["csv", "json", "sql"] {
        let path = dir.path().join(format!("small.{format}"));
        let (rows, _) =
            export_query_to_path(&driver, database, small_sql, format, "q", path.clone())
                .await
                .unwrap();
        assert_eq!(rows, shown.rows.len() as u64);
        let expected = match format {
            "csv" => to_csv(&shown.columns, &shown.rows),
            "json" => to_json(&shown.columns, &shown.rows),
            _ => to_sql_inserts(None, "q", &shown.columns, &shown.rows),
        };
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            expected,
            "{format} differs from the console's rows"
        );
    }

    // Scripts are refused before anything runs.
    let script = format!("SELECT 1; {small_sql}");
    let err = export_query_to_path(
        &driver,
        database,
        &script,
        "csv",
        "q",
        dir.path().join("script.csv"),
    )
    .await
    .unwrap_err();
    assert!(err.contains("Only a single statement"), "{err}");
    assert!(!dir.path().join("script.csv").exists());
}

/// A statement that would change data changes nothing. With
/// `must_fail` the export must also fail (refused or rejected by a
/// read-only transaction) and leave no file; otherwise it may succeed
/// with its change rolled back.
pub async fn assert_full_export_cannot_write(
    driver: Arc<dyn DatabaseDriver>,
    database: &str,
    writing_sql: &str,
    count_sql: &str,
    must_fail: bool,
) {
    let count = |r: QueryResult| -> String {
        let v = &r.rows[0][0];
        v.as_str()
            .map(str::to_string)
            .unwrap_or_else(|| v.to_string())
    };
    let before = count(driver.execute_query(database, count_sql).await.unwrap());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("w.csv");
    let result =
        export_query_to_path(&driver, database, writing_sql, "csv", "q", path.clone()).await;
    if must_fail {
        assert!(result.is_err(), "{writing_sql} was exported: {result:?}");
        assert!(!path.exists());
    }
    let after = count(driver.execute_query(database, count_sql).await.unwrap());
    assert_eq!(before, after, "{writing_sql} changed data");
}
