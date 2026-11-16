//! Shaping a Query Console run into the single [`QueryResult`] the
//! console shows, shared by the drivers.
//!
//! Whether a statement shows rows is decided by what the server sent
//! back (a result set), not by its leading keyword, so `INSERT ...
//! RETURNING`, `CALL p()`, `EXEC proc` or `INSERT ... OUTPUT` show their
//! rows. A statement that sends several result sets (a procedure, a
//! T-SQL batch) shows the last one, with a notice saying so.

use crate::models::QueryResult;
use serde_json::Value;

/// Most rows a console result keeps. Larger results are counted but not
/// kept: a million-row SELECT used to be held in full (~350 MB in the
/// backend, ~80 MB of JSON for the webview).
pub const MAX_CONSOLE_ROWS: usize = 50_000;

/// One result set: its column names and (up to [`MAX_CONSOLE_ROWS`]) rows.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ResultSet {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    /// Every row the server sent, including those not kept.
    pub total_rows: u64,
}

impl ResultSet {
    pub fn new(columns: Vec<String>) -> Self {
        Self {
            columns,
            ..Default::default()
        }
    }

    /// Count a row, and keep it (calling `values`) while under the cap.
    /// Rows past the cap are never decoded.
    pub fn push_row(&mut self, values: impl FnOnce() -> Vec<Value>) {
        self.total_rows += 1;
        if self.rows.len() < MAX_CONSOLE_ROWS {
            self.rows.push(values());
        }
    }
}

/// Notice for a result cut at [`MAX_CONSOLE_ROWS`].
pub fn truncated_notice(shown: usize, total: u64) -> String {
    format!(
        "Showing the first {} of {} rows. Add a LIMIT or filter to see the others, or use \
         Export to write all of them to a file (it runs the query again).",
        group_digits(shown as u64),
        group_digits(total)
    )
}

fn group_digits(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

/// Shown when a script changed session state and its connection was
/// discarded afterwards so the change can't leak into later queries.
pub const SESSION_RESET_NOTICE: &str = "This script changed session state (SET, temporary \
     tables, ...). That only applied to this run: the connection was closed afterwards so \
     it doesn't affect later queries.";

/// Notice for a run that returned `n > 1` result sets.
pub fn multiple_result_sets_notice(n: usize) -> String {
    format!("The query returned {n} result sets; showing the last one.")
}

/// Build the console result.
///
/// * `sets`: every result set the last statement produced, in order.
/// * `select_like`: the statement is a query by its shape (`SELECT`,
///   `SHOW`, ...). When it produced no result set the driver could see
///   (zero rows, nothing to read the column names from) it is still
///   shown as an empty row set with `fallback_columns`.
/// * `rows_affected`: what the server reported for a statement without
///   a result set.
pub fn console_result(
    mut sets: Vec<ResultSet>,
    select_like: bool,
    fallback_columns: Vec<String>,
    rows_affected: u64,
    execution_time_ms: u64,
) -> QueryResult {
    let mut notices = Vec::new();
    if sets.len() > 1 {
        notices.push(multiple_result_sets_notice(sets.len()));
    }
    match sets.pop() {
        Some(set) => {
            let shown = set.rows.len();
            let truncated = set.total_rows > shown as u64;
            if truncated {
                notices.push(truncated_notice(shown, set.total_rows));
            }
            QueryResult {
                columns: set.columns,
                rows: set.rows,
                rows_affected,
                execution_time_ms,
                is_select: true,
                notices,
                total_rows: truncated.then_some(set.total_rows),
            }
        }
        None if select_like => QueryResult {
            columns: fallback_columns,
            execution_time_ms,
            is_select: true,
            ..Default::default()
        },
        None => QueryResult {
            rows_affected,
            execution_time_ms,
            ..Default::default()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn set(cols: &[&str], rows: Vec<Vec<Value>>) -> ResultSet {
        ResultSet {
            columns: cols.iter().map(|c| c.to_string()).collect(),
            total_rows: rows.len() as u64,
            rows,
        }
    }

    #[test]
    fn last_result_set_wins_with_notice() {
        let r = console_result(
            vec![set(&["a"], vec![vec![json!(1)]]), set(&["b"], vec![])],
            false,
            vec![],
            0,
            5,
        );
        assert!(r.is_select);
        assert_eq!(r.columns, vec!["b"]);
        assert!(r.rows.is_empty());
        assert_eq!(r.notices.len(), 1);
        assert!(r.notices[0].contains("2 result sets"));
    }

    #[test]
    fn rows_from_a_non_select_are_shown() {
        let r = console_result(
            vec![set(&["id"], vec![vec![json!(7)]])],
            false,
            vec![],
            1,
            0,
        );
        assert!(r.is_select);
        assert_eq!(r.rows, vec![vec![json!(7)]]);
        assert!(r.notices.is_empty());
    }

    #[test]
    fn empty_query_keeps_fallback_columns() {
        let r = console_result(vec![], true, vec!["a".into()], 0, 0);
        assert!(r.is_select);
        assert_eq!(r.columns, vec!["a"]);
    }

    #[test]
    fn plain_statement_reports_rows_affected() {
        let r = console_result(vec![], false, vec![], 3, 0);
        assert!(!r.is_select);
        assert_eq!(r.rows_affected, 3);
    }

    #[test]
    fn notices_are_omitted_from_json_when_empty() {
        let r = console_result(vec![], false, vec![], 0, 0);
        let v = serde_json::to_value(&r).unwrap();
        assert!(v.get("notices").is_none());
    }

    #[test]
    fn rows_past_the_cap_are_counted_but_not_kept_or_decoded() {
        let mut set = ResultSet::new(vec!["n".into()]);
        let mut decoded = 0;
        for i in 0..(MAX_CONSOLE_ROWS + 25) {
            set.push_row(|| {
                decoded += 1;
                vec![json!(i)]
            });
        }
        assert_eq!(set.rows.len(), MAX_CONSOLE_ROWS);
        assert_eq!(decoded, MAX_CONSOLE_ROWS);
        assert_eq!(set.total_rows, (MAX_CONSOLE_ROWS + 25) as u64);
        let r = console_result(vec![set], true, vec![], 0, 1);
        assert_eq!(r.rows.len(), MAX_CONSOLE_ROWS);
        assert_eq!(
            r.notices,
            vec![truncated_notice(
                MAX_CONSOLE_ROWS,
                (MAX_CONSOLE_ROWS + 25) as u64
            )]
        );
        assert!(r.notices[0].starts_with("Showing the first 50,000 of 50,025 rows."));
        assert_eq!(r.total_rows, Some((MAX_CONSOLE_ROWS + 25) as u64));
    }

    #[test]
    fn results_under_the_cap_have_no_notice() {
        let r = console_result(vec![set(&["a"], vec![vec![json!(1)]])], true, vec![], 0, 1);
        assert!(r.notices.is_empty());
        assert_eq!(r.total_rows, None);
        let v = serde_json::to_value(&r).unwrap();
        assert!(v.get("total_rows").is_none());
    }
}
