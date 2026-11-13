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

/// One result set: its column names and rows.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ResultSet {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
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
        Some(set) => QueryResult {
            columns: set.columns,
            rows: set.rows,
            rows_affected,
            execution_time_ms,
            is_select: true,
            notices,
        },
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
}
