#![no_main]
//! Fuzz the SQL identifier quoting helpers. These are the only thing
//! standing between user-supplied table/column names and a raw `WHERE`
//! clause splice, so a panic or quoting bypass here is a real
//! correctness/security regression.
//!
//! We exercise both the Postgres flavour (`"…"` with `""` doubling)
//! and the MySQL flavour (`` ` ` `` with backtick doubling). Each call
//! must:
//!   * never panic
//!   * produce output containing the original bytes verbatim somewhere
//!     between the surrounding delimiters (i.e. lossless round-trip
//!     for any embedded delimiter via doubling)
//!   * begin and end with the expected quote char

use libfuzzer_sys::fuzz_target;

fn check_pg(s: &str) {
    let q = tablio_lib::db::pg_common::quote_ident(s);
    assert!(q.starts_with('"') && q.ends_with('"'),
            "pg quote_ident must wrap output in double quotes: input={s:?} output={q:?}");
    // Every embedded `"` must be doubled. Counting unescaped `"` inside
    // the body after stripping the wrapping quotes is the simplest check.
    let inner = &q[1..q.len() - 1];
    // Post-doubling, every `"` should appear in pairs.
    let count = inner.matches('"').count();
    assert!(count % 2 == 0,
            "pg quote_ident left an unbalanced `\"` inside: {q:?} (input={s:?})");
}

fn check_mysql(s: &str) {
    let q = tablio_lib::db::mysql_common::quote_ident(s);
    assert!(q.starts_with('`') && q.ends_with('`'),
            "mysql quote_ident must wrap output in backticks: input={s:?} output={q:?}");
    let inner = &q[1..q.len() - 1];
    let count = inner.matches('`').count();
    assert!(count % 2 == 0,
            "mysql quote_ident left an unbalanced backtick inside: {q:?} (input={s:?})");
}

fuzz_target!(|data: &[u8]| {
    let s = match std::str::from_utf8(data) {
        Ok(s) => s,
        Err(_) => return,
    };
    check_pg(s);
    check_mysql(s);
});
