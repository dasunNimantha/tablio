//! SQL helpers that span multiple drivers.
//!
//! The functions here are deliberately written as a *superset* of
//! what any single engine recognises: they handle every quote form
//! Tablio's supported engines use, so the same splitter can be
//! called from sqlite.rs, pg_common.rs, mysql_common.rs, and
//! mssql.rs without dialect-specific variants.
//!
//! Recognising more quote forms than a given engine uses is
//! harmless: e.g. on MySQL input, the parser will never enter
//! dollar-quote mode because the input won't contain `$$` outside a
//! single-quoted string. The only real cost is a slightly larger
//! state machine.

/// Split a SQL script into individual statements, respecting
/// strings, identifiers, comments, and `BEGIN ... END` compound
/// blocks so semicolons inside them don't accidentally split a
/// statement in two.
///
/// Used by `execute_query` in every Tablio driver to support
/// multi-statement input from the Query Console (e.g.
/// `CREATE VIEW v AS ...; SELECT * FROM v;`). The previous logic
/// in each driver routed on the FIRST keyword of the whole input,
/// which incorrectly chose the exec path for any script that
/// started with DDL — even if the LAST statement was a SELECT that
/// the user expected rows from.
///
/// Statement separator: `;` at the top level (i.e. outside every
/// quoted form, every comment, and every compound block).
///
/// Quote forms recognised (semicolons inside are kept as content):
///
/// | Form              | Notes                                             |
/// |-------------------|---------------------------------------------------|
/// | `'...'`           | Standard SQL string literal. `''` is the escape.  |
/// | `"..."`           | Standard SQL identifier. `""` is the escape.      |
/// | `` `...` ``       | MySQL/SQLite identifier. `` `` `` is the escape.  |
/// | `[...]`           | SQL Server / SQLite bracket identifier. No escape; first `]` closes. |
/// | `$$...$$`         | PostgreSQL anonymous dollar-quoted string.        |
/// | `$tag$...$tag$`   | PostgreSQL tagged dollar-quoted string.           |
///
/// Comments recognised (everything inside is treated as content):
/// - `-- line comment` until end of line
/// - `/* block comment */` non-nested
///
/// Compound blocks recognised:
///
/// SQLite triggers, MySQL stored programs, and SQL Server trigger
/// bodies wrap multiple inner statements in `BEGIN ... END` and
/// separate them with `;`. Without special handling, those inner
/// semicolons would split the outer `CREATE TRIGGER ...` in two
/// and break the script.
///
/// To handle this we track `BEGIN ... END` nesting, but ONLY after
/// we've seen `TRIGGER`, `PROCEDURE`, or `FUNCTION` at top level
/// in the current statement. That way the transaction-control
/// shape `BEGIN; ... ; COMMIT;` still splits the way users expect
/// (it never enters compound-block mode because there's no
/// preceding `TRIGGER`/`PROCEDURE`/`FUNCTION` keyword).
///
/// Inside compound-block mode, both `BEGIN` and `CASE` open a
/// block; `END` closes one. We count `CASE` because a CASE
/// expression's `END` would otherwise prematurely decrement the
/// block depth back to zero — e.g.
/// `CREATE TRIGGER ... BEGIN SELECT CASE WHEN x THEN 1 ELSE 0 END FROM t; END;`.
///
/// Blank fragments between semicolons are dropped so callers can
/// `.last()` on the result without surprises.
pub fn split_sql_statements(sql: &str) -> Vec<String> {
    split_sql_statements_for(sql, SqlDialect::Generic)
}

/// Which engine's string-literal rules to apply while scanning SQL.
///
/// The structural rules (comments, identifiers, compound blocks) are
/// shared, but backslash handling inside string literals is not:
///
/// * `MySql` — MySQL / MariaDB / TiDB treat `\` as an escape inside
///   both `'...'` and `"..."` by default (unless the server runs with
///   `NO_BACKSLASH_ESCAPES`), so `'it\'s'` is one literal.
/// * `Postgres` — standard strings have no backslash escapes, but
///   `E'...'` escape strings do.
/// * `Generic` — SQL-standard rules only (`''` escape, no backslash).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    Generic,
    Postgres,
    MySql,
}

/// Dialect-aware variant of [`split_sql_statements`].
pub fn split_sql_statements_for(sql: &str, dialect: SqlDialect) -> Vec<String> {
    let mut statements: Vec<String> = Vec::new();
    let mut current = String::new();
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0;

    // Compound-block tracking. `compound_keyword_seen` flips on at
    // the first top-level `TRIGGER` / `PROCEDURE` / `FUNCTION`
    // keyword in the current statement. `block_depth` then counts
    // `BEGIN ... END` / `CASE ... END` nesting; while > 0,
    // semicolons are content, not separators.
    let mut compound_keyword_seen = false;
    let mut block_depth: u32 = 0;

    while i < chars.len() {
        let c = chars[i];

        // Compound-block keyword tracking. Done first because we
        // want to consume keywords like `BEGIN` into `current` as a
        // single unit and update state at the same time. The
        // is_word_start guard makes sure we don't false-fire on
        // mid-identifier substrings like the `END` in `EXTEND`.
        if is_word_start(&chars, i) {
            // Compound-context entry: TRIGGER / PROCEDURE /
            // FUNCTION. Once seen, BEGIN/CASE/END become
            // meaningful for THIS statement.
            if !compound_keyword_seen && block_depth == 0 {
                if let Some(consumed) = consume_keyword(&chars, i, "TRIGGER")
                    .or_else(|| consume_keyword(&chars, i, "PROCEDURE"))
                    .or_else(|| consume_keyword(&chars, i, "FUNCTION"))
                {
                    compound_keyword_seen = true;
                    push_slice(&mut current, &chars, i, consumed);
                    i += consumed;
                    continue;
                }
            }

            // Block opener/closer tracking inside compound mode.
            if compound_keyword_seen {
                if let Some(consumed) = consume_keyword(&chars, i, "BEGIN") {
                    // `BEGIN TRAN` / `BEGIN TRANSACTION` / `BEGIN
                    // DISTRIBUTED TRANSACTION` inside a T-SQL body
                    // start a transaction, not a block: they are
                    // closed by COMMIT/ROLLBACK, never by END.
                    let next = skip_ws(&chars, i + consumed);
                    let is_txn = ["TRAN", "TRANSACTION", "DISTRIBUTED"]
                        .iter()
                        .any(|kw| consume_keyword(&chars, next, kw).is_some());
                    if !is_txn {
                        block_depth = block_depth.saturating_add(1);
                    }
                    push_slice(&mut current, &chars, i, consumed);
                    i += consumed;
                    continue;
                }
                if let Some(consumed) = consume_keyword(&chars, i, "CASE") {
                    block_depth = block_depth.saturating_add(1);
                    push_slice(&mut current, &chars, i, consumed);
                    i += consumed;
                    continue;
                }
                if let Some(consumed) = consume_keyword(&chars, i, "END") {
                    // `END IF` / `END LOOP` / `END WHILE` / `END
                    // REPEAT` close constructs whose openers are not
                    // counted (an `IF` keyword is ambiguous with the
                    // `IF()` function and `IF EXISTS`), so they must
                    // not decrement either. `END CASE` closes a CASE
                    // statement, whose `CASE` *was* counted, so it
                    // decrements exactly once and the trailing `CASE`
                    // is consumed here so it is not counted as a new
                    // opener.
                    //
                    // T-SQL has no `END IF` but does allow a new `IF` /
                    // `WHILE` statement right after a block's `END`
                    // without a separating `;`, so outside the MySQL
                    // dialect the suffix only counts when it is
                    // immediately terminated (`END IF;`).
                    let next = skip_ws(&chars, i + consumed);
                    let suffix_counts = |len: usize| {
                        if dialect == SqlDialect::MySql {
                            return true;
                        }
                        let after = skip_ws(&chars, next + len);
                        after >= chars.len() || chars[after] == ';'
                    };
                    if let Some(kw_len) = ["IF", "LOOP", "WHILE", "REPEAT"]
                        .iter()
                        .find_map(|kw| consume_keyword(&chars, next, kw))
                        .filter(|len| suffix_counts(*len))
                    {
                        let total = next + kw_len - i;
                        push_slice(&mut current, &chars, i, total);
                        i += total;
                        continue;
                    }
                    let mut total = consumed;
                    if let Some(kw_len) =
                        consume_keyword(&chars, next, "CASE").filter(|len| suffix_counts(*len))
                    {
                        total = next + kw_len - i;
                    }
                    block_depth = block_depth.saturating_sub(1);
                    push_slice(&mut current, &chars, i, total);
                    i += total;
                    continue;
                }
            }
        }

        // Line comment: scan until newline. The newline itself is
        // kept on the current statement so subsequent line-based
        // diagnostics still resolve correctly.
        if c == '-' && i + 1 < chars.len() && chars[i + 1] == '-' {
            while i < chars.len() && chars[i] != '\n' {
                current.push(chars[i]);
                i += 1;
            }
            continue;
        }

        // Block comment: scan to the closing `*/`.
        if c == '/' && i + 1 < chars.len() && chars[i + 1] == '*' {
            current.push(chars[i]);
            current.push(chars[i + 1]);
            i += 2;
            while i + 1 < chars.len() && !(chars[i] == '*' && chars[i + 1] == '/') {
                current.push(chars[i]);
                i += 1;
            }
            if i + 1 < chars.len() {
                current.push(chars[i]);
                current.push(chars[i + 1]);
                i += 2;
            }
            continue;
        }

        // PostgreSQL dollar-quoted string. The opening tag is
        // `$<ident>?$` where `<ident>` is optional and made up of
        // alphanumerics + underscores. Anything until the matching
        // `$<ident>?$` is content. Closing tag must match the
        // opening tag exactly.
        if c == '$' {
            // Try to parse an opening dollar-quote tag starting at `i`.
            let mut tag_end = i + 1;
            let mut tag_buf = String::new();
            while tag_end < chars.len() {
                let t = chars[tag_end];
                if t == '$' {
                    break;
                }
                // Tags can only contain identifier chars. If we see
                // anything else, this isn't a tag — it's just a bare
                // `$` token in the SQL.
                if !(t.is_alphanumeric() || t == '_') {
                    tag_end = i; // sentinel — not a tag
                    break;
                }
                tag_buf.push(t);
                tag_end += 1;
            }

            if tag_end > i && tag_end < chars.len() && chars[tag_end] == '$' {
                // Found opening `$tag$` (tag may be empty).
                let tag = tag_buf;
                let closer: String = format!("${}$", tag);
                let opener_len = closer.len(); // same shape

                // Emit the opener verbatim.
                for c in chars.iter().take(i + opener_len).skip(i) {
                    current.push(*c);
                }
                i += opener_len;

                // Scan until we find the matching closer.
                while i < chars.len() {
                    if chars[i] == '$' {
                        // Look ahead to see if this is the closing
                        // tag.
                        let slice: String = chars.iter().skip(i).take(closer.len()).collect();
                        if slice == closer {
                            for c in chars.iter().take(i + closer.len()).skip(i) {
                                current.push(*c);
                            }
                            i += closer.len();
                            break;
                        }
                    }
                    current.push(chars[i]);
                    i += 1;
                }
                continue;
            }
            // Else: fall through and treat `$` as a regular character.
        }

        // Single-quoted string. `''` is the SQL-standard escape;
        // backslash escapes apply on MySQL and in Postgres `E'...'`.
        if c == '\'' {
            let backslash = match dialect {
                SqlDialect::MySql => true,
                SqlDialect::Postgres => {
                    i > 0 && matches!(chars[i - 1], 'E' | 'e') && is_word_start(&chars, i - 1)
                }
                SqlDialect::Generic => false,
            };
            current.push(c);
            i += 1;
            while i < chars.len() {
                let q = chars[i];
                current.push(q);
                i += 1;
                if backslash && q == '\\' {
                    if i < chars.len() {
                        current.push(chars[i]);
                        i += 1;
                    }
                    continue;
                }
                if q == '\'' {
                    if i < chars.len() && chars[i] == '\'' {
                        current.push(chars[i]);
                        i += 1;
                        continue;
                    }
                    break;
                }
            }
            continue;
        }

        // Double-quoted identifier (a string on MySQL). `""` escape,
        // plus backslash escapes on MySQL.
        if c == '"' {
            let backslash = dialect == SqlDialect::MySql;
            current.push(c);
            i += 1;
            while i < chars.len() {
                let q = chars[i];
                current.push(q);
                i += 1;
                if backslash && q == '\\' {
                    if i < chars.len() {
                        current.push(chars[i]);
                        i += 1;
                    }
                    continue;
                }
                if q == '"' {
                    if i < chars.len() && chars[i] == '"' {
                        current.push(chars[i]);
                        i += 1;
                        continue;
                    }
                    break;
                }
            }
            continue;
        }

        // Backtick-quoted identifier (MySQL/SQLite). `` `` `` escape.
        if c == '`' {
            current.push(c);
            i += 1;
            while i < chars.len() {
                let q = chars[i];
                current.push(q);
                i += 1;
                if q == '`' {
                    if i < chars.len() && chars[i] == '`' {
                        current.push(chars[i]);
                        i += 1;
                        continue;
                    }
                    break;
                }
            }
            continue;
        }

        // Bracket-quoted identifier (SQLite + SQL Server). No escape;
        // first `]` closes.
        if c == '[' {
            current.push(c);
            i += 1;
            while i < chars.len() {
                let q = chars[i];
                current.push(q);
                i += 1;
                if q == ']' {
                    break;
                }
            }
            continue;
        }

        // Statement terminator at the top level. Suppressed while
        // we're inside a compound `BEGIN ... END` block — those
        // inner semicolons belong to the block body.
        if c == ';' && block_depth == 0 {
            let trimmed = current.trim();
            if !trimmed.is_empty() {
                statements.push(trimmed.to_string());
            }
            current.clear();
            compound_keyword_seen = false;
            i += 1;
            continue;
        }

        current.push(c);
        i += 1;
    }

    // Trailing statement without a terminating `;`.
    let trimmed = current.trim();
    if !trimmed.is_empty() {
        statements.push(trimmed.to_string());
    }

    statements
}

/// Returns true when `chars[pos]` is the start of an identifier-like
/// word: either the start of the input or preceded by a non-word
/// character.
fn is_word_start(chars: &[char], pos: usize) -> bool {
    if pos == 0 {
        return true;
    }
    let prev = chars[pos - 1];
    !is_word_char(prev)
}

fn is_word_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// If `chars[pos..]` begins with the case-insensitive keyword `kw`
/// AND is followed by a non-word character (or end of input),
/// returns the number of source chars consumed. Otherwise `None`.
fn consume_keyword(chars: &[char], pos: usize, kw: &str) -> Option<usize> {
    let kw_chars: Vec<char> = kw.chars().collect();
    if pos + kw_chars.len() > chars.len() {
        return None;
    }
    for (offset, kc) in kw_chars.iter().enumerate() {
        if !chars[pos + offset].eq_ignore_ascii_case(kc) {
            return None;
        }
    }
    let after = pos + kw_chars.len();
    if after < chars.len() && is_word_char(chars[after]) {
        return None;
    }
    Some(kw_chars.len())
}

/// Index of the first non-whitespace char at or after `pos`.
fn skip_ws(chars: &[char], mut pos: usize) -> usize {
    while pos < chars.len() && chars[pos].is_whitespace() {
        pos += 1;
    }
    pos
}

fn push_slice(dst: &mut String, chars: &[char], start: usize, len: usize) {
    for c in chars.iter().skip(start).take(len) {
        dst.push(*c);
    }
}

/// Replace the *contents* of every quoted literal / identifier in
/// `s` with spaces, leaving the quote characters themselves (and
/// everything outside quotes) in place. Used by the filter safety
/// check so that `name = 'a;b'` or `note LIKE '%--%'` are not
/// mistaken for statement separators or comments.
///
/// Returns `None` when a quote is left unterminated, because then
/// the server would see everything after it as one literal and the
/// masked view would no longer describe what actually runs.
pub fn mask_quoted_contents(s: &str, dialect: SqlDialect) -> Option<String> {
    let chars: Vec<char> = s.chars().collect();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let close = match c {
            '\'' | '"' | '`' => c,
            '[' if dialect == SqlDialect::Generic => ']',
            _ => {
                out.push(c);
                i += 1;
                continue;
            }
        };
        let backslash = match (dialect, c) {
            (SqlDialect::MySql, '\'' | '"') => true,
            (SqlDialect::Postgres, '\'') => {
                i > 0 && matches!(chars[i - 1], 'E' | 'e') && is_word_start(&chars, i - 1)
            }
            _ => false,
        };
        out.push(c);
        i += 1;
        let mut closed = false;
        while i < chars.len() {
            let q = chars[i];
            if backslash && q == '\\' {
                out.push(' ');
                if i + 1 < chars.len() {
                    out.push(' ');
                }
                i += 2;
                continue;
            }
            if q == close {
                // Doubled closing quote is an escaped quote (not for `]`).
                if close != ']' && i + 1 < chars.len() && chars[i + 1] == close {
                    out.push(' ');
                    out.push(' ');
                    i += 2;
                    continue;
                }
                out.push(q);
                i += 1;
                closed = true;
                break;
            }
            out.push(' ');
            i += 1;
        }
        if !closed {
            return None;
        }
    }
    Some(out)
}

/// Shared filter-fragment safety check (see
/// `pg_common::filter_is_unsafe` for the threat model). Characters
/// inside properly terminated quoted literals are ignored, so values
/// such as `'a;b'` or `'x--y'` are accepted while real statement
/// separators, comments, subqueries and `UNION SELECT` outside quotes
/// are still rejected. Returns a human-readable reason, or `None`
/// when the fragment is acceptable.
pub fn filter_unsafe_reason_for(filter: &str, dialect: SqlDialect) -> Option<&'static str> {
    let Some(s) = mask_quoted_contents(filter.trim(), dialect) else {
        return Some("unterminated quoted string");
    };
    if s.contains(';') {
        return Some("statement terminator (`;`)");
    }
    if s.contains("--") || s.contains("/*") || s.contains("*/") {
        return Some("SQL comment markers (`--` / `/* */`)");
    }
    // Collapse whitespace so `( SELECT` / `UNION\nSELECT` are caught too.
    let u = s
        .to_uppercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if u.contains("(SELECT") || u.contains("( SELECT") {
        return Some("nested `(SELECT ...)` subquery");
    }
    let padded = format!(" {}", u);
    if padded.contains(" UNION SELECT")
        || padded.contains(" UNION ALL SELECT")
        || padded.contains(" UNION DISTINCT SELECT")
    {
        return Some("`UNION SELECT` clause");
    }
    None
}

/// Largest integer magnitude a JavaScript `number` represents exactly
/// (`Number.MAX_SAFE_INTEGER`).
pub const JS_MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;

/// JSON for an integer cell. Values the frontend's `number` cannot hold
/// exactly are sent as the exact decimal string instead, so primary-key
/// based edits never target a rounded (wrong) row.
pub fn i64_to_json(v: i64) -> serde_json::Value {
    if (-JS_MAX_SAFE_INTEGER..=JS_MAX_SAFE_INTEGER).contains(&v) {
        serde_json::Value::Number(v.into())
    } else {
        serde_json::Value::String(v.to_string())
    }
}

/// Unsigned counterpart of [`i64_to_json`].
pub fn u64_to_json(v: u64) -> serde_json::Value {
    if v <= JS_MAX_SAFE_INTEGER as u64 {
        serde_json::Value::Number(v.into())
    } else {
        serde_json::Value::String(v.to_string())
    }
}

/// Normalised `(negative, significant digits, exponent)` form of a
/// plain decimal literal, used to compare two spellings of the same
/// value. `None` when `s` is not a decimal literal.
fn normalize_decimal(s: &str) -> Option<(bool, String, i64)> {
    let lit = numeric_literal(s)?;
    let (neg, body) = match lit.as_bytes()[0] {
        b'-' => (true, &lit[1..]),
        b'+' => (false, &lit[1..]),
        _ => (false, lit),
    };
    let (mantissa, exp) = match body.find(['e', 'E']) {
        Some(idx) => (&body[..idx], body[idx + 1..].parse::<i64>().ok()?),
        None => (body, 0),
    };
    let (int_part, frac_part) = match mantissa.find('.') {
        Some(idx) => (&mantissa[..idx], &mantissa[idx + 1..]),
        None => (mantissa, ""),
    };
    let mut digits = format!("{}{}", int_part, frac_part);
    let mut exp = exp - frac_part.len() as i64;
    let trimmed_lead = digits.trim_start_matches('0').to_string();
    digits = trimmed_lead;
    while digits.ends_with('0') {
        digits.pop();
        exp += 1;
    }
    if digits.is_empty() {
        return Some((false, String::new(), 0));
    }
    Some((neg, digits, exp))
}

/// JSON for a NUMERIC / DECIMAL value given its exact decimal text.
///
/// The value stays a JSON number only when that number denotes exactly
/// the same decimal value (and, for integers, is within the JS safe
/// range); otherwise the exact text is sent as a string. Non-numeric
/// text such as `NaN` / `Infinity` is passed through as a string.
pub fn exact_numeric_text_to_json(text: &str) -> serde_json::Value {
    let text = text.trim();
    let Some(norm) = normalize_decimal(text) else {
        return serde_json::Value::String(text.to_string());
    };
    if let Ok(i) = text.trim_start_matches('+').parse::<i64>() {
        return i64_to_json(i);
    }
    let as_number = text
        .parse::<f64>()
        .ok()
        .filter(|f| f.is_finite())
        .and_then(serde_json::Number::from_f64);
    if let Some(n) = as_number {
        let integral_too_big = norm.2 >= 0
            && n.as_f64()
                .map(|f| f.abs() > JS_MAX_SAFE_INTEGER as f64)
                .unwrap_or(true);
        if !integral_too_big && normalize_decimal(&n.to_string()) == Some(norm) {
            return serde_json::Value::Number(n);
        }
    }
    serde_json::Value::String(text.to_string())
}

/// Returns the trimmed input when it is a plain decimal numeric
/// literal (`-12`, `3.50`, `.5`, `1e10`), otherwise `None`. Used to
/// validate string values before splicing them unquoted into SQL.
pub fn numeric_literal(s: &str) -> Option<&str> {
    let t = s.trim();
    let b = t.as_bytes();
    let mut i = 0;
    if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
        i += 1;
    }
    let int_start = i;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
    }
    let mut digits = i - int_start;
    if i < b.len() && b[i] == b'.' {
        i += 1;
        let frac_start = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        digits += i - frac_start;
    }
    if digits == 0 {
        return None;
    }
    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        i += 1;
        if i < b.len() && (b[i] == b'+' || b[i] == b'-') {
            i += 1;
        }
        let exp_start = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == exp_start {
            return None;
        }
    }
    if i == b.len() {
        Some(t)
    } else {
        None
    }
}

/// True for integer / exact-numeric / floating column types whose
/// values are written as bare numeric literals (`BIGINT UNSIGNED`,
/// `decimal(20,4)`, `int8`, `varint`, ...). Deliberately excludes
/// types that merely contain "int" (`point`, `interval`) and money.
pub fn is_numeric_type_name(data_type: &str) -> bool {
    let lower = data_type.to_ascii_lowercase();
    let base = lower.split('(').next().unwrap_or("").trim();
    let base = base
        .trim_end_matches(" zerofill")
        .trim_end_matches(" unsigned")
        .trim_end_matches(" signed")
        .trim();
    matches!(
        base,
        "tinyint"
            | "smallint"
            | "mediumint"
            | "int"
            | "integer"
            | "bigint"
            | "int2"
            | "int4"
            | "int8"
            | "serial"
            | "smallserial"
            | "bigserial"
            | "decimal"
            | "numeric"
            | "dec"
            | "fixed"
            | "float"
            | "float4"
            | "float8"
            | "double"
            | "double precision"
            | "real"
            | "varint"
            | "counter"
            | "number"
    )
}

/// Returns true when the SQL statement's leading keyword indicates
/// it should be run via `fetch_all` to return rows. Engine-specific
/// keyword sets are passed by the caller; this is just the
/// leading-keyword extraction logic.
pub fn statement_is_select_like(sql: &str, select_keywords: &[&str]) -> bool {
    let trimmed = sql.trim_start();
    // Strip leading comments before the first keyword. Reuses the
    // same comment forms `split_sql_statements` recognises so a
    // statement that starts with `-- ...\nSELECT ...` is correctly
    // classified.
    let body = strip_leading_comments(trimmed);
    let upper = body.trim_start().to_ascii_uppercase();
    select_keywords
        .iter()
        .any(|k| upper.starts_with(&k.to_ascii_uppercase()))
}

fn strip_leading_comments(mut s: &str) -> &str {
    loop {
        s = s.trim_start();
        if let Some(rest) = s.strip_prefix("--") {
            // Skip until newline.
            match rest.find('\n') {
                Some(idx) => s = &rest[idx + 1..],
                None => return "",
            }
        } else if let Some(rest) = s.strip_prefix("/*") {
            // Skip until */.
            match rest.find("*/") {
                Some(idx) => s = &rest[idx + 2..],
                None => return "",
            }
        } else {
            return s;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_empty() {
        assert!(split_sql_statements("").is_empty());
        assert!(split_sql_statements("   \n\t ").is_empty());
        assert!(split_sql_statements(";;;").is_empty());
    }

    #[test]
    fn split_single_no_terminator() {
        assert_eq!(split_sql_statements("SELECT 1"), vec!["SELECT 1"]);
    }

    #[test]
    fn split_single_with_terminator() {
        assert_eq!(split_sql_statements("SELECT 1;"), vec!["SELECT 1"]);
    }

    #[test]
    fn split_pair_ddl_then_select() {
        let out = split_sql_statements("CREATE VIEW v AS SELECT 1; SELECT * FROM v");
        assert_eq!(out.len(), 2);
        assert_eq!(out[0], "CREATE VIEW v AS SELECT 1");
        assert_eq!(out[1], "SELECT * FROM v");
    }

    #[test]
    fn split_handles_semicolon_in_single_quoted_string() {
        let out = split_sql_statements("SELECT ';'; SELECT 1");
        assert_eq!(out, vec!["SELECT ';'", "SELECT 1"]);
    }

    #[test]
    fn split_handles_escaped_quote_in_string() {
        let out = split_sql_statements("SELECT 'don''t; split'; SELECT 1");
        assert_eq!(out, vec!["SELECT 'don''t; split'", "SELECT 1"]);
    }

    #[test]
    fn split_handles_semicolon_in_double_quoted_identifier() {
        let out = split_sql_statements(r#"SELECT "weird;name" FROM t; SELECT 1"#);
        assert_eq!(out, vec![r#"SELECT "weird;name" FROM t"#, "SELECT 1"]);
    }

    #[test]
    fn split_handles_semicolon_in_bracket_identifier() {
        let out = split_sql_statements("SELECT [oh; no] FROM t; SELECT 1");
        assert_eq!(out, vec!["SELECT [oh; no] FROM t", "SELECT 1"]);
    }

    #[test]
    fn split_handles_semicolon_in_backtick_identifier() {
        let out = split_sql_statements("SELECT `back; tick` FROM t; SELECT 1");
        assert_eq!(out, vec!["SELECT `back; tick` FROM t", "SELECT 1"]);
    }

    #[test]
    fn split_handles_semicolon_in_line_comment() {
        let out = split_sql_statements("SELECT 1 -- yes; really\n; SELECT 2");
        assert_eq!(out.len(), 2);
        assert!(out[0].contains("yes; really"));
        assert_eq!(out[1], "SELECT 2");
    }

    #[test]
    fn split_handles_semicolon_in_block_comment() {
        let out = split_sql_statements("SELECT 1 /* a; b; c */; SELECT 2");
        assert_eq!(out.len(), 2);
        assert!(out[0].contains("a; b; c"));
        assert_eq!(out[1], "SELECT 2");
    }

    #[test]
    fn split_handles_anonymous_dollar_quote() {
        // PostgreSQL DO blocks routinely use `$$...$$` to delimit
        // PL/pgSQL bodies. Semicolons inside must not split the
        // outer DO statement.
        let out = split_sql_statements("DO $$ BEGIN PERFORM 1; PERFORM 2; END $$; SELECT 3;");
        assert_eq!(out.len(), 2);
        assert!(out[0].starts_with("DO $$ BEGIN"));
        assert!(out[0].contains("PERFORM 1;"));
        assert!(out[0].ends_with("END $$"));
        assert_eq!(out[1], "SELECT 3");
    }

    #[test]
    fn split_handles_tagged_dollar_quote() {
        // Tagged dollar quote: `$func$ ... $func$`. The closer must
        // match the opener; a stray `$other$` in the body is treated
        // as content.
        let out = split_sql_statements(
            "CREATE FUNCTION f() RETURNS void AS $func$ \
             BEGIN \
                RAISE NOTICE 'hi $other$ middle'; \
                RAISE NOTICE 'more; stuff'; \
             END $func$ LANGUAGE plpgsql; \
             SELECT 1;",
        );
        assert_eq!(out.len(), 2);
        assert!(out[0].starts_with("CREATE FUNCTION f()"));
        assert!(out[0].contains("$other$ middle"));
        assert!(out[0].contains("more; stuff"));
        assert!(out[0].contains("$func$ LANGUAGE plpgsql"));
        assert_eq!(out[1], "SELECT 1");
    }

    #[test]
    fn split_treats_bare_dollar_as_normal_char() {
        // A `$N` parameter placeholder or a money column name
        // (e.g. `$amount`) is not a dollar quote — there's no
        // closing `$`. The splitter should treat `$` as normal and
        // keep walking.
        let out = split_sql_statements("SELECT $1, '$money'; SELECT $2");
        assert_eq!(out, vec!["SELECT $1, '$money'", "SELECT $2"]);
    }

    #[test]
    fn split_drops_blank_fragments_between_semicolons() {
        let out = split_sql_statements("SELECT 1;\n\n; SELECT 2;\n");
        assert_eq!(out, vec!["SELECT 1", "SELECT 2"]);
    }

    #[test]
    fn split_real_world_create_view_then_select() {
        // The exact shape that triggered the user's bug report
        // while testing #126 — see PR #131 for the full story.
        let input = r#"
            CREATE VIEW order_summary AS
            SELECT
              o.id,
              if(o.paid = 1, 'paid', 'unpaid') AS payment_status,
              if(o.shipped_at IS NULL, 'pending', 'shipped') AS fulfillment,
              printf('$%.2f', o.total_cents / 100.0) AS total
            FROM orders o
            JOIN users u ON u.id = o.user_id;

            SELECT * FROM order_summary LIMIT 10;
        "#;
        let out = split_sql_statements(input);
        assert_eq!(out.len(), 2);
        assert!(out[0].starts_with("CREATE VIEW order_summary"));
        assert!(out[0].contains("if(o.paid = 1"));
        assert!(out[1].starts_with("SELECT * FROM order_summary"));
    }

    #[test]
    fn statement_is_select_like_handles_leading_whitespace() {
        let keywords = &["SELECT", "WITH"];
        assert!(statement_is_select_like("  SELECT 1", keywords));
        assert!(statement_is_select_like("\n\tWITH cte AS ...", keywords));
        assert!(!statement_is_select_like(
            "INSERT INTO t VALUES (1)",
            keywords
        ));
    }

    #[test]
    fn statement_is_select_like_strips_leading_comments() {
        let keywords = &["SELECT"];
        assert!(statement_is_select_like(
            "-- explain what this does\nSELECT 1",
            keywords
        ));
        assert!(statement_is_select_like(
            "/* block */ /* and another */ SELECT 1",
            keywords
        ));
        assert!(!statement_is_select_like(
            "-- comment\nCREATE TABLE t (x INT)",
            keywords
        ));
    }

    // ---------------------------------------------------------------
    // BEGIN...END compound block tracking (regression for the
    // `CREATE TRIGGER ... BEGIN SELECT 1; END` case discovered by
    // the SQLite trigger integration test).
    // ---------------------------------------------------------------

    #[test]
    fn split_keeps_create_trigger_with_inner_semicolons_intact() {
        let input = r#"CREATE TRIGGER "tr_x" AFTER INSERT ON "t" FOR EACH ROW BEGIN SELECT 1; END"#;
        let out = split_sql_statements(input);
        assert_eq!(out.len(), 1, "got: {:?}", out);
        assert!(out[0].starts_with("CREATE TRIGGER"));
        assert!(out[0].contains("BEGIN SELECT 1;"));
        assert!(out[0].ends_with("END"));
    }

    #[test]
    fn split_create_trigger_then_select() {
        let input = "CREATE TRIGGER tr AFTER INSERT ON t \
            BEGIN INSERT INTO log VALUES (1); UPDATE log SET n = n + 1; END; \
            SELECT * FROM log;";
        let out = split_sql_statements(input);
        assert_eq!(out.len(), 2, "got: {:?}", out);
        assert!(out[0].starts_with("CREATE TRIGGER"));
        assert!(out[0].contains("INSERT INTO log"));
        assert!(out[0].contains("UPDATE log"));
        assert!(out[0].ends_with("END"));
        assert!(out[1].starts_with("SELECT"));
    }

    #[test]
    fn split_trigger_body_with_inner_case_end() {
        // A `CASE ... END` inside the trigger body must not be
        // misread as the trigger's closing END.
        let input = "CREATE TRIGGER tr AFTER INSERT ON t \
                     BEGIN \
                        SELECT CASE WHEN x > 0 THEN 'p' ELSE 'n' END FROM t; \
                        INSERT INTO log VALUES (1); \
                     END; \
                     SELECT 1;";
        let out = split_sql_statements(input);
        assert_eq!(out.len(), 2, "got: {:?}", out);
        assert!(out[0].contains("CASE WHEN x > 0"));
        assert!(out[0].contains("INSERT INTO log"));
        assert!(out[0].ends_with("END"));
        assert_eq!(out[1], "SELECT 1");
    }

    #[test]
    fn split_transaction_begin_commit_is_not_compound() {
        // `BEGIN; ... ; COMMIT;` is a transaction, not a compound
        // block. No preceding TRIGGER/PROCEDURE/FUNCTION keyword
        // was seen, so the splitter must NOT enter compound mode
        // and the user's `;` separators continue to work.
        let input = "BEGIN; INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); COMMIT;";
        let out = split_sql_statements(input);
        assert_eq!(
            out,
            vec![
                "BEGIN",
                "INSERT INTO t VALUES (1)",
                "INSERT INTO t VALUES (2)",
                "COMMIT",
            ]
        );
    }

    #[test]
    fn split_create_procedure_body_keeps_inner_semicolons() {
        let input = "CREATE PROCEDURE p() \
                     BEGIN \
                        DECLARE x INT; \
                        SET x = 1; \
                        INSERT INTO t VALUES (x); \
                     END";
        let out = split_sql_statements(input);
        assert_eq!(out.len(), 1, "got: {:?}", out);
        assert!(out[0].starts_with("CREATE PROCEDURE"));
        assert!(out[0].contains("DECLARE x INT;"));
    }

    #[test]
    fn split_keyword_matcher_respects_word_boundaries() {
        // `extend(...)` contains the substring `end` but must not
        // be parsed as a block closer (left side of `end` is `t`,
        // i.e. inside an identifier).
        let input = "CREATE TRIGGER tr AFTER INSERT ON t \
                     BEGIN \
                        SELECT extend(x) FROM t; \
                     END";
        let out = split_sql_statements(input);
        assert_eq!(out.len(), 1, "got: {:?}", out);
        assert!(out[0].contains("extend(x)"));
        assert!(out[0].ends_with("END"));
    }

    #[test]
    fn split_keyword_matcher_is_case_insensitive() {
        let input = "create trigger tr after insert on t \
                     begin \
                        select 1; \
                     end";
        let out = split_sql_statements(input);
        assert_eq!(out.len(), 1, "got: {:?}", out);
        let lower = out[0].to_lowercase();
        assert!(lower.contains("begin select 1;"));
        assert!(lower.ends_with("end"));
    }

    #[test]
    fn split_double_quoted_begin_identifier_is_not_compound_opener() {
        // A column named "BEGIN" inside a CREATE TRIGGER body
        // should NOT increment block depth, because the double
        // quotes consume the identifier before keyword matching
        // runs.
        let input =
            r#"CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT "BEGIN" FROM t; END; SELECT 1;"#;
        let out = split_sql_statements(input);
        assert_eq!(out.len(), 2, "got: {:?}", out);
        assert!(out[0].contains(r#""BEGIN""#));
        assert!(out[0].ends_with("END"));
        assert_eq!(out[1], "SELECT 1");
    }

    #[test]
    fn statement_is_select_like_case_insensitive() {
        let keywords = &["SELECT", "WITH"];
        assert!(statement_is_select_like("select 1", keywords));
        assert!(statement_is_select_like("SeLeCt 1", keywords));
        assert!(statement_is_select_like("with cte as ...", keywords));
    }

    #[test]
    fn split_mysql_procedure_with_if_block() {
        let input =
            "CREATE PROCEDURE p() BEGIN IF x THEN SELECT 1; END IF; SELECT 2; END; SELECT 3";
        for d in [SqlDialect::MySql, SqlDialect::Generic, SqlDialect::Postgres] {
            let out = split_sql_statements_for(input, d);
            assert_eq!(out.len(), 2, "{d:?} got: {out:?}");
            assert!(out[0].ends_with("SELECT 2; END"));
            assert_eq!(out[1], "SELECT 3");
        }
    }

    #[test]
    fn split_mysql_procedure_with_loops_and_case_statement() {
        let input = "CREATE PROCEDURE p() BEGIN \
            lbl: LOOP SELECT 1; LEAVE lbl; END LOOP lbl; \
            WHILE a DO SET a = a - 1; END WHILE; \
            REPEAT SET a = a + 1; UNTIL a > 3 END REPEAT; \
            CASE a WHEN 1 THEN SELECT 1; ELSE SELECT 2; END CASE; \
            SELECT CASE WHEN a THEN 1 ELSE 0 END; \
            END; SELECT 9";
        let out = split_sql_statements_for(input, SqlDialect::MySql);
        assert_eq!(out.len(), 2, "got: {out:?}");
        assert_eq!(out[1], "SELECT 9");
    }

    #[test]
    fn split_plain_case_expression_still_splits() {
        let input = "SELECT CASE WHEN 1 THEN 2 END; SELECT 3";
        for d in [SqlDialect::MySql, SqlDialect::Generic, SqlDialect::Postgres] {
            assert_eq!(
                split_sql_statements_for(input, d),
                vec!["SELECT CASE WHEN 1 THEN 2 END", "SELECT 3"]
            );
        }
    }

    #[test]
    fn split_tsql_end_followed_by_new_if_statement() {
        let input = "CREATE PROCEDURE p AS BEGIN IF 1 = 1 BEGIN SELECT 1; END \
                     IF 2 = 2 BEGIN SELECT 2; END END; SELECT 3";
        let out = split_sql_statements(input);
        assert_eq!(out.len(), 2, "got: {out:?}");
        assert_eq!(out[1], "SELECT 3");
    }

    #[test]
    fn split_tsql_begin_tran_inside_procedure() {
        let input =
            "CREATE PROCEDURE p AS BEGIN BEGIN TRAN; UPDATE t SET a = 1; COMMIT; END; SELECT 1";
        let out = split_sql_statements(input);
        assert_eq!(out.len(), 2, "got: {out:?}");
        assert_eq!(out[1], "SELECT 1");
    }

    #[test]
    fn split_mysql_backslash_escaped_quote() {
        let out = split_sql_statements_for(r"SELECT 'it\'s; here'; SELECT 1", SqlDialect::MySql);
        assert_eq!(out, vec![r"SELECT 'it\'s; here'", "SELECT 1"]);
        let out = split_sql_statements_for(r#"SELECT "a\"; b"; SELECT 1"#, SqlDialect::MySql);
        assert_eq!(out, vec![r#"SELECT "a\"; b""#, "SELECT 1"]);
        // An escaped backslash does not escape the closing quote.
        let out = split_sql_statements_for(r"SELECT 'a\\'; SELECT 1", SqlDialect::MySql);
        assert_eq!(out, vec![r"SELECT 'a\\'", "SELECT 1"]);
    }

    #[test]
    fn split_postgres_escape_string() {
        let out =
            split_sql_statements_for(r"SELECT E'it\'s; here'; SELECT 1", SqlDialect::Postgres);
        assert_eq!(out, vec![r"SELECT E'it\'s; here'", "SELECT 1"]);
        // A standard string keeps a literal backslash and ends at the quote.
        let out = split_sql_statements_for(r"SELECT 'a\'; SELECT 1", SqlDialect::Postgres);
        assert_eq!(out, vec![r"SELECT 'a\'", "SELECT 1"]);
    }

    #[test]
    fn integers_beyond_js_safe_range_become_strings() {
        use serde_json::json;
        assert_eq!(i64_to_json(42), json!(42));
        assert_eq!(
            i64_to_json(9_007_199_254_740_991),
            json!(9_007_199_254_740_991i64)
        );
        assert_eq!(
            i64_to_json(9_007_199_254_740_992),
            json!("9007199254740992")
        );
        assert_eq!(
            i64_to_json(-1234567890123456789),
            json!("-1234567890123456789")
        );
        assert_eq!(u64_to_json(u64::MAX), json!("18446744073709551615"));
        assert_eq!(u64_to_json(7), json!(7));
    }

    #[test]
    fn exact_numeric_text_keeps_numbers_only_when_lossless() {
        use serde_json::json;
        assert_eq!(exact_numeric_text_to_json("12.5"), json!(12.5));
        assert_eq!(exact_numeric_text_to_json("-5"), json!(-5));
        assert_eq!(exact_numeric_text_to_json("0.10"), json!(0.1));
        assert_eq!(exact_numeric_text_to_json("0"), json!(0));
        assert_eq!(
            exact_numeric_text_to_json("1234567890123456789"),
            json!("1234567890123456789")
        );
        assert_eq!(
            exact_numeric_text_to_json("0.12345678901234567890"),
            json!("0.12345678901234567890")
        );
        assert_eq!(
            exact_numeric_text_to_json("123456789012345678901234567890.5"),
            json!("123456789012345678901234567890.5")
        );
        assert_eq!(exact_numeric_text_to_json("NaN"), json!("NaN"));
        assert_eq!(exact_numeric_text_to_json("Infinity"), json!("Infinity"));
    }

    #[test]
    fn numeric_literal_validation() {
        for ok in [
            "1", "-1", "+1", "1.5", ".5", "5.", "1e10", "-1.5E-3", " 42 ",
        ] {
            assert!(numeric_literal(ok).is_some(), "{ok}");
        }
        for bad in [
            "", "-", ".", "1e", "1 OR 1=1", "0x10", "1;", "abc", "1.2.3", "NaN",
        ] {
            assert!(numeric_literal(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn numeric_type_names() {
        for t in [
            "bigint",
            "BIGINT UNSIGNED",
            "bigint(20) unsigned zerofill",
            "decimal(20,4)",
            "numeric",
            "int8",
            "varint",
            "double precision",
        ] {
            assert!(is_numeric_type_name(t), "{t}");
        }
        for t in ["point", "interval", "varchar(20)", "money", "text", "uuid"] {
            assert!(!is_numeric_type_name(t), "{t}");
        }
    }

    #[test]
    fn filter_check_ignores_quoted_contents() {
        for d in [SqlDialect::MySql, SqlDialect::Generic, SqlDialect::Postgres] {
            assert_eq!(filter_unsafe_reason_for("name = 'a;b'", d), None);
            assert_eq!(filter_unsafe_reason_for("note LIKE '%--%'", d), None);
            assert_eq!(filter_unsafe_reason_for("note = '/* x */'", d), None);
            assert_eq!(
                filter_unsafe_reason_for("note = 'x UNION SELECT y'", d),
                None
            );
            assert_eq!(filter_unsafe_reason_for("note = 'it''s; ok'", d), None);
            assert_eq!(filter_unsafe_reason_for(r#""we;ird" = 1"#, d), None);
        }
        assert_eq!(
            filter_unsafe_reason_for(r"note = 'it\'s; ok'", SqlDialect::MySql),
            None
        );
        assert_eq!(
            filter_unsafe_reason_for(r"note = E'it\'s; ok'", SqlDialect::Postgres),
            None
        );
        assert_eq!(
            filter_unsafe_reason_for("[we;ird] = 1", SqlDialect::Generic),
            None
        );
    }

    #[test]
    fn filter_check_still_rejects_outside_quotes() {
        for d in [SqlDialect::MySql, SqlDialect::Generic, SqlDialect::Postgres] {
            assert!(filter_unsafe_reason_for("a = 'x'; DROP TABLE t", d).is_some());
            assert!(filter_unsafe_reason_for("a = 'x' -- c", d).is_some());
            assert!(filter_unsafe_reason_for("a = 1 /* c */", d).is_some());
            assert!(filter_unsafe_reason_for("a IN (SELECT 1)", d).is_some());
            assert!(filter_unsafe_reason_for("a = 1 UNION SELECT 2", d).is_some());
            assert!(filter_unsafe_reason_for("a = 'unterminated; x", d).is_some());
        }
        // In a standard Postgres string the backslash is literal, so the
        // quote after it closes the string and the `;` is outside.
        assert!(
            filter_unsafe_reason_for(r"a = 'x\'; DROP TABLE t", SqlDialect::Postgres).is_some()
        );
        // On MySQL the same input is one unterminated literal.
        assert!(filter_unsafe_reason_for(r"a = 'x\'; DROP TABLE t", SqlDialect::MySql).is_some());
    }
}
