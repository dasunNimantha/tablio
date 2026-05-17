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
                if let Some(consumed) = consume_keyword(&chars, i, "BEGIN")
                    .or_else(|| consume_keyword(&chars, i, "CASE"))
                {
                    block_depth = block_depth.saturating_add(1);
                    push_slice(&mut current, &chars, i, consumed);
                    i += consumed;
                    continue;
                }
                if let Some(consumed) = consume_keyword(&chars, i, "END") {
                    block_depth = block_depth.saturating_sub(1);
                    push_slice(&mut current, &chars, i, consumed);
                    i += consumed;
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

        // Single-quoted string. `''` is the SQL-standard escape.
        if c == '\'' {
            current.push(c);
            i += 1;
            while i < chars.len() {
                let q = chars[i];
                current.push(q);
                i += 1;
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

        // Double-quoted identifier. `""` escape.
        if c == '"' {
            current.push(c);
            i += 1;
            while i < chars.len() {
                let q = chars[i];
                current.push(q);
                i += 1;
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

fn push_slice(dst: &mut String, chars: &[char], start: usize, len: usize) {
    for c in chars.iter().skip(start).take(len) {
        dst.push(*c);
    }
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
}
