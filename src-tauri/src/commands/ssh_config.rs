//! Lightweight read-only parser for `~/.ssh/config`.
//!
//! The goal is to let the connection dialog auto-fill the SSH section
//! when a user types a host alias they already use from a terminal
//! (e.g. `ssh prod-bastion`). We support the most common, unambiguous
//! directives and ignore everything else:
//!
//! * `HostName <fqdn>`
//! * `Port <num>`
//! * `User <name>`
//! * `IdentityFile <path>`
//!
//! Match wildcards (`*`, `?`) inside `Host` lines are honoured. `Match`
//! blocks, `Include` directives, `ProxyJump`, and any other advanced
//! features are deliberately out of scope — they require a
//! shell-evaluation + transitive lookup engine that doesn't fit in a
//! one-shot dialog assist. The frontend surfaces a clear "advanced
//! directives ignored" hint when those are present.

use crate::util::path::expand_tilde;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Resolved subset of a `~/.ssh/config` entry. Every field is optional
/// because real configs frequently set only a couple of these.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResolvedSshHost {
    /// `Host` value that matched (the literal, not the resolved hostname).
    pub alias: String,
    #[serde(rename = "hostName")]
    pub host_name: Option<String>,
    pub port: Option<u16>,
    pub user: Option<String>,
    /// Tilde-expanded absolute path when present.
    #[serde(rename = "identityFile")]
    pub identity_file: Option<String>,
    /// True when the matching block (or any block we walked through to
    /// fill in defaults) referenced an unsupported directive
    /// (`ProxyJump`, `Match`, `Include`, ...). The UI uses this to warn
    /// that the auto-fill may be incomplete.
    #[serde(rename = "hasUnsupportedDirectives")]
    pub has_unsupported_directives: bool,
}

fn config_path() -> Option<PathBuf> {
    dirs::home_dir().map(|h| h.join(".ssh").join("config"))
}

/// Glob-style match of `pat` against `name` for the OpenSSH-style `*`
/// and `?` wildcards. `!pattern` negation is handled by the caller.
fn glob_match(pat: &str, name: &str) -> bool {
    fn rec(p: &[u8], n: &[u8]) -> bool {
        match (p.first(), n.first()) {
            (None, None) => true,
            (Some(b'*'), _) => {
                // `*` matches zero or more chars.
                if rec(&p[1..], n) {
                    return true;
                }
                if !n.is_empty() && rec(p, &n[1..]) {
                    return true;
                }
                false
            }
            (Some(b'?'), Some(_)) => rec(&p[1..], &n[1..]),
            (Some(a), Some(b)) if a.eq_ignore_ascii_case(b) => rec(&p[1..], &n[1..]),
            _ => false,
        }
    }
    rec(pat.as_bytes(), name.as_bytes())
}

/// `Host pat1 pat2 !pat3 ...` — match if any positive pattern matches
/// AND no negated pattern matches.
fn host_line_matches(patterns: &[&str], target: &str) -> bool {
    let mut any_positive = false;
    for p in patterns {
        if let Some(neg) = p.strip_prefix('!') {
            if glob_match(neg, target) {
                return false;
            }
        } else if glob_match(p, target) {
            any_positive = true;
        }
    }
    any_positive
}

/// Parse the contents of an SSH config file and resolve `target`.
///
/// Returns `None` if the file has no `Host` block whose patterns match
/// `target`. When multiple blocks match (e.g. a specific block plus a
/// `Host *` block), values from the more specific block win — OpenSSH
/// uses "first match wins per directive", which is what we implement by
/// walking blocks top-down and only filling fields that are still empty.
pub fn resolve_target(content: &str, target: &str) -> Option<ResolvedSshHost> {
    let mut active_match = false;
    let mut resolved = ResolvedSshHost::default();
    let mut matched_any = false;

    for raw in content.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // Tokenise on whitespace; OpenSSH also accepts `Key = value` so
        // strip a leading `=` from the second token.
        let mut parts = line.splitn(2, |c: char| c.is_whitespace() || c == '=');
        let key = match parts.next() {
            Some(k) => k.to_ascii_lowercase(),
            None => continue,
        };
        let value = parts
            .next()
            .unwrap_or("")
            .trim()
            .trim_start_matches('=')
            .trim();

        match key.as_str() {
            "host" => {
                let patterns: Vec<&str> = value.split_whitespace().collect();
                active_match = host_line_matches(&patterns, target);
                if active_match {
                    matched_any = true;
                    // Record the most-specific (i.e. first) matching alias.
                    if resolved.alias.is_empty() {
                        resolved.alias = patterns
                            .iter()
                            .find(|p| !p.starts_with('!') && !p.contains('*') && !p.contains('?'))
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| target.to_string());
                    }
                }
            }
            "match" => {
                // `Match` blocks need shell expansion; we can't honour
                // them. Mark the entry so the UI shows a warning.
                active_match = false;
                resolved.has_unsupported_directives = true;
            }
            "include" | "proxyjump" | "proxycommand" if active_match => {
                resolved.has_unsupported_directives = true;
            }
            "hostname" if active_match && resolved.host_name.is_none() => {
                resolved.host_name = Some(value.to_string());
            }
            "port" if active_match && resolved.port.is_none() => {
                if let Ok(p) = value.parse::<u16>() {
                    resolved.port = Some(p);
                }
            }
            "user" if active_match && resolved.user.is_none() => {
                resolved.user = Some(value.to_string());
            }
            "identityfile" if active_match && resolved.identity_file.is_none() => {
                // Strip optional surrounding quotes — OpenSSH allows them.
                let v = value.trim_matches(|c| c == '"' || c == '\'');
                resolved.identity_file = Some(expand_tilde(v));
            }
            _ => {}
        }
    }

    if matched_any {
        Some(resolved)
    } else {
        None
    }
}

/// Tauri command: look up `alias` in the user's `~/.ssh/config`.
///
/// Returns `Ok(None)` when the file doesn't exist or no Host block
/// matches. Errors are reserved for hard IO failures (permission denied
/// etc.) so the dialog can keep working when the user simply has no
/// config.
#[tauri::command]
pub fn ssh_config_lookup(alias: String) -> Result<Option<ResolvedSshHost>, String> {
    let alias = alias.trim();
    if alias.is_empty() {
        return Ok(None);
    }
    let path = match config_path() {
        Some(p) => p,
        None => return Ok(None),
    };
    if !path.exists() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(&path)
        .map_err(|e| format!("Failed to read {}: {}", path.display(), e))?;
    Ok(resolve_target(&content, alias))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_basic() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("foo", "foo"));
        assert!(!glob_match("foo", "bar"));
        assert!(glob_match("prod-*", "prod-bastion"));
        assert!(glob_match("*.example.com", "host.example.com"));
        assert!(!glob_match("*.example.com", "example.com"));
        assert!(glob_match("host?", "hosta"));
        assert!(!glob_match("host?", "host"));
        assert!(glob_match("Foo", "foo"), "case-insensitive");
    }

    #[test]
    fn host_line_negation_filters_out_match() {
        let pats = vec!["prod-*", "!prod-secret"];
        assert!(host_line_matches(&pats, "prod-web"));
        assert!(!host_line_matches(&pats, "prod-secret"));
    }

    #[test]
    fn resolve_returns_none_when_no_match() {
        let cfg = "Host other\n  HostName x.example.com\n";
        assert!(resolve_target(cfg, "prod-bastion").is_none());
    }

    #[test]
    fn resolve_specific_block_wins_over_wildcard() {
        let cfg = "\
Host prod-bastion
  HostName 10.0.0.5
  User admin
  Port 2222
  IdentityFile ~/.ssh/prod_ed25519

Host *
  User defaultuser
  IdentityFile ~/.ssh/id_rsa
";
        let r = resolve_target(cfg, "prod-bastion").expect("must match");
        assert_eq!(r.host_name.as_deref(), Some("10.0.0.5"));
        assert_eq!(r.port, Some(2222));
        assert_eq!(r.user.as_deref(), Some("admin"));
        // First-match-wins per directive: the specific block's
        // IdentityFile must be preferred even though `Host *` also has
        // one.
        let id = r.identity_file.as_deref().unwrap();
        assert!(id.ends_with("prod_ed25519"), "got {id}");
    }

    #[test]
    fn resolve_wildcard_block_supplies_defaults() {
        // The specific block intentionally has only HostName so the
        // wildcard block's User/IdentityFile fill the gaps.
        let cfg = "\
Host stage
  HostName stage.example.com

Host *
  User deploy
  IdentityFile ~/.ssh/id_ed25519
";
        let r = resolve_target(cfg, "stage").expect("must match");
        assert_eq!(r.host_name.as_deref(), Some("stage.example.com"));
        assert_eq!(r.user.as_deref(), Some("deploy"));
        assert!(r
            .identity_file
            .as_deref()
            .map(|p| p.ends_with("id_ed25519"))
            .unwrap_or(false));
    }

    #[test]
    fn resolve_flags_unsupported_directives() {
        let cfg = "\
Host bastion
  HostName 10.0.0.1
  ProxyJump jumpbox
";
        let r = resolve_target(cfg, "bastion").expect("must match");
        assert_eq!(r.host_name.as_deref(), Some("10.0.0.1"));
        assert!(
            r.has_unsupported_directives,
            "ProxyJump must be flagged as unsupported"
        );
    }

    #[test]
    fn resolve_handles_equals_separator() {
        let cfg = "Host foo\n  HostName=10.0.0.42\n  Port=2200\n";
        let r = resolve_target(cfg, "foo").expect("must match");
        assert_eq!(r.host_name.as_deref(), Some("10.0.0.42"));
        assert_eq!(r.port, Some(2200));
    }

    #[test]
    fn negated_pattern_excludes_target() {
        let cfg = "\
Host prod-* !prod-secret
  HostName 10.0.0.10
  User admin
";
        assert!(resolve_target(cfg, "prod-web").is_some());
        assert!(resolve_target(cfg, "prod-secret").is_none());
    }
}

#[cfg(test)]
mod proptests {
    //! Property-based coverage for the SSH config parser. Real users
    //! drop arbitrary text into `~/.ssh/config` and the parser is
    //! invoked on every alias lookup in the connection dialog —
    //! panicking there would crash the renderer. The contract is
    //! simple: arbitrary input must always return cleanly.
    use super::{glob_match, host_line_matches, resolve_target};
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// The top-level entry point must never panic on arbitrary
        /// config text + target. Returns either `Some(_)` or `None`
        /// — never aborts.
        #[test]
        fn resolve_target_never_panics(
            content in ".{0,4096}",
            target in "[A-Za-z0-9._-]{0,128}",
        ) {
            let _ = resolve_target(&content, &target);
        }

        /// Glob matching must terminate (no infinite recursion on
        /// pathological `*?*?*` patterns) and never panic.
        #[test]
        fn glob_match_never_panics(
            // Limit pattern length so worst-case `*?*?*?*?*?` doesn't
            // blow the recursion budget on the test runner — the
            // parser has the same implicit budget.
            pat in "[*?A-Za-z0-9.-]{0,32}",
            name in "[A-Za-z0-9.-]{0,64}",
        ) {
            let _ = glob_match(&pat, &name);
        }

        /// Negation guard: if a positive pattern matches AND a
        /// negated pattern also matches, the line must not match.
        /// This is the OpenSSH semantic our SSH-config import relies
        /// on; a regression here would silently leak credentials
        /// across host aliases.
        #[test]
        fn negation_always_wins(name in "prod-[a-z]{1,8}") {
            // Build `prod-* !<name>` — the literal target is excluded.
            let neg = format!("!{name}");
            let pats = vec!["prod-*", neg.as_str()];
            prop_assert!(!host_line_matches(&pats, &name));
        }

        /// A `Host *` block must always match any non-empty target.
        /// Regressions here would silently break every wildcard
        /// fallback users rely on for default username / identity.
        #[test]
        fn star_block_matches_any_target(target in "[A-Za-z0-9.-]{1,64}") {
            let cfg = format!(
                "Host *\n  HostName {target}.example.com\n  User deploy\n"
            );
            let r = resolve_target(&cfg, &target).expect("Host * must match");
            prop_assert_eq!(r.user.as_deref(), Some("deploy"));
        }
    }
}
