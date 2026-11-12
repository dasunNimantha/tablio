//! Tauri commands backing the SSH "Known Hosts" management UI.
//!
//! These commands operate purely on Tablio's own `~/.tablio/known_hosts`
//! file (resolved through [`crate::db::ssh_tunnel::known_hosts_path_pub`])
//! and never touch the user's `~/.ssh/known_hosts`. The format is the
//! standard OpenSSH `host [type] [base64-key]` form that `russh` writes.
//!
//! Parsing is intentionally tolerant: unknown / hashed / `@cert-authority`
//! lines are preserved across rewrites but filtered out of the listing,
//! since they can't be displayed (or matched by fingerprint) in a useful
//! way.

use data_encoding::BASE64_MIME;
use hmac::{Hmac, KeyInit, Mac};
use russh::keys::parse_public_key_base64;
use serde::{Deserialize, Serialize};
use sha1::Sha1;

use crate::db::ssh_tunnel::{fingerprint_sha256, known_hosts_path_pub};
use crate::util::json_store;

/// A single recognised entry in `~/.tablio/known_hosts` rendered for the
/// frontend. The `fingerprint` is recomputed from the stored key so the UI
/// can match what users see in the host-key-mismatch prompt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KnownHostEntry {
    pub host: String,
    pub port: u16,
    #[serde(rename = "keyType")]
    pub key_type: String,
    pub fingerprint: String,
}

#[tauri::command]
pub async fn list_known_hosts() -> Result<Vec<KnownHostEntry>, String> {
    let path = known_hosts_path_pub().map_err(|e| e.to_string())?;
    if !path.exists() {
        return Ok(Vec::new());
    }
    let content = tokio::fs::read_to_string(&path)
        .await
        .map_err(|e| format!("Failed to read known_hosts: {e}"))?;
    Ok(parse_known_hosts(&content))
}

/// Remove recorded host keys for `host:port`.
///
/// Two callers use this:
///
/// * The Known Hosts dialog deletes one specific key: `fingerprint` is the
///   fingerprint of a recorded entry and only that entry is removed.
/// * "Forget & Retry" after a host-key mismatch passes the fingerprint of
///   the *new* key the server just presented, which by definition is not
///   recorded. In that case (or when `all_keys` is `true`) every entry for
///   `host:port` is removed, including hashed (`|1|...`) and multi-host
///   lines, so the retry can record the new key on first contact.
#[tauri::command]
pub async fn forget_known_host(
    host: String,
    port: u16,
    fingerprint: String,
    all_keys: Option<bool>,
) -> Result<(), String> {
    let path = known_hosts_path_pub().map_err(|e| e.to_string())?;
    let _guard = json_store::STORE_LOCK.lock().await;
    let content = match tokio::fs::read_to_string(&path).await {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(format!("Failed to read known_hosts: {e}")),
    };

    let new_content = if all_keys.unwrap_or(false) {
        remove_host_entries(&content, &host, port)
    } else {
        remove_entry(&content, &host, port, &fingerprint)
    };
    if new_content == content {
        return Ok(());
    }

    tokio::task::spawn_blocking(move || {
        json_store::write_atomic_private(&path, new_content.as_bytes())
    })
    .await
    .map_err(|e| format!("Failed to write known_hosts: {e}"))?
    .map_err(|e| format!("Failed to write known_hosts: {e}"))?;
    Ok(())
}

fn parse_known_hosts(content: &str) -> Vec<KnownHostEntry> {
    content.lines().filter_map(parse_line).collect()
}

/// Rewrite `content` without the entries for `host:port` whose key has
/// `fingerprint`. If no entry for `host:port` has that fingerprint (the
/// host-key-mismatch recovery case, where the caller only knows the new,
/// unrecorded key) every entry for `host:port` is removed instead.
fn remove_entry(content: &str, host: &str, port: u16, fingerprint: &str) -> String {
    let wanted = normalize_fingerprint(fingerprint);
    let fingerprint_recorded = content.lines().any(|line| {
        line_matches_host(line, host, port)
            && line_fingerprint(line).as_deref() == Some(wanted.as_str())
    });
    if !fingerprint_recorded {
        return remove_host_entries(content, host, port);
    }
    rewrite(content, |line| {
        line_matches_host(line, host, port)
            && line_fingerprint(line).as_deref() == Some(wanted.as_str())
    })
}

/// Rewrite `content` without any entry recorded for `host:port`.
/// Comments, blank lines, marker lines (`@cert-authority`, `@revoked`) and
/// entries for other hosts are preserved verbatim.
fn remove_host_entries(content: &str, host: &str, port: u16) -> String {
    rewrite(content, |line| line_matches_host(line, host, port))
}

fn rewrite(content: &str, drop_line: impl Fn(&str) -> bool) -> String {
    let kept: Vec<&str> = content.lines().filter(|line| !drop_line(line)).collect();
    let mut out = kept.join("\n");
    // Preserve trailing newline if we still have any content so the file
    // stays POSIX-clean.
    if !out.is_empty() && !out.ends_with('\n') {
        out.push('\n');
    }
    out
}

/// Accept both the `ssh-keygen` form (`SHA256:abc`) and the doubled prefix
/// (`SHA256:SHA256:abc`) older builds produced.
fn normalize_fingerprint(fp: &str) -> String {
    let mut s = fp.trim();
    while let Some(rest) = s.strip_prefix("SHA256:") {
        if rest.starts_with("SHA256:") {
            s = rest;
        } else {
            break;
        }
    }
    s.to_string()
}

/// Fingerprint of the key stored on a known_hosts line, if it parses.
fn line_fingerprint(line: &str) -> Option<String> {
    let mut parts = line.split_whitespace();
    let first = parts.next()?;
    if first.starts_with('@') || first.starts_with('#') {
        return None;
    }
    let _key_type = parts.next()?;
    let key = parse_public_key_base64(parts.next()?).ok()?;
    Some(fingerprint_sha256(&key))
}

/// Does this known_hosts line record a key for `host:port`? Handles
/// comma-separated host lists, `[host]:port` syntax and hashed
/// (`|1|salt|hash`) entries the same way OpenSSH and russh match them.
/// Marker lines (`@cert-authority`, `@revoked`) never match: they are not
/// plain trust entries and must not be removed as a side effect.
fn line_matches_host(line: &str, host: &str, port: u16) -> bool {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with('@') {
        return false;
    }
    let Some(field) = trimmed.split_whitespace().next() else {
        return false;
    };
    let host_port = if port == 22 {
        host.to_string()
    } else {
        format!("[{host}]:{port}")
    };
    field
        .split(',')
        .any(|pattern| host_pattern_matches(pattern, &host_port))
}

fn host_pattern_matches(pattern: &str, host_port: &str) -> bool {
    if let Some(hashed) = pattern.strip_prefix("|1|") {
        let mut parts = hashed.split('|');
        let (Some(salt), Some(hash)) = (parts.next(), parts.next()) else {
            return false;
        };
        let (Ok(salt), Ok(hash)) = (
            BASE64_MIME.decode(salt.as_bytes()),
            BASE64_MIME.decode(hash.as_bytes()),
        ) else {
            return false;
        };
        let Ok(mac) = Hmac::<Sha1>::new_from_slice(&salt) else {
            return false;
        };
        return mac.chain_update(host_port).verify_slice(&hash).is_ok();
    }
    pattern.eq_ignore_ascii_case(host_port)
}

fn parse_line(line: &str) -> Option<KnownHostEntry> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return None;
    }
    let mut parts = trimmed.split_whitespace();
    let host_field = parts.next()?;
    let key_type = parts.next()?;
    let key_b64 = parts.next()?;

    // Skip hashed (|1|...) and marker (@cert-authority, @revoked) lines:
    // we can't surface a host name to the user so they'd be useless in
    // the dialog. They survive `remove_entry` because parse_line returns
    // None and the filter keeps them.
    if host_field.starts_with('|') || host_field.starts_with('@') {
        return None;
    }

    let (host, port) = parse_host_field(host_field)?;
    let key = parse_public_key_base64(key_b64).ok()?;
    let fingerprint = fingerprint_sha256(&key);
    Some(KnownHostEntry {
        host,
        port,
        key_type: key_type.to_string(),
        fingerprint,
    })
}

/// Parse the `host[,host…]` / `[host]:port` field at the start of a
/// known_hosts line. Returns the *first* listed host (russh writes a
/// single host per line, but we tolerate OpenSSH-style multi-host lines
/// for hand-edited files) and the explicit port if present, defaulting
/// to 22 otherwise.
fn parse_host_field(field: &str) -> Option<(String, u16)> {
    if let Some(stripped) = field.strip_prefix('[') {
        let (host, rest) = stripped.split_once("]:")?;
        let port: u16 = rest.parse().ok()?;
        Some((host.to_string(), port))
    } else if let Some(comma) = field.find(',') {
        Some((field[..comma].to_string(), 22))
    } else {
        Some((field.to_string(), 22))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Two real ed25519 public keys from russh's test corpus. Their
    // SHA-256 fingerprints are deterministic so we can assert on them.
    const KEY_A: &str = "AAAAC3NzaC1lZDI1NTE5AAAAIJdD7y3aLq454yWBdwLWbieU1ebz9/cu7/QEXn9OIeZJ";
    const KEY_B: &str = "AAAAC3NzaC1lZDI1NTE5AAAAILIG2T/B0l0gaqj3puu510tu9N1OkQ4znY3LYuEm5zCF";

    fn fingerprint_of(b64: &str) -> String {
        let key = parse_public_key_base64(b64).unwrap();
        fingerprint_sha256(&key)
    }

    #[test]
    fn parses_default_port_entry() {
        let line = format!("host.example ssh-ed25519 {KEY_A}");
        let entry = parse_line(&line).expect("must parse");
        assert_eq!(entry.host, "host.example");
        assert_eq!(entry.port, 22);
        assert_eq!(entry.key_type, "ssh-ed25519");
        assert_eq!(entry.fingerprint, fingerprint_of(KEY_A));
    }

    #[test]
    fn parses_explicit_port_entry() {
        let line = format!("[bastion.internal]:2222 ssh-ed25519 {KEY_A}");
        let entry = parse_line(&line).expect("must parse");
        assert_eq!(entry.host, "bastion.internal");
        assert_eq!(entry.port, 2222);
    }

    #[test]
    fn skips_hashed_and_comment_lines() {
        assert!(parse_line("").is_none());
        assert!(parse_line("# my comment").is_none());
        assert!(parse_line(&format!("|1|abc|def= ssh-ed25519 {KEY_A}")).is_none());
        assert!(parse_line(&format!("@cert-authority ca ssh-ed25519 {KEY_A}")).is_none());
    }

    #[test]
    fn skips_garbled_lines() {
        assert!(parse_line("not enough fields").is_none());
        assert!(parse_line("host ssh-ed25519 not-real-base64!!").is_none());
        // Bracketed host without port is malformed.
        assert!(parse_line(&format!("[host ssh-ed25519 {KEY_A}")).is_none());
    }

    #[test]
    fn parses_multi_host_line_using_first_host() {
        let line = format!("alias.example,host.example ssh-ed25519 {KEY_A}");
        let entry = parse_line(&line).expect("must parse");
        assert_eq!(entry.host, "alias.example");
        assert_eq!(entry.port, 22);
    }

    #[test]
    fn list_returns_only_recognised_entries() {
        let content = format!(
            "# managed by tablio\n\
             host.example ssh-ed25519 {KEY_A}\n\
             |1|hashed|line= ssh-ed25519 {KEY_A}\n\
             [bastion]:2222 ssh-ed25519 {KEY_B}\n",
        );
        let entries = parse_known_hosts(&content);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].host, "host.example");
        assert_eq!(entries[1].host, "bastion");
        assert_eq!(entries[1].port, 2222);
    }

    #[test]
    fn remove_entry_drops_only_matching_line() {
        let fp_a = fingerprint_of(KEY_A);
        let content = format!(
            "host.example ssh-ed25519 {KEY_A}\n\
             other.example ssh-ed25519 {KEY_B}\n",
        );
        let after = remove_entry(&content, "host.example", 22, &fp_a);
        assert!(
            !after.contains("host.example"),
            "matching entry should be removed: {after}"
        );
        assert!(
            after.contains("other.example"),
            "non-matching entry must survive: {after}"
        );
        assert!(after.ends_with('\n'), "trailing newline preserved");
    }

    #[test]
    fn remove_entry_preserves_unparseable_lines() {
        let fp_a = fingerprint_of(KEY_A);
        let content = format!(
            "# tablio managed\n\
             |1|hashed|line= ssh-ed25519 {KEY_A}\n\
             host.example ssh-ed25519 {KEY_A}\n",
        );
        let after = remove_entry(&content, "host.example", 22, &fp_a);
        assert!(after.contains("# tablio managed"));
        assert!(after.contains("|1|hashed|line="));
        assert!(!after.contains("host.example"));
    }

    #[test]
    fn remove_entry_with_unrecorded_fingerprint_forgets_host() {
        // Host-key mismatch recovery: the caller only knows the new key's
        // fingerprint (KEY_B) while the file holds the old key (KEY_A).
        let fp_b = fingerprint_of(KEY_B);
        let content = format!(
            "host.example ssh-ed25519 {KEY_A}\n\
             other.example ssh-ed25519 {KEY_A}\n",
        );
        let after = remove_entry(&content, "host.example", 22, &fp_b);
        assert!(!after.contains("host.example"), "{after}");
        assert!(after.contains("other.example"), "{after}");
    }

    #[test]
    fn remove_entry_with_recorded_fingerprint_keeps_other_keys_of_host() {
        let fp_a = fingerprint_of(KEY_A);
        let content = format!(
            "host.example ssh-ed25519 {KEY_A}\n\
             host.example ssh-ed25519 {KEY_B}\n",
        );
        let after = remove_entry(&content, "host.example", 22, &fp_a);
        assert!(!after.contains(KEY_A), "{after}");
        assert!(after.contains(KEY_B), "{after}");
    }

    #[test]
    fn remove_entry_accepts_doubled_prefix_fingerprint() {
        let doubled = format!("SHA256:{}", fingerprint_of(KEY_A));
        let content = format!(
            "host.example ssh-ed25519 {KEY_A}\n\
             host.example ssh-ed25519 {KEY_B}\n",
        );
        let after = remove_entry(&content, "host.example", 22, &doubled);
        assert!(!after.contains(KEY_A), "{after}");
        assert!(after.contains(KEY_B), "{after}");
    }

    #[test]
    fn remove_host_entries_handles_ports_aliases_and_hashes() {
        // `|1|salt|hash` for "[bastion]:2222", computed with
        // HMAC-SHA1(salt, host) as OpenSSH's HashKnownHosts does.
        let salt = [7u8; 20];
        let mac = Hmac::<Sha1>::new_from_slice(&salt)
            .unwrap()
            .chain_update("[bastion]:2222")
            .finalize()
            .into_bytes();
        let hashed = format!(
            "|1|{}|{}",
            data_encoding::BASE64.encode(&salt),
            data_encoding::BASE64.encode(&mac)
        );
        let content = format!(
            "# keep me\n\
             [bastion]:2222 ssh-ed25519 {KEY_A}\n\
             alias,[bastion]:2222 ssh-ed25519 {KEY_B}\n\
             {hashed} ssh-ed25519 {KEY_A}\n\
             bastion ssh-ed25519 {KEY_B}\n\
             [bastion]:2200 ssh-ed25519 {KEY_B}\n\
             @cert-authority [bastion]:2222 ssh-ed25519 {KEY_A}\n",
        );
        let after = remove_host_entries(&content, "bastion", 2222);
        let lines: Vec<&str> = after.lines().collect();
        assert_eq!(
            lines,
            vec![
                "# keep me".to_string(),
                format!("bastion ssh-ed25519 {KEY_B}"),
                format!("[bastion]:2200 ssh-ed25519 {KEY_B}"),
                format!("@cert-authority [bastion]:2222 ssh-ed25519 {KEY_A}"),
            ]
        );
    }

    #[test]
    fn round_trip_list_then_forget() {
        let fp_a = fingerprint_of(KEY_A);
        let fp_b = fingerprint_of(KEY_B);
        let content = format!(
            "host.example ssh-ed25519 {KEY_A}\n\
             [bastion]:2222 ssh-ed25519 {KEY_B}\n",
        );
        let entries = parse_known_hosts(&content);
        assert_eq!(entries.len(), 2);
        let host_to_forget = &entries[0];
        let after = remove_entry(
            &content,
            &host_to_forget.host,
            host_to_forget.port,
            &host_to_forget.fingerprint,
        );
        let remaining = parse_known_hosts(&after);
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].host, "bastion");
        assert_eq!(remaining[0].fingerprint, fp_b);
        // And forgetting an already-removed entry is a no-op.
        let after_again = remove_entry(&after, &host_to_forget.host, 22, &fp_a);
        assert_eq!(after_again, after);
    }

    #[test]
    fn entry_serialises_with_camelcase_key_type() {
        let entry = KnownHostEntry {
            host: "h".into(),
            port: 22,
            key_type: "ssh-ed25519".into(),
            fingerprint: "SHA256:xyz".into(),
        };
        let json = serde_json::to_string(&entry).unwrap();
        // The TS contract uses `keyType`; serde rename guards against
        // accidental drift.
        assert!(
            json.contains("\"keyType\":\"ssh-ed25519\""),
            "expected keyType in JSON, got {json}"
        );
        let back: KnownHostEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back, entry);
    }
}
