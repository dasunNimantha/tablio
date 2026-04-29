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

use russh::keys::{parse_public_key_base64, HashAlg};
use serde::{Deserialize, Serialize};

use crate::db::ssh_tunnel::known_hosts_path_pub;

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

#[tauri::command]
pub async fn forget_known_host(host: String, port: u16, fingerprint: String) -> Result<(), String> {
    let path = known_hosts_path_pub().map_err(|e| e.to_string())?;
    if !path.exists() {
        return Ok(());
    }
    let content = tokio::fs::read_to_string(&path)
        .await
        .map_err(|e| format!("Failed to read known_hosts: {e}"))?;

    let new_content = remove_entry(&content, &host, port, &fingerprint);

    tokio::fs::write(&path, new_content)
        .await
        .map_err(|e| format!("Failed to write known_hosts: {e}"))?;
    Ok(())
}

fn parse_known_hosts(content: &str) -> Vec<KnownHostEntry> {
    content.lines().filter_map(parse_line).collect()
}

/// Rewrite `content` with any line whose parsed `(host, port, fingerprint)`
/// triple matches the request removed. Comments, blank lines, and
/// unparseable entries (hashed hosts, `@cert-authority`, multi-host blobs
/// we don't recognise) are preserved verbatim so we never silently mutate
/// state we don't fully understand.
fn remove_entry(content: &str, host: &str, port: u16, fingerprint: &str) -> String {
    let kept: Vec<&str> = content
        .lines()
        .filter(|line| match parse_line(line) {
            Some(entry) => {
                !(entry.host == host && entry.port == port && entry.fingerprint == fingerprint)
            }
            None => true,
        })
        .collect();
    let mut out = kept.join("\n");
    // Preserve trailing newline if the original had one (or if we still
    // have any content) so the file stays POSIX-clean.
    if !out.is_empty() && !out.ends_with('\n') {
        out.push('\n');
    }
    out
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
    let fingerprint = format!("SHA256:{}", key.fingerprint(HashAlg::Sha256));
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
        format!("SHA256:{}", key.fingerprint(HashAlg::Sha256))
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
    fn remove_entry_no_match_leaves_content_unchanged_modulo_newline() {
        let fp_b = fingerprint_of(KEY_B);
        let content = format!("host.example ssh-ed25519 {KEY_A}\n");
        let after = remove_entry(&content, "host.example", 22, &fp_b);
        // Same fingerprint mismatch -> entry must survive.
        assert!(after.contains("host.example"));
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
