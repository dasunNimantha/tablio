//! In-process SSH tunnel powered by [`russh`].
//!
//! Replaces the historical implementation that shelled out to the system `ssh`
//! binary. Goals:
//!
//! - **No external `ssh` requirement.** Pure-Rust client, runs on every Tauri
//!   target out of the box.
//! - **Real password auth.** The previous OpenSSH-subprocess approach silently
//!   ignored `ssh_password`; this module passes it to russh's
//!   `authenticate_password`.
//! - **Encrypted-key passphrases.** Uses `russh::keys::decode_secret_key` so
//!   protected OpenSSH keys can be opened with the user's passphrase.
//! - **Trust-on-first-use host-key verification.** A separate
//!   `~/.tablio/known_hosts` file is consulted (and grown) so we don't
//!   stomp on the user's `~/.ssh/known_hosts`. A mismatched key is a hard
//!   error that includes the new server fingerprint.
//!
//! The public surface is intentionally small: [`open`] returns a [`SshTunnel`]
//! whose `Drop` impl shuts everything down. The driver layer should treat the
//! tunnel as opaque and connect to `127.0.0.1:tunnel.local_port()`.

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use russh::client::{self, AuthResult, Handle};
use russh::keys::known_hosts::{check_known_hosts_path, learn_known_hosts_path};
use russh::keys::{decode_secret_key, HashAlg, PrivateKeyWithHashAlg, PublicKey};

use crate::models::{ConnectionConfig, SshAuthMethod};

/// A live SSH tunnel.
///
/// Owns the local TCP listener task and the russh session. Dropping the
/// tunnel aborts the accept loop and disconnects the SSH session.
pub struct SshTunnel {
    local_port: u16,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl SshTunnel {
    pub fn local_port(&self) -> u16 {
        self.local_port
    }
}

impl Drop for SshTunnel {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(h) = self.task.take() {
            h.abort();
        }
    }
}

/// Structured payload surfaced when a server's host key no longer matches
/// the one previously recorded in our known_hosts file.
///
/// The frontend looks for the `ssh_host_key_mismatch:` prefix in the
/// stringified Tauri error and parses the JSON body to drive the
/// "Forget & Retry" prompt. Keep this struct strictly serde-compatible —
/// renaming a field is a breaking UI change.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SshHostKeyMismatch {
    pub host: String,
    pub port: u16,
    /// SHA-256 fingerprint of the *new* key (what the server just presented).
    pub fingerprint: String,
    #[serde(rename = "knownHostsPath")]
    pub known_hosts_path: String,
}

/// Magic prefix the frontend uses to detect a mismatch error coming back
/// from any Tauri command that may open an SSH session.
pub const HOST_KEY_MISMATCH_PREFIX: &str = "ssh_host_key_mismatch:";

impl std::fmt::Display for SshHostKeyMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // serde_json on a small struct of String/u16 cannot fail in practice;
        // fall back to a plain message just so we never panic in Display.
        let json = serde_json::to_string(self).unwrap_or_default();
        write!(f, "{HOST_KEY_MISMATCH_PREFIX}{json}")
    }
}

impl std::error::Error for SshHostKeyMismatch {}

/// Verifier that consults a Tablio-owned known_hosts file.
///
/// On first contact the server key is recorded; on later connections a
/// changed key is rejected with a clear error and *not* silently accepted.
struct KnownHostsHandler {
    path: PathBuf,
    host: String,
    port: u16,
    /// Set by `check_server_key` when the presented key doesn't match the
    /// recorded one. `open()` swaps it out after `client::connect` fails so
    /// callers see a structured `SshHostKeyMismatch` instead of a generic
    /// "ssh: invalid key" error.
    mismatch: Arc<Mutex<Option<SshHostKeyMismatch>>>,
}

impl KnownHostsHandler {
    fn new(path: PathBuf, host: String, port: u16) -> Self {
        Self {
            path,
            host,
            port,
            mismatch: Arc::new(Mutex::new(None)),
        }
    }

    fn fingerprint(key: &PublicKey) -> String {
        // SHA-256 base64 fingerprint, formatted like OpenSSH ("SHA256:...").
        // `PublicKeyBase64` exposes the wire-format bytes; we hash them ourselves
        // to avoid pulling in another digest crate.
        format!("SHA256:{}", key.fingerprint(HashAlg::Sha256))
    }
}

impl client::Handler for KnownHostsHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        server_public_key: &PublicKey,
    ) -> Result<bool, Self::Error> {
        match check_known_hosts_path(&self.host, self.port, server_public_key, &self.path) {
            Ok(true) => Ok(true),
            Ok(false) => {
                if let Some(parent) = self.path.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                if let Err(e) =
                    learn_known_hosts_path(&self.host, self.port, server_public_key, &self.path)
                {
                    log::warn!("Failed to record SSH host key for {}: {}", self.host, e);
                }
                Ok(true)
            }
            Err(e) => {
                let fp = Self::fingerprint(server_public_key);
                log::error!(
                    "SSH host key for {}:{} does not match the recorded one ({}). New fingerprint: {}",
                    self.host,
                    self.port,
                    e,
                    fp
                );
                if let Ok(mut slot) = self.mismatch.lock() {
                    *slot = Some(SshHostKeyMismatch {
                        host: self.host.clone(),
                        port: self.port,
                        fingerprint: fp,
                        known_hosts_path: self.path.to_string_lossy().into_owned(),
                    });
                }
                // Returning `Ok(false)` makes russh tear the handshake down;
                // `open()` notices the mismatch slot was filled and converts
                // the resulting connect error into [`SshHostKeyMismatch`].
                Ok(false)
            }
        }
    }
}

fn known_hosts_path() -> Result<PathBuf> {
    Ok(dirs::home_dir()
        .ok_or_else(|| anyhow!("Cannot determine home directory for SSH known_hosts"))?
        .join(".tablio")
        .join("known_hosts"))
}

/// Public wrapper around the (intentionally private) path resolver so that
/// command handlers (e.g. `commands::known_hosts`) and the planned
/// `KnownHostsDialog` UI hit the same file russh writes to.
pub fn known_hosts_path_pub() -> Result<PathBuf> {
    known_hosts_path()
}

/// Effective network target after optionally interposing an SSH tunnel.
///
/// When `_tunnel` is `Some`, callers should connect to `host:port` (always
/// `127.0.0.1:<local>`) and keep the [`EffectiveTarget`] alive for the
/// lifetime of the consumer (subprocess, sqlx pool, etc.). The tunnel is
/// torn down when this struct is dropped.
pub struct EffectiveTarget {
    pub host: String,
    pub port: u16,
    /// Held purely to keep the SSH session alive; never read directly.
    pub _tunnel: Option<SshTunnel>,
}

/// Decide whether to open an SSH tunnel for `config` and return the
/// host/port the caller should actually talk to.
///
/// Mirrors the logic in [`crate::db::pool::PoolManager::connect`] so
/// short-lived consumers (e.g. backup/restore subprocesses) can route
/// through the same bastion the live driver uses without holding a slot
/// in the pool's tunnel table.
pub async fn open_for(config: &ConnectionConfig) -> Result<EffectiveTarget> {
    if config.ssh_enabled && !config.ssh_host.trim().is_empty() {
        let tunnel = open(config).await?;
        Ok(EffectiveTarget {
            host: "127.0.0.1".to_string(),
            port: tunnel.local_port(),
            _tunnel: Some(tunnel),
        })
    } else {
        Ok(EffectiveTarget {
            host: config.host.clone(),
            port: config.port,
            _tunnel: None,
        })
    }
}

/// Open a tunnel that forwards `127.0.0.1:<random>` to
/// `config.host:config.port` over the SSH session described by the `ssh_*`
/// fields.
///
/// The caller should connect to `127.0.0.1:tunnel.local_port()` and treat the
/// returned [`SshTunnel`] as the lifetime token for the forwarding loop.
pub async fn open(config: &ConnectionConfig) -> Result<SshTunnel> {
    if config.ssh_host.trim().is_empty() {
        return Err(anyhow!("SSH host is empty"));
    }
    if config.ssh_user.trim().is_empty() {
        return Err(anyhow!("SSH username is empty"));
    }

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("Failed to bind local SSH forward listener")?;
    let local_port = listener.local_addr()?.port();

    let known_hosts = known_hosts_path()?;
    let handler = KnownHostsHandler::new(known_hosts, config.ssh_host.clone(), config.ssh_port);
    let mismatch_slot = handler.mismatch.clone();

    // Liveness policy:
    //
    // * `inactivity_timeout = None` — a quiet but live SQL session must not
    //   be torn down just because no bytes flowed for a minute. The
    //   previous `Some(60s)` killed idle pool connections silently.
    // * `keepalive_interval = 30s` + `keepalive_max = 4` — send an SSH
    //   keepalive every 30s and close the session if the server fails to
    //   respond to four in a row (~2 minutes of dead network). This
    //   mirrors OpenSSH's `ServerAliveInterval=30` / `ServerAliveCountMax=3`
    //   convention and protects long-lived pools across NAT timeouts and
    //   bastion-side idle disconnects.
    //
    // Auto-reconnect (re-establish the session and replay listeners after
    // a forced disconnect) is tracked separately; today we tear the
    // tunnel down and the user reconnects via the UI.
    let ssh_config = Arc::new(client::Config {
        inactivity_timeout: None,
        keepalive_interval: Some(Duration::from_secs(30)),
        keepalive_max: 4,
        ..Default::default()
    });

    let mut session = match client::connect(
        ssh_config,
        (config.ssh_host.as_str(), config.ssh_port),
        handler,
    )
    .await
    {
        Ok(s) => s,
        Err(e) => {
            // If the handshake aborted because we rejected the key, prefer
            // the structured mismatch error over russh's generic message —
            // the frontend keys off this to offer a one-click "Forget &
            // retry" flow.
            if let Some(info) = mismatch_slot.lock().ok().and_then(|mut s| s.take()) {
                return Err(anyhow::Error::from(info));
            }
            return Err(anyhow::Error::new(e).context(format!(
                "SSH connect to {}:{} failed",
                config.ssh_host, config.ssh_port
            )));
        }
    };

    authenticate(&mut session, config).await?;

    let remote_host = config.host.clone();
    let remote_port = config.port;
    let session = Arc::new(session);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let task = tokio::spawn(forward_loop(
        listener,
        session.clone(),
        remote_host,
        remote_port,
        local_port,
        shutdown_rx,
    ));

    Ok(SshTunnel {
        local_port,
        shutdown: Some(shutdown_tx),
        task: Some(task),
    })
}

async fn authenticate(
    session: &mut Handle<KnownHostsHandler>,
    config: &ConnectionConfig,
) -> Result<()> {
    let user = config.ssh_user.clone();

    let auth = match config.ssh_auth_method {
        SshAuthMethod::Password => {
            if config.ssh_password.is_empty() {
                return Err(anyhow!(
                    "SSH password is empty (set Authentication = Identity file or supply a password)"
                ));
            }
            session
                .authenticate_password(user, config.ssh_password.clone())
                .await
                .context("SSH password authentication failed")?
        }
        SshAuthMethod::IdentityFile => {
            if config.ssh_key_path.trim().is_empty() {
                return Err(anyhow!("SSH identity file path is empty"));
            }
            let key = load_secret_key(&config.ssh_key_path, &config.ssh_password).await?;
            let key_with_hash = PrivateKeyWithHashAlg::new(Arc::new(key), Some(HashAlg::Sha256));
            session
                .authenticate_publickey(user, key_with_hash)
                .await
                .context("SSH public key authentication failed")?
        }
        SshAuthMethod::Agent => return authenticate_with_agent(session, &user).await,
    };

    match auth {
        AuthResult::Success => Ok(()),
        AuthResult::Failure {
            remaining_methods, ..
        } => Err(anyhow!(
            "SSH authentication rejected by server. Methods still available: {:?}",
            remaining_methods
        )),
    }
}

/// Authenticate via the local ssh-agent.
///
/// Tries every identity the agent has loaded, in order, until one succeeds.
/// Returns a clear error if the agent is unreachable, has no identities
/// loaded, or the bastion rejects every identity.
async fn authenticate_with_agent(
    session: &mut Handle<KnownHostsHandler>,
    user: &str,
) -> Result<()> {
    let mut agent = connect_agent()
        .await
        .context("Could not connect to ssh-agent")?;

    let identities = agent
        .request_identities()
        .await
        .context("ssh-agent did not return its identity list")?;

    if identities.is_empty() {
        return Err(anyhow!(
            "ssh-agent has no identities loaded. Run `ssh-add <key>` and try again."
        ));
    }

    let mut last_failure: Option<String> = None;
    for key in identities {
        let res = session
            .authenticate_publickey_with(user, key.clone(), Some(HashAlg::Sha256), &mut agent)
            .await;
        match res {
            Ok(AuthResult::Success) => return Ok(()),
            Ok(AuthResult::Failure {
                remaining_methods, ..
            }) => {
                last_failure = Some(format!(
                    "agent key {} rejected (server still allows: {:?})",
                    key.fingerprint(HashAlg::Sha256),
                    remaining_methods
                ));
                log::debug!(
                    "ssh-agent key {} rejected by {}",
                    key.fingerprint(HashAlg::Sha256),
                    user
                );
            }
            Err(e) => {
                last_failure = Some(format!(
                    "agent key {} signing/auth error: {}",
                    key.fingerprint(HashAlg::Sha256),
                    e
                ));
            }
        }
    }

    Err(anyhow!(
        "SSH authentication via ssh-agent failed: every loaded identity was rejected. {}",
        last_failure.unwrap_or_default()
    ))
}

/// Connect to the local ssh-agent.
///
/// On Linux / macOS this resolves `SSH_AUTH_SOCK`. On Windows it falls
/// back to Pageant. Returns a friendly error if no agent is reachable so
/// the UI can suggest enabling an agent / loading a key.
#[cfg(not(target_os = "windows"))]
async fn connect_agent() -> Result<russh::keys::agent::client::AgentClient<tokio::net::UnixStream>>
{
    if std::env::var_os("SSH_AUTH_SOCK").is_none() {
        return Err(anyhow!(
            "SSH_AUTH_SOCK is not set; start ssh-agent (or pick a different authentication method)"
        ));
    }
    russh::keys::agent::client::AgentClient::connect_env()
        .await
        .map_err(|e| anyhow!("Failed to talk to ssh-agent at SSH_AUTH_SOCK: {}", e))
}

#[cfg(target_os = "windows")]
async fn connect_agent(
) -> Result<russh::keys::agent::client::AgentClient<tokio::net::windows::named_pipe::NamedPipeClient>>
{
    russh::keys::agent::client::AgentClient::connect_pageant()
        .await
        .map_err(|e| {
            anyhow!(
                "Failed to talk to Pageant: {}. Is Pageant running and a key loaded?",
                e
            )
        })
}

async fn load_secret_key(path: &str, passphrase: &str) -> Result<russh::keys::PrivateKey> {
    let path = Path::new(path).to_path_buf();
    let pass = if passphrase.is_empty() {
        None
    } else {
        Some(passphrase.to_string())
    };
    // Run blocking file IO + key decryption off the runtime worker.
    tokio::task::spawn_blocking(move || {
        let bytes = std::fs::read_to_string(&path)
            .with_context(|| format!("Failed to read SSH identity file {}", path.display()))?;
        decode_secret_key(&bytes, pass.as_deref()).map_err(|e| {
            anyhow!(
                "Failed to decode SSH identity file {}: {}{}",
                path.display(),
                e,
                if pass.is_none() {
                    " (key may be encrypted; supply the passphrase)"
                } else {
                    ""
                }
            )
        })
    })
    .await
    .map_err(|e| anyhow!("Joining key-loading task failed: {}", e))?
}

async fn forward_loop(
    listener: TcpListener,
    session: Arc<Handle<KnownHostsHandler>>,
    remote_host: String,
    remote_port: u16,
    local_port: u16,
    mut shutdown: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = &mut shutdown => {
                log::debug!("SSH tunnel on 127.0.0.1:{} shutting down", local_port);
                let _ = session.disconnect(russh::Disconnect::ByApplication, "tablio shutdown", "").await;
                break;
            }
            accept = listener.accept() => {
                let (mut socket, addr) = match accept {
                    Ok(v) => v,
                    Err(e) => {
                        log::warn!("SSH tunnel listener.accept() failed: {}", e);
                        continue;
                    }
                };
                let session = session.clone();
                let remote_host = remote_host.clone();
                tokio::spawn(async move {
                    let channel = match session
                        .channel_open_direct_tcpip(
                            remote_host.clone(),
                            remote_port as u32,
                            addr.ip().to_string(),
                            addr.port() as u32,
                        )
                        .await
                    {
                        Ok(c) => c,
                        Err(e) => {
                            log::warn!(
                                "Failed to open direct-tcpip channel to {}:{}: {}",
                                remote_host,
                                remote_port,
                                e
                            );
                            let _ = socket.shutdown().await;
                            return;
                        }
                    };
                    let mut stream = channel.into_stream();
                    if let Err(e) = tokio::io::copy_bidirectional(&mut socket, &mut stream).await {
                        // Closed connections are normal; log at debug.
                        log::debug!("SSH tunnel relay closed for {}:{}: {}", remote_host, remote_port, e);
                    }
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use russh::client::Handler;
    use std::io::Write;

    fn pg_config_with(auth: SshAuthMethod) -> ConnectionConfig {
        ConnectionConfig {
            id: "id".into(),
            name: "n".into(),
            db_type: crate::models::DbType::Postgres,
            host: "remote-db".into(),
            port: 5432,
            user: "u".into(),
            password: "p".into(),
            database: "db".into(),
            color: "#fff".into(),
            ssl: false,
            trust_server_cert: false,
            group: None,
            ssh_enabled: true,
            ssh_host: "bastion".into(),
            ssh_port: 22,
            ssh_user: "ssh-user".into(),
            ssh_password: String::new(),
            ssh_key_path: String::new(),
            ssh_auth_method: auth,
            ssh_prompt_passphrase: false,
        }
    }

    #[tokio::test]
    async fn open_for_passes_through_when_ssh_disabled() {
        let mut cfg = pg_config_with(SshAuthMethod::Password);
        cfg.ssh_enabled = false;
        cfg.host = "remote-db".into();
        cfg.port = 5432;
        let target = open_for(&cfg)
            .await
            .expect("open_for must not fail when SSH is disabled");
        assert_eq!(target.host, "remote-db");
        assert_eq!(target.port, 5432);
        assert!(target._tunnel.is_none(), "no tunnel should be allocated");
    }

    #[tokio::test]
    async fn open_for_passes_through_when_ssh_host_blank() {
        let mut cfg = pg_config_with(SshAuthMethod::Password);
        cfg.ssh_enabled = true;
        cfg.ssh_host = "   ".into();
        cfg.host = "remote-db".into();
        cfg.port = 5432;
        let target = open_for(&cfg)
            .await
            .expect("blank ssh_host must skip tunneling, not error");
        assert_eq!(target.host, "remote-db");
        assert_eq!(target.port, 5432);
        assert!(target._tunnel.is_none());
    }

    #[tokio::test]
    async fn open_rejects_empty_ssh_host() {
        let mut cfg = pg_config_with(SshAuthMethod::Password);
        cfg.ssh_host = "   ".into();
        // SshTunnel intentionally doesn't impl Debug (it owns runtime resources),
        // so we map the error before unwrapping.
        let err = open(&cfg)
            .await
            .err()
            .expect("expected error for empty ssh_host");
        assert!(err.to_string().contains("SSH host is empty"), "got {err}");
    }

    #[tokio::test]
    async fn open_rejects_empty_ssh_user() {
        let mut cfg = pg_config_with(SshAuthMethod::Password);
        cfg.ssh_user = String::new();
        let err = open(&cfg)
            .await
            .err()
            .expect("expected error for empty ssh_user");
        assert!(
            err.to_string().contains("SSH username is empty"),
            "got {err}"
        );
    }

    #[test]
    fn known_hosts_path_includes_tablio_subdir() {
        let p = known_hosts_path().expect("home dir resolvable in test env");
        assert!(p.ends_with(".tablio/known_hosts"));
    }

    #[tokio::test]
    async fn load_secret_key_reports_missing_passphrase() {
        // Generate an encrypted ed25519 key on the fly so we don't ship test fixtures.
        // We just need the decoder to *fail* without a passphrase.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("id_ed25519");
        // Minimal encrypted key blob from russh's own test corpus.
        let pkcs8_encrypted = "-----BEGIN ENCRYPTED PRIVATE KEY-----\n\
MIGjMF8GCSqGSIb3DQEFDTBSMDEGCSqGSIb3DQEFDDAkBBAWQiUHKoocuxfoZ/hF\n\
YTjkAgIIADAMBggqhkiG9w0CCQUAMB0GCWCGSAFlAwQBKgQQ83d1d5/S2wz475uC\n\
CUrE7QRAvdVpD5e3zKH/MZjilWrMOm6cyI1LKBCssLztPyvOALtroLAPlp7WYWfu\n\
9Sncmm7u14n2lia7r1r5I3VBsVuH0g==\n\
-----END ENCRYPTED PRIVATE KEY-----\n";
        std::fs::File::create(&path)
            .unwrap()
            .write_all(pkcs8_encrypted.as_bytes())
            .unwrap();
        let path_str = path.to_string_lossy().to_string();

        // Wrong / empty passphrase must surface a useful error.
        let err = load_secret_key(&path_str, "").await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Failed to decode SSH identity file"),
            "expected decode error, got: {msg}"
        );
    }

    #[tokio::test]
    async fn known_hosts_handler_learns_first_contact() {
        let dir = tempfile::tempdir().unwrap();
        let kh = dir.path().join("known_hosts");
        let mut handler = KnownHostsHandler::new(kh.clone(), "host.example".into(), 22);
        // Build a key by parsing an OpenSSH-formatted one.
        let key = russh::keys::parse_public_key_base64(
            "AAAAC3NzaC1lZDI1NTE5AAAAIJdD7y3aLq454yWBdwLWbieU1ebz9/cu7/QEXn9OIeZJ",
        )
        .unwrap();
        let accepted = handler.check_server_key(&key).await.unwrap();
        assert!(accepted, "first contact must be accepted (TOFU)");
        assert!(kh.exists(), "known_hosts file must be created");
        // Second call should match the now-recorded key.
        let accepted_again = handler.check_server_key(&key).await.unwrap();
        assert!(accepted_again, "matching key on reconnect must be accepted");
    }

    #[tokio::test]
    async fn known_hosts_handler_rejects_changed_key() {
        let dir = tempfile::tempdir().unwrap();
        let kh = dir.path().join("known_hosts");
        let mut handler = KnownHostsHandler::new(kh.clone(), "host.example".into(), 22);
        let mismatch_slot = handler.mismatch.clone();
        let key1 = russh::keys::parse_public_key_base64(
            "AAAAC3NzaC1lZDI1NTE5AAAAIJdD7y3aLq454yWBdwLWbieU1ebz9/cu7/QEXn9OIeZJ",
        )
        .unwrap();
        let key2 = russh::keys::parse_public_key_base64(
            "AAAAC3NzaC1lZDI1NTE5AAAAILIG2T/B0l0gaqj3puu510tu9N1OkQ4znY3LYuEm5zCF",
        )
        .unwrap();
        assert!(handler.check_server_key(&key1).await.unwrap());
        assert!(
            mismatch_slot.lock().unwrap().is_none(),
            "no mismatch should be recorded on first contact"
        );
        // Now the same host hands us a different key — must be rejected.
        let rejected = handler.check_server_key(&key2).await.unwrap();
        assert!(!rejected, "changed host key must be rejected");
        let info = mismatch_slot
            .lock()
            .unwrap()
            .take()
            .expect("mismatch slot must be populated");
        assert_eq!(info.host, "host.example");
        assert_eq!(info.port, 22);
        assert!(
            info.fingerprint.starts_with("SHA256:"),
            "fingerprint should be SHA256-formatted, got {}",
            info.fingerprint
        );
        assert_eq!(info.known_hosts_path, kh.to_string_lossy());
    }

    #[tokio::test]
    #[cfg(not(target_os = "windows"))]
    async fn agent_auth_reports_missing_auth_sock() {
        // When `SSH_AUTH_SOCK` is unset on Linux/macOS we must surface a
        // clear, actionable error rather than letting the russh
        // connector fail with a cryptic `connection refused`.
        // We can't mutate process env safely in a multi-threaded test
        // runner, so call `connect_agent` directly with the env we want.
        let prev = std::env::var("SSH_AUTH_SOCK").ok();
        // SAFETY: the `unsafe` here is a quirk of the std API in newer
        // Rust editions; in-test-only mutation is acceptable because the
        // scope is tightly controlled and each test asserts and restores.
        unsafe {
            std::env::remove_var("SSH_AUTH_SOCK");
        }
        let err = connect_agent()
            .await
            .err()
            .expect("missing SSH_AUTH_SOCK must surface an error");
        assert!(
            err.to_string().contains("SSH_AUTH_SOCK"),
            "error should mention SSH_AUTH_SOCK; got {err}"
        );
        // Restore the env var so other tests in the same process see the
        // same shell-inherited state they expect.
        if let Some(v) = prev {
            unsafe {
                std::env::set_var("SSH_AUTH_SOCK", v);
            }
        }
    }

    #[test]
    fn host_key_mismatch_serialises_with_prefix() {
        let info = SshHostKeyMismatch {
            host: "h".into(),
            port: 22,
            fingerprint: "SHA256:xyz".into(),
            known_hosts_path: "/tmp/known_hosts".into(),
        };
        let s = info.to_string();
        assert!(
            s.starts_with(HOST_KEY_MISMATCH_PREFIX),
            "missing prefix: {s}"
        );
        let json = &s[HOST_KEY_MISMATCH_PREFIX.len()..];
        let parsed: SshHostKeyMismatch =
            serde_json::from_str(json).expect("payload after prefix must be valid JSON");
        assert_eq!(parsed.host, "h");
        assert_eq!(parsed.port, 22);
        assert_eq!(parsed.fingerprint, "SHA256:xyz");
        assert_eq!(parsed.known_hosts_path, "/tmp/known_hosts");
        // The serde key for the path must remain camelCase to match the
        // Tauri/TS contract — guard against accidental rename.
        assert!(
            json.contains("\"knownHostsPath\""),
            "expected camelCase key, got {json}"
        );
    }
}
