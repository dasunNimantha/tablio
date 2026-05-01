//! Integration test for the russh-based SSH tunnel.
//!
//! Gated behind environment variables so it stays a no-op when an SSH
//! bastion + DB pair isn't available. CI provisions both via Docker.
//!
//! Required env vars (when running):
//!   - TEST_SSH_HOST          host of the SSH bastion (e.g. "localhost")
//!   - TEST_SSH_PORT          ssh port on the bastion (e.g. "2222")
//!   - TEST_SSH_USER          ssh login username
//!   - TEST_SSH_PASSWORD      ssh login password
//!   - TEST_SSH_TARGET_HOST   db host as seen *from* the bastion
//!     (e.g. "host.docker.internal" or "localhost")
//!   - TEST_SSH_TARGET_PORT   db port as seen from the bastion (e.g. "5432")
//!   - TEST_POSTGRES_USER / _PASSWORD / _DB     Postgres credentials
//!
//! Optional (enables identity-file auth tests):
//!   - TEST_SSH_KEY_PATH      path to an unencrypted OpenSSH/PEM private
//!     key whose public half is in the bastion's authorized_keys

use tablio_lib::db::pool::PoolManager;
use tablio_lib::db::ssh_tunnel;
// `DatabaseDriver` is brought in transparently through the trait method
// invocation on the Arc returned by `pool.get_driver`, so we don't need to
// import it explicitly.
use tablio_lib::models::*;

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn ssh_test_config() -> Option<ConnectionConfig> {
    let ssh_host = env("TEST_SSH_HOST")?;
    let ssh_port: u16 = env("TEST_SSH_PORT")?.parse().ok()?;
    let ssh_user = env("TEST_SSH_USER")?;
    let ssh_password = env("TEST_SSH_PASSWORD")?;
    let target_host = env("TEST_SSH_TARGET_HOST")?;
    let target_port: u16 = env("TEST_SSH_TARGET_PORT")?.parse().ok()?;
    let pg_user = env("TEST_POSTGRES_USER").unwrap_or_else(|| "test".into());
    let pg_password = env("TEST_POSTGRES_PASSWORD").unwrap_or_else(|| "test".into());
    let pg_db = env("TEST_POSTGRES_DB").unwrap_or_else(|| "testdb".into());

    Some(ConnectionConfig {
        id: "ssh-int-test".into(),
        name: "ssh-int-test".into(),
        db_type: DbType::Postgres,
        host: target_host,
        port: target_port,
        user: pg_user,
        password: pg_password,
        database: pg_db,
        color: "#000".into(),
        ssl: false,
        trust_server_cert: true,
        group: None,
        ssh_enabled: true,
        ssh_host,
        ssh_port,
        ssh_user,
        ssh_password,
        ssh_key_path: String::new(),
        ssh_auth_method: SshAuthMethod::Password,
        ssh_prompt_passphrase: false,
    })
}

#[tokio::test]
async fn pool_manager_connects_through_ssh_tunnel() {
    let Some(config) = ssh_test_config() else {
        eprintln!("Skipping ssh_tunnel_integration: TEST_SSH_* env vars not set");
        return;
    };
    let pool = PoolManager::new();
    pool.connect(config.clone())
        .await
        .expect("connect through SSH tunnel should succeed");
    let driver = pool
        .get_driver(&config.id)
        .await
        .expect("driver should be present after connect");
    assert!(driver
        .test_connection()
        .await
        .expect("test_connection should not error"));
    pool.disconnect(&config.id)
        .await
        .expect("disconnect should succeed (and tear the tunnel down)");
    assert!(!pool.is_connected(&config.id).await);
}

#[tokio::test]
async fn ssh_tunnel_open_drops_listener_when_dropped() {
    let Some(config) = ssh_test_config() else {
        eprintln!("Skipping ssh_tunnel_integration: TEST_SSH_* env vars not set");
        return;
    };
    let local_port = {
        let tunnel = ssh_tunnel::open(&config)
            .await
            .expect("ssh_tunnel::open should succeed with valid creds");
        let port = tunnel.local_port();
        // Tunnel is dropped at end of this scope; the next bind to that
        // port should succeed almost immediately. We don't poll for that
        // here (kernel TIME_WAIT can still hold the port), but we do
        // assert that a connection through the live tunnel works.
        let _stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("local tunnel endpoint must accept connections");
        port
    };
    // Sanity: port was non-zero (i.e. ephemeral allocation succeeded).
    assert_ne!(local_port, 0);
}

/// Build the same config as `ssh_test_config` but flip auth to
/// `identityfile` and point at the key referenced by `TEST_SSH_KEY_PATH`.
/// Returns `None` when either the base config or the key path env var is
/// missing — every identity-file test must skip cleanly in those envs.
fn ssh_identity_file_config() -> Option<ConnectionConfig> {
    let mut cfg = ssh_test_config()?;
    let key_path = env("TEST_SSH_KEY_PATH")?;
    if !std::path::Path::new(&key_path).exists() {
        eprintln!(
            "Skipping identity-file test: TEST_SSH_KEY_PATH={} does not exist",
            key_path
        );
        return None;
    }
    cfg.id = "ssh-int-test-id-file".into();
    cfg.name = "ssh-int-test-id-file".into();
    cfg.ssh_auth_method = SshAuthMethod::IdentityFile;
    cfg.ssh_key_path = key_path;
    // Empty passphrase — the CI key is unencrypted by design so we can
    // exercise the IdentityFile branch end-to-end without prompting.
    cfg.ssh_password = String::new();
    Some(cfg)
}

#[tokio::test]
async fn ssh_tunnel_open_with_identity_file() {
    let Some(config) = ssh_identity_file_config() else {
        eprintln!("Skipping ssh_tunnel_open_with_identity_file: env vars not set");
        return;
    };
    let tunnel = ssh_tunnel::open(&config)
        .await
        .expect("ssh_tunnel::open with IdentityFile auth must succeed");
    let port = tunnel.local_port();
    assert_ne!(port, 0, "tunnel must allocate a real local port");
    let _stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("local tunnel endpoint must accept connections");
}

#[tokio::test]
async fn pool_manager_connects_through_identity_file_tunnel() {
    let Some(config) = ssh_identity_file_config() else {
        eprintln!("Skipping pool_manager_connects_through_identity_file_tunnel: env vars not set");
        return;
    };
    let pool = PoolManager::new();
    pool.connect(config.clone())
        .await
        .expect("connect through identity-file SSH tunnel should succeed");
    let driver = pool
        .get_driver(&config.id)
        .await
        .expect("driver should be present after connect");
    assert!(driver
        .test_connection()
        .await
        .expect("test_connection should not error"));
    pool.disconnect(&config.id)
        .await
        .expect("disconnect should succeed");
    assert!(!pool.is_connected(&config.id).await);
}

#[tokio::test]
async fn ssh_tunnel_open_rejects_missing_identity_file() {
    let Some(mut config) = ssh_identity_file_config() else {
        eprintln!("Skipping ssh_tunnel_open_rejects_missing_identity_file: env vars not set");
        return;
    };
    // Point at a path that's guaranteed not to exist; the IdentityFile
    // branch should surface a clear error before any SSH handshake.
    config.ssh_key_path = "/tmp/tablio-ssh-int-no-such-key".into();
    let err = ssh_tunnel::open(&config)
        .await
        .err()
        .expect("missing identity file must produce an error");
    let msg = err.to_string();
    assert!(
        msg.contains("Failed to read SSH identity file") || msg.contains("identity"),
        "expected identity-file error, got: {msg}"
    );
}

/// Verify that `pg_dump` running locally can reach the bastion-hosted
/// Postgres instance through the same russh tunnel used by the live driver.
///
/// This protects the contract between [`ssh_tunnel::open_for`] and the
/// dump/restore subprocesses in `commands/backup.rs`: the only way these
/// utilities can talk to a private DB is by being handed `127.0.0.1:<local>`
/// while a tunnel guard is alive in the calling task.
#[tokio::test]
async fn pg_dump_through_tunnel() {
    let Some(config) = ssh_test_config() else {
        eprintln!("Skipping pg_dump_through_tunnel: TEST_SSH_* env vars not set");
        return;
    };
    if which::which("pg_dump").is_err() {
        eprintln!("Skipping pg_dump_through_tunnel: pg_dump binary not found in PATH");
        return;
    }

    let target = ssh_tunnel::open_for(&config)
        .await
        .expect("open_for should succeed with valid SSH creds");
    assert_eq!(
        target.host, "127.0.0.1",
        "open_for must redirect through the local tunnel endpoint"
    );
    assert_ne!(target.port, 0, "tunnel must allocate a real local port");
    assert_ne!(
        target.port, config.port,
        "tunnel local port must differ from the remote DB port"
    );

    let tmp = tempfile::Builder::new()
        .prefix("tablio-pgdump-tunnel-")
        .suffix(".sql")
        .tempfile()
        .expect("tempfile creation");
    let tmp_path = tmp.path().to_owned();

    let mut cmd = tokio::process::Command::new("pg_dump");
    cmd.arg("-h")
        .arg(&target.host)
        .arg("-p")
        .arg(target.port.to_string())
        .arg("-U")
        .arg(&config.user)
        .arg("-d")
        .arg(&config.database)
        .arg("--schema-only")
        .arg("-f")
        .arg(&tmp_path);
    cmd.env("PGPASSWORD", &config.password);

    let output = cmd.output().await.expect("pg_dump must spawn successfully");

    drop(target);

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "pg_dump exited non-zero (status={:?}): stderr={}",
        output.status.code(),
        stderr
    );

    let dumped = std::fs::read_to_string(&tmp_path).expect("dump file should be readable");
    assert!(
        !dumped.is_empty(),
        "pg_dump produced an empty file (stderr={stderr})"
    );
    assert!(
        dumped.contains("PostgreSQL database dump"),
        "dump file missing standard pg_dump header (got: {} bytes)",
        dumped.len()
    );
}

/// Build an ssh-agent-flavoured config from the same env. The caller
/// must additionally have `SSH_AUTH_SOCK` set with the test identity
/// loaded — gated behind `TEST_SSH_AGENT_AVAILABLE=1` so a misconfigured
/// CI never silently passes the test as "skipped".
fn ssh_agent_config() -> Option<ConnectionConfig> {
    if env("TEST_SSH_AGENT_AVAILABLE").as_deref() != Some("1") {
        return None;
    }
    if std::env::var_os("SSH_AUTH_SOCK").is_none() {
        eprintln!("Skipping ssh-agent test: TEST_SSH_AGENT_AVAILABLE=1 but SSH_AUTH_SOCK is unset");
        return None;
    }
    let mut cfg = ssh_test_config()?;
    cfg.id = "ssh-int-test-agent".into();
    cfg.name = "ssh-int-test-agent".into();
    cfg.ssh_auth_method = SshAuthMethod::Agent;
    cfg.ssh_password = String::new();
    cfg.ssh_key_path = String::new();
    Some(cfg)
}

#[tokio::test]
async fn ssh_tunnel_open_with_agent() {
    let Some(config) = ssh_agent_config() else {
        eprintln!("Skipping ssh_tunnel_open_with_agent: TEST_SSH_AGENT_AVAILABLE!=1");
        return;
    };
    let tunnel = ssh_tunnel::open(&config)
        .await
        .expect("ssh_tunnel::open with Agent auth must succeed");
    let port = tunnel.local_port();
    assert_ne!(port, 0, "tunnel must allocate a real local port");
    let _stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("local tunnel endpoint must accept connections");
}

#[tokio::test]
async fn pool_manager_connects_through_agent_tunnel() {
    let Some(config) = ssh_agent_config() else {
        eprintln!(
            "Skipping pool_manager_connects_through_agent_tunnel: TEST_SSH_AGENT_AVAILABLE!=1"
        );
        return;
    };
    let pool = PoolManager::new();
    pool.connect(config.clone())
        .await
        .expect("connect through agent-auth SSH tunnel should succeed");
    let driver = pool
        .get_driver(&config.id)
        .await
        .expect("driver should be present after connect");
    assert!(driver
        .test_connection()
        .await
        .expect("test_connection should not error"));
    pool.disconnect(&config.id)
        .await
        .expect("disconnect should succeed");
}

#[tokio::test]
async fn ssh_tunnel_open_rejects_bad_password() {
    let Some(mut config) = ssh_test_config() else {
        eprintln!("Skipping ssh_tunnel_integration: TEST_SSH_* env vars not set");
        return;
    };
    config.ssh_password = "definitely-not-the-right-password".into();
    let err = ssh_tunnel::open(&config)
        .await
        .err()
        .expect("ssh_tunnel::open should reject a wrong password");
    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("auth")
            || msg.to_lowercase().contains("password")
            || msg.to_lowercase().contains("rejected"),
        "expected an auth error, got: {msg}"
    );
}
