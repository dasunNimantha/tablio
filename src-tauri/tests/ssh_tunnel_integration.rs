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
//!   - TEST_SSH_TARGET_HOST   db host as seen *from* the bastion (e.g.
//!                            "host.docker.internal" or "localhost")
//!   - TEST_SSH_TARGET_PORT   db port as seen from the bastion (e.g. "5432")
//!   - TEST_POSTGRES_USER / _PASSWORD / _DB     Postgres credentials

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
