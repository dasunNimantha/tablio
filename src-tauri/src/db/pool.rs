use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::db::cassandra::CassandraDriver;
use crate::db::cockroachdb::CockroachdbDriver;
use crate::db::mariadb::MariadbDriver;
use crate::db::mssql::MssqlDriver;
use crate::db::mysql::MysqlDriver;
use crate::db::postgres::PostgresDriver;
use crate::db::sqlite::SqliteDriver;
use crate::db::ssh_tunnel::{self, SshTunnel};
use crate::db::tidb::TidbDriver;
use crate::db::DatabaseDriver;
use crate::models::*;

/// Everything owned by one live connection. Dropping it drops the driver
/// (pool) handle and shuts down the SSH tunnel, if any.
struct Entry {
    driver: Arc<dyn DatabaseDriver>,
    config: ConnectionConfig,
    _tunnel: Option<SshTunnel>,
}

/// Registry of live connections plus a per-id "latest operation" ticket.
///
/// `connect` is slow (SSH handshake, driver pool setup) and must not hold
/// the lock while it runs, otherwise one hung host would stall every other
/// connection. Instead each `connect` takes a ticket up front and only
/// commits its result if its ticket is still the latest one for that id:
///
/// * a newer `connect` for the same id supersedes an older one, so the
///   older one can never clobber (or tear down) the newer one's tunnel;
/// * a `disconnect` removes the ticket, so a `connect` still in flight is
///   cancelled instead of resurrecting a zombie connection afterwards;
/// * a failed `connect` that is still the latest operation leaves no entry
///   behind, so `get_config` (used by backup/restore) can't return a stale
///   target while the UI shows the connection as disconnected.
#[derive(Default)]
struct Registry {
    entries: HashMap<String, Entry>,
    latest: HashMap<String, u64>,
    next_ticket: u64,
}

/// Result of trying to commit a finished connect attempt.
#[derive(Debug, PartialEq, Eq)]
enum Commit {
    /// The attempt was the latest operation and is now registered.
    Registered,
    /// A newer connect for the same id started after this one; the newer
    /// attempt decides the final state.
    SupersededByConnect,
    /// The id was disconnected while this attempt was in flight.
    CancelledByDisconnect,
}

impl Registry {
    /// Start a connect attempt: tears down the current entry (returned so
    /// the caller can drop it outside the lock) and issues a ticket.
    fn begin_connect(&mut self, id: &str) -> (u64, Option<Entry>) {
        self.next_ticket += 1;
        let ticket = self.next_ticket;
        self.latest.insert(id.to_string(), ticket);
        (ticket, self.entries.remove(id))
    }

    /// Register `entry` if `ticket` is still current. On any other outcome
    /// the entry is handed back so the caller drops it outside the lock.
    fn commit(&mut self, id: &str, ticket: u64, entry: Entry) -> (Commit, Option<Entry>) {
        match self.latest.get(id) {
            Some(&t) if t == ticket => {
                let replaced = self.entries.insert(id.to_string(), entry);
                (Commit::Registered, replaced)
            }
            Some(_) => (Commit::SupersededByConnect, Some(entry)),
            None => (Commit::CancelledByDisconnect, Some(entry)),
        }
    }

    /// Record a failed attempt. Only the latest attempt may clean up, so a
    /// stale failure never removes a newer attempt's connection.
    fn fail(&mut self, id: &str, ticket: u64) -> Option<Entry> {
        if self.latest.get(id) == Some(&ticket) {
            self.latest.remove(id);
            self.entries.remove(id)
        } else {
            None
        }
    }

    fn disconnect(&mut self, id: &str) -> Option<Entry> {
        self.latest.remove(id);
        self.entries.remove(id)
    }
}

pub struct PoolManager {
    registry: RwLock<Registry>,
}

impl Default for PoolManager {
    fn default() -> Self {
        Self::new()
    }
}

impl PoolManager {
    pub fn new() -> Self {
        Self {
            registry: RwLock::new(Registry::default()),
        }
    }

    pub async fn connect(&self, config: ConnectionConfig) -> Result<String> {
        let id = config.id.clone();

        let (ticket, old) = self.registry.write().await.begin_connect(&id);
        // Dropping the old entry shuts down its SSH tunnel and pool.
        drop(old);

        match Self::open_entry(config).await {
            Ok(entry) => {
                let (outcome, leftover) = self.registry.write().await.commit(&id, ticket, entry);
                drop(leftover);
                match outcome {
                    Commit::Registered | Commit::SupersededByConnect => Ok(id),
                    Commit::CancelledByDisconnect => Err(anyhow!(
                        "Connection attempt was cancelled because the connection was closed"
                    )),
                }
            }
            Err(e) => {
                let stale = self.registry.write().await.fail(&id, ticket);
                drop(stale);
                Err(e)
            }
        }
    }

    /// Open the SSH tunnel (if configured) and the driver for `config`.
    /// Everything opened here is dropped again if a later step fails.
    async fn open_entry(config: ConnectionConfig) -> Result<Entry> {
        let mut tunnel = None;
        let effective_config = if config.ssh_enabled && !config.ssh_host.is_empty() {
            let t = ssh_tunnel::open(&config).await?;
            let local_port = t.local_port();
            tunnel = Some(t);
            ConnectionConfig {
                host: "127.0.0.1".to_string(),
                port: local_port,
                ..config.clone()
            }
        } else {
            config.clone()
        };

        let driver_result = match effective_config.db_type {
            DbType::Postgres => PostgresDriver::connect(&effective_config)
                .await
                .map(|d| Arc::new(d) as Arc<dyn DatabaseDriver>),
            DbType::Cockroachdb => CockroachdbDriver::connect(&effective_config)
                .await
                .map(|d| Arc::new(d) as Arc<dyn DatabaseDriver>),
            DbType::Mysql => MysqlDriver::connect(&effective_config)
                .await
                .map(|d| Arc::new(d) as Arc<dyn DatabaseDriver>),
            DbType::Mariadb => MariadbDriver::connect(&effective_config)
                .await
                .map(|d| Arc::new(d) as Arc<dyn DatabaseDriver>),
            DbType::Tidb => TidbDriver::connect(&effective_config)
                .await
                .map(|d| Arc::new(d) as Arc<dyn DatabaseDriver>),
            DbType::Sqlite => SqliteDriver::connect(&effective_config)
                .await
                .map(|d| Arc::new(d) as Arc<dyn DatabaseDriver>),
            DbType::Cassandra => CassandraDriver::connect(&effective_config)
                .await
                .map(|d| Arc::new(d) as Arc<dyn DatabaseDriver>),
            DbType::Mssql => MssqlDriver::connect(&effective_config)
                .await
                .map(|d| Arc::new(d) as Arc<dyn DatabaseDriver>),
        };

        // On error `tunnel` is dropped here, releasing the russh session.
        let driver = driver_result?;
        Ok(Entry {
            driver,
            config,
            _tunnel: tunnel,
        })
    }

    pub async fn disconnect(&self, id: &str) -> Result<()> {
        let old = self.registry.write().await.disconnect(id);
        // Drop the entry (SSH tunnel + driver) outside the lock.
        drop(old);
        Ok(())
    }

    /// Unregister `id` and hand back its driver and config so the caller
    /// can shut the driver down deterministically (e.g. before replacing a
    /// SQLite database file). The SSH tunnel, if any, is closed.
    pub async fn detach(&self, id: &str) -> Option<(Arc<dyn DatabaseDriver>, ConnectionConfig)> {
        let entry = self.registry.write().await.disconnect(id)?;
        Some((entry.driver, entry.config))
    }

    pub async fn get_driver(&self, id: &str) -> Result<Arc<dyn DatabaseDriver>> {
        self.registry
            .read()
            .await
            .entries
            .get(id)
            .map(|e| e.driver.clone())
            .ok_or_else(|| anyhow!("Connection '{}' not found", id))
    }

    pub async fn get_config(&self, id: &str) -> Result<ConnectionConfig> {
        self.registry
            .read()
            .await
            .entries
            .get(id)
            .map(|e| e.config.clone())
            .ok_or_else(|| anyhow!("Config for '{}' not found", id))
    }

    pub async fn test_connection(config: &ConnectionConfig) -> Result<bool> {
        // Tunnel is owned by this scope; its Drop shuts the russh session
        // down once the test completes (or panics).
        let mut _tunnel_guard: Option<SshTunnel> = None;
        let effective_config = if config.ssh_enabled && !config.ssh_host.is_empty() {
            let tunnel = ssh_tunnel::open(config).await?;
            let local_port = tunnel.local_port();
            _tunnel_guard = Some(tunnel);
            ConnectionConfig {
                host: "127.0.0.1".to_string(),
                port: local_port,
                ..config.clone()
            }
        } else {
            config.clone()
        };

        let driver: Box<dyn DatabaseDriver> = match effective_config.db_type {
            DbType::Postgres => Box::new(PostgresDriver::connect(&effective_config).await?),
            DbType::Cockroachdb => Box::new(CockroachdbDriver::connect(&effective_config).await?),
            DbType::Mysql => Box::new(MysqlDriver::connect(&effective_config).await?),
            DbType::Mariadb => Box::new(MariadbDriver::connect(&effective_config).await?),
            DbType::Tidb => Box::new(TidbDriver::connect(&effective_config).await?),
            DbType::Sqlite => Box::new(SqliteDriver::connect(&effective_config).await?),
            DbType::Cassandra => Box::new(CassandraDriver::connect(&effective_config).await?),
            DbType::Mssql => Box::new(MssqlDriver::connect(&effective_config).await?),
        };
        driver.test_connection().await
    }

    pub async fn is_connected(&self, id: &str) -> bool {
        self.registry.read().await.entries.contains_key(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_manager_new_is_empty() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let pm = PoolManager::new();
            assert!(!pm.is_connected("nonexistent").await);
        });
    }

    #[test]
    fn pool_manager_get_driver_missing_returns_error() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let pm = PoolManager::new();
            let result = pm.get_driver("missing").await;
            let err = result.err().expect("expected error");
            assert!(err.to_string().contains("not found"));
        });
    }

    #[test]
    fn pool_manager_get_config_missing_returns_error() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let pm = PoolManager::new();
            let err = pm.get_config("missing").await.expect_err("expected error");
            assert!(err.to_string().contains("not found"));
        });
    }

    #[test]
    fn pool_manager_disconnect_nonexistent_succeeds() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let pm = PoolManager::new();
            let result = pm.disconnect("nonexistent").await;
            assert!(result.is_ok());
        });
    }

    async fn sqlite_entry(id: &str) -> Entry {
        let config = ConnectionConfig {
            id: id.into(),
            db_type: DbType::Sqlite,
            database: "sqlite::memory:".into(),
            ..Default::default()
        };
        PoolManager::open_entry(config)
            .await
            .expect("open in-memory sqlite")
    }

    #[tokio::test]
    async fn registry_newer_connect_wins_over_older() {
        let mut reg = Registry::default();
        let (a, _) = reg.begin_connect("x");
        let (b, _) = reg.begin_connect("x");
        // B finishes first and registers.
        let (outcome, _) = reg.commit("x", b, sqlite_entry("x").await);
        assert_eq!(outcome, Commit::Registered);
        // A finishing later must not replace B ...
        let (outcome, leftover) = reg.commit("x", a, sqlite_entry("x").await);
        assert_eq!(outcome, Commit::SupersededByConnect);
        assert!(leftover.is_some(), "stale entry handed back for dropping");
        // ... and A failing must not tear B down either.
        assert!(reg.fail("x", a).is_none());
        assert!(reg.entries.contains_key("x"));
    }

    #[tokio::test]
    async fn registry_disconnect_cancels_in_flight_connect() {
        let mut reg = Registry::default();
        let (a, _) = reg.begin_connect("x");
        assert!(reg.disconnect("x").is_none());
        let (outcome, leftover) = reg.commit("x", a, sqlite_entry("x").await);
        assert_eq!(outcome, Commit::CancelledByDisconnect);
        assert!(leftover.is_some());
        assert!(!reg.entries.contains_key("x"), "no zombie connection");
    }

    #[tokio::test]
    async fn registry_failed_reconnect_clears_old_entry() {
        let mut reg = Registry::default();
        let (a, _) = reg.begin_connect("x");
        reg.commit("x", a, sqlite_entry("x").await);
        let (b, old) = reg.begin_connect("x");
        assert!(old.is_some(), "reconnect tears down the previous entry");
        reg.fail("x", b);
        assert!(!reg.entries.contains_key("x"));
        assert!(!reg.latest.contains_key("x"));
    }

    #[tokio::test]
    async fn connect_failure_leaves_no_config_behind() {
        let pm = PoolManager::new();
        let good = ConnectionConfig {
            id: "c1".into(),
            db_type: DbType::Sqlite,
            database: "sqlite::memory:".into(),
            ..Default::default()
        };
        pm.connect(good.clone()).await.expect("connect");
        assert!(pm.get_config("c1").await.is_ok());

        let bad = ConnectionConfig {
            database: "/nonexistent-dir-for-tablio-tests/x.db".into(),
            ..good
        };
        assert!(pm.connect(bad).await.is_err());
        assert!(!pm.is_connected("c1").await);
        assert!(
            pm.get_config("c1").await.is_err(),
            "stale config must not survive a failed reconnect"
        );
    }

    #[test]
    fn pool_manager_default_trait() {
        let pm = PoolManager::default();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            assert!(!pm.is_connected("any").await);
        });
    }
}
