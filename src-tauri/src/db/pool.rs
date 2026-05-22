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

pub struct PoolManager {
    connections: RwLock<HashMap<String, Arc<dyn DatabaseDriver>>>,
    configs: RwLock<HashMap<String, ConnectionConfig>>,
    ssh_tunnels: RwLock<HashMap<String, SshTunnel>>,
}

impl Default for PoolManager {
    fn default() -> Self {
        Self::new()
    }
}

impl PoolManager {
    pub fn new() -> Self {
        Self {
            connections: RwLock::new(HashMap::new()),
            configs: RwLock::new(HashMap::new()),
            ssh_tunnels: RwLock::new(HashMap::new()),
        }
    }

    pub async fn connect(&self, config: ConnectionConfig) -> Result<String> {
        let id = config.id.clone();

        // Clean up any existing connection and tunnel for this id. Dropping
        // the SshTunnel below shuts down its accept loop and SSH session.
        let _old_tunnel = self.ssh_tunnels.write().await.remove(&id);
        self.connections.write().await.remove(&id);

        let effective_config = if config.ssh_enabled && !config.ssh_host.is_empty() {
            let tunnel = ssh_tunnel::open(&config).await?;
            let local_port = tunnel.local_port();
            self.ssh_tunnels.write().await.insert(id.clone(), tunnel);
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

        let driver = match driver_result {
            Ok(d) => d,
            Err(e) => {
                // Drop the tunnel (if any) to release the russh session and
                // forwarding task before returning the driver error.
                let _ = self.ssh_tunnels.write().await.remove(&id);
                return Err(e);
            }
        };

        self.connections.write().await.insert(id.clone(), driver);
        self.configs.write().await.insert(id.clone(), config);
        Ok(id)
    }

    pub async fn disconnect(&self, id: &str) -> Result<()> {
        self.connections.write().await.remove(id);
        self.configs.write().await.remove(id);
        // Drop the SshTunnel (if any) to shut down the russh session.
        let _ = self.ssh_tunnels.write().await.remove(id);
        Ok(())
    }

    pub async fn get_driver(&self, id: &str) -> Result<Arc<dyn DatabaseDriver>> {
        self.connections
            .read()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| anyhow!("Connection '{}' not found", id))
    }

    pub async fn get_config(&self, id: &str) -> Result<ConnectionConfig> {
        self.configs
            .read()
            .await
            .get(id)
            .cloned()
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
        self.connections.read().await.contains_key(id)
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

    #[test]
    fn pool_manager_default_trait() {
        let pm = PoolManager::default();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            assert!(!pm.is_connected("any").await);
        });
    }
}
