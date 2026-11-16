//! Pools for the other databases on a PostgreSQL-wire server.
//!
//! A PG connection is bound to one database, so browsing another database
//! needs its own pool. These used to be 4-connection pools with a 30 minute
//! idle timeout that were never closed before disconnect: browsing 20
//! databases kept ~70 server connections open, enough to exhaust a default
//! `max_connections = 100` server. Pools here are small, release idle
//! connections quickly, and only the most recently used few are kept.

use anyhow::Result;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Connections per extra database (the main database keeps the driver's
/// own pool).
pub const EXTRA_DB_MAX_CONNECTIONS: u32 = 2;
/// Idle connections of extra databases are closed after this long.
pub const EXTRA_DB_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// How many extra databases keep a pool; the least recently used one is
/// closed when another database is opened.
pub const MAX_EXTRA_DB_POOLS: usize = 4;

#[derive(Default)]
pub struct DbPools {
    pools: Mutex<HashMap<String, (PgPool, Instant)>>,
}

impl DbPools {
    pub fn new() -> Self {
        Self::default()
    }

    /// The pool for `database`, connecting with `url` if there is none yet.
    pub async fn get_or_connect(
        &self,
        database: &str,
        url: impl FnOnce() -> String,
    ) -> Result<PgPool> {
        if let Some((pool, used)) = self.pools.lock().await.get_mut(database) {
            *used = Instant::now();
            return Ok(pool.clone());
        }
        // Connect without holding the lock, so other databases (and the
        // sidebar) aren't blocked behind a slow handshake.
        let pool = PgPoolOptions::new()
            .max_connections(EXTRA_DB_MAX_CONNECTIONS)
            .min_connections(0)
            .idle_timeout(EXTRA_DB_IDLE_TIMEOUT)
            .connect(&url())
            .await?;
        let mut pools = self.pools.lock().await;
        if let Some((existing, used)) = pools.get_mut(database) {
            // Another caller connected first: use theirs, close ours.
            *used = Instant::now();
            let existing = existing.clone();
            close_in_background(pool);
            return Ok(existing);
        }
        pools.insert(database.to_string(), (pool.clone(), Instant::now()));
        while pools.len() > MAX_EXTRA_DB_POOLS {
            let Some(oldest) = pools
                .iter()
                .filter(|(name, _)| name.as_str() != database)
                .min_by_key(|(_, (_, used))| *used)
                .map(|(name, _)| name.clone())
            else {
                break;
            };
            if let Some((evicted, _)) = pools.remove(&oldest) {
                // Callers still holding a clone finish their query first;
                // `close` waits for checked-out connections.
                close_in_background(evicted);
            }
        }
        Ok(pool)
    }

    /// Databases that currently have a pool (for tests and diagnostics).
    pub async fn open_databases(&self) -> Vec<String> {
        let mut names: Vec<String> = self.pools.lock().await.keys().cloned().collect();
        names.sort();
        names
    }
}

fn close_in_background(pool: PgPool) {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            handle.spawn(async move { pool.close().await });
        }
        Err(_) => drop(pool),
    }
}
