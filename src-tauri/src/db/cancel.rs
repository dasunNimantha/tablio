//! Cancelling a Query Console run that is still in flight.
//!
//! The console tags every execute with a request id. The command layer
//! keeps a [`CancelRegistry`] of request id -> [`CancellationToken`] and a
//! `cancel_execution(request_id)` command fires the token. Drivers then
//! stop the work in the most precise way the engine allows:
//!
//! * **server-side interrupt** ([`run_interruptible`]): the driver keeps
//!   polling the query future while it asks the server to stop the
//!   statement (`pg_cancel_backend`, `KILL QUERY`, SQLite's progress
//!   handler). The future then finishes on its own with an error, so the
//!   wire protocol stays in sync and the usual cleanup (rolling back a
//!   transaction the script opened) runs. If the server ignores the
//!   interrupt the future is dropped after a few attempts and the
//!   connection must be discarded.
//! * **abort** ([`run_abortable`]): for engines without a usable cancel
//!   (Cassandra, SQL Server through tiberius) the future is dropped and
//!   the driver throws away whatever connection state it may have left.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Result;
pub use tokio_util::sync::CancellationToken;

/// Error a cancelled run resolves to. The message is what the console
/// shows (and matches on) instead of a regular error.
#[derive(Debug, thiserror::Error)]
#[error("{CANCELLED_MESSAGE}")]
pub struct QueryCancelled;

pub const CANCELLED_MESSAGE: &str = "Query cancelled";

pub fn cancelled_error() -> anyhow::Error {
    QueryCancelled.into()
}

pub fn is_cancelled_error(e: &anyhow::Error) -> bool {
    e.is::<QueryCancelled>()
}

/// How a run under [`run_interruptible`] ended.
#[derive(Debug)]
pub enum Interruptible<T> {
    /// Finished without a cancel request.
    Done(Result<T>),
    /// A cancel was requested and the future then finished by itself
    /// (normally with the server's "query interrupted" error). The
    /// connection is still protocol-consistent.
    Interrupted(Result<T>),
    /// A cancel was requested but the future did not finish after
    /// repeated interrupts, so it was dropped mid-protocol. The
    /// connection it used must not be reused.
    Abandoned,
}

/// How many times the server is asked to stop before giving up.
const INTERRUPT_ATTEMPTS: u32 = 3;
/// Upper bound for one interrupt round trip (it runs on another pooled
/// connection, which may have to be opened first).
const INTERRUPT_TIMEOUT: Duration = Duration::from_secs(2);
/// How long to wait for the statement to end after each interrupt.
/// Re-sending covers the race where the interrupt reached the server
/// between two statements and was ignored as "nothing running".
const INTERRUPT_GRACE: Duration = Duration::from_millis(1000);

/// Drive `fut`; once `token` fires, call `interrupt` (repeatedly if
/// needed) while continuing to poll `fut` until it ends by itself.
pub async fn run_interruptible<T, F, I, IF>(
    fut: F,
    token: &CancellationToken,
    interrupt: I,
) -> Interruptible<T>
where
    F: Future<Output = Result<T>>,
    I: FnMut() -> IF,
    IF: Future<Output = ()>,
{
    run_interruptible_with(fut, token, interrupt, INTERRUPT_ATTEMPTS, INTERRUPT_GRACE).await
}

async fn run_interruptible_with<T, F, I, IF>(
    fut: F,
    token: &CancellationToken,
    mut interrupt: I,
    attempts: u32,
    grace: Duration,
) -> Interruptible<T>
where
    F: Future<Output = Result<T>>,
    I: FnMut() -> IF,
    IF: Future<Output = ()>,
{
    tokio::pin!(fut);
    tokio::select! {
        biased;
        r = &mut fut => return Interruptible::Done(r),
        _ = token.cancelled() => {}
    }
    for _ in 0..attempts {
        let kill = interrupt();
        let step = async move {
            let _ = tokio::time::timeout(INTERRUPT_TIMEOUT, kill).await;
            tokio::time::sleep(grace).await;
        };
        tokio::select! {
            biased;
            r = &mut fut => return Interruptible::Interrupted(r),
            _ = step => {}
        }
    }
    Interruptible::Abandoned
}

/// Drive `fut` and drop it as soon as `token` fires.
pub async fn run_abortable<T, F>(fut: F, token: &CancellationToken) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    tokio::select! {
        biased;
        r = fut => r,
        _ = token.cancelled() => Err(cancelled_error()),
    }
}

/// Wait for `fut` (typically `pool.acquire()`) unless the run is
/// cancelled first.
pub async fn unless_cancelled<T, E, F>(fut: F, token: &CancellationToken) -> Result<T>
where
    F: Future<Output = std::result::Result<T, E>>,
    E: Into<anyhow::Error>,
{
    if token.is_cancelled() {
        return Err(cancelled_error());
    }
    tokio::select! {
        biased;
        r = fut => r.map_err(Into::into),
        _ = token.cancelled() => Err(cancelled_error()),
    }
}

/// Map an outcome to the cancelled error when the run was cancelled.
///
/// A statement interrupted by the server reports e.g. "canceling
/// statement due to user request"; MySQL's `SLEEP()` even returns a
/// normal row. Either way the user asked to stop, so the console shows
/// one consistent "Query cancelled".
pub fn finish<T>(outcome: Result<T>, token: &CancellationToken) -> Result<T> {
    if token.is_cancelled() {
        Err(cancelled_error())
    } else {
        outcome
    }
}

/// Upper bound on remembered "cancelled before it started" ids, so a
/// cancel for a request that never arrives cannot grow the set forever.
const MAX_EARLY_CANCELS: usize = 256;

#[derive(Default)]
struct RegistryInner {
    running: HashMap<String, CancellationToken>,
    /// Ids cancelled before their execute registered (the two IPC calls
    /// race). The execute then starts out cancelled.
    early: HashSet<String>,
}

/// In-flight console requests, keyed by the frontend's request id.
#[derive(Default)]
pub struct CancelRegistry {
    inner: Mutex<RegistryInner>,
}

/// Removes its request from the registry when dropped, including when
/// the command future is dropped or panics.
pub struct Registration<'a> {
    registry: &'a CancelRegistry,
    id: String,
    pub token: CancellationToken,
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        let mut inner = self
            .registry
            .inner
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        inner.running.remove(&self.id);
    }
}

impl CancelRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, id: &str) -> Registration<'_> {
        let token = CancellationToken::new();
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if inner.early.remove(id) {
            token.cancel();
        }
        inner.running.insert(id.to_string(), token.clone());
        Registration {
            registry: self,
            id: id.to_string(),
            token,
        }
    }

    /// Fire the token for `id`. Returns whether a running request was
    /// found; if not, the id is remembered in case its execute is still
    /// on its way.
    pub fn cancel(&self, id: &str) -> bool {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(token) = inner.running.get(id) {
            token.cancel();
            return true;
        }
        if inner.early.len() >= MAX_EARLY_CANCELS {
            inner.early.clear();
        }
        inner.early.insert(id.to_string());
        false
    }

    pub fn is_running(&self, id: &str) -> bool {
        let inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        inner.running.contains_key(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[test]
    fn registry_cancel_fires_running_token() {
        let reg = CancelRegistry::new();
        let r = reg.register("a");
        assert!(!r.token.is_cancelled());
        assert!(reg.cancel("a"));
        assert!(r.token.is_cancelled());
    }

    #[test]
    fn registry_drop_unregisters() {
        let reg = CancelRegistry::new();
        {
            let _r = reg.register("a");
            assert!(reg.is_running("a"));
        }
        assert!(!reg.is_running("a"));
    }

    #[test]
    fn registry_cancel_before_register_starts_cancelled() {
        let reg = CancelRegistry::new();
        assert!(!reg.cancel("early"));
        let r = reg.register("early");
        assert!(r.token.is_cancelled());
        // The early marker is consumed: a later run with the same id is
        // not cancelled.
        drop(r);
        assert!(!reg.register("early").token.is_cancelled());
    }

    #[test]
    fn registry_early_cancels_are_bounded() {
        let reg = CancelRegistry::new();
        for i in 0..(MAX_EARLY_CANCELS * 2) {
            reg.cancel(&format!("x{i}"));
        }
        assert!(reg.inner.lock().unwrap().early.len() <= MAX_EARLY_CANCELS);
    }

    #[tokio::test]
    async fn interruptible_done_without_cancel() {
        let token = CancellationToken::new();
        let out = run_interruptible(async { Ok(7) }, &token, || async {}).await;
        assert!(matches!(out, Interruptible::Done(Ok(7))));
    }

    #[tokio::test]
    async fn interruptible_calls_interrupt_and_waits_for_future() {
        let token = CancellationToken::new();
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let tx = std::sync::Mutex::new(Some(tx));
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        token.cancel();
        let out: Interruptible<()> = run_interruptible(
            async move {
                rx.await.ok();
                anyhow::bail!("interrupted by server")
            },
            &token,
            || {
                c.fetch_add(1, Ordering::SeqCst);
                if let Some(tx) = tx.lock().unwrap().take() {
                    let _ = tx.send(());
                }
                async {}
            },
        )
        .await;
        assert!(matches!(out, Interruptible::Interrupted(Err(_))));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn interruptible_abandons_when_server_ignores_interrupt() {
        let token = CancellationToken::new();
        token.cancel();
        let calls = Arc::new(AtomicU32::new(0));
        let c = calls.clone();
        let out: Interruptible<()> = run_interruptible_with(
            std::future::pending(),
            &token,
            || {
                c.fetch_add(1, Ordering::SeqCst);
                async {}
            },
            3,
            Duration::from_millis(5),
        )
        .await;
        assert!(matches!(out, Interruptible::Abandoned));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn abortable_returns_cancelled() {
        let token = CancellationToken::new();
        let t2 = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            t2.cancel();
        });
        let err = run_abortable(std::future::pending::<Result<()>>(), &token)
            .await
            .unwrap_err();
        assert!(is_cancelled_error(&err));
        assert_eq!(err.to_string(), CANCELLED_MESSAGE);
    }

    #[test]
    fn finish_maps_to_cancelled_only_when_token_fired() {
        let token = CancellationToken::new();
        assert_eq!(finish(Ok(1), &token).unwrap(), 1);
        token.cancel();
        assert!(is_cancelled_error(&finish(Ok(1), &token).unwrap_err()));
    }
}
