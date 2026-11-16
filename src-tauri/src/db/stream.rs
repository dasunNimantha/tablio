//! Handing rows from a driver to an export writer as they arrive.
//!
//! The producer (a driver reading a query, or the command layer paging
//! through a table) pushes rows into a [`RowSink`]; they travel in small
//! batches over a bounded channel to a writer running on a blocking
//! thread. The bound keeps memory flat: a producer that is faster than
//! the writer waits instead of buffering the whole result.

use crate::export::SqlColumnKind;
use anyhow::{bail, Result};
use serde_json::Value;
use tokio::sync::mpsc;

/// Rows per message.
pub const BATCH_ROWS: usize = 1_000;
/// Messages the channel holds: about one 50,000-row table page, so the
/// next page can be fetched while the writer works through this one.
pub const QUEUE_DEPTH: usize = 50;

/// What travels from a [`RowSink`] to the writer.
#[derive(Debug)]
pub enum Chunk {
    /// Sent once, before any rows. `kinds` may be empty (unknown types).
    Columns {
        names: Vec<String>,
        kinds: Vec<SqlColumnKind>,
    },
    Rows(Vec<Vec<Value>>),
    /// Every row was sent. A channel that closes without it means the
    /// producer failed, and the writer must discard its output.
    End,
}

/// The writer went away (it failed); its own error explains why.
#[derive(Debug, thiserror::Error)]
#[error("The export was stopped.")]
pub struct SinkClosed;

pub const MULTIPLE_RESULT_SETS_MESSAGE: &str = "The query returned more than one result set; \
     only a query with a single result can be exported in full.";

/// Producer side of an export stream.
pub struct RowSink {
    tx: mpsc::Sender<Chunk>,
    buf: Vec<Vec<Value>>,
    has_columns: bool,
    rows: u64,
}

impl RowSink {
    /// A sink and the receiver its writer reads from.
    pub fn channel() -> (Self, mpsc::Receiver<Chunk>) {
        let (tx, rx) = mpsc::channel(QUEUE_DEPTH);
        (
            Self {
                tx,
                buf: Vec::new(),
                has_columns: false,
                rows: 0,
            },
            rx,
        )
    }

    pub fn has_columns(&self) -> bool {
        self.has_columns
    }

    /// Rows pushed so far.
    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// Start the result: its column names (and, for table exports, how
    /// SQL should write each column). A second call means the statement
    /// sent a second result set, which is an error.
    pub async fn columns(&mut self, names: Vec<String>, kinds: Vec<SqlColumnKind>) -> Result<()> {
        if self.has_columns {
            bail!(MULTIPLE_RESULT_SETS_MESSAGE);
        }
        self.has_columns = true;
        self.send(Chunk::Columns { names, kinds }).await
    }

    pub async fn row(&mut self, row: Vec<Value>) -> Result<()> {
        debug_assert!(self.has_columns, "columns must be sent before rows");
        self.buf.push(row);
        self.rows += 1;
        if self.buf.len() >= BATCH_ROWS {
            self.flush().await?;
        }
        Ok(())
    }

    /// Push a page of rows.
    pub async fn extend(&mut self, rows: Vec<Vec<Value>>) -> Result<()> {
        for row in rows {
            self.row(row).await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        if !self.buf.is_empty() {
            let batch = std::mem::replace(&mut self.buf, Vec::with_capacity(BATCH_ROWS));
            self.send(Chunk::Rows(batch)).await?;
        }
        Ok(())
    }

    async fn send(&mut self, chunk: Chunk) -> Result<()> {
        self.tx.send(chunk).await.map_err(|_| SinkClosed.into())
    }

    /// Send what is left and mark the stream complete. A statement that
    /// produced no result set at all is written as one without columns.
    /// Returns the number of rows sent.
    pub async fn finish(mut self) -> Result<u64> {
        if !self.has_columns {
            self.columns(Vec::new(), Vec::new()).await?;
        }
        self.flush().await?;
        self.send(Chunk::End).await?;
        Ok(self.rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn rows_arrive_in_batches_then_end() {
        let (mut sink, mut rx) = RowSink::channel();
        let producer = tokio::spawn(async move {
            sink.columns(vec!["n".into()], vec![]).await.unwrap();
            for i in 0..(BATCH_ROWS + 5) {
                sink.row(vec![json!(i)]).await.unwrap();
            }
            sink.finish().await.unwrap()
        });
        let mut batches = Vec::new();
        let mut ended = false;
        while let Some(chunk) = rx.recv().await {
            match chunk {
                Chunk::Columns { names, .. } => assert_eq!(names, vec!["n"]),
                Chunk::Rows(r) => batches.push(r.len()),
                Chunk::End => ended = true,
            }
        }
        assert!(ended);
        assert_eq!(batches, vec![BATCH_ROWS, 5]);
        assert_eq!(producer.await.unwrap(), (BATCH_ROWS + 5) as u64);
    }

    #[tokio::test]
    async fn second_result_set_is_an_error_and_closed_writer_stops_producer() {
        let (mut sink, rx) = RowSink::channel();
        sink.columns(vec!["a".into()], vec![]).await.unwrap();
        let err = sink.columns(vec!["b".into()], vec![]).await.unwrap_err();
        assert_eq!(err.to_string(), MULTIPLE_RESULT_SETS_MESSAGE);
        drop(rx);
        let err = sink.finish().await.unwrap_err();
        assert!(err.is::<SinkClosed>());
    }
}
