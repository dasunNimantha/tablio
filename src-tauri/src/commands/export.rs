//! Export commands.
//!
//! Table exports and full query exports stream: rows are fetched page
//! by page (or read off the wire for a query) and handed through a
//! bounded channel ([`RowSink`]) to a writer on a blocking thread, which
//! writes each batch to the file as it arrives. Memory stays around one
//! page however large the table is, and no tokio worker is blocked by
//! file IO or encoding.

use crate::db::pool::PoolManager;
use crate::db::stream::{Chunk, RowSink, SinkClosed};
use crate::db::DatabaseDriver;
use crate::export::{self, RowWriter, SqlColumnKind, SqlExportDialect, TextExport, TextFormat};
use crate::models::*;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tauri::State;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

/// Rows fetched per round trip when paging through a table.
pub const PAGE_SIZE: u64 = 50_000;

/// Upper bound for exports that have to travel back over IPC as one
/// string, and for tables that can't be paged deterministically.
const MAX_UNPAGED_ROWS: u64 = 1_000_000;

/// Push every row matched by `request` into `sink`, or fail; never
/// truncate silently.
///
/// With a primary key the table is paged in PK order, which is stable
/// across pages; the column list is read once and pages skip the
/// `COUNT(*)` the data grid needs. Without a primary key (or on
/// Cassandra, which can't sort by arbitrary columns) OFFSET paging could
/// skip or repeat rows, so a single fetch is used and the export fails
/// if it would exceed [`MAX_UNPAGED_ROWS`]. `max_rows` caps the total
/// for callers that must hold everything in one IPC payload.
///
/// The columns sent to the sink are the ones the fetch returned, with
/// SQL value kinds from their types.
pub async fn stream_table_rows(
    driver: &Arc<dyn DatabaseDriver>,
    db_type: &DbType,
    request: &ExportRequest,
    page_size: u64,
    max_rows: Option<u64>,
    sink: &mut RowSink,
) -> anyhow::Result<()> {
    let columns: Vec<ColumnInfo> = if matches!(db_type, DbType::Cassandra) {
        Vec::new()
    } else {
        driver
            .list_columns(&request.database, &request.schema, &request.table)
            .await
            .unwrap_or_default()
    };
    let pk_sort: Vec<SortSpec> = columns
        .iter()
        .filter(|c| c.is_primary_key)
        .map(|c| SortSpec {
            column: c.name.clone(),
            direction: SortDirection::Asc,
        })
        .collect();

    let too_many = |limit: u64| {
        anyhow::anyhow!(
            "'{}' has more than {} matching rows; the export was stopped instead of being \
             truncated. Narrow it down with a filter{}.",
            request.table,
            limit,
            if pk_sort.is_empty() {
                " (tables without a primary key can't be paged reliably)"
            } else {
                ", or export to a file"
            }
        )
    };

    if pk_sort.is_empty() {
        let limit = max_rows.unwrap_or(MAX_UNPAGED_ROWS).min(MAX_UNPAGED_ROWS);
        let data = driver
            .fetch_rows(
                &request.database,
                &request.schema,
                &request.table,
                0,
                limit + 1,
                Vec::new(),
                request.filter.clone(),
            )
            .await?;
        if data.rows.len() as u64 > limit {
            return Err(too_many(limit));
        }
        sink.columns(column_names(&data), column_kinds(&data))
            .await?;
        return sink.extend(data.rows).await;
    }

    let mut offset = 0u64;
    loop {
        let page = driver
            .fetch_export_page(
                &request.database,
                &request.schema,
                &request.table,
                &columns,
                offset,
                page_size,
                pk_sort.clone(),
                request.filter.clone(),
            )
            .await?;
        let got = page.rows.len() as u64;
        offset += got;
        if let Some(max) = max_rows {
            if offset > max {
                return Err(too_many(max));
            }
        }
        if !sink.has_columns() {
            sink.columns(column_names(&page), column_kinds(&page))
                .await?;
        }
        sink.extend(page.rows).await?;
        if got < page_size {
            return Ok(());
        }
    }
}

fn column_names(data: &TableData) -> Vec<String> {
    data.columns.iter().map(|c| c.name.clone()).collect()
}

fn column_kinds(data: &TableData) -> Vec<SqlColumnKind> {
    data.columns
        .iter()
        .map(|c| SqlColumnKind::for_type(&c.data_type))
        .collect()
}

/// The writer saw the channel close without [`Chunk::End`]: the producer
/// failed and reports why.
#[derive(Debug, thiserror::Error)]
#[error("The export did not finish.")]
struct ExportAborted;

/// Feed every chunk from `rx` to `writer` until the end marker.
fn drain(rx: &mut Receiver<Chunk>, writer: &mut dyn RowWriter) -> anyhow::Result<()> {
    while let Some(chunk) = rx.blocking_recv() {
        match chunk {
            Chunk::Columns { names, kinds } => writer.begin(&names, &kinds)?,
            Chunk::Rows(rows) => writer.write_rows(&rows)?,
            Chunk::End => return Ok(()),
        }
    }
    Err(ExportAborted.into())
}

/// What to write a file export as.
#[derive(Debug, Clone)]
pub enum FileFormat {
    Text(TextFormat),
    Xlsx { sheet_name: String },
}

impl FileFormat {
    /// `format` as sent by the frontend. `sql_target` is the dialect,
    /// schema and table for `INSERT` statements.
    pub fn parse(
        format: &str,
        sql_target: (SqlExportDialect, Option<String>, String),
    ) -> Result<Self, String> {
        let (dialect, schema, table) = sql_target;
        Ok(match format {
            "csv" => Self::Text(TextFormat::Csv),
            "json" => Self::Text(TextFormat::Json),
            "sql" => Self::Text(TextFormat::Sql {
                dialect,
                schema,
                table,
            }),
            "xlsx" => Self::Xlsx { sheet_name: table },
            other => return Err(format!("Unsupported format: {}", other)),
        })
    }
}

/// The file an export is written to. Rows go to a hidden sibling file
/// that replaces `dest` only once the export has finished, so a failed
/// export leaves neither a partial file nor a clobbered earlier one.
struct PartFile {
    part: PathBuf,
    dest: PathBuf,
    done: bool,
}

impl PartFile {
    fn new(dest: &Path) -> Self {
        let name = dest
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "export".to_string());
        let id = uuid::Uuid::new_v4().simple().to_string();
        Self {
            part: dest.with_file_name(format!(".{name}.{}.part", &id[..8])),
            dest: dest.to_path_buf(),
            done: false,
        }
    }

    fn dir(&self) -> Option<&Path> {
        self.dest.parent().filter(|p| !p.as_os_str().is_empty())
    }

    fn commit(mut self) -> anyhow::Result<()> {
        std::fs::rename(&self.part, &self.dest)?;
        self.done = true;
        Ok(())
    }
}

impl Drop for PartFile {
    fn drop(&mut self) {
        if !self.done {
            let _ = std::fs::remove_file(&self.part);
        }
    }
}

enum FileWriter {
    Text(TextExport<BufWriter<File>>),
    Xlsx(Box<export::XlsxStreamWriter>),
}

/// A file export in progress.
struct FileExport {
    writer: FileWriter,
    file: PartFile,
}

impl FileExport {
    fn create(format: &FileFormat, dest: &Path) -> anyhow::Result<Self> {
        let file = PartFile::new(dest);
        let writer = match format {
            FileFormat::Text(f) => {
                let out = File::create(&file.part)
                    .map_err(|e| anyhow::anyhow!("Failed to write file: {}", e))?;
                FileWriter::Text(TextExport::new(
                    BufWriter::with_capacity(256 * 1024, out),
                    f.clone(),
                ))
            }
            // The worksheet's rows are spooled next to the target rather
            // than in the system temp dir, which may be RAM-backed.
            FileFormat::Xlsx { sheet_name } => FileWriter::Xlsx(Box::new(
                export::XlsxStreamWriter::new(sheet_name, file.dir())
                    .or_else(|_| export::XlsxStreamWriter::new(sheet_name, None))?,
            )),
        };
        Ok(Self { writer, file })
    }

    fn writer(&mut self) -> &mut dyn RowWriter {
        match &mut self.writer {
            FileWriter::Text(w) => w,
            FileWriter::Xlsx(w) => w.as_mut(),
        }
    }

    /// Finish the file and move it into place; returns the xlsx
    /// truncation warning, if any.
    fn finish(self) -> anyhow::Result<Option<String>> {
        let truncated = match self.writer {
            FileWriter::Text(w) => {
                w.finish()?
                    .into_inner()
                    .map_err(|e| anyhow::anyhow!("Failed to write file: {}", e.into_error()))?;
                0
            }
            FileWriter::Xlsx(w) => w
                .finish(&self.file.part)
                .map_err(|e| anyhow::anyhow!("xlsx encoding failed: {e}"))?,
        };
        self.file
            .commit()
            .map_err(|e| anyhow::anyhow!("Failed to write file: {}", e))?;
        Ok(export::xlsx_truncation_warning(truncated))
    }
}

/// Start writing the stream from `rx` to `dest` on a blocking thread.
fn spawn_file_writer(
    format: FileFormat,
    dest: PathBuf,
    mut rx: Receiver<Chunk>,
) -> JoinHandle<anyhow::Result<Option<String>>> {
    tokio::task::spawn_blocking(move || {
        let mut file = FileExport::create(&format, &dest)?;
        drain(&mut rx, file.writer())?;
        file.finish()
    })
}

/// Start collecting the stream from `rx` as text on a blocking thread.
fn spawn_text_writer(
    format: TextFormat,
    mut rx: Receiver<Chunk>,
) -> JoinHandle<anyhow::Result<String>> {
    tokio::task::spawn_blocking(move || {
        let mut w = TextExport::new(Vec::new(), format);
        drain(&mut rx, &mut w)?;
        Ok(String::from_utf8(w.finish()?)?)
    })
}

/// Combine the producer's and the writer's outcome. The writer's error
/// wins when the producer only stopped because the writer had gone.
async fn join_export<T>(
    produced: anyhow::Result<u64>,
    writer: JoinHandle<anyhow::Result<T>>,
) -> Result<(u64, T), String> {
    let written = writer
        .await
        .map_err(|e| format!("The export failed: {e}"))?;
    match (produced, written) {
        (Ok(rows), Ok(out)) => Ok((rows, out)),
        (Ok(_), Err(w)) => Err(w.to_string()),
        (Err(p), Err(w)) if p.is::<SinkClosed>() => Err(w.to_string()),
        (Err(p), _) => Err(p.to_string()),
    }
}

/// Export a table to `dest`, streaming. Returns the number of rows
/// written and the xlsx truncation warning, if any.
pub async fn export_table_to_path(
    driver: &Arc<dyn DatabaseDriver>,
    db_type: &DbType,
    request: &ExportRequest,
    dest: PathBuf,
    page_size: u64,
) -> Result<(u64, Option<String>), String> {
    let format = FileFormat::parse(
        &request.format,
        (
            SqlExportDialect::for_db(db_type),
            Some(request.schema.clone()),
            request.table.clone(),
        ),
    )?;
    let (mut sink, rx) = RowSink::channel();
    let writer = spawn_file_writer(format, dest, rx);
    let produced =
        match stream_table_rows(driver, db_type, request, page_size, None, &mut sink).await {
            Ok(()) => sink.finish().await,
            // Dropping the sink closes the channel, which stops the writer.
            Err(e) => {
                drop(sink);
                Err(e)
            }
        };
    join_export(produced, writer).await
}

/// Export a table as one string (CSV, JSON or SQL), for the IPC
/// variant. Capped at [`MAX_UNPAGED_ROWS`].
pub async fn export_table_to_string(
    driver: &Arc<dyn DatabaseDriver>,
    db_type: &DbType,
    request: &ExportRequest,
    page_size: u64,
) -> Result<String, String> {
    let format = match FileFormat::parse(
        &request.format,
        (
            SqlExportDialect::for_db(db_type),
            Some(request.schema.clone()),
            request.table.clone(),
        ),
    )? {
        FileFormat::Text(f) => f,
        // xlsx is binary; the text-IPC variant of this command would
        // require base64 round-tripping. The frontend calls
        // `export_table_to_file` instead.
        FileFormat::Xlsx { .. } => {
            return Err("xlsx export must use export_table_to_file".to_string())
        }
    };
    let (mut sink, rx) = RowSink::channel();
    let writer = spawn_text_writer(format, rx);
    let produced = match stream_table_rows(
        driver,
        db_type,
        request,
        page_size,
        Some(MAX_UNPAGED_ROWS),
        &mut sink,
    )
    .await
    {
        Ok(()) => sink.finish().await,
        // Dropping the sink closes the channel, which stops the writer.
        Err(e) => {
            drop(sink);
            Err(e)
        }
    };
    join_export(produced, writer).await.map(|(_, s)| s)
}

/// Run `sql` again and export every row it returns to `dest`. Returns
/// the number of rows written and the xlsx truncation warning, if any.
pub async fn export_query_to_path(
    driver: &Arc<dyn DatabaseDriver>,
    database: &str,
    sql: &str,
    format: &str,
    table_name: &str,
    dest: PathBuf,
) -> Result<(u64, Option<String>), String> {
    // Query results don't know their target engine or column types.
    let format = FileFormat::parse(
        format,
        (SqlExportDialect::Standard, None, table_name.to_string()),
    )?;
    let (mut sink, rx) = RowSink::channel();
    let writer = spawn_file_writer(format, dest, rx);
    let produced = match driver.stream_query(database, sql, &mut sink).await {
        Ok(()) => sink.finish().await,
        // Dropping the sink closes the channel, which stops the writer.
        Err(e) => {
            drop(sink);
            Err(e)
        }
    };
    join_export(produced, writer).await
}

#[tauri::command]
pub async fn export_table_data(
    pool: State<'_, Arc<PoolManager>>,
    request: ExportRequest,
) -> Result<String, String> {
    if !matches!(request.format.as_str(), "csv" | "json" | "sql") {
        return Err(match request.format.as_str() {
            "xlsx" => "xlsx export must use export_table_to_file".to_string(),
            other => format!("Unsupported format: {}", other),
        });
    }
    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;
    let db_type = pool
        .get_config(&request.connection_id)
        .await
        .map(|c| c.db_type)
        .unwrap_or_default();
    export_table_to_string(&driver, &db_type, &request, PAGE_SIZE).await
}

/// Writes the export to `file_path`. Resolves to a warning to show the
/// user when the export had to alter data (XLSX cells cut to Excel's
/// length limit), otherwise `null`.
#[tauri::command]
pub async fn export_table_to_file(
    pool: State<'_, Arc<PoolManager>>,
    request: ExportRequest,
    file_path: String,
) -> Result<Option<String>, String> {
    let file_path = crate::util::path::expand_tilde(&file_path);

    if !matches!(request.format.as_str(), "csv" | "json" | "sql" | "xlsx") {
        return Err(format!("Unsupported format: {}", request.format));
    }

    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;
    let db_type = pool
        .get_config(&request.connection_id)
        .await
        .map(|c| c.db_type)
        .unwrap_or_default();

    export_table_to_path(
        &driver,
        &db_type,
        &request,
        PathBuf::from(file_path),
        PAGE_SIZE,
    )
    .await
    .map(|(_, warning)| warning)
}

#[tauri::command]
pub async fn export_query_result(request: ExportResultRequest) -> Result<String, String> {
    let table_name = request
        .table_name
        .unwrap_or_else(|| "query_result".to_string());
    let content = match request.format.as_str() {
        "csv" => export::to_csv(&request.columns, &request.rows),
        "json" => export::to_json(&request.columns, &request.rows),
        // No schema available from the query-result path -- the caller
        // doesn't know which logical table the rows belong to.
        "sql" => export::to_sql_inserts(None, &table_name, &request.columns, &request.rows),
        "xlsx" => return Err("xlsx export must use export_query_result_to_file".to_string()),
        _ => return Err(format!("Unsupported format: {}", request.format)),
    };
    Ok(content)
}

/// Like [`export_table_to_file`], for the rows a console result shows.
#[tauri::command]
pub async fn export_query_result_to_file(
    request: ExportResultRequest,
    file_path: String,
) -> Result<Option<String>, String> {
    let dest = PathBuf::from(crate::util::path::expand_tilde(&file_path));
    let table_name = request
        .table_name
        .unwrap_or_else(|| "query_result".to_string());
    let format = FileFormat::parse(
        &request.format,
        (SqlExportDialect::Standard, None, table_name),
    )?;
    let (columns, rows) = (request.columns, request.rows);
    tokio::task::spawn_blocking(move || {
        let mut file = FileExport::create(&format, &dest)?;
        let w = file.writer();
        w.begin(&columns, &[])?;
        w.write_rows(&rows)?;
        file.finish()
    })
    .await
    .map_err(|e| format!("The export failed: {e}"))?
    .map_err(|e| e.to_string())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryExportRequest {
    pub connection_id: String,
    pub database: String,
    pub sql: String,
    pub format: String,
    pub table_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryExportSummary {
    /// Rows written to the file.
    pub rows: u64,
    /// Shown to the user when the export had to alter data (XLSX cells
    /// cut to Excel's length limit).
    pub warning: Option<String>,
}

/// Run a console query again and write every row it returns to
/// `file_path`, for results the console showed only partly. Only a
/// single statement is accepted (see `DatabaseDriver::stream_query`).
#[tauri::command]
pub async fn export_query_to_file(
    pool: State<'_, Arc<PoolManager>>,
    request: QueryExportRequest,
    file_path: String,
) -> Result<QueryExportSummary, String> {
    let dest = PathBuf::from(crate::util::path::expand_tilde(&file_path));
    let driver = pool
        .get_driver(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;
    let table_name = request
        .table_name
        .unwrap_or_else(|| "query_result".to_string());
    let (rows, warning) = export_query_to_path(
        &driver,
        &request.database,
        &request.sql,
        &request.format,
        &table_name,
        dest,
    )
    .await?;
    Ok(QueryExportSummary { rows, warning })
}
