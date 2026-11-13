use crate::db::pool::PoolManager;
use crate::db::ssh_tunnel::{self, EffectiveTarget};
use crate::models::*;
use crate::util::path::{expand_tilde, resolve_sqlite_target};
use std::path::Path;
use std::sync::Arc;
use tauri::{AppHandle, Emitter, State};

/// Open an SSH tunnel for `config` if one is configured, returning the
/// `host:port` the dump/restore subprocess should actually contact.
///
/// The returned [`EffectiveTarget`] keeps the russh session alive while the
/// subprocess is running; dropping it shuts the bastion connection down.
async fn effective_target(config: &ConnectionConfig) -> Result<EffectiveTarget, String> {
    ssh_tunnel::open_for(config)
        .await
        .map_err(|e| format!("Failed to open SSH tunnel: {}", e))
}

fn emit_log(app: &AppHandle, line: &str) {
    let _ = app.emit("dump-restore-log", line.to_string());
}

/// Forward the child's stderr to the UI log line by line and return the
/// collected lines so the caller can classify the failure.
async fn stream_stderr(app: &AppHandle, child: &mut tokio::process::Child) -> Vec<String> {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let mut lines = Vec::new();
    if let Some(stderr) = child.stderr.take() {
        let mut reader = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            emit_log(app, &line);
            lines.push(line);
        }
    }
    lines
}

/// How a `pg_restore` run ended, derived from its exit code and stderr.
#[derive(Debug, PartialEq, Eq)]
enum PgRestoreOutcome {
    Success,
    /// Exit code 1 where pg_restore reported only SQL errors it skipped
    /// (`errors ignored on restore: N`), e.g. `ALTER ... OWNER TO` a role
    /// that doesn't exist on the target.
    IgnoredErrors(u64),
    /// pg_restore never got a working session (bad password, unreachable
    /// host, missing database). Nothing was changed on the target.
    ConnectionFailed(String),
    Failed(String),
}

/// Lines that mean pg_restore could not open (or lost) its connection.
fn is_connection_error(line: &str) -> bool {
    const MARKERS: &[&str] = &[
        "connection to server",
        "connection to database",
        "could not connect to",
        "password authentication failed",
        "no password supplied",
        "could not translate host name",
        "connection refused",
        "server closed the connection unexpectedly",
        "timeout expired",
        "no pg_hba.conf entry",
        "role \"",
        "fatal:",
    ];
    let lower = line.to_ascii_lowercase();
    // `role "x" does not exist` is only a connection failure when reported
    // as FATAL (login role); as a plain error it's an ignorable OWNER TO.
    MARKERS.iter().any(|m| {
        if *m == "role \"" {
            lower.contains("fatal:") && lower.contains(m)
        } else {
            lower.contains(m)
        }
    })
}

fn ignored_error_count(line: &str) -> Option<u64> {
    let idx = line.find("errors ignored on restore:")?;
    line[idx + "errors ignored on restore:".len()..]
        .trim()
        .parse()
        .ok()
}

fn first_error_line(stderr: &[String]) -> String {
    stderr
        .iter()
        .find(|l| {
            let lower = l.to_ascii_lowercase();
            lower.contains("error") || lower.contains("fatal")
        })
        .or_else(|| stderr.last())
        .cloned()
        .unwrap_or_default()
}

fn classify_pg_restore(code: Option<i32>, stderr: &[String]) -> PgRestoreOutcome {
    if let Some(line) = stderr.iter().find(|l| is_connection_error(l)) {
        return PgRestoreOutcome::ConnectionFailed(line.clone());
    }
    match code {
        Some(0) => PgRestoreOutcome::Success,
        Some(1) => match stderr.iter().find_map(|l| ignored_error_count(l)) {
            Some(n) => PgRestoreOutcome::IgnoredErrors(n),
            None => PgRestoreOutcome::Failed(first_error_line(stderr)),
        },
        _ => PgRestoreOutcome::Failed(first_error_line(stderr)),
    }
}

/// Turn a failed `psql -v ON_ERROR_STOP=1 -f <file>` run into a short
/// message for the UI: the first error with its line in the file plus
/// psql's DETAIL / HINT lines, instead of the raw stderr dump.
fn summarize_psql_failure(code: Option<i32>, stderr: &str) -> String {
    let lines: Vec<&str> = stderr.lines().map(str::trim_end).collect();
    let Some(idx) = lines.iter().position(|l| {
        l.contains("ERROR:") || l.contains("FATAL:") || l.starts_with("psql: error:")
    }) else {
        let tail = lines
            .iter()
            .rev()
            .find(|l| !l.trim().is_empty())
            .map(|l| l.trim())
            .unwrap_or("no error output");
        return format!(
            "psql restore failed (exit code {}): {}",
            code.map_or_else(|| "none".to_string(), |c| c.to_string()),
            tail
        );
    };
    let line = lines[idx];
    // Script errors look like `psql:/path/file.sql:12: ERROR:  message`.
    // The path may itself contain ':', so locate the severity first and
    // take the number right before it.
    let (location, message) = match ["ERROR:", "FATAL:"].iter().find_map(|sev| line.find(sev)) {
        Some(sev_at) if line.starts_with("psql:") => {
            let head = line[..sev_at].trim_end().trim_end_matches(':');
            let lineno = head
                .rsplit_once(':')
                .and_then(|(_, n)| n.trim().parse::<u64>().ok());
            (lineno, line[sev_at..].to_string())
        }
        _ => (
            None,
            line.trim_start_matches("psql: error:").trim().to_string(),
        ),
    };
    let message = message.split_whitespace().collect::<Vec<_>>().join(" ");
    let extra: Vec<String> = lines[idx + 1..]
        .iter()
        .take_while(|l| !l.contains("ERROR:") && !l.starts_with("psql:"))
        .map(|l| l.trim())
        .filter(|l| l.starts_with("DETAIL:") || l.starts_with("HINT:"))
        .map(|l| l.split_whitespace().collect::<Vec<_>>().join(" "))
        .collect();
    let mut out = match (code, location) {
        // 3: a statement failed and ON_ERROR_STOP ended the script.
        (Some(3), Some(n)) => format!(
            "Restore stopped at line {n} of the SQL file: {message}. \
             Statements before it were already applied; nothing after it ran."
        ),
        (Some(3), None) => format!(
            "Restore stopped at the first error: {message}. \
             Statements before it were already applied; nothing after it ran."
        ),
        // 2: the connection could not be opened or was lost.
        (Some(2), _) => format!("psql could not connect to the database: {message}"),
        (_, Some(n)) => format!("psql restore failed at line {n}: {message}"),
        _ => format!("psql restore failed: {message}"),
    };
    for e in extra {
        out.push(' ');
        out.push_str(&e);
    }
    out
}

struct PgRestoreArgs<'a> {
    host: &'a str,
    port: u16,
    user: &'a str,
    password: &'a str,
    database: &'a str,
    archive: &'a Path,
    single_transaction: bool,
}

async fn run_pg_restore(
    app: &AppHandle,
    args: PgRestoreArgs<'_>,
) -> Result<(Option<i32>, Vec<String>), String> {
    let mut cmd = tokio::process::Command::new("pg_restore");
    cmd.arg("-h")
        .arg(args.host)
        .arg("-p")
        .arg(args.port.to_string())
        .arg("-U")
        .arg(args.user)
        .arg("-d")
        .arg(args.database)
        // Fail instead of hanging on an interactive password prompt.
        .arg("--no-password")
        .arg("--clean")
        .arg("--if-exists");
    if args.single_transaction {
        // Implies --exit-on-error.
        cmd.arg("--single-transaction");
    }
    cmd.arg("--verbose").arg(args.archive);
    cmd.env("PGPASSWORD", args.password);
    cmd.stderr(std::process::Stdio::piped());
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("Failed to run pg_restore: {}", e))?;
    let stderr = stream_stderr(app, &mut child).await;
    let status = child
        .wait()
        .await
        .map_err(|e| format!("pg_restore process error: {}", e))?;
    Ok((status.code(), stderr))
}

#[tauri::command]
pub async fn dump_and_restore(
    app: AppHandle,
    pool: State<'_, Arc<PoolManager>>,
    request: DumpRestoreRequest,
) -> Result<String, String> {
    let src_config = pool
        .get_config(&request.source_connection_id)
        .await
        .map_err(|e| e.to_string())?;
    let tgt_config = pool
        .get_config(&request.target_connection_id)
        .await
        .map_err(|e| e.to_string())?;

    if !matches!(src_config.db_type, DbType::Postgres)
        || !matches!(tgt_config.db_type, DbType::Postgres)
    {
        return Err("Dump & Restore currently only supports PostgreSQL connections".to_string());
    }

    if request.source_connection_id == request.target_connection_id
        && request.source_database == request.target_database
    {
        return Err("Source and target database cannot be the same".to_string());
    }

    // The dump is a full copy of the source database. Keep it in a private
    // (0700) directory that is removed when `work_dir` is dropped, on every
    // return path including errors. The file name is fixed so nothing from
    // the request (e.g. a database name containing `/`) reaches the path.
    let work_dir = tempfile::Builder::new()
        .prefix("tablio-dump-")
        .tempdir()
        .map_err(|e| format!("Failed to create a private temp directory: {}", e))?;
    let dump_path = work_dir.path().join("dump.backup");

    emit_log(
        &app,
        &format!(
            "Starting pg_dump from {} / {}…",
            src_config.host, request.source_database
        ),
    );

    let src_target = effective_target(&src_config).await?;
    if src_config.ssh_enabled {
        emit_log(
            &app,
            &format!(
                "Routing pg_dump through SSH tunnel {}:{} → {}:{}",
                src_config.ssh_host, src_config.ssh_port, src_config.host, src_config.port
            ),
        );
    }

    let dump_status = {
        let mut cmd = tokio::process::Command::new("pg_dump");
        cmd.arg("-h")
            .arg(&src_target.host)
            .arg("-p")
            .arg(src_target.port.to_string())
            .arg("-U")
            .arg(&src_config.user)
            .arg("-d")
            .arg(&request.source_database)
            .arg("--no-password")
            .arg("-Fc")
            .arg("--verbose")
            .arg("-f")
            .arg(&dump_path);
        cmd.env("PGPASSWORD", &src_config.password);
        cmd.stderr(std::process::Stdio::piped());
        let mut child = cmd
            .spawn()
            .map_err(|e| format!("Failed to run pg_dump: {}. Is it installed?", e))?;
        let stderr = stream_stderr(&app, &mut child).await;
        let status = child
            .wait()
            .await
            .map_err(|e| format!("pg_dump process error: {}", e))?;
        (status, stderr)
    };
    drop(src_target);

    if !dump_status.0.success() {
        return Err(format!(
            "pg_dump failed: {}",
            first_error_line(&dump_status.1)
        ));
    }

    emit_log(&app, "Dump complete. Starting pg_restore (transactional)…");

    let tgt_target = effective_target(&tgt_config).await?;
    if tgt_config.ssh_enabled {
        emit_log(
            &app,
            &format!(
                "Routing pg_restore through SSH tunnel {}:{} → {}:{}",
                tgt_config.ssh_host, tgt_config.ssh_port, tgt_config.host, tgt_config.port
            ),
        );
    }

    let restore_args = |single_transaction| PgRestoreArgs {
        host: &tgt_target.host,
        port: tgt_target.port,
        user: &tgt_config.user,
        password: &tgt_config.password,
        database: &request.target_database,
        archive: &dump_path,
        single_transaction,
    };

    // First attempt: `--clean --if-exists` inside `--single-transaction`.
    // The drops and the recreation run in one transaction, so any error
    // rolls everything back and leaves the target exactly as it was.
    let (code, stderr) = run_pg_restore(&app, restore_args(true)).await?;
    match classify_pg_restore(code, &stderr) {
        PgRestoreOutcome::Success => {
            emit_log(&app, "Restore complete (transactional).");
            return Ok(format!(
                "Successfully dumped '{}' and restored to '{}'",
                request.source_database, request.target_database
            ));
        }
        PgRestoreOutcome::ConnectionFailed(line) => {
            // Nothing was touched; retrying non-transactionally would fail
            // the same way at best.
            emit_log(&app, "pg_restore could not connect to the target.");
            return Err(format!(
                "pg_restore could not connect to the target database: {}",
                line
            ));
        }
        PgRestoreOutcome::IgnoredErrors(_) | PgRestoreOutcome::Failed(_) => {}
    }

    // The transactional attempt rolled back (often for non-fatal reasons
    // such as a role referenced by `OWNER TO` missing on the target).
    // Retry without the transaction so pg_restore can skip those errors.
    // This run is NOT atomic: objects are dropped and recreated one by one.
    emit_log(
        &app,
        "Transactional restore rolled back. Retrying without --single-transaction (not atomic)…",
    );
    let (code, stderr) = run_pg_restore(&app, restore_args(false)).await?;
    drop(tgt_target);

    match classify_pg_restore(code, &stderr) {
        PgRestoreOutcome::Success => {
            emit_log(&app, "Restore complete.");
            Ok(format!(
                "Successfully dumped '{}' and restored to '{}'",
                request.source_database, request.target_database
            ))
        }
        PgRestoreOutcome::IgnoredErrors(n) => {
            emit_log(
                &app,
                &format!(
                    "Restore finished; pg_restore ignored {} error(s) — see logs above.",
                    n
                ),
            );
            Ok(format!(
                "Dumped '{}' and restored to '{}', but pg_restore ignored {} error(s) — check the logs",
                request.source_database, request.target_database, n
            ))
        }
        PgRestoreOutcome::ConnectionFailed(line) => Err(format!(
            "pg_restore lost its connection to the target database: {}",
            line
        )),
        PgRestoreOutcome::Failed(line) => {
            let code = code.unwrap_or(-1);
            emit_log(&app, &format!("pg_restore failed with exit code {}", code));
            Err(format!(
                "pg_restore failed (exit code {}): {} — see logs for details",
                code, line
            ))
        }
    }
}

#[tauri::command]
pub async fn backup_database(
    pool: State<'_, Arc<PoolManager>>,
    mut request: BackupRequest,
) -> Result<String, String> {
    // Most callers come from the native save dialog (already absolute),
    // but the field is also typeable, so mirror shell tilde expansion.
    request.output_path = expand_tilde(&request.output_path);

    let config = pool
        .get_config(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;

    match config.db_type {
        DbType::Postgres | DbType::Cockroachdb => backup_postgres(&config, &request).await,
        DbType::Mysql | DbType::Mariadb | DbType::Tidb => backup_mysql(&config, &request).await,
        DbType::Sqlite => backup_sqlite(&config.database, Path::new(&request.output_path))
            .await
            .map(|_| format!("Backup completed: {}", request.output_path)),
        DbType::Cassandra => Err("Backup is not supported for Cassandra/ScyllaDB".to_string()),
        DbType::Mssql => Err("Backup is not supported for Microsoft SQL Server".to_string()),
    }
}

#[tauri::command]
pub async fn restore_database(
    pool: State<'_, Arc<PoolManager>>,
    mut request: RestoreRequest,
) -> Result<String, String> {
    request.input_path = expand_tilde(&request.input_path);

    let config = pool
        .get_config(&request.connection_id)
        .await
        .map_err(|e| e.to_string())?;

    match config.db_type {
        DbType::Postgres | DbType::Cockroachdb => restore_postgres(&config, &request).await,
        DbType::Mysql | DbType::Mariadb | DbType::Tidb => restore_mysql(&config, &request).await,
        DbType::Sqlite => restore_sqlite(&pool, &config, Path::new(&request.input_path)).await,
        DbType::Cassandra => Err("Restore is not supported for Cassandra/ScyllaDB".to_string()),
        DbType::Mssql => Err("Restore is not supported for Microsoft SQL Server".to_string()),
    }
}

async fn backup_postgres(config: &ConnectionConfig, req: &BackupRequest) -> Result<String, String> {
    if req.schema_only && req.data_only {
        return Err("Cannot use both schema-only and data-only at the same time".to_string());
    }

    let target = effective_target(config).await?;

    let mut cmd = tokio::process::Command::new("pg_dump");
    cmd.arg("-h")
        .arg(&target.host)
        .arg("-p")
        .arg(target.port.to_string())
        .arg("-U")
        .arg(&config.user)
        .arg("-d")
        .arg(&req.database)
        .arg("--no-password")
        .arg("-f")
        .arg(&req.output_path);

    cmd.env("PGPASSWORD", &config.password);

    match req.format.as_str() {
        "custom" => {
            cmd.arg("-Fc");
        }
        "tar" => {
            cmd.arg("-Ft");
        }
        "plain" => {
            cmd.arg("-Fp");
        }
        "directory" => {
            cmd.arg("-Fd");
        }
        _ => {
            cmd.arg("-Fp");
        }
    }

    if req.schema_only {
        cmd.arg("-s");
    }
    if req.data_only {
        cmd.arg("-a");
    }

    let output = cmd
        .output()
        .await
        .map_err(|e| format!("Failed to run pg_dump: {}. Is it installed?", e))?;
    drop(target);

    if output.status.success() {
        Ok(format!("Backup completed: {}", req.output_path))
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(format!("pg_dump failed: {}", stderr))
    }
}

/// Restore a Postgres / CockroachDB database from `req.input_path`
/// (`psql` for plain SQL, `pg_restore` for archives).
pub async fn restore_postgres(
    config: &ConnectionConfig,
    req: &RestoreRequest,
) -> Result<String, String> {
    let is_sql = req.format == "plain" || req.input_path.ends_with(".sql");

    let target = effective_target(config).await?;

    if is_sql {
        let mut cmd = tokio::process::Command::new("psql");
        cmd.arg("-h")
            .arg(&target.host)
            .arg("-p")
            .arg(target.port.to_string())
            .arg("-U")
            .arg(&config.user)
            .arg("-d")
            .arg(&req.database)
            .arg("--no-password")
            // Ignore the user's ~/.psqlrc: it could change output or
            // error handling (e.g. turn ON_ERROR_STOP back off).
            .arg("-X")
            // Without this psql logs a failed statement and carries on,
            // exiting 0, so a broken restore would look successful and
            // later statements would run against a half-built schema.
            // Stopping at the first error (exit code 3) lets us report
            // exactly which statement failed.
            .arg("-v")
            .arg("ON_ERROR_STOP=1")
            .arg("-f")
            .arg(&req.input_path);
        cmd.env("PGPASSWORD", &config.password);

        let output = cmd
            .output()
            .await
            .map_err(|e| format!("Failed to run psql: {}. Is it installed?", e))?;
        drop(target);
        if output.status.success() {
            Ok("Restore completed".to_string())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(summarize_psql_failure(output.status.code(), &stderr))
        }
    } else {
        let mut cmd = tokio::process::Command::new("pg_restore");
        cmd.arg("-h")
            .arg(&target.host)
            .arg("-p")
            .arg(target.port.to_string())
            .arg("-U")
            .arg(&config.user)
            .arg("-d")
            .arg(&req.database)
            .arg("--no-password");

        match req.format.as_str() {
            "custom" => {
                cmd.arg("-Fc");
            }
            "tar" => {
                cmd.arg("-Ft");
            }
            "directory" => {
                cmd.arg("-Fd");
            }
            _ => {} // auto: let pg_restore detect format
        }

        cmd.arg(&req.input_path);
        cmd.env("PGPASSWORD", &config.password);

        let output = cmd
            .output()
            .await
            .map_err(|e| format!("Failed to run pg_restore: {}", e))?;
        drop(target);
        if output.status.success() {
            Ok("Restore completed".to_string())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(format!("pg_restore failed: {}", stderr))
        }
    }
}

async fn backup_mysql(config: &ConnectionConfig, req: &BackupRequest) -> Result<String, String> {
    let target = effective_target(config).await?;

    let mut cmd = tokio::process::Command::new("mysqldump");
    cmd.arg("-h")
        .arg(&target.host)
        .arg("-P")
        .arg(target.port.to_string())
        .arg("-u")
        .arg(&config.user)
        .arg(&req.database)
        .arg("--result-file")
        .arg(&req.output_path);

    if !config.password.is_empty() {
        cmd.env("MYSQL_PWD", &config.password);
    }
    if req.schema_only {
        cmd.arg("--no-data");
    }
    if req.data_only {
        cmd.arg("--no-create-info");
    }

    let output = cmd
        .output()
        .await
        .map_err(|e| format!("Failed to run mysqldump: {}", e))?;
    drop(target);
    if output.status.success() {
        Ok(format!("Backup completed: {}", req.output_path))
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(format!("mysqldump failed: {}", stderr))
    }
}

async fn restore_mysql(config: &ConnectionConfig, req: &RestoreRequest) -> Result<String, String> {
    use std::process::Stdio;

    // Open the dump before starting the client so a bad path fails fast.
    let mut input = tokio::fs::File::open(&req.input_path)
        .await
        .map_err(|e| format!("Failed to open {}: {}", req.input_path, e))?;

    let target = effective_target(config).await?;

    let mut cmd = tokio::process::Command::new("mysql");
    cmd.arg("-h")
        .arg(&target.host)
        .arg("-P")
        .arg(target.port.to_string())
        .arg("-u")
        .arg(&config.user)
        .arg(&req.database);

    if !config.password.is_empty() {
        cmd.env("MYSQL_PWD", &config.password);
    }
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = cmd
        .spawn()
        .map_err(|e| format!("Failed to run mysql: {}", e))?;

    // Stream the dump into stdin while `wait_with_output` drains stdout and
    // stderr concurrently; writing everything first could deadlock once the
    // client fills its stderr pipe, and reading the whole dump into memory
    // doesn't scale to multi-GB files.
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| "Failed to open mysql stdin".to_string())?;
    let feeder = tokio::spawn(async move {
        let res = tokio::io::copy(&mut input, &mut stdin).await;
        drop(stdin); // EOF tells mysql the script is complete.
        res
    });

    let output = child
        .wait_with_output()
        .await
        .map_err(|e| format!("mysql process error: {}", e))?;
    let fed = feeder
        .await
        .map_err(|e| format!("Failed to stream dump to mysql: {}", e))?;
    drop(target);

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !output.status.success() {
        let detail = if stderr.is_empty() {
            format!("exit status {}", output.status)
        } else {
            stderr
        };
        return Err(format!("mysql restore failed: {}", detail));
    }
    // mysql exited 0 but we couldn't hand it the whole file.
    fed.map_err(|e| format!("Failed to stream dump to mysql: {}", e))?;
    Ok("Restore completed".to_string())
}

/// Back up a SQLite database with `VACUUM INTO`, which produces a
/// consistent, self-contained copy that includes changes still sitting in
/// the WAL (a plain file copy would lose them, or capture a torn state).
///
/// The copy is written to a private temp file next to `output` and renamed
/// into place, so a failed backup never leaves a half-written file behind.
pub async fn backup_sqlite(database: &str, output: &Path) -> Result<(), String> {
    use sqlx::sqlite::SqliteConnectOptions;
    use sqlx::{ConnectOptions, Connection};
    use std::str::FromStr;

    let target = resolve_sqlite_target(database);
    if target.in_memory {
        return Err("Backup is not supported for in-memory SQLite databases".to_string());
    }

    let out_dir = output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let tmp = tempfile::Builder::new()
        .prefix(".tablio-backup-")
        .tempfile_in(out_dir)
        .map_err(|e| {
            format!(
                "Failed to create backup file in {}: {}",
                out_dir.display(),
                e
            )
        })?;
    let tmp_path = tmp.path().to_string_lossy().into_owned();

    let opts = SqliteConnectOptions::from_str(&target.url)
        .map_err(|e| format!("Invalid SQLite path {}: {}", target.path, e))?
        .create_if_missing(false);
    let mut conn = opts
        .connect()
        .await
        .map_err(|e| format!("Failed to open SQLite database {}: {}", target.path, e))?;
    // VACUUM INTO refuses to write into a non-empty file; the temp file was
    // just created empty.
    let res = sqlx::query("VACUUM INTO ?")
        .bind(&tmp_path)
        .execute(&mut conn)
        .await;
    let _ = conn.close().await;
    res.map_err(|e| format!("SQLite backup failed: {}", e))?;

    tmp.persist(output)
        .map_err(|e| format!("Failed to write backup {}: {}", output.display(), e.error))?;
    Ok(())
}

const SQLITE_HEADER: &[u8; 16] = b"SQLite format 3\0";

/// Replace the database file of a SQLite connection with `input`.
///
/// The live pool is closed first (waiting for in-flight queries), stale
/// `-wal` / `-shm` / `-journal` files of the old database are removed so
/// SQLite can't replay them onto the restored file, the new file is moved
/// into place atomically, and the connection is reopened.
pub async fn restore_sqlite(
    pool: &PoolManager,
    config: &ConnectionConfig,
    input: &Path,
) -> Result<String, String> {
    let target = resolve_sqlite_target(&config.database);
    if target.in_memory {
        return Err("Restore is not supported for in-memory SQLite databases".to_string());
    }
    let dest = Path::new(&target.path).to_path_buf();
    if let (Ok(a), Ok(b)) = (std::fs::canonicalize(input), std::fs::canonicalize(&dest)) {
        if a == b {
            return Err("The backup file is the database itself".to_string());
        }
    }

    // Validate before disconnecting anything.
    let mut header = [0u8; 16];
    {
        use std::io::Read;
        let mut f = std::fs::File::open(input)
            .map_err(|e| format!("Failed to open {}: {}", input.display(), e))?;
        f.read_exact(&mut header)
            .map_err(|_| format!("{} is not a SQLite database", input.display()))?;
    }
    if &header != SQLITE_HEADER {
        return Err(format!("{} is not a SQLite database", input.display()));
    }

    // Stage the copy next to the destination first: if that fails (disk
    // full, permissions) the live database is untouched and still open.
    let dest_dir = dest
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let staged = {
        let input = input.to_path_buf();
        let dest = dest.clone();
        tokio::task::spawn_blocking(move || -> Result<tempfile::NamedTempFile, String> {
            let mut tmp = tempfile::Builder::new()
                .prefix(".tablio-restore-")
                .tempfile_in(&dest_dir)
                .map_err(|e| format!("Failed to stage restore in {}: {}", dest_dir.display(), e))?;
            let mut src = std::fs::File::open(&input)
                .map_err(|e| format!("Failed to open {}: {}", input.display(), e))?;
            std::io::copy(&mut src, tmp.as_file_mut())
                .map_err(|e| format!("Failed to copy backup: {}", e))?;
            tmp.as_file()
                .sync_all()
                .map_err(|e| format!("Failed to flush restored file: {}", e))?;
            // Keep the original file's permissions rather than the temp
            // file's 0600.
            if let Ok(meta) = std::fs::metadata(&dest) {
                let _ = std::fs::set_permissions(tmp.path(), meta.permissions());
            }
            Ok(tmp)
        })
        .await
        .map_err(|e| format!("Restore task failed: {}", e))??
    };

    // Close every pooled handle on the old file before replacing it.
    if let Some((driver, _)) = pool.detach(&config.id).await {
        driver.close().await;
    }

    let swap = {
        let dest = dest.clone();
        tokio::task::spawn_blocking(move || -> Result<(), String> {
            for suffix in ["-wal", "-shm", "-journal"] {
                let mut side = dest.clone().into_os_string();
                side.push(suffix);
                match std::fs::remove_file(&side) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => {
                        return Err(format!(
                            "Failed to remove stale {}: {}",
                            Path::new(&side).display(),
                            e
                        ))
                    }
                }
            }
            staged
                .persist(&dest)
                .map_err(|e| format!("Failed to replace {}: {}", dest.display(), e.error))?;
            Ok(())
        })
        .await
        .map_err(|e| format!("Restore task failed: {}", e))?
    };

    // Reopen the connection whether or not the swap worked, so the UI's
    // "connected" state stays truthful.
    let reconnect = pool.connect(config.clone()).await;
    swap?;
    match reconnect {
        Ok(_) => Ok("Restore completed".to_string()),
        Err(e) => Err(format!(
            "Restore completed, but reconnecting failed: {}. Reconnect manually.",
            e
        )),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn psql_failure_reports_line_message_and_detail() {
        let stderr = "psql:/tmp/my:dump.sql:7: ERROR:  relation \"missing\" does not exist\n\
                      LINE 1: INSERT INTO missing VALUES (1);\n\
                      \x20                   ^\n";
        let msg = summarize_psql_failure(Some(3), stderr);
        assert!(msg.starts_with("Restore stopped at line 7 of the SQL file: ERROR: relation \"missing\" does not exist."), "{msg}");
        assert!(msg.contains("already applied"), "{msg}");
        assert!(!msg.contains("LINE 1"), "{msg}");

        let stderr =
            "psql:dump.sql:3: ERROR:  duplicate key value violates unique constraint \"t_pkey\"\n\
                      DETAIL:  Key (id)=(1) already exists.\n";
        let msg = summarize_psql_failure(Some(3), stderr);
        assert!(msg.contains("line 3"), "{msg}");
        assert!(
            msg.ends_with("DETAIL: Key (id)=(1) already exists."),
            "{msg}"
        );
    }

    #[test]
    fn psql_failure_connection_and_unknown_output() {
        let stderr = "psql: error: connection to server at \"127.0.0.1\", port 1 failed: Connection refused\n";
        let msg = summarize_psql_failure(Some(2), stderr);
        assert!(
            msg.starts_with("psql could not connect to the database: connection to server"),
            "{msg}"
        );

        let msg = summarize_psql_failure(Some(1), "psql: could not open file\n");
        assert_eq!(
            msg,
            "psql restore failed (exit code 1): psql: could not open file"
        );
        assert_eq!(
            summarize_psql_failure(None, ""),
            "psql restore failed (exit code none): no error output"
        );
    }

    use super::*;

    fn lines(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn pg_restore_exit_1_with_connection_error_is_not_success() {
        let stderr = lines(&[
            "pg_restore: error: connection to server at \"127.0.0.1\", port 5432 failed: FATAL:  password authentication failed for user \"postgres\"",
        ]);
        assert!(matches!(
            classify_pg_restore(Some(1), &stderr),
            PgRestoreOutcome::ConnectionFailed(_)
        ));
        let stderr = lines(&[
            "pg_restore: error: connection to server at \"db\" (10.0.0.1), port 5432 failed: Connection refused",
        ]);
        assert!(matches!(
            classify_pg_restore(Some(1), &stderr),
            PgRestoreOutcome::ConnectionFailed(_)
        ));
    }

    #[test]
    fn pg_restore_exit_1_without_ignored_summary_is_failure() {
        let stderr = lines(&["pg_restore: error: could not read input file: end of file"]);
        assert!(matches!(
            classify_pg_restore(Some(1), &stderr),
            PgRestoreOutcome::Failed(_)
        ));
        assert!(matches!(
            classify_pg_restore(Some(1), &[]),
            PgRestoreOutcome::Failed(_)
        ));
    }

    #[test]
    fn pg_restore_ignored_errors_are_reported_with_count() {
        let stderr = lines(&[
            "pg_restore: error: could not execute query: ERROR:  role \"app\" does not exist",
            "Command was: ALTER TABLE public.t OWNER TO app;",
            "pg_restore: warning: errors ignored on restore: 3",
        ]);
        assert_eq!(
            classify_pg_restore(Some(1), &stderr),
            PgRestoreOutcome::IgnoredErrors(3)
        );
    }

    #[test]
    fn pg_restore_success_and_hard_failure() {
        assert_eq!(classify_pg_restore(Some(0), &[]), PgRestoreOutcome::Success);
        assert!(matches!(
            classify_pg_restore(None, &lines(&["killed"])),
            PgRestoreOutcome::Failed(_)
        ));
    }

    async fn sqlite_rows(url: &str) -> Vec<i64> {
        use sqlx::Connection;
        let mut conn = sqlx::SqliteConnection::connect(url).await.unwrap();
        let rows: Vec<(i64,)> = sqlx::query_as("SELECT v FROM t ORDER BY v")
            .fetch_all(&mut conn)
            .await
            .unwrap();
        conn.close().await.unwrap();
        rows.into_iter().map(|r| r.0).collect()
    }

    #[tokio::test]
    async fn sqlite_backup_includes_uncheckpointed_wal_content() {
        use sqlx::Connection;
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("live.db");
        let url = format!("sqlite:{}?mode=rwc", db.display());
        let mut writer = sqlx::SqliteConnection::connect(&url).await.unwrap();
        sqlx::query("PRAGMA journal_mode=WAL")
            .execute(&mut writer)
            .await
            .unwrap();
        sqlx::query("PRAGMA wal_autocheckpoint=0")
            .execute(&mut writer)
            .await
            .unwrap();
        sqlx::query("CREATE TABLE t (v INTEGER)")
            .execute(&mut writer)
            .await
            .unwrap();
        sqlx::query("INSERT INTO t VALUES (1), (2), (3)")
            .execute(&mut writer)
            .await
            .unwrap();
        // `writer` stays open, so the rows live only in live.db-wal.
        let mut wal = db.clone().into_os_string();
        wal.push("-wal");
        assert!(std::fs::metadata(&wal)
            .map(|m| m.len() > 0)
            .unwrap_or(false));

        let out = dir.path().join("sub dir");
        std::fs::create_dir(&out).unwrap();
        let out = out.join("backup.db");
        // Pre-existing output gets replaced, not appended to.
        std::fs::write(&out, b"old junk").unwrap();
        // The `sqlite:...?mode=` connection-string form must work too.
        backup_sqlite(&format!("sqlite:{}?mode=rw", db.display()), &out)
            .await
            .expect("backup");
        writer.close().await.unwrap();

        assert_eq!(
            sqlite_rows(&format!("sqlite:{}", out.display())).await,
            vec![1, 2, 3]
        );
    }

    #[tokio::test]
    async fn sqlite_restore_replaces_live_database_and_reconnects() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("app.db");
        let backup = dir.path().join("app.backup");
        let url = format!("sqlite:{}?mode=rwc", db.display());

        {
            use sqlx::Connection;
            let mut c = sqlx::SqliteConnection::connect(&url).await.unwrap();
            sqlx::query("PRAGMA journal_mode=WAL")
                .execute(&mut c)
                .await
                .unwrap();
            sqlx::query("CREATE TABLE t (v INTEGER)")
                .execute(&mut c)
                .await
                .unwrap();
            sqlx::query("INSERT INTO t VALUES (10)")
                .execute(&mut c)
                .await
                .unwrap();
            c.close().await.unwrap();
        }
        backup_sqlite(db.to_str().unwrap(), &backup).await.unwrap();

        let pool = PoolManager::new();
        let config = ConnectionConfig {
            id: "sqlite-restore".into(),
            db_type: DbType::Sqlite,
            database: db.to_string_lossy().into_owned(),
            ..Default::default()
        };
        pool.connect(config.clone()).await.unwrap();
        let driver = pool.get_driver(&config.id).await.unwrap();
        driver
            .execute_query("main", "INSERT INTO t VALUES (20)")
            .await
            .unwrap();
        drop(driver);

        // Not a SQLite file: rejected before anything is touched.
        let junk = dir.path().join("junk.db");
        std::fs::write(&junk, b"definitely not sqlite").unwrap();
        assert!(restore_sqlite(&pool, &config, &junk).await.is_err());
        assert!(pool.is_connected(&config.id).await);

        restore_sqlite(&pool, &config, &backup)
            .await
            .expect("restore");
        assert!(
            pool.is_connected(&config.id).await,
            "reconnected after restore"
        );
        let driver = pool.get_driver(&config.id).await.unwrap();
        let res = driver
            .execute_query("main", "SELECT v FROM t ORDER BY v")
            .await
            .unwrap();
        assert_eq!(res.rows.len(), 1, "restored content only: {:?}", res.rows);
        assert_eq!(res.rows[0][0], serde_json::json!(10));
        drop(driver);
        pool.disconnect(&config.id).await.unwrap();
        assert_eq!(
            sqlite_rows(&format!("sqlite:{}", db.display())).await,
            vec![10]
        );
    }

    #[tokio::test]
    async fn sqlite_backup_rejects_in_memory() {
        let dir = tempfile::tempdir().unwrap();
        let err = backup_sqlite("sqlite::memory:", &dir.path().join("x.db"))
            .await
            .unwrap_err();
        assert!(err.contains("in-memory"), "{err}");
    }

    #[test]
    fn missing_target_database_is_a_connection_failure() {
        let stderr = lines(&[
            "pg_restore: error: connection to server at \"h\", port 5432 failed: FATAL:  database \"nope\" does not exist",
        ]);
        assert!(matches!(
            classify_pg_restore(Some(1), &stderr),
            PgRestoreOutcome::ConnectionFailed(_)
        ));
    }
}
