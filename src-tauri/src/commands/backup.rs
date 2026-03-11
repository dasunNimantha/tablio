use crate::db::pool::PoolManager;
use crate::models::*;
use std::sync::Arc;
use tauri::{AppHandle, Emitter, State};

fn emit_log(app: &AppHandle, line: &str) {
    let _ = app.emit("dump-restore-log", line.to_string());
}

async fn stream_stderr(app: &AppHandle, child: &mut tokio::process::Child) {
    use tokio::io::{AsyncBufReadExt, BufReader};
    if let Some(stderr) = child.stderr.take() {
        let mut reader = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = reader.next_line().await {
            emit_log(app, &line);
        }
    }
}

#[tauri::command]
pub async fn dump_and_restore(
    app: AppHandle,
    pool: State<'_, Arc<PoolManager>>,
    request: DumpRestoreRequest,
) -> Result<String, String> {
    let src_config = pool.get_config(&request.source_connection_id).await.map_err(|e| e.to_string())?;
    let tgt_config = pool.get_config(&request.target_connection_id).await.map_err(|e| e.to_string())?;

    if !matches!(src_config.db_type, DbType::Postgres) || !matches!(tgt_config.db_type, DbType::Postgres) {
        return Err("Dump & Restore currently only supports PostgreSQL connections".to_string());
    }

    if request.source_connection_id == request.target_connection_id
        && request.source_database == request.target_database
    {
        return Err("Source and target database cannot be the same".to_string());
    }

    let tmp_path = std::env::temp_dir().join(format!(
        "dbstudio-dump-{}-{}.backup",
        request.source_database,
        chrono::Utc::now().format("%Y%m%d-%H%M%S")
    ));
    let tmp_str = tmp_path.to_string_lossy().to_string();

    emit_log(&app, &format!("Starting pg_dump from {} / {}…", src_config.host, request.source_database));

    // pg_dump with --verbose to get progress output on stderr
    let dump_status = {
        let mut cmd = tokio::process::Command::new("pg_dump");
        cmd.arg("-h").arg(&src_config.host)
            .arg("-p").arg(src_config.port.to_string())
            .arg("-U").arg(&src_config.user)
            .arg("-d").arg(&request.source_database)
            .arg("-Fc")
            .arg("--clean")
            .arg("--no-owner")
            .arg("--no-privileges")
            .arg("--verbose")
            .arg("-f").arg(&tmp_str);
        cmd.env("PGPASSWORD", &src_config.password);
        cmd.stderr(std::process::Stdio::piped());
        let mut child = cmd.spawn().map_err(|e| format!("Failed to run pg_dump: {}. Is it installed?", e))?;
        stream_stderr(&app, &mut child).await;
        child.wait().await.map_err(|e| format!("pg_dump process error: {}", e))?
    };

    if !dump_status.success() {
        let _ = tokio::fs::remove_file(&tmp_path).await;
        return Err("pg_dump failed — see logs above for details".to_string());
    }

    emit_log(&app, "Dump complete. Starting pg_restore…");

    // pg_restore with --verbose for progress
    let restore_status = {
        let mut cmd = tokio::process::Command::new("pg_restore");
        cmd.arg("-h").arg(&tgt_config.host)
            .arg("-p").arg(tgt_config.port.to_string())
            .arg("-U").arg(&tgt_config.user)
            .arg("-d").arg(&request.target_database)
            .arg("--clean")
            .arg("--if-exists")
            .arg("--single-transaction")
            .arg("--no-owner")
            .arg("--no-privileges")
            .arg("--verbose")
            .arg(&tmp_str);
        cmd.env("PGPASSWORD", &tgt_config.password);
        cmd.stderr(std::process::Stdio::piped());
        let mut child = cmd.spawn().map_err(|e| format!("Failed to run pg_restore: {}", e))?;
        stream_stderr(&app, &mut child).await;
        child.wait().await.map_err(|e| format!("pg_restore process error: {}", e))?
    };

    let _ = tokio::fs::remove_file(&tmp_path).await;

    if restore_status.success() {
        emit_log(&app, "Restore complete.");
        Ok(format!(
            "Successfully dumped '{}' and restored to '{}'",
            request.source_database, request.target_database
        ))
    } else {
        Err("pg_restore failed — see logs above for details".to_string())
    }
}

#[tauri::command]
pub async fn backup_database(
    pool: State<'_, Arc<PoolManager>>,
    request: BackupRequest,
) -> Result<String, String> {
    let config = pool.get_config(&request.connection_id).await.map_err(|e| e.to_string())?;

    match config.db_type {
        DbType::Postgres => backup_postgres(&config, &request).await,
        DbType::Mysql => backup_mysql(&config, &request).await,
        DbType::Sqlite => backup_sqlite(&config, &request).await,
    }
}

#[tauri::command]
pub async fn restore_database(
    pool: State<'_, Arc<PoolManager>>,
    request: RestoreRequest,
) -> Result<String, String> {
    let config = pool.get_config(&request.connection_id).await.map_err(|e| e.to_string())?;

    match config.db_type {
        DbType::Postgres => restore_postgres(&config, &request).await,
        DbType::Mysql => restore_mysql(&config, &request).await,
        DbType::Sqlite => restore_sqlite(&config, &request).await,
    }
}

async fn backup_postgres(config: &ConnectionConfig, req: &BackupRequest) -> Result<String, String> {
    let mut cmd = tokio::process::Command::new("pg_dump");
    cmd.arg("-h").arg(&config.host)
        .arg("-p").arg(config.port.to_string())
        .arg("-U").arg(&config.user)
        .arg("-d").arg(&req.database)
        .arg("-f").arg(&req.output_path);

    cmd.env("PGPASSWORD", &config.password);

    match req.format.as_str() {
        "custom" => { cmd.arg("-Fc"); }
        "tar" => { cmd.arg("-Ft"); }
        "plain" => { cmd.arg("-Fp"); }
        "directory" => { cmd.arg("-Fd"); }
        _ => { cmd.arg("-Fp"); }
    }

    if req.schema_only {
        cmd.arg("-s");
    }
    if req.data_only {
        cmd.arg("-a");
    }

    let output = cmd.output().await.map_err(|e| format!("Failed to run pg_dump: {}. Is it installed?", e))?;

    if output.status.success() {
        Ok(format!("Backup completed: {}", req.output_path))
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(format!("pg_dump failed: {}", stderr))
    }
}

async fn restore_postgres(config: &ConnectionConfig, req: &RestoreRequest) -> Result<String, String> {
    let is_sql = req.format == "plain" || req.input_path.ends_with(".sql");

    if is_sql {
        let mut cmd = tokio::process::Command::new("psql");
        cmd.arg("-h").arg(&config.host)
            .arg("-p").arg(config.port.to_string())
            .arg("-U").arg(&config.user)
            .arg("-d").arg(&req.database)
            .arg("-f").arg(&req.input_path);
        cmd.env("PGPASSWORD", &config.password);

        let output = cmd.output().await.map_err(|e| format!("Failed to run psql: {}", e))?;
        if output.status.success() {
            Ok("Restore completed".to_string())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(format!("psql restore failed: {}", stderr))
        }
    } else {
        let mut cmd = tokio::process::Command::new("pg_restore");
        cmd.arg("-h").arg(&config.host)
            .arg("-p").arg(config.port.to_string())
            .arg("-U").arg(&config.user)
            .arg("-d").arg(&req.database)
            .arg(&req.input_path);
        cmd.env("PGPASSWORD", &config.password);

        let output = cmd.output().await.map_err(|e| format!("Failed to run pg_restore: {}", e))?;
        if output.status.success() {
            Ok("Restore completed".to_string())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(format!("pg_restore failed: {}", stderr))
        }
    }
}

async fn backup_mysql(config: &ConnectionConfig, req: &BackupRequest) -> Result<String, String> {
    let mut cmd = tokio::process::Command::new("mysqldump");
    cmd.arg("-h").arg(&config.host)
        .arg("-P").arg(config.port.to_string())
        .arg("-u").arg(&config.user)
        .arg(&req.database)
        .arg("--result-file").arg(&req.output_path);

    if !config.password.is_empty() {
        cmd.arg(format!("-p{}", config.password));
    }
    if req.schema_only {
        cmd.arg("--no-data");
    }
    if req.data_only {
        cmd.arg("--no-create-info");
    }

    let output = cmd.output().await.map_err(|e| format!("Failed to run mysqldump: {}", e))?;
    if output.status.success() {
        Ok(format!("Backup completed: {}", req.output_path))
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(format!("mysqldump failed: {}", stderr))
    }
}

async fn restore_mysql(config: &ConnectionConfig, req: &RestoreRequest) -> Result<String, String> {
    let mut cmd = tokio::process::Command::new("mysql");
    cmd.arg("-h").arg(&config.host)
        .arg("-P").arg(config.port.to_string())
        .arg("-u").arg(&config.user)
        .arg(&req.database);

    if !config.password.is_empty() {
        cmd.arg(format!("-p{}", config.password));
    }

    let input = tokio::fs::read(&req.input_path).await
        .map_err(|e| format!("Failed to read file: {}", e))?;
    cmd.stdin(std::process::Stdio::piped());

    let mut child = cmd.spawn().map_err(|e| format!("Failed to run mysql: {}", e))?;
    if let Some(mut stdin) = child.stdin.take() {
        use tokio::io::AsyncWriteExt;
        stdin.write_all(&input).await.map_err(|e| format!("Failed to write to stdin: {}", e))?;
    }

    let output = child.wait_with_output().await.map_err(|e| format!("Failed: {}", e))?;
    if output.status.success() {
        Ok("Restore completed".to_string())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(format!("mysql restore failed: {}", stderr))
    }
}

async fn backup_sqlite(config: &ConnectionConfig, req: &BackupRequest) -> Result<String, String> {
    tokio::fs::copy(&config.database, &req.output_path).await
        .map_err(|e| format!("Failed to copy SQLite file: {}", e))?;
    Ok(format!("Backup completed: {}", req.output_path))
}

async fn restore_sqlite(config: &ConnectionConfig, req: &RestoreRequest) -> Result<String, String> {
    tokio::fs::copy(&req.input_path, &config.database).await
        .map_err(|e| format!("Failed to restore SQLite file: {}", e))?;
    Ok("Restore completed".to_string())
}
