use crate::db::pool::PoolManager;
use crate::models::*;
use std::sync::Arc;
use tauri::State;

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
