use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::collections::HashSet;
use std::time::Instant;
use tiberius::numeric::Numeric;
use tiberius::{AuthMethod, Client, ColumnData, Config, EncryptionLevel, Row};
use tokio::net::TcpStream;
use tokio::sync::{MappedMutexGuard, Mutex, MutexGuard};
use tokio_util::compat::TokioAsyncWriteCompatExt;

use crate::db::cancel::{
    cancelled_error, is_cancelled_error, run_abortable, unless_cancelled, CancellationToken,
};
use crate::db::DatabaseDriver;
use crate::models::*;
use crate::util::sql::{split_sql_statements, statement_is_select_like};

type MssqlStream = tokio_util::compat::Compat<tokio::net::TcpStream>;

pub struct MssqlDriver {
    session: Mutex<MssqlSession>,
    /// Kept so a session discarded by a console cancel can be reopened.
    tcfg: Config,
}

/// The driver's one shared connection. `client` is `None` after a
/// console cancel dropped it mid-protocol and reopening failed; the
/// next [`MssqlDriver::lock`] reconnects.
struct MssqlSession {
    client: Option<MssqlClient>,
    /// Identity of `client`'s server session, used to `KILL` it after
    /// a cancel.
    spid: Option<SessionIdentity>,
}

/// `@@SPID` alone is not enough to target a discarded session: once its
/// socket is closed the server may hand the same id to an unrelated new
/// connection. The login time pins the exact session.
#[derive(Debug, Clone)]
struct SessionIdentity {
    spid: i16,
    login_time: String,
}

impl SessionIdentity {
    /// `KILL` the session only if the id still belongs to it.
    fn kill_sql(&self) -> String {
        format!(
            "IF EXISTS (SELECT 1 FROM sys.dm_exec_sessions WHERE session_id = {spid} \
             AND CONVERT(varchar(33), login_time, 126) = '{lt}') EXEC('KILL {spid}')",
            spid = self.spid,
            lt = self.login_time.replace('\'', "''"),
        )
    }
}

fn bracket(ident: &str) -> String {
    format!("[{}]", ident.replace(']', "]]"))
}

/// Statement terminators / comment markers / quotes can't appear in a
/// column type; matches the checks the other drivers apply.
fn type_fragment_is_unsafe(s: &str) -> bool {
    s.contains(';') || s.contains("--") || s.contains("/*") || s.contains("*/") || s.contains('\'')
}

/// Like `type_fragment_is_unsafe` but allows `'` so string defaults
/// such as `DEFAULT 'pending'` survive.
fn default_fragment_is_unsafe(s: &str) -> bool {
    s.contains(';') || s.contains("--") || s.contains("/*") || s.contains("*/")
}

/// `[name] TYPE [DEFAULT x] NULL|NOT NULL` for CREATE TABLE / ADD.
fn column_definition_sql(col: &ColumnDefinition) -> Result<String> {
    let data_type = col.data_type.trim();
    if data_type.is_empty() {
        anyhow::bail!("Data type is required for column {}", col.name);
    }
    if type_fragment_is_unsafe(data_type) {
        anyhow::bail!("Invalid character in data type for column {}", col.name);
    }
    let mut def = format!("{} {}", bracket(&col.name), data_type);
    if let Some(d) = col
        .default_value
        .as_deref()
        .filter(|d| !d.trim().is_empty())
    {
        if default_fragment_is_unsafe(d) {
            anyhow::bail!("Invalid character in default value for column {}", col.name);
        }
        def.push_str(&format!(" DEFAULT {}", d));
    }
    def.push_str(if col.is_nullable {
        " NULL"
    } else {
        " NOT NULL"
    });
    Ok(def)
}

fn create_table_sql(
    schema: &str,
    table_name: &str,
    columns: &[ColumnDefinition],
) -> Result<String> {
    if columns.is_empty() {
        anyhow::bail!("At least one column is required");
    }
    let mut parts = Vec::with_capacity(columns.len() + 1);
    for col in columns {
        parts.push(column_definition_sql(col)?);
    }
    let pk: Vec<String> = columns
        .iter()
        .filter(|c| c.is_primary_key)
        .map(|c| bracket(&c.name))
        .collect();
    if !pk.is_empty() {
        parts.push(format!("PRIMARY KEY ({})", pk.join(", ")));
    }
    Ok(format!(
        "CREATE TABLE {}.{} ({})",
        bracket(schema),
        bracket(table_name),
        parts.join(", ")
    ))
}

/// Mirrors `pg_common::filter_is_unsafe` so the MSSQL driver rejects the
/// same set of filter patterns as Postgres / MySQL / SQLite. See the
/// doc comment on the Postgres helper for the threat model.
fn filter_is_unsafe(filter: &str) -> bool {
    filter_unsafe_reason(filter).is_some()
}

fn filter_unsafe_reason(filter: &str) -> Option<&'static str> {
    crate::util::sql::filter_unsafe_reason_for(filter, crate::util::sql::SqlDialect::Generic)
}

fn three_part_table(database: &str, schema: &str, table: &str) -> String {
    format!(
        "{}.{}.{}",
        bracket(database),
        bracket(schema),
        bracket(table)
    )
}

fn json_to_mssql_literal(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => {
            if *b {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => format!("N'{}'", s.replace('\'', "''")),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            format!("N'{}'", val.to_string().replace('\'', "''"))
        }
    }
}

/// Text rendering of the raw TDS date/time values, matching the other
/// drivers (`YYYY-MM-DD HH:MM:SS[.fraction]`, fraction only when
/// non-zero, `+HH:MM` offset for datetimeoffset). The strings are valid
/// SQL Server literals, so a date/time primary key round-trips through
/// grid edits.
mod tds_time {
    use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
    use tiberius::time as t;

    fn base_1900() -> NaiveDate {
        NaiveDate::from_ymd_opt(1900, 1, 1).unwrap_or_default()
    }

    fn base_0001() -> NaiveDate {
        NaiveDate::from_ymd_opt(1, 1, 1).unwrap_or_default()
    }

    /// `HH:MM:SS` plus `.fraction` (trailing zeros trimmed) for a time of
    /// day given in units of `10^-scale` seconds.
    fn time_of_day(increments: u64, scale: u8) -> String {
        let scale = u32::from(scale.min(9));
        let per_sec = 10u64.pow(scale);
        let secs = increments / per_sec;
        let frac = increments % per_sec;
        let (h, m, s) = (secs / 3600, (secs / 60) % 60, secs % 60);
        let mut out = format!("{:02}:{:02}:{:02}", h, m, s);
        if frac > 0 {
            let digits = format!("{:0width$}", frac, width = scale as usize);
            out.push('.');
            out.push_str(digits.trim_end_matches('0'));
        }
        out
    }

    fn date_str(d: NaiveDate) -> String {
        d.format("%Y-%m-%d").to_string()
    }

    pub fn date(d: t::Date) -> String {
        base_0001()
            .checked_add_signed(Duration::days(i64::from(d.days())))
            .map(date_str)
            .unwrap_or_else(|| format!("{:?}", d))
    }

    pub fn time(tm: t::Time) -> String {
        time_of_day(tm.increments(), tm.scale())
    }

    pub fn datetime(d: t::DateTime) -> String {
        // seconds_fragments are 1/300 s; render as milliseconds the
        // way SQL Server does (.000 / .003 / .007).
        let millis = (u64::from(d.seconds_fragments()) * 10 + 1) / 3;
        match base_1900().checked_add_signed(Duration::days(i64::from(d.days()))) {
            Some(day) => format!("{} {}", date_str(day), time_of_day(millis, 3)),
            None => format!("{:?}", d),
        }
    }

    pub fn smalldatetime(d: t::SmallDateTime) -> String {
        let minutes = u64::from(d.seconds_fragments());
        match base_1900().checked_add_signed(Duration::days(i64::from(d.days()))) {
            Some(day) => format!("{} {}", date_str(day), time_of_day(minutes * 60, 0)),
            None => format!("{:?}", d),
        }
    }

    pub fn datetime2(d: t::DateTime2) -> String {
        format!("{} {}", date(d.date()), time(d.time()))
    }

    pub fn datetimeoffset(d: t::DateTimeOffset) -> String {
        // The wire value is UTC; show local time with its offset.
        let dt2 = d.datetime2();
        let offset = i64::from(d.offset());
        let tm = dt2.time();
        let scale = u32::from(tm.scale().min(9));
        let per_sec = 10u64.pow(scale);
        let utc_day =
            match base_0001().checked_add_signed(Duration::days(i64::from(dt2.date().days()))) {
                Some(day) => day,
                None => return format!("{:?}", d),
            };
        let secs = tm.increments() / per_sec;
        let frac = tm.increments() % per_sec;
        let utc = NaiveDateTime::new(utc_day, NaiveTime::MIN) + Duration::seconds(secs as i64);
        let local = utc + Duration::minutes(offset);
        let local_secs = u64::from(local.time().num_seconds_from_midnight());
        let sign = if offset < 0 { '-' } else { '+' };
        format!(
            "{} {}{}{:02}:{:02}",
            date_str(local.date()),
            time_of_day(local_secs * per_sec + frac, scale as u8),
            sign,
            offset.abs() / 60,
            offset.abs() % 60
        )
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn renders_iso_like_strings() {
            // 2024-01-02 = 739_251 days after 0001-01-01.
            let days_0001 =
                (NaiveDate::from_ymd_opt(2024, 1, 2).unwrap() - base_0001()).num_days() as u32;
            let days_1900 =
                (NaiveDate::from_ymd_opt(2024, 1, 2).unwrap() - base_1900()).num_days() as i32;
            assert_eq!(date(t::Date::new(days_0001)), "2024-01-02");
            let tm = t::Time::new((3 * 3600 + 4 * 60 + 5) * 10_000_000 + 1_234_500, 7);
            assert_eq!(time(tm), "03:04:05.12345");
            assert_eq!(
                datetime2(t::DateTime2::new(
                    t::Date::new(days_0001),
                    t::Time::new(3600 * 10_000_000, 7)
                )),
                "2024-01-02 01:00:00"
            );
            // 1/300 s fragments: 300 = 1 s, 1 = .003
            assert_eq!(
                datetime(t::DateTime::new(days_1900, 3600 * 300 + 1)),
                "2024-01-02 01:00:00.003"
            );
            assert_eq!(
                smalldatetime(t::SmallDateTime::new(days_1900 as u16, 61)),
                "2024-01-02 01:01:00"
            );
            // 23:30 UTC at +05:30 is 05:00 the next day locally.
            let utc = t::DateTime2::new(
                t::Date::new(days_0001),
                t::Time::new((23 * 3600 + 30 * 60) * 10_000_000 + 5_000_000, 7),
            );
            assert_eq!(
                datetimeoffset(t::DateTimeOffset::new(utc, 330)),
                "2024-01-03 05:00:00.5+05:30"
            );
            assert_eq!(
                datetimeoffset(t::DateTimeOffset::new(utc, -60)),
                "2024-01-02 22:30:00.5-01:00"
            );
        }
    }
}

fn column_data_to_json(data: &ColumnData<'_>) -> serde_json::Value {
    match data {
        ColumnData::U8(v) => v
            .map(|x| serde_json::json!(x))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::I16(v) => v
            .map(|x| serde_json::json!(x))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::I32(v) => v
            .map(|x| serde_json::json!(x))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::I64(v) => v
            .map(crate::util::sql::i64_to_json)
            .unwrap_or(serde_json::Value::Null),
        ColumnData::F32(v) => v
            .map(|x| serde_json::json!(x))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::F64(v) => v
            .map(|x| serde_json::json!(x))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::Bit(v) => v
            .map(|x| serde_json::json!(x))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::String(v) => v
            .as_ref()
            .map(|s| serde_json::Value::String(s.to_string()))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::Guid(v) => v
            .map(|g| serde_json::Value::String(g.to_string()))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::Binary(v) => v
            .as_ref()
            .map(|b| {
                serde_json::Value::String(format!(
                    "0x{}",
                    b.iter()
                        .map(|byte| format!("{:02x}", byte))
                        .collect::<String>()
                ))
            })
            .unwrap_or(serde_json::Value::Null),
        ColumnData::Numeric(v) => v
            .as_ref()
            .map(|n: &Numeric| serde_json::Value::String(format!("{}", n)))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::Xml(v) => v
            .as_ref()
            .map(|x| serde_json::Value::String(x.to_string()))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::DateTime(v) => v
            .map(|d| serde_json::Value::String(tds_time::datetime(d)))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::SmallDateTime(v) => v
            .map(|d| serde_json::Value::String(tds_time::smalldatetime(d)))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::Time(v) => v
            .map(|t| serde_json::Value::String(tds_time::time(t)))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::Date(v) => v
            .map(|d| serde_json::Value::String(tds_time::date(d)))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::DateTime2(v) => v
            .map(|d| serde_json::Value::String(tds_time::datetime2(d)))
            .unwrap_or(serde_json::Value::Null),
        ColumnData::DateTimeOffset(v) => v
            .map(|d| serde_json::Value::String(tds_time::datetimeoffset(d)))
            .unwrap_or(serde_json::Value::Null),
    }
}

/// Decode a `serde_json::Value` cell as `i64`, accepting integer numbers,
/// floats with fractional part `.0`, and BIGINT-as-string (some drivers
/// serialize BIGINT as a JSON string to avoid 53-bit precision loss).
fn json_as_i64(v: &serde_json::Value) -> Option<i64> {
    match v {
        serde_json::Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_u64().and_then(|u| i64::try_from(u).ok()))
            .or_else(|| n.as_f64().map(|f| f as i64)),
        serde_json::Value::String(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

/// Decode a `serde_json::Value` cell as `f64`, with the same string-fallback
/// the BIGINT case requires.
fn json_as_f64(v: &serde_json::Value) -> Option<f64> {
    match v {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn rows_to_grid(rows: Vec<Row>) -> (Vec<String>, Vec<Vec<serde_json::Value>>) {
    if rows.is_empty() {
        return (vec![], vec![]);
    }
    let columns: Vec<String> = rows[0]
        .columns()
        .iter()
        .map(|c| c.name().to_string())
        .collect();
    let data: Vec<Vec<serde_json::Value>> = rows
        .iter()
        .map(|row| row.cells().map(|(_, d)| column_data_to_json(d)).collect())
        .collect();
    (columns, data)
}

/// `IF @@ROWCOUNT = 0 THROW ...` placed right after a keyed UPDATE /
/// DELETE so an edit that matches no row fails the batch instead of
/// silently succeeding.
fn no_row_matched_guard(action: &str, pk: &[(String, serde_json::Value)]) -> String {
    let msg = match crate::util::sql::ensure_row_matched(0, action, pk) {
        Err(e) => e.to_string(),
        Ok(()) => String::new(),
    };
    // THROW messages are limited to 2048 characters.
    let msg: String = msg.chars().take(2000).collect();
    format!(
        "IF @@ROWCOUNT = 0 THROW 50000, N'{}', 1;",
        msg.replace('\'', "''")
    )
}

/// Like `json_to_mssql_literal`, but uses the column type: numeric
/// strings (big ids / exact decimals) become bare literals, and
/// `YYYY-MM-DD HH:MM:SS` strings for datetime / smalldatetime columns
/// use the ISO 8601 `T` separator, the only form those types parse
/// independently of the session language / DATEFORMAT.
fn typed_mssql_literal(data_type: Option<&str>, val: &serde_json::Value) -> String {
    if let (Some(t), serde_json::Value::String(s)) = (data_type, val) {
        if crate::util::sql::is_numeric_type_name(t) {
            if let Some(n) = crate::util::sql::numeric_literal(s) {
                return n.to_string();
            }
        }
        let lower = t.to_ascii_lowercase();
        if lower == "datetime" || lower == "smalldatetime" {
            let b = s.as_bytes();
            let looks_iso = b.len() >= 19
                && b[4] == b'-'
                && b[7] == b'-'
                && b[10] == b' '
                && b[13] == b':'
                && b[..10]
                    .iter()
                    .enumerate()
                    .all(|(i, c)| i == 4 || i == 7 || c.is_ascii_digit());
            if looks_iso {
                let iso = format!("{}T{}", &s[..10], &s[11..]);
                return format!("N'{}'", iso.replace('\'', "''"));
            }
        }
    }
    json_to_mssql_literal(val)
}

type MssqlClient = Client<MssqlStream>;

async fn client_select(
    c: &mut MssqlClient,
    sql: &str,
) -> Result<(Vec<String>, Vec<Vec<serde_json::Value>>)> {
    let stream = c.simple_query(sql).await.map_err(|e| anyhow!("{}", e))?;
    let rows = stream
        .into_first_result()
        .await
        .map_err(|e| anyhow!("{}", e))?;
    Ok(rows_to_grid(rows))
}

async fn client_exec(c: &mut MssqlClient, sql: &str) -> Result<u64> {
    let r = c.execute(sql, &[]).await.map_err(|e| anyhow!("{}", e))?;
    Ok(r.rows_affected().iter().copied().sum())
}

async fn client_batch(c: &mut MssqlClient, sql: &str) -> Result<()> {
    c.simple_query(sql)
        .await
        .map_err(|e| anyhow!("{}", e))?
        .into_results()
        .await
        .map_err(|e| anyhow!("{}", e))?;
    Ok(())
}

async fn client_use_database(c: &mut MssqlClient, database: &str) -> Result<()> {
    if !database.trim().is_empty() {
        client_batch(c, &format!("USE {}", bracket(database))).await?;
    }
    Ok(())
}

/// Open a connection and read its session identity.
async fn open_client(tcfg: &Config) -> Result<(MssqlClient, Option<SessionIdentity>)> {
    let tcp = TcpStream::connect(tcfg.get_addr())
        .await
        .map_err(|e| anyhow!("TCP connect failed: {}", e))?;
    tcp.set_nodelay(true)
        .map_err(|e| anyhow!("set_nodelay: {}", e))?;
    let mut client = Client::connect(tcfg.clone(), tcp.compat_write())
        .await
        .map_err(|e| anyhow!("SQL Server login failed: {}", e))?;
    let spid = match client
        .simple_query(
            "SELECT @@SPID, CONVERT(varchar(33), login_time, 126) \
             FROM sys.dm_exec_sessions WHERE session_id = @@SPID",
        )
        .await
    {
        Ok(stream) => stream.into_row().await.ok().flatten().and_then(|r| {
            Some(SessionIdentity {
                spid: r.get::<i16, _>(0)?,
                login_time: r.get::<&str, _>(1)?.to_string(),
            })
        }),
        Err(_) => None,
    };
    Ok((client, spid))
}

/// Run a console script: see the matching comment in
/// `pg_common::pg_execute_query`. The input is split on top-level
/// semicolons, each leading statement runs on the same `Client`, and the
/// exec path for the LAST statement is picked from its shape — so
/// `CREATE VIEW v ...; SELECT * FROM v;` returns rows from the SELECT.
/// The caller holds the lock for the `USE` and every statement so no
/// other task can switch the database in between.
async fn mssql_run_statements(
    c: &mut MssqlClient,
    database: &str,
    statements: &[String],
) -> Result<QueryResult> {
    let start = Instant::now();
    let Some((last, leading)) = statements.split_last() else {
        return Err(anyhow!("Query is empty"));
    };

    client_use_database(c, database).await?;

    for stmt in leading {
        if MssqlDriver::statement_is_select_like(stmt) || MssqlDriver::is_ddl(stmt) {
            // Even a leading SELECT could materialise a temp
            // table via SELECT INTO. Run it as a batch so any
            // side effects land, and discard the rowset.
            client_batch(c, stmt).await?;
        } else {
            client_exec(c, stmt).await?;
        }
    }

    if MssqlDriver::statement_is_select_like(last) {
        let (columns, rows) = client_select(c, last).await?;
        let elapsed = start.elapsed().as_millis() as u64;
        let n = rows.len() as u64;
        return Ok(QueryResult {
            columns,
            rows,
            rows_affected: n,
            execution_time_ms: elapsed,
            is_select: true,
        });
    }

    if MssqlDriver::is_ddl(last) {
        client_batch(c, last).await?;
        let elapsed = start.elapsed().as_millis() as u64;
        return Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            execution_time_ms: elapsed,
            is_select: false,
        });
    }

    let rows_affected = client_exec(c, last).await?;
    let elapsed = start.elapsed().as_millis() as u64;
    Ok(QueryResult {
        columns: vec![],
        rows: vec![],
        rows_affected,
        execution_time_ms: elapsed,
        is_select: false,
    })
}

/// Upper bound for reopening the connection (and killing the old
/// session) after a cancel. On timeout the next use reconnects.
const MSSQL_RECONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

impl MssqlDriver {
    /// Throw away the shared client after a cancel dropped it
    /// mid-protocol, open a fresh one and `KILL` the old session.
    async fn discard_session(&self) {
        let mut session = self.session.lock().await;
        let old_spid = session.spid.take();
        // Dropping the client closes the socket.
        drop(session.client.take());
        let reopen = async {
            let (mut client, spid) = open_client(&self.tcfg).await?;
            if let Some(old) = old_spid {
                // Fails harmlessly if the server already ended the session
                // or the login lacks ALTER ANY CONNECTION.
                let _ = client_batch(&mut client, &old.kill_sql()).await;
            }
            Ok::<_, anyhow::Error>((client, spid))
        };
        if let Ok(Ok((client, spid))) = tokio::time::timeout(MSSQL_RECONNECT_TIMEOUT, reopen).await
        {
            session.client = Some(client);
            session.spid = spid;
        }
    }

    pub async fn connect(config: &ConnectionConfig) -> Result<Self> {
        let mut tcfg = Config::new();
        tcfg.host(&config.host);
        tcfg.port(config.port);
        if config.database.trim().is_empty() {
            tcfg.database("master");
        } else {
            tcfg.database(&config.database);
        }
        tcfg.authentication(AuthMethod::sql_server(&config.user, &config.password));
        tcfg.application_name("Tablio");
        if config.ssl {
            tcfg.encryption(EncryptionLevel::Required);
            if config.trust_server_cert {
                tcfg.trust_cert();
            }
        } else {
            tcfg.encryption(EncryptionLevel::Off);
            tcfg.trust_cert();
        }
        let (client, spid) = open_client(&tcfg).await?;
        Ok(Self {
            session: Mutex::new(MssqlSession {
                client: Some(client),
                spid,
            }),
            tcfg,
        })
    }

    /// Lock the shared connection, reopening it first if a cancelled
    /// console run discarded it.
    async fn lock(&self) -> Result<MappedMutexGuard<'_, MssqlClient>> {
        let mut session = self.session.lock().await;
        if session.client.is_none() {
            let (client, spid) = open_client(&self.tcfg).await?;
            session.client = Some(client);
            session.spid = spid;
        }
        Ok(MutexGuard::map(session, |s| {
            s.client.as_mut().expect("client was just ensured")
        }))
    }

    async fn run_select(&self, sql: &str) -> Result<(Vec<String>, Vec<Vec<serde_json::Value>>)> {
        let mut c = self.lock().await?;
        client_select(&mut c, sql).await
    }

    async fn run_exec(&self, sql: &str) -> Result<u64> {
        let mut c = self.lock().await?;
        client_exec(&mut c, sql).await
    }

    async fn run_batch(&self, sql: &str) -> Result<()> {
        let mut c = self.lock().await?;
        client_batch(&mut c, sql).await
    }

    // The driver shares one connection, so a `USE` and the statements
    // it governs must run under a single lock acquisition; otherwise
    // another task can switch the database in between. The `*_in`
    // helpers do exactly that.

    async fn run_select_in(
        &self,
        database: &str,
        sql: &str,
    ) -> Result<(Vec<String>, Vec<Vec<serde_json::Value>>)> {
        let mut c = self.lock().await?;
        client_use_database(&mut c, database).await?;
        client_select(&mut c, sql).await
    }

    async fn run_batch_in(&self, database: &str, sql: &str) -> Result<()> {
        let mut c = self.lock().await?;
        client_use_database(&mut c, database).await?;
        client_batch(&mut c, sql).await
    }

    fn is_ddl(sql: &str) -> bool {
        let u = sql.trim().to_ascii_uppercase();
        u.starts_with("CREATE ")
            || u.starts_with("ALTER ")
            || u.starts_with("DROP ")
            || u.starts_with("TRUNCATE ")
            || u.starts_with("GRANT ")
            || u.starts_with("REVOKE ")
            || u.starts_with("DENY ")
    }

    #[cfg(test)]
    fn with_use_database(database: &str, sql: &str) -> String {
        if database.trim().is_empty() {
            return sql.to_string();
        }
        format!("USE {}; {}", bracket(database), sql)
    }

    /// `true` when the first non-comment keyword in `sql` indicates
    /// a rows-returning statement. Used to pick between
    /// `run_select` / `run_exec` / `run_batch` and shared with the
    /// multi-statement splitter so the LAST statement of a
    /// `CREATE VIEW ...; SELECT * FROM v;` script picks the
    /// SELECT path.
    fn statement_is_select_like(sql: &str) -> bool {
        statement_is_select_like(sql, &["SELECT", "WITH", "SHOWPLAN", "EXPLAIN"])
    }

    /// Back-compat alias retained for the existing unit tests in
    /// this file. Production callers go through
    /// `statement_is_select_like`.
    #[cfg(test)]
    fn is_select_like(sql: &str) -> bool {
        Self::statement_is_select_like(sql)
    }

    async fn primary_key_set(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<HashSet<String>> {
        let sql = format!(
            "SELECT c.name
             FROM {}.sys.indexes i
             INNER JOIN {}.sys.index_columns ic
               ON i.object_id = ic.object_id AND i.index_id = ic.index_id
             INNER JOIN {}.sys.columns c
               ON ic.object_id = c.object_id AND ic.column_id = c.column_id
             INNER JOIN {}.sys.tables t ON i.object_id = t.object_id
             INNER JOIN {}.sys.schemas s ON t.schema_id = s.schema_id
             WHERE i.is_primary_key = 1 AND t.name = N'{}' AND s.name = N'{}'
             ORDER BY ic.key_ordinal",
            bracket(database),
            bracket(database),
            bracket(database),
            bracket(database),
            bracket(database),
            table.replace('\'', "''"),
            schema.replace('\'', "''"),
        );
        let (_, rows) = self.run_select(&sql).await?;
        let mut set = HashSet::new();
        for r in rows {
            if let Some(serde_json::Value::String(name)) = r.first() {
                set.insert(name.clone());
            }
        }
        Ok(set)
    }
}

#[async_trait]
impl DatabaseDriver for MssqlDriver {
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        let sql = "SELECT name FROM sys.databases WHERE database_id > 4 ORDER BY name";
        let (_, rows) = self.run_select(sql).await?;
        let mut out = Vec::new();
        for r in rows {
            if let Some(serde_json::Value::String(name)) = r.first() {
                out.push(DatabaseInfo { name: name.clone() });
            }
        }
        Ok(out)
    }

    async fn list_schemas(&self, database: &str) -> Result<Vec<SchemaInfo>> {
        let sql = format!(
            "SELECT s.name FROM {}.sys.schemas s
             INNER JOIN {}.sys.database_principals p ON s.principal_id = p.principal_id
             WHERE p.type IN (N'S', N'U', N'G')
               AND s.name NOT IN (N'sys', N'INFORMATION_SCHEMA', N'guest')
             ORDER BY s.name",
            bracket(database),
            bracket(database),
        );
        let (_, rows) = self.run_select(&sql).await?;
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                r.into_iter().next().and_then(|v| match v {
                    serde_json::Value::String(s) => Some(SchemaInfo { name: s }),
                    _ => None,
                })
            })
            .collect())
    }

    async fn list_tables(&self, database: &str, schema: &str) -> Result<Vec<TableInfo>> {
        let sql = format!(
            "SELECT t.name, SUM(CASE WHEN p.index_id IN (0, 1) THEN p.rows ELSE 0 END) AS rc
             FROM {}.sys.tables t
             INNER JOIN {}.sys.schemas s ON t.schema_id = s.schema_id
             LEFT JOIN {}.sys.partitions p ON t.object_id = p.object_id
             WHERE s.name = N'{}'
             GROUP BY t.name
             ORDER BY t.name",
            bracket(database),
            bracket(database),
            bracket(database),
            schema.replace('\'', "''"),
        );
        let (_, rows) = self.run_select(&sql).await?;
        let mut tables = Vec::new();
        for r in rows {
            if r.len() >= 2 {
                let name = match &r[0] {
                    serde_json::Value::String(s) => s.clone(),
                    _ => continue,
                };
                let row_count = match &r[1] {
                    serde_json::Value::Number(n) => n.as_i64(),
                    serde_json::Value::String(s) => s.parse().ok(),
                    _ => None,
                };
                tables.push(TableInfo {
                    name,
                    schema: schema.to_string(),
                    table_type: "BASE TABLE".to_string(),
                    row_count_estimate: row_count,
                    ..Default::default()
                });
            }
        }
        Ok(tables)
    }

    async fn list_columns(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ColumnInfo>> {
        let pk = self.primary_key_set(database, schema, table).await?;
        let sql = format!(
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, ORDINAL_POSITION,
                    COLUMNPROPERTY(
                      OBJECT_ID(QUOTENAME(N'{}') + N'.' + QUOTENAME(TABLE_SCHEMA) + N'.' + QUOTENAME(TABLE_NAME)),
                      COLUMN_NAME,
                      N'IsIdentity'
                    ) AS is_identity
             FROM {}.INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_SCHEMA = N'{}' AND TABLE_NAME = N'{}'
             ORDER BY ORDINAL_POSITION",
            database.replace('\'', "''"),
            bracket(database),
            schema.replace('\'', "''"),
            table.replace('\'', "''"),
        );
        let (_, rows) = self.run_select(&sql).await?;
        let mut cols = Vec::new();
        for r in rows {
            if r.len() < 6 {
                continue;
            }
            let name = match &r[0] {
                serde_json::Value::String(s) => s.clone(),
                _ => continue,
            };
            let data_type = match &r[1] {
                serde_json::Value::String(s) => s.clone(),
                _ => String::new(),
            };
            let is_nullable =
                matches!(&r[2], serde_json::Value::String(s) if s.eq_ignore_ascii_case("YES"));
            let default_value = match &r[3] {
                serde_json::Value::String(s) => Some(s.clone()),
                serde_json::Value::Null => None,
                _ => None,
            };
            let ordinal = match &r[4] {
                serde_json::Value::Number(n) => n.as_i64().unwrap_or(0) as i32,
                _ => 0,
            };
            let is_auto = matches!(&r[5], serde_json::Value::Number(n) if n.as_i64() == Some(1));
            cols.push(ColumnInfo {
                name,
                data_type,
                is_nullable,
                is_primary_key: false,
                default_value,
                ordinal_position: ordinal,
                is_auto_generated: is_auto,
            });
        }
        for c in &mut cols {
            c.is_primary_key = pk.contains(&c.name);
        }
        Ok(cols)
    }

    async fn list_indexes(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<IndexInfo>> {
        let sql = format!(
            "SELECT i.name, i.is_unique, i.type_desc,
                    STRING_AGG(c.name, N',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS cols
             FROM {}.sys.tables t
             INNER JOIN {}.sys.schemas s ON t.schema_id = s.schema_id
             INNER JOIN {}.sys.indexes i ON t.object_id = i.object_id
             INNER JOIN {}.sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
             INNER JOIN {}.sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
             WHERE s.name = N'{}' AND t.name = N'{}' AND i.is_hypothetical = 0 AND i.index_id > 0
             GROUP BY i.index_id, i.name, i.is_unique, i.type_desc, i.is_primary_key
             HAVING MAX(CASE WHEN i.is_primary_key = 1 THEN 1 ELSE 0 END) = 0
             ORDER BY i.name",
            bracket(database),
            bracket(database),
            bracket(database),
            bracket(database),
            bracket(database),
            schema.replace('\'', "''"),
            table.replace('\'', "''"),
        );
        let result = self.run_select(&sql).await;
        let (rows, from_string_agg) = match &result {
            Ok((_, r)) if !r.is_empty() && r[0].len() == 4 => (r.clone(), true),
            _ => {
                let fallback = format!(
                    "SELECT i.name, i.is_unique, i.type_desc, c.name AS col, ic.key_ordinal
                     FROM {}.sys.tables t
                     INNER JOIN {}.sys.schemas s ON t.schema_id = s.schema_id
                     INNER JOIN {}.sys.indexes i ON t.object_id = i.object_id
                     INNER JOIN {}.sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                     INNER JOIN {}.sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                     WHERE s.name = N'{}' AND t.name = N'{}' AND i.is_hypothetical = 0 AND i.is_primary_key = 0
                     ORDER BY i.name, ic.key_ordinal",
                    bracket(database),
                    bracket(database),
                    bracket(database),
                    bracket(database),
                    bracket(database),
                    schema.replace('\'', "''"),
                    table.replace('\'', "''"),
                );
                let (_, fr) = self.run_select(&fallback).await?;
                (fr, false)
            }
        };

        if from_string_agg {
            return Ok(rows
                .into_iter()
                .filter_map(|r| {
                    let name = r.first()?.as_str()?.to_string();
                    let is_unique = matches!(r.get(1), Some(serde_json::Value::Bool(b)) if *b)
                        || matches!(r.get(1), Some(serde_json::Value::Number(n)) if n.as_i64() == Some(1));
                    let index_type = r
                        .get(2)
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let cols = r
                        .get(3)
                        .and_then(|v| v.as_str())
                        .map(|s| s.split(',').map(|x| x.to_string()).collect())
                        .unwrap_or_default();
                    Some(IndexInfo {
                        name,
                        columns: cols,
                        is_unique,
                        index_type,
                    })
                })
                .collect());
        }

        use std::collections::BTreeMap;
        let mut by_index: BTreeMap<String, (bool, String, Vec<String>)> = BTreeMap::new();
        for r in rows {
            if r.len() < 5 {
                continue;
            }
            let name = match r[0].as_str() {
                Some(s) => s.to_string(),
                None => continue,
            };
            let is_unique = matches!(r.get(1), Some(serde_json::Value::Bool(b)) if *b)
                || matches!(r.get(1), Some(serde_json::Value::Number(n)) if n.as_i64() == Some(1));
            let index_type = r.get(2).and_then(|v| v.as_str()).unwrap_or("").to_string();
            let col = r.get(3).and_then(|v| v.as_str()).unwrap_or("").to_string();
            let ord = r.get(4).and_then(|v| v.as_u64()).unwrap_or(0) as usize;
            let e =
                by_index
                    .entry(name.clone())
                    .or_insert((is_unique, index_type.clone(), Vec::new()));
            e.0 = is_unique;
            e.1 = index_type;
            while e.2.len() <= ord {
                e.2.push(String::new());
            }
            if ord < e.2.len() {
                e.2[ord] = col;
            }
        }
        Ok(by_index
            .into_iter()
            .map(|(name, (is_unique, index_type, mut cols))| {
                cols.retain(|s| !s.is_empty());
                IndexInfo {
                    name,
                    columns: cols,
                    is_unique,
                    index_type,
                }
            })
            .collect())
    }

    async fn list_foreign_keys(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        let sql = format!(
            "SELECT fk.name, cp.name AS col,
                    OBJECT_SCHEMA_NAME(fk.referenced_object_id, DB_ID(N'{}')) AS rschema,
                    OBJECT_NAME(fk.referenced_object_id, DB_ID(N'{}')) AS rtable,
                    cr.name AS rcol,
                    fk.delete_referential_action_desc,
                    fk.update_referential_action_desc
             FROM {}.sys.foreign_keys fk
             INNER JOIN {}.sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
             INNER JOIN {}.sys.tables pt ON fk.parent_object_id = pt.object_id
             INNER JOIN {}.sys.schemas ps ON pt.schema_id = ps.schema_id
             INNER JOIN {}.sys.columns cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
             INNER JOIN {}.sys.columns cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
             WHERE ps.name = N'{}' AND pt.name = N'{}'
             ORDER BY fk.name, fkc.constraint_column_id",
            database.replace('\'', "''"),
            database.replace('\'', "''"),
            bracket(database),
            bracket(database),
            bracket(database),
            bracket(database),
            bracket(database),
            bracket(database),
            schema.replace('\'', "''"),
            table.replace('\'', "''"),
        );
        let (_, rows) = self.run_select(&sql).await?;
        let mut out = Vec::new();
        for r in rows {
            if r.len() < 7 {
                continue;
            }
            let name = r[0].as_str().unwrap_or("").to_string();
            let column = r[1].as_str().unwrap_or("").to_string();
            let rschema = r[2].as_str().unwrap_or("dbo");
            let rtable = r[3].as_str().unwrap_or("").to_string();
            let rcol = r[4].as_str().unwrap_or("").to_string();
            let referenced_table = format!("{}.{}", rschema, rtable);
            let on_delete = r[5].as_str().unwrap_or("").to_string();
            let on_update = r[6].as_str().unwrap_or("").to_string();
            out.push(ForeignKeyInfo {
                name,
                column,
                referenced_table,
                referenced_column: rcol,
                on_delete,
                on_update,
            });
        }
        Ok(out)
    }

    async fn list_functions(&self, database: &str, schema: &str) -> Result<Vec<FunctionInfo>> {
        let sql = format!(
            "SELECT ROUTINE_NAME, ROUTINE_SCHEMA,
                    ISNULL(DATA_TYPE, N'') AS ret,
                    ISNULL(ROUTINE_TYPE, N'') AS kind
             FROM {}.INFORMATION_SCHEMA.ROUTINES
             WHERE ROUTINE_SCHEMA = N'{}'
             ORDER BY ROUTINE_NAME",
            bracket(database),
            schema.replace('\'', "''"),
        );
        let (_, rows) = self.run_select(&sql).await?;
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                if r.len() < 4 {
                    return None;
                }
                Some(FunctionInfo {
                    name: r[0].as_str()?.to_string(),
                    schema: r[1].as_str()?.to_string(),
                    return_type: r[2].as_str()?.to_string(),
                    language: "SQL".to_string(),
                    kind: r[3].as_str()?.to_string(),
                })
            })
            .collect())
    }

    async fn list_triggers(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<Vec<TriggerInfo>> {
        let sql = format!(
            "SELECT tr.name, OBJECT_NAME(tr.parent_id) AS table_name,
                    te.type_desc AS event_s, N'INSTEAD OF' AS timing
             FROM {}.sys.triggers tr
             CROSS APPLY (SELECT TOP 1 type_desc FROM {}.sys.trigger_events te WHERE te.object_id = tr.object_id) te
             INNER JOIN {}.sys.tables t ON tr.parent_id = t.object_id
             INNER JOIN {}.sys.schemas s ON t.schema_id = s.schema_id
             WHERE s.name = N'{}' AND t.name = N'{}' AND tr.parent_class = 1",
            bracket(database),
            bracket(database),
            bracket(database),
            bracket(database),
            schema.replace('\'', "''"),
            table.replace('\'', "''"),
        );
        let result = self.run_select(&sql).await;
        let (_, rows) = match result {
            Ok(r) => r,
            Err(_) => {
                let simple = format!(
                    "SELECT tr.name, t.name AS table_name, N'MODIFY' AS event_s, N'AFTER' AS timing
                     FROM {}.sys.triggers tr
                     INNER JOIN {}.sys.tables t ON tr.parent_id = t.object_id
                     INNER JOIN {}.sys.schemas s ON t.schema_id = s.schema_id
                     WHERE s.name = N'{}' AND t.name = N'{}' AND tr.parent_class = 1",
                    bracket(database),
                    bracket(database),
                    bracket(database),
                    schema.replace('\'', "''"),
                    table.replace('\'', "''"),
                );
                self.run_select(&simple).await.unwrap_or((vec![], vec![]))
            }
        };
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                if r.len() < 4 {
                    return None;
                }
                Some(TriggerInfo {
                    name: r[0].as_str()?.to_string(),
                    table_name: r[1].as_str()?.to_string(),
                    event: r[2].as_str()?.to_string(),
                    timing: r[3].as_str()?.to_string(),
                })
            })
            .collect())
    }

    async fn get_table_stats(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<TableStats> {
        let fq = three_part_table(database, schema, table);
        let count_sql = format!("SELECT COUNT_BIG(*) AS c FROM {}", fq);
        let (_, cr) = self.run_select(&count_sql).await?;
        let row_count = cr
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.as_i64())
            .unwrap_or(-1);

        let size_sql = format!(
            "SELECT
               SUM(a.total_pages) * 8 AS total_kb,
               SUM(CASE WHEN i.index_id <= 1 THEN a.used_pages * 8 ELSE 0 END) AS data_kb,
               SUM(CASE WHEN i.index_id > 1 THEN a.used_pages * 8 ELSE 0 END) AS index_kb
             FROM {}.sys.tables t
             INNER JOIN {}.sys.schemas s ON t.schema_id = s.schema_id
             INNER JOIN {}.sys.indexes i ON t.object_id = i.object_id
             INNER JOIN {}.sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
             INNER JOIN {}.sys.allocation_units a ON p.partition_id = a.container_id
             WHERE s.name = N'{}' AND t.name = N'{}'",
            bracket(database),
            bracket(database),
            bracket(database),
            bracket(database),
            bracket(database),
            schema.replace('\'', "''"),
            table.replace('\'', "''"),
        );
        let (_, sz) = self.run_select(&size_sql).await.unwrap_or((vec![], vec![]));
        let (total, data, index) = sz
            .first()
            .map(|r| {
                let t = r.first().and_then(|v| v.as_i64()).unwrap_or(0);
                let d = r.get(1).and_then(|v| v.as_i64()).unwrap_or(0);
                let i = r.get(2).and_then(|v| v.as_i64()).unwrap_or(0);
                (t, d, i)
            })
            .unwrap_or((0, 0, 0));

        Ok(TableStats {
            table_name: format!("{}.{}.{}", database, schema, table),
            row_count,
            total_size: format!("{} KB", total),
            data_size: format!("{} KB", data),
            index_size: format!("{} KB", index),
            last_vacuum: None,
            last_analyze: None,
            dead_tuples: None,
            live_tuples: None,
        })
    }

    async fn fetch_rows(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        offset: u64,
        limit: u64,
        sort: Vec<SortSpec>,
        filter: Option<String>,
    ) -> Result<TableData> {
        let columns = self.list_columns(database, schema, table).await?;
        if columns.is_empty() {
            anyhow::bail!("Table has no columns (or table not found)");
        }
        let fq = three_part_table(database, schema, table);
        let pk_cols: Vec<String> = columns
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| c.name.clone())
            .collect();
        // ORDER BY is mandatory for `OFFSET ... FETCH NEXT` on
        // SQL Server, so unlike pg/my we always end up with a
        // non-empty clause. Three-step ladder:
        //   1. multi-column user sort (issue #57)
        //   2. PK columns
        //   3. first column of the table (last-resort
        //      deterministic ordering)
        let order = if !sort.is_empty() {
            sort.iter()
                .map(|s| {
                    format!(
                        "{} {}",
                        bracket(&s.column),
                        match s.direction {
                            SortDirection::Asc => "ASC",
                            SortDirection::Desc => "DESC",
                        }
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        } else if pk_cols.is_empty() {
            // First column that can be sorted on; text / ntext / image /
            // xml / spatial columns cannot appear in ORDER BY.
            columns
                .iter()
                .find(|c| {
                    let t = c.data_type.to_ascii_lowercase();
                    !matches!(
                        t.as_str(),
                        "text" | "ntext" | "image" | "xml" | "geography" | "geometry"
                    )
                })
                .map(|c| bracket(&c.name))
                .unwrap_or_else(|| "(SELECT NULL)".to_string())
        } else {
            pk_cols
                .iter()
                .map(|c| bracket(c))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let mut where_clause = String::new();
        if let Some(f) = filter {
            let t = f.trim();
            if !t.is_empty() {
                if let Some(reason) = filter_unsafe_reason(t) {
                    anyhow::bail!("Filter rejected: {}", reason);
                }
                debug_assert!(!filter_is_unsafe(t));
                where_clause = format!(" WHERE {}", t);
            }
        }

        let count_sql = format!("SELECT COUNT_BIG(*) AS c FROM {}{}", fq, where_clause);
        let total_rows = self
            .run_select(&count_sql)
            .await?
            .1
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let data_sql = format!(
            "SELECT * FROM {}{} ORDER BY {} OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
            fq, where_clause, order, offset, limit
        );
        let (select_names, row_values) = self.run_select(&data_sql).await?;

        let columns_out = if select_names.is_empty() {
            columns.clone()
        } else {
            let ordered_columns: Vec<ColumnInfo> = select_names
                .iter()
                .filter_map(|name| columns.iter().find(|c| &c.name == name).cloned())
                .collect();
            if ordered_columns.len() == select_names.len() {
                ordered_columns
            } else {
                columns.clone()
            }
        };

        Ok(TableData {
            columns: columns_out,
            rows: row_values,
            total_rows,
            offset,
            limit,
        })
    }

    async fn execute_query(&self, database: &str, sql: &str) -> Result<QueryResult> {
        self.execute_query_cancellable(database, sql, CancellationToken::new())
            .await
    }

    /// tiberius cannot send a TDS attention (cancel) packet, so a cancel
    /// drops the in-flight future. That leaves the shared client
    /// mid-protocol, so it is discarded: closing the socket makes SQL
    /// Server abort the batch and roll back the session's transaction,
    /// and the old session is additionally `KILL`ed from the replacement
    /// connection so server-side work stops even if the server has not
    /// noticed the disconnect yet.
    async fn execute_query_cancellable(
        &self,
        database: &str,
        sql: &str,
        cancel: CancellationToken,
    ) -> Result<QueryResult> {
        let statements = split_sql_statements(sql);
        if statements.is_empty() {
            return Err(anyhow!("Query is empty"));
        }
        // Wait for the shared connection unless cancelled meanwhile.
        let mut c = unless_cancelled(self.lock(), &cancel).await?;
        let outcome =
            run_abortable(mssql_run_statements(&mut c, database, &statements), &cancel).await;
        drop(c);
        if cancel.is_cancelled() {
            if outcome.as_ref().err().is_some_and(is_cancelled_error) {
                self.discard_session().await;
            }
            return Err(cancelled_error());
        }
        outcome
    }

    async fn explain_query(&self, database: &str, sql: &str) -> Result<ExplainResult> {
        let start = Instant::now();
        // `SET SHOWPLAN_XML` must be the only statement in its batch,
        // so ON / query / OFF are three batches on the same connection,
        // under one lock, and OFF runs even when the query fails.
        let result = {
            let mut c = self.lock().await?;
            let planned = async {
                client_use_database(&mut c, database).await?;
                client_batch(&mut c, "SET SHOWPLAN_XML ON").await?;
                client_select(&mut c, sql).await
            }
            .await;
            let off = client_batch(&mut c, "SET SHOWPLAN_XML OFF").await;
            match (planned, off) {
                (Ok(r), Ok(())) => Ok(r),
                (Err(e), _) => Err(e),
                (Ok(_), Err(e)) => Err(e),
            }
        };
        let elapsed = start.elapsed().as_millis() as u64;
        match result {
            Ok((cols, rows)) => {
                let raw = rows
                    .first()
                    .and_then(|r| r.first())
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "(no plan output)".to_string());
                Ok(ExplainResult {
                    plan: ExplainNode {
                        node_type: "SQL Server SHOWPLAN_XML".to_string(),
                        relation: None,
                        startup_cost: 0.0,
                        total_cost: 0.0,
                        actual_time_ms: Some(elapsed as f64),
                        rows_estimated: 0,
                        rows_actual: None,
                        width: 0,
                        filter: cols.first().cloned(),
                        children: vec![],
                    },
                    raw_text: raw,
                    execution_time_ms: elapsed,
                })
            }
            Err(e) => Ok(ExplainResult {
                plan: ExplainNode {
                    node_type: "Explain".to_string(),
                    relation: None,
                    startup_cost: 0.0,
                    total_cost: 0.0,
                    actual_time_ms: Some(elapsed as f64),
                    rows_estimated: 0,
                    rows_actual: None,
                    width: 0,
                    filter: Some(sql.to_string()),
                    children: vec![],
                },
                raw_text: format!("Could not run SHOWPLAN_XML: {}", e),
                execution_time_ms: elapsed,
            }),
        }
    }

    async fn validate_query(&self, database: &str, sql: &str) -> Result<Option<ValidationError>> {
        if sql.trim().is_empty() {
            return Ok(Some(ValidationError {
                message: "Empty query".to_string(),
                position: None,
            }));
        }
        let batch = format!("SET PARSEONLY ON; {}; SET PARSEONLY OFF;", sql);
        let mut client = self.lock().await?;
        client_use_database(&mut client, database).await?;
        match client.execute(&*batch, &[]).await {
            Ok(_) => Ok(None),
            Err(e) => {
                let message = e.to_string();
                Ok(Some(ValidationError {
                    message,
                    position: None,
                }))
            }
        }
    }

    async fn get_ddl(
        &self,
        database: &str,
        schema: &str,
        object_name: &str,
        object_type: &str,
    ) -> Result<String> {
        let ot = object_type.to_uppercase();
        let full_obj = three_part_table(database, schema, object_name);
        let full_lit = full_obj.replace('\'', "''");
        let sql = match ot.as_str() {
            "VIEW" => {
                format!(
                    "SELECT OBJECT_DEFINITION(OBJECT_ID(N'{}')) AS ddl",
                    full_lit
                )
            }
            "TABLE" | "BASE TABLE" => {
                let cols = self.list_columns(database, schema, object_name).await?;
                let pk_cols: Vec<String> = cols
                    .iter()
                    .filter(|c| c.is_primary_key)
                    .map(|c| bracket(&c.name))
                    .collect();
                let col_defs: Vec<String> = cols
                    .iter()
                    .map(|c| {
                        let mut def = format!("    {} {}", bracket(&c.name), c.data_type);
                        if !c.is_nullable {
                            def.push_str(" NOT NULL");
                        }
                        if c.is_auto_generated {
                            def.push_str(" IDENTITY");
                        }
                        if let Some(d) = &c.default_value {
                            def.push_str(&format!(" DEFAULT {}", d));
                        }
                        def
                    })
                    .collect();
                let mut parts = col_defs;
                if !pk_cols.is_empty() {
                    parts.push(format!("    PRIMARY KEY ({})", pk_cols.join(", ")));
                }
                return Ok(format!(
                    "CREATE TABLE {}.{} (\n{}\n);",
                    bracket(schema),
                    bracket(object_name),
                    parts.join(",\n")
                ));
            }
            _ => anyhow::bail!("DDL for '{}' is not supported on SQL Server", object_type),
        };
        let (_, rows) = self.run_select(&sql).await?;
        let ddl = rows
            .first()
            .and_then(|r| r.first())
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow!("No DDL returned"))?;
        Ok(ddl)
    }

    async fn apply_changes(&self, changes: &DataChanges) -> Result<()> {
        let fq = three_part_table(&changes.database, &changes.schema, &changes.table);
        // Column types decide how string values are written (numeric
        // strings unquoted, datetime strings in a language-independent
        // form).
        let col_types: std::collections::HashMap<String, String> = self
            .list_columns(&changes.database, &changes.schema, &changes.table)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|c| (c.name, c.data_type))
            .collect();
        let lit = |col: &str, val: &serde_json::Value| -> String {
            typed_mssql_literal(col_types.get(col).map(String::as_str), val)
        };
        let mut stmts = Vec::new();
        stmts.push("SET XACT_ABORT ON; BEGIN TRAN;".to_string());
        for update in &changes.updates {
            if update.primary_key_values.is_empty() {
                anyhow::bail!("Cannot update row: no primary key values provided");
            }
            let set_clause = format!(
                "{} = {}",
                bracket(&update.column_name),
                lit(&update.column_name, &update.new_value)
            );
            let where_clause: Vec<String> = update
                .primary_key_values
                .iter()
                .map(|(col, val)| format!("{} = {}", bracket(col), lit(col, val)))
                .collect();
            stmts.push(format!(
                "UPDATE {} SET {} WHERE {};",
                fq,
                set_clause,
                where_clause.join(" AND ")
            ));
            // XACT_ABORT makes THROW roll back the whole batch.
            stmts.push(no_row_matched_guard("update", &update.primary_key_values));
        }
        for insert in &changes.inserts {
            let cols: Vec<String> = insert.values.iter().map(|(n, _)| bracket(n)).collect();
            let vals: Vec<String> = insert.values.iter().map(|(c, v)| lit(c, v)).collect();
            stmts.push(format!(
                "INSERT INTO {} ({}) VALUES ({});",
                fq,
                cols.join(", "),
                vals.join(", ")
            ));
        }
        for delete in &changes.deletes {
            if delete.primary_key_values.is_empty() {
                anyhow::bail!("Cannot delete row: no primary key values provided");
            }
            let where_clause: Vec<String> = delete
                .primary_key_values
                .iter()
                .map(|(col, val)| format!("{} = {}", bracket(col), lit(col, val)))
                .collect();
            stmts.push(format!(
                "DELETE FROM {} WHERE {};",
                fq,
                where_clause.join(" AND ")
            ));
            stmts.push(no_row_matched_guard("delete", &delete.primary_key_values));
        }
        stmts.push("COMMIT TRAN;".to_string());

        let batch = stmts.join("\n");
        self.run_exec(&batch).await?;
        Ok(())
    }

    async fn create_table(
        &self,
        database: &str,
        schema: &str,
        table_name: &str,
        columns: &[ColumnDefinition],
    ) -> Result<()> {
        let sql = create_table_sql(schema, table_name, columns)?;
        self.run_batch_in(database, &sql).await?;
        Ok(())
    }

    async fn alter_table(
        &self,
        database: &str,
        schema: &str,
        table_name: &str,
        operations: &[AlterTableOperation],
    ) -> Result<()> {
        // Track the live table name across the loop so ops queued
        // after `RenameTable` target the renamed object (matches
        // pg_alter_table / my_alter_table / sqlite alter_table).
        // Previously `fq` was computed once and went stale after a
        // rename, which silently broke editor saves that paired
        // rename_table with column alterations.
        let mut current_table = table_name.to_string();
        let mut current_name_for_rename = table_name.to_string();
        for op in operations {
            let fq = format!("{}.{}", bracket(schema), bracket(&current_table));
            let stmt = match op {
                AlterTableOperation::AddColumn { column } => {
                    format!("ALTER TABLE {} ADD {}", fq, column_definition_sql(column)?)
                }
                AlterTableOperation::DropColumn { column_name } => {
                    format!("ALTER TABLE {} DROP COLUMN {}", fq, bracket(column_name))
                }
                AlterTableOperation::RenameColumn { old_name, new_name } => {
                    let current = format!(
                        "{}.{}.{}",
                        schema.replace('\'', "''"),
                        current_table.replace('\'', "''"),
                        old_name.replace('\'', "''")
                    );
                    format!(
                        "EXEC sp_rename N'{}', N'{}', N'COLUMN'",
                        current,
                        new_name.replace('\'', "''")
                    )
                }
                AlterTableOperation::ChangeColumnType {
                    column_name,
                    new_type,
                } => format!(
                    "ALTER TABLE {} ALTER COLUMN {} {}",
                    fq,
                    bracket(column_name),
                    new_type
                ),
                AlterTableOperation::RenameTable { new_name } => {
                    let current = format!(
                        "{}.{}",
                        schema.replace('\'', "''"),
                        current_name_for_rename.replace('\'', "''")
                    );
                    let stmt = format!(
                        "EXEC sp_rename N'{}', N'{}'",
                        current,
                        new_name.replace('\'', "''")
                    );
                    // Advance the live table name so subsequent ops in
                    // this same call qualify against the renamed object.
                    current_table = new_name.clone();
                    current_name_for_rename = new_name.clone();
                    stmt
                }
                AlterTableOperation::SetNullable {
                    column_name,
                    nullable,
                } => {
                    let cols = self.list_columns(database, schema, &current_table).await?;
                    let col = cols
                        .iter()
                        .find(|c| c.name == *column_name)
                        .ok_or_else(|| anyhow!("Column {} not found", column_name))?;
                    let null_str = if *nullable { "NULL" } else { "NOT NULL" };
                    format!(
                        "ALTER TABLE {} ALTER COLUMN {} {} {}",
                        fq,
                        bracket(column_name),
                        col.data_type,
                        null_str
                    )
                }
                AlterTableOperation::SetDefault {
                    column_name,
                    default_value,
                } => match default_value {
                    Some(d) if !d.is_empty() => {
                        format!(
                            "ALTER TABLE {} ADD DEFAULT {} FOR {}",
                            fq,
                            d,
                            bracket(column_name)
                        )
                    }
                    _ => {
                        let find_constraint = format!(
                                "SELECT dc.name FROM sys.default_constraints dc \
                                 INNER JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id \
                                 INNER JOIN sys.tables t ON c.object_id = t.object_id \
                                 INNER JOIN sys.schemas s ON t.schema_id = s.schema_id \
                                 WHERE s.name = N'{}' AND t.name = N'{}' AND c.name = N'{}'",
                                schema.replace('\'', "''"), current_table.replace('\'', "''"), column_name.replace('\'', "''")
                            );
                        let (_, rows) = self.run_select_in(database, &find_constraint).await?;
                        if let Some(constraint_name) = rows
                            .first()
                            .and_then(|r| r.first())
                            .and_then(|v| v.as_str())
                        {
                            format!(
                                "ALTER TABLE {} DROP CONSTRAINT {}",
                                fq,
                                bracket(constraint_name)
                            )
                        } else {
                            continue;
                        }
                    }
                },
            };
            self.run_batch_in(database, &stmt).await?;
        }
        Ok(())
    }

    async fn drop_object(
        &self,
        database: &str,
        schema: &str,
        object_name: &str,
        object_type: &str,
    ) -> Result<()> {
        let fq = format!("{}.{}", bracket(schema), bracket(object_name));
        let sql = match object_type.to_uppercase().as_str() {
            "TABLE" | "BASE TABLE" => format!("DROP TABLE {}", fq),
            "VIEW" => format!("DROP VIEW {}", fq),
            "INDEX" => {
                let q = format!(
                    "SELECT t.name FROM sys.indexes i
                     INNER JOIN sys.tables t ON i.object_id = t.object_id
                     INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                     WHERE i.name = N'{}' AND s.name = N'{}'",
                    object_name.replace('\'', "''"),
                    schema.replace('\'', "''"),
                );
                let (_, rows) = self.run_select_in(database, &q).await?;
                let tbl = rows
                    .first()
                    .and_then(|r| r.first())
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        anyhow!("Index '{}' not found in schema '{}'", object_name, schema)
                    })?;
                format!(
                    "DROP INDEX {} ON {}.{}",
                    bracket(object_name),
                    bracket(schema),
                    bracket(tbl),
                )
            }
            _ => anyhow::bail!("Unsupported drop type {}", object_type),
        };
        self.run_batch_in(database, &sql).await?;
        Ok(())
    }

    async fn truncate_table(&self, database: &str, schema: &str, table_name: &str) -> Result<()> {
        let sql = format!("TRUNCATE TABLE {}.{}", bracket(schema), bracket(table_name));
        self.run_batch_in(database, &sql).await?;
        Ok(())
    }

    async fn import_data(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        columns: &[String],
        rows: &[Vec<serde_json::Value>],
    ) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }
        let fq = format!("{}.{}", bracket(schema), bracket(table));
        let cols = columns
            .iter()
            .map(|c| bracket(c))
            .collect::<Vec<_>>()
            .join(", ");
        const BATCH_SIZE: usize = 100;
        let sqls: Vec<String> = rows
            .chunks(BATCH_SIZE)
            .map(|chunk| {
                let values_list: Vec<String> = chunk
                    .iter()
                    .map(|row| {
                        let vals = row
                            .iter()
                            .map(json_to_mssql_literal)
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("({})", vals)
                    })
                    .collect();
                format!(
                    "INSERT INTO {} ({}) VALUES {}",
                    fq,
                    cols,
                    values_list.join(", ")
                )
            })
            .collect();

        // All-or-nothing like the Postgres / MySQL drivers: one
        // transaction on the (shared) connection, held under one lock.
        let mut c = self.lock().await?;
        client_use_database(&mut c, database).await?;
        client_batch(&mut c, "BEGIN TRANSACTION").await?;
        let inserted = async {
            let mut n = 0u64;
            for sql in &sqls {
                n += client_exec(&mut c, sql).await?;
            }
            Ok::<u64, anyhow::Error>(n)
        }
        .await;
        match inserted {
            Ok(n) => {
                client_batch(&mut c, "COMMIT TRANSACTION").await?;
                Ok(n)
            }
            Err(e) => {
                let _ = client_batch(&mut c, "IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION").await;
                Err(e)
            }
        }
    }

    async fn get_server_activity(&self) -> Result<Vec<ServerActivity>> {
        let sql = "SELECT CAST(r.session_id AS varchar(20)), s.login_name,
                          DB_NAME(r.database_id), r.status, t.text,
                          CAST(r.total_elapsed_time AS float) / 1000.0,
                          CONVERT(varchar(48), c.client_net_address)
                   FROM sys.dm_exec_requests r
                   INNER JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
                   CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
                   LEFT JOIN sys.dm_exec_connections c ON r.session_id = c.session_id
                   WHERE r.session_id <> @@SPID AND r.sql_handle IS NOT NULL";
        let (_, rows) = self.run_select(sql).await.unwrap_or((vec![], vec![]));
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                if r.len() < 7 {
                    return None;
                }
                Some(ServerActivity {
                    pid: r[0].as_str()?.to_string(),
                    user: r[1].as_str()?.to_string(),
                    database: r[2].as_str().unwrap_or("").to_string(),
                    state: r[3].as_str().unwrap_or("").to_string(),
                    query: r[4].as_str().unwrap_or("").to_string(),
                    duration_ms: r[5].as_f64(),
                    client_addr: r[6].as_str().unwrap_or("").to_string(),
                })
            })
            .collect())
    }

    async fn get_database_stats(&self) -> Result<DatabaseStats> {
        Ok(DatabaseStats {
            active_connections: 0,
            idle_connections: 0,
            idle_in_transaction: 0,
            total_connections: 0,
            xact_commit: 0,
            xact_rollback: 0,
            tup_inserted: 0,
            tup_updated: 0,
            tup_deleted: 0,
            tup_fetched: 0,
            blks_read: 0,
            blks_hit: 0,
            timestamp_ms: 0.0,
        })
    }

    async fn get_locks(&self) -> Result<Vec<LockInfo>> {
        Ok(vec![])
    }

    async fn get_server_config(&self) -> Result<Vec<ServerConfigEntry>> {
        Ok(vec![])
    }

    async fn get_query_stats(&self) -> Result<QueryStatsResponse> {
        // `sys.dm_exec_query_stats` is always-on; the only realistic failure
        // mode is the connecting login lacking `VIEW SERVER STATE`. A small
        // probe lets us classify that into a structured `kind` rather than
        // bubbling a raw error to the UI.
        if let Err(e) = self
            .run_select("SELECT TOP 1 1 AS probe FROM sys.dm_exec_query_stats")
            .await
        {
            let msg = e.to_string();
            let lower = msg.to_ascii_lowercase();
            if lower.contains("view server state") || lower.contains("permission") {
                return Ok(QueryStatsResponse {
                    available: false,
                    kind: QueryStatsKind::MssqlMissingViewServerState,
                    message: Some(
                        "The connecting login does not have VIEW SERVER STATE.\n\n\
                         Ask a sysadmin to run:\n\
                         \u{2003}GRANT VIEW SERVER STATE TO [<your_login>];\n\n\
                         Without it, sys.dm_exec_query_stats is not readable."
                            .to_string(),
                    ),
                    entries: vec![],
                });
            }
            return Err(e);
        }

        // `*_worker_time` columns are microseconds; divide by 1000.0 (force
        // float division) to land in milliseconds. `query_hash` is binary(8)
        // and casts cleanly to BIGINT for an i64 `queryid`.
        let sql = "SELECT TOP 200 \
                SUBSTRING(t.text, (qs.statement_start_offset/2)+1, \
                  ((CASE qs.statement_end_offset \
                       WHEN -1 THEN DATALENGTH(t.text) \
                       ELSE qs.statement_end_offset \
                    END - qs.statement_start_offset)/2) + 1) AS query, \
                CAST(qs.query_hash AS BIGINT)                AS queryid, \
                qs.execution_count                           AS calls, \
                (qs.total_worker_time / 1000.0)              AS total_exec_time_ms, \
                ((qs.total_worker_time * 1.0 / NULLIF(qs.execution_count, 0)) / 1000.0) \
                                                             AS mean_exec_time_ms, \
                (qs.min_worker_time / 1000.0)                AS min_exec_time_ms, \
                (qs.max_worker_time / 1000.0)                AS max_exec_time_ms, \
                qs.total_rows                                AS rows_count, \
                qs.total_logical_reads                       AS shared_blks_hit, \
                qs.total_physical_reads                      AS shared_blks_read, \
                CASE WHEN (qs.total_logical_reads + qs.total_physical_reads) > 0 \
                     THEN qs.total_logical_reads * 100.0 / \
                          (qs.total_logical_reads + qs.total_physical_reads) \
                     ELSE 0 END                              AS cache_hit_ratio \
            FROM sys.dm_exec_query_stats qs \
            CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) t \
            ORDER BY qs.total_worker_time DESC";

        let (cols, rows) = self.run_select(sql).await?;

        // Build a name -> index map so the row-decode loop is column-order
        // agnostic and survives reordering of the SELECT list.
        let idx = |name: &str| cols.iter().position(|c| c.eq_ignore_ascii_case(name));
        let i_query = idx("query");
        let i_queryid = idx("queryid");
        let i_calls = idx("calls");
        let i_total = idx("total_exec_time_ms");
        let i_mean = idx("mean_exec_time_ms");
        let i_min = idx("min_exec_time_ms");
        let i_max = idx("max_exec_time_ms");
        let i_rows = idx("rows_count");
        let i_hit = idx("shared_blks_hit");
        let i_read = idx("shared_blks_read");
        let i_ratio = idx("cache_hit_ratio");

        let entries = rows
            .into_iter()
            .map(|row| QueryStatEntry {
                query: i_query
                    .and_then(|i| row.get(i))
                    .and_then(|v| v.as_str().map(String::from))
                    .unwrap_or_default(),
                queryid: i_queryid.and_then(|i| row.get(i)).and_then(json_as_i64),
                user: String::new(),
                calls: i_calls
                    .and_then(|i| row.get(i))
                    .and_then(json_as_i64)
                    .unwrap_or(0),
                total_exec_time_ms: i_total
                    .and_then(|i| row.get(i))
                    .and_then(json_as_f64)
                    .unwrap_or(0.0),
                mean_exec_time_ms: i_mean
                    .and_then(|i| row.get(i))
                    .and_then(json_as_f64)
                    .unwrap_or(0.0),
                min_exec_time_ms: i_min
                    .and_then(|i| row.get(i))
                    .and_then(json_as_f64)
                    .unwrap_or(0.0),
                max_exec_time_ms: i_max
                    .and_then(|i| row.get(i))
                    .and_then(json_as_f64)
                    .unwrap_or(0.0),
                rows: i_rows
                    .and_then(|i| row.get(i))
                    .and_then(json_as_i64)
                    .unwrap_or(0),
                shared_blks_hit: i_hit
                    .and_then(|i| row.get(i))
                    .and_then(json_as_i64)
                    .unwrap_or(0),
                shared_blks_read: i_read
                    .and_then(|i| row.get(i))
                    .and_then(json_as_i64)
                    .unwrap_or(0),
                cache_hit_ratio: i_ratio
                    .and_then(|i| row.get(i))
                    .and_then(json_as_f64)
                    .unwrap_or(0.0),
                total_plan_time_ms: None,
                mean_plan_time_ms: None,
            })
            .collect();

        Ok(QueryStatsResponse {
            available: true,
            kind: QueryStatsKind::Available,
            message: None,
            entries,
        })
    }

    async fn cancel_query(&self, pid: &str) -> Result<()> {
        let id: i32 = pid.parse().map_err(|_| anyhow!("Invalid session id"))?;
        let sql = format!("KILL {}", id);
        self.run_batch(&sql).await?;
        Ok(())
    }

    async fn list_roles(&self) -> Result<Vec<RoleInfo>> {
        let sql = "SELECT name FROM sys.server_principals WHERE type IN ('S','U') ORDER BY name";
        let (_, rows) = self.run_select(sql).await.unwrap_or((vec![], vec![]));
        Ok(rows
            .into_iter()
            .filter_map(|r| {
                let name = r.first()?.as_str()?.to_string();
                Some(RoleInfo {
                    name,
                    is_superuser: false,
                    can_login: true,
                    can_create_db: false,
                    can_create_role: false,
                    is_replication: false,
                    connection_limit: -1,
                    valid_until: None,
                    member_of: vec![],
                })
            })
            .collect())
    }

    async fn create_role(&self, _req: &CreateRoleRequest) -> Result<()> {
        Err(anyhow!(
            "Creating SQL Server logins from Tablio is not supported yet"
        ))
    }

    async fn drop_role(&self, _name: &str) -> Result<()> {
        Err(anyhow!(
            "Dropping SQL Server logins from Tablio is not supported yet"
        ))
    }

    async fn alter_role(&self, _req: &AlterRoleRequest) -> Result<()> {
        Err(anyhow!(
            "Altering SQL Server logins from Tablio is not supported yet"
        ))
    }

    async fn test_connection(&self) -> Result<bool> {
        self.run_select("SELECT 1").await?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn coldef(
        name: &str,
        ty: &str,
        nullable: bool,
        pk: bool,
        default: Option<&str>,
    ) -> ColumnDefinition {
        ColumnDefinition {
            name: name.into(),
            data_type: ty.into(),
            is_nullable: nullable,
            is_primary_key: pk,
            default_value: default.map(str::to_string),
        }
    }

    #[test]
    fn create_table_sql_identity_pk_and_default() {
        let sql = create_table_sql(
            "dbo",
            "t",
            &[
                coldef("id", "INT IDENTITY(1,1)", false, true, None),
                coldef("status", "NVARCHAR(20)", true, false, Some("'new'")),
            ],
        )
        .unwrap();
        assert_eq!(
            sql,
            "CREATE TABLE [dbo].[t] ([id] INT IDENTITY(1,1) NOT NULL, [status] NVARCHAR(20) DEFAULT 'new' NULL, PRIMARY KEY ([id]))"
        );
    }

    #[test]
    fn create_table_sql_rejects_unsafe_type_and_default() {
        assert!(create_table_sql(
            "dbo",
            "t",
            &[coldef("a", "INT; DROP TABLE x", true, false, None)]
        )
        .is_err());
        assert!(create_table_sql(
            "dbo",
            "t",
            &[coldef("a", "INT", true, false, Some("1; DROP TABLE x"))]
        )
        .is_err());
        assert!(create_table_sql("dbo", "t", &[coldef("a", "  ", true, false, None)]).is_err());
        assert!(create_table_sql("dbo", "t", &[]).is_err());
    }

    #[test]
    fn column_definition_sql_ignores_blank_default() {
        assert_eq!(
            column_definition_sql(&coldef("n", "INT", false, false, Some(" "))).unwrap(),
            "[n] INT NOT NULL"
        );
        assert_eq!(
            column_definition_sql(&coldef("n", "INT", true, false, Some("0"))).unwrap(),
            "[n] INT DEFAULT 0 NULL"
        );
    }

    #[test]
    fn bracket_plain_identifier() {
        assert_eq!(bracket("users"), "[users]");
    }

    #[test]
    fn bracket_identifier_with_closing_bracket() {
        assert_eq!(bracket("ta]ble"), "[ta]]ble]");
    }

    #[test]
    fn bracket_empty_identifier() {
        assert_eq!(bracket(""), "[]");
    }

    #[test]
    fn three_part_table_basic() {
        assert_eq!(
            three_part_table("mydb", "dbo", "orders"),
            "[mydb].[dbo].[orders]"
        );
    }

    #[test]
    fn three_part_table_special_chars() {
        assert_eq!(
            three_part_table("my]db", "d]bo", "or]ders"),
            "[my]]db].[d]]bo].[or]]ders]"
        );
    }

    #[test]
    fn json_to_mssql_literal_null() {
        assert_eq!(json_to_mssql_literal(&serde_json::Value::Null), "NULL");
    }

    #[test]
    fn json_to_mssql_literal_bool() {
        assert_eq!(json_to_mssql_literal(&serde_json::json!(true)), "1");
        assert_eq!(json_to_mssql_literal(&serde_json::json!(false)), "0");
    }

    #[test]
    fn json_to_mssql_literal_number() {
        assert_eq!(json_to_mssql_literal(&serde_json::json!(42)), "42");
        assert_eq!(json_to_mssql_literal(&serde_json::json!(7.25)), "7.25");
    }

    #[test]
    fn json_to_mssql_literal_string() {
        assert_eq!(
            json_to_mssql_literal(&serde_json::json!("hello")),
            "N'hello'"
        );
    }

    #[test]
    fn json_to_mssql_literal_string_with_quotes() {
        assert_eq!(
            json_to_mssql_literal(&serde_json::json!("it's")),
            "N'it''s'"
        );
    }

    #[test]
    fn json_to_mssql_literal_array() {
        let val = serde_json::json!([1, 2, 3]);
        let lit = json_to_mssql_literal(&val);
        assert!(lit.starts_with("N'"));
        assert!(lit.ends_with('\''));
        assert!(lit.contains("[1,2,3]"));
    }

    #[test]
    fn json_to_mssql_literal_object() {
        let val = serde_json::json!({"key": "value"});
        let lit = json_to_mssql_literal(&val);
        assert!(lit.starts_with("N'"));
        assert!(lit.contains("key"));
    }

    #[test]
    fn column_data_to_json_u8() {
        assert_eq!(
            column_data_to_json(&ColumnData::U8(Some(42))),
            serde_json::json!(42)
        );
        assert_eq!(
            column_data_to_json(&ColumnData::U8(None)),
            serde_json::Value::Null
        );
    }

    #[test]
    fn column_data_to_json_i16() {
        assert_eq!(
            column_data_to_json(&ColumnData::I16(Some(-1))),
            serde_json::json!(-1)
        );
        assert_eq!(
            column_data_to_json(&ColumnData::I16(None)),
            serde_json::Value::Null
        );
    }

    #[test]
    fn column_data_to_json_i32() {
        assert_eq!(
            column_data_to_json(&ColumnData::I32(Some(100))),
            serde_json::json!(100)
        );
        assert_eq!(
            column_data_to_json(&ColumnData::I32(None)),
            serde_json::Value::Null
        );
    }

    #[test]
    fn column_data_to_json_i64() {
        assert_eq!(
            column_data_to_json(&ColumnData::I64(Some(999999999999i64))),
            serde_json::json!(999999999999i64)
        );
    }

    #[test]
    fn column_data_to_json_f32() {
        let val = column_data_to_json(&ColumnData::F32(Some(1.5)));
        assert!(val.as_f64().unwrap() - 1.5 < 0.01);
    }

    #[test]
    fn column_data_to_json_f64() {
        let val = column_data_to_json(&ColumnData::F64(Some(7.25)));
        assert!((val.as_f64().unwrap() - 7.25).abs() < 0.001);
    }

    #[test]
    fn column_data_to_json_bit() {
        assert_eq!(
            column_data_to_json(&ColumnData::Bit(Some(true))),
            serde_json::json!(true)
        );
        assert_eq!(
            column_data_to_json(&ColumnData::Bit(Some(false))),
            serde_json::json!(false)
        );
        assert_eq!(
            column_data_to_json(&ColumnData::Bit(None)),
            serde_json::Value::Null
        );
    }

    #[test]
    fn column_data_to_json_string() {
        assert_eq!(
            column_data_to_json(&ColumnData::String(Some("test".into()))),
            serde_json::json!("test")
        );
        assert_eq!(
            column_data_to_json(&ColumnData::String(None)),
            serde_json::Value::Null
        );
    }

    #[test]
    fn column_data_to_json_guid() {
        let u = uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
        let val = column_data_to_json(&ColumnData::Guid(Some(u)));
        assert_eq!(
            val,
            serde_json::json!("550e8400-e29b-41d4-a716-446655440000")
        );
        assert_eq!(
            column_data_to_json(&ColumnData::Guid(None)),
            serde_json::Value::Null
        );
    }

    #[test]
    fn column_data_to_json_binary() {
        let data = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let val = column_data_to_json(&ColumnData::Binary(Some(data.into())));
        assert_eq!(val, serde_json::json!("0xdeadbeef"));
        assert_eq!(
            column_data_to_json(&ColumnData::Binary(None)),
            serde_json::Value::Null
        );
    }

    #[test]
    fn column_data_to_json_xml_none() {
        assert_eq!(
            column_data_to_json(&ColumnData::Xml(None)),
            serde_json::Value::Null
        );
    }

    #[test]
    fn column_data_to_json_datetime_none() {
        assert_eq!(
            column_data_to_json(&ColumnData::DateTime(None)),
            serde_json::Value::Null
        );
    }

    #[test]
    fn column_data_to_json_smalldatetime_none() {
        assert_eq!(
            column_data_to_json(&ColumnData::SmallDateTime(None)),
            serde_json::Value::Null
        );
    }

    #[test]
    fn column_data_to_json_numeric_none() {
        assert_eq!(
            column_data_to_json(&ColumnData::Numeric(None)),
            serde_json::Value::Null
        );
    }

    #[test]
    fn rows_to_grid_empty() {
        let (cols, rows) = rows_to_grid(vec![]);
        assert!(cols.is_empty());
        assert!(rows.is_empty());
    }

    #[test]
    fn is_select_like_variants() {
        assert!(MssqlDriver::is_select_like("SELECT 1"));
        assert!(MssqlDriver::is_select_like("  select * from t"));
        assert!(MssqlDriver::is_select_like(
            "WITH cte AS (SELECT 1) SELECT * FROM cte"
        ));
        assert!(!MssqlDriver::is_select_like("INSERT INTO t VALUES (1)"));
        assert!(!MssqlDriver::is_select_like("UPDATE t SET a = 1"));
        assert!(!MssqlDriver::is_select_like("DELETE FROM t"));
        assert!(!MssqlDriver::is_select_like("CREATE TABLE t (id INT)"));
    }

    #[test]
    fn with_use_database_non_empty() {
        let sql = MssqlDriver::with_use_database("mydb", "SELECT 1");
        assert!(sql.starts_with("USE [mydb]; "));
        assert!(sql.contains("SELECT 1"));
    }

    #[test]
    fn with_use_database_empty() {
        let sql = MssqlDriver::with_use_database("", "SELECT 1");
        assert_eq!(sql, "SELECT 1");
    }

    #[test]
    fn with_use_database_whitespace_only() {
        let sql = MssqlDriver::with_use_database("  ", "SELECT 1");
        assert_eq!(sql, "SELECT 1");
    }

    // -----------------------------------------------------------------------
    // Regression tests for fixes applied to prevent future issues
    // -----------------------------------------------------------------------

    #[test]
    fn is_ddl_create() {
        assert!(MssqlDriver::is_ddl("CREATE TABLE t (id INT)"));
        assert!(MssqlDriver::is_ddl("CREATE VIEW v AS SELECT 1"));
        assert!(MssqlDriver::is_ddl("  CREATE INDEX ix ON t(c)"));
    }

    #[test]
    fn is_ddl_alter() {
        assert!(MssqlDriver::is_ddl("ALTER TABLE t ADD col INT"));
    }

    #[test]
    fn is_ddl_drop() {
        assert!(MssqlDriver::is_ddl("DROP TABLE t"));
        assert!(MssqlDriver::is_ddl("DROP VIEW v"));
    }

    #[test]
    fn is_ddl_truncate() {
        assert!(MssqlDriver::is_ddl("TRUNCATE TABLE t"));
    }

    #[test]
    fn is_ddl_grant_revoke_deny() {
        assert!(MssqlDriver::is_ddl("GRANT SELECT ON t TO u"));
        assert!(MssqlDriver::is_ddl("REVOKE SELECT ON t FROM u"));
        assert!(MssqlDriver::is_ddl("DENY SELECT ON t TO u"));
    }

    #[test]
    fn is_ddl_false_for_dml() {
        assert!(!MssqlDriver::is_ddl("SELECT * FROM t"));
        assert!(!MssqlDriver::is_ddl("INSERT INTO t VALUES (1)"));
        assert!(!MssqlDriver::is_ddl("UPDATE t SET a = 1"));
        assert!(!MssqlDriver::is_ddl("DELETE FROM t"));
    }

    #[test]
    fn json_to_mssql_literal_string_with_backslash() {
        assert_eq!(
            json_to_mssql_literal(&serde_json::json!("path\\to\\file")),
            "N'path\\to\\file'"
        );
    }

    #[test]
    fn json_to_mssql_literal_injection_attempt() {
        let val = serde_json::json!("'; DROP TABLE users; --");
        let lit = json_to_mssql_literal(&val);
        assert_eq!(lit, "N'''; DROP TABLE users; --'");
        assert!(lit.starts_with("N'"));
        assert!(lit.ends_with('\''));
    }

    #[test]
    fn bracket_prevents_injection() {
        let evil = "]; DROP TABLE users; --";
        let quoted = bracket(evil);
        assert_eq!(quoted, "[]]; DROP TABLE users; --]");
        assert!(quoted.starts_with('['));
        assert!(quoted.ends_with(']'));
    }

    #[test]
    fn bracket_double_close_bracket() {
        assert_eq!(bracket("a]]b"), "[a]]]]b]");
    }

    #[test]
    fn filter_validation_rejects_semicolon() {
        let f = "id = 1; DROP TABLE users";
        assert!(f.contains(';'));
    }

    #[test]
    fn filter_validation_rejects_comments() {
        let f1 = "id = 1 -- comment";
        let f2 = "id = 1 /* evil */";
        assert!(f1.contains("--"));
        assert!(f2.contains("/*"));
    }

    #[test]
    fn three_part_table_with_injection_attempt() {
        let result = three_part_table("db]; DROP TABLE x; --", "dbo", "t");
        assert_eq!(result, "[db]]; DROP TABLE x; --].[dbo].[t]");
    }
}
