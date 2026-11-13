use anyhow::{anyhow, Result};
use async_trait::async_trait;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::response::query_result::QueryRowsResult;
use scylla::response::{PagingState, PagingStateResponse};
use std::time::Instant;

use crate::db::DatabaseDriver;
use crate::models::*;
use crate::util::sql::i64_to_json;

pub struct CassandraDriver {
    session: Session,
}

/// Mirrors `pg_common::filter_is_unsafe`. Cassandra/CQL has its own
/// expression grammar but the threat model (typo or confused-deputy
/// statement injection from the local UI) is identical, so we apply
/// the same heuristic across drivers for consistency.
fn filter_is_unsafe(filter: &str) -> bool {
    filter_unsafe_reason(filter).is_some()
}

fn filter_unsafe_reason(filter: &str) -> Option<&'static str> {
    match crate::util::sql::filter_unsafe_reason_for(filter, crate::util::sql::SqlDialect::Postgres)
    {
        Some("SQL comment markers (`--` / `/* */`)") => {
            Some("CQL comment markers (`--` / `/* */`)")
        }
        other => other,
    }
}

const SYSTEM_KEYSPACES: &[&str] = &[
    "system",
    "system_auth",
    "system_distributed",
    "system_distributed_everywhere",
    "system_schema",
    "system_traces",
    "system_views",
    "system_virtual_schema",
];

/// The primary key of the one row that `changes` edits, when the edit
/// qualifies for [`CassandraDriver::apply_single_row_edit`]: only updates
/// or only deletes (no inserts), all keyed by the same complete primary
/// key, on a table without counter columns (CQL rejects conditional
/// updates on counter tables). Anything else keeps the plain batch.
fn single_row_edit<'a>(
    changes: &'a DataChanges,
    columns: &[ColumnInfo],
) -> Option<&'a [(String, serde_json::Value)]> {
    if !changes.inserts.is_empty() || changes.updates.is_empty() == changes.deletes.is_empty() {
        return None;
    }
    if columns.iter().any(|c| c.data_type == "counter") {
        return None;
    }
    let mut pk_cols: Vec<&str> = columns
        .iter()
        .filter(|c| c.is_primary_key)
        .map(|c| c.name.as_str())
        .collect();
    if pk_cols.is_empty() {
        return None;
    }
    pk_cols.sort_unstable();
    let normalized = |key: &[(String, serde_json::Value)]| {
        let mut k: Vec<(String, String)> = key
            .iter()
            .map(|(c, v)| (c.clone(), v.to_string()))
            .collect();
        k.sort();
        k
    };
    let mut keys = changes
        .updates
        .iter()
        .map(|u| u.primary_key_values.as_slice())
        .chain(
            changes
                .deletes
                .iter()
                .map(|d| d.primary_key_values.as_slice()),
        );
    let first = keys.next()?;
    let first_norm = normalized(first);
    let key_cols: Vec<&str> = first_norm.iter().map(|(c, _)| c.as_str()).collect();
    if key_cols != pk_cols {
        return None;
    }
    keys.all(|k| normalized(k) == first_norm).then_some(first)
}

impl CassandraDriver {
    /// Apply grid edits that all target one row, identified by its full
    /// primary key, as a lightweight transaction (`IF EXISTS`).
    ///
    /// A plain CQL `UPDATE` is an upsert and `DELETE` of a missing row is
    /// a no-op, so without the condition editing a row someone else just
    /// deleted would silently re-create it (with only the edited cells)
    /// and a delete would report success. The LWT result's `[applied]`
    /// column tells us whether the row was still there.
    async fn apply_single_row_edit(
        &self,
        qualified: &str,
        changes: &DataChanges,
        row: &[(String, serde_json::Value)],
        lit: &(dyn Fn(&str, &serde_json::Value) -> Result<String> + Sync),
    ) -> Result<()> {
        let pk_clause = row
            .iter()
            .map(|(col, val)| Ok(format!("{} = {}", quote_ident(col), lit(col, val)?)))
            .collect::<Result<Vec<_>>>()?
            .join(" AND ");
        let (action, cql) = if changes.deletes.is_empty() {
            // Every edited cell of the row in one statement; a column
            // edited twice keeps its last value.
            let mut sets: Vec<(&str, &serde_json::Value)> = Vec::new();
            for u in &changes.updates {
                match sets.iter_mut().find(|(c, _)| *c == u.column_name) {
                    Some(slot) => slot.1 = &u.new_value,
                    None => sets.push((u.column_name.as_str(), &u.new_value)),
                }
            }
            let set_clause = sets
                .iter()
                .map(|(c, v)| Ok(format!("{} = {}", quote_ident(c), lit(c, v)?)))
                .collect::<Result<Vec<_>>>()?
                .join(", ");
            (
                "update",
                format!("UPDATE {qualified} SET {set_clause} WHERE {pk_clause} IF EXISTS"),
            )
        } else {
            (
                "delete",
                format!("DELETE FROM {qualified} WHERE {pk_clause} IF EXISTS"),
            )
        };
        let result = self
            .session
            .query_unpaged(cql.as_str(), &[])
            .await
            .map_err(|e| anyhow!("{}", e))?
            .into_rows_result()
            .map_err(|e| anyhow!("{}", e))?;
        // `[applied]` is always the first column of an LWT result.
        let applied = result
            .rows::<scylla::value::Row>()
            .ok()
            .and_then(|mut rows| rows.next())
            .and_then(|r| r.ok())
            .and_then(|r| r.columns.into_iter().next().flatten())
            .map(|v| matches!(v, scylla::value::CqlValue::Boolean(true)));
        match applied {
            Some(true) => Ok(()),
            Some(false) => {
                let key = row
                    .iter()
                    .map(|(c, v)| format!("{} = {}", c, v))
                    .collect::<Vec<_>>()
                    .join(", ");
                anyhow::bail!(
                    "Cannot {action} row ({key}): the row no longer exists. It may have been \
                     deleted concurrently; refresh and try again. No changes were saved."
                )
            }
            None => {
                anyhow::bail!("Cannot {action} row: the server did not report whether it applied")
            }
        }
    }

    /// Field names and types of every user-defined type in `keyspace`.
    async fn load_udt_defs(&self, keyspace: &str) -> Result<UdtDefs> {
        let result = self
            .session
            .query_unpaged(
                "SELECT type_name, field_names, field_types FROM system_schema.types \
                 WHERE keyspace_name = ?",
                (keyspace,),
            )
            .await
            .map_err(|e| anyhow!("{}", e))?
            .into_rows_result()
            .map_err(|e| anyhow!("{}", e))?;
        let mut defs = UdtDefs::new();
        if let Ok(rows) = result.rows::<(String, Vec<String>, Vec<String>)>() {
            for (name, names, types) in rows.flatten() {
                let fields = names
                    .into_iter()
                    .zip(types.iter().map(|t| parse_cql_type(t)))
                    .collect();
                defs.insert(name, fields);
            }
        }
        Ok(defs)
    }

    pub async fn connect(config: &ConnectionConfig) -> Result<Self> {
        // Bracket IPv6 literals so the port separator is unambiguous.
        let host = config.host.trim();
        let addr = if host.contains(':') && !host.starts_with('[') {
            format!("[{}]:{}", host, config.port)
        } else {
            format!("{}:{}", host, config.port)
        };
        let mut builder = SessionBuilder::new().known_node(&addr);

        if !config.user.is_empty() {
            builder = builder.user(&config.user, &config.password);
        }

        let session = builder.build().await.map_err(|e| anyhow!("{}", e))?;
        Ok(Self { session })
    }
}

fn cql_value_to_json(_col_type: &str, raw: &scylla::value::CqlValue) -> serde_json::Value {
    use scylla::value::CqlValue;
    use serde_json::Value;
    match raw {
        CqlValue::Ascii(s) | CqlValue::Text(s) => Value::String(s.clone()),
        CqlValue::Boolean(b) => Value::Bool(*b),
        CqlValue::Int(i) => serde_json::json!(*i),
        CqlValue::BigInt(i) => i64_to_json(*i),
        CqlValue::SmallInt(i) => serde_json::json!(*i),
        CqlValue::TinyInt(i) => serde_json::json!(*i),
        CqlValue::Float(f) => crate::util::values::f32_to_json(*f),
        CqlValue::Double(d) => crate::util::values::f64_to_json(*d),
        CqlValue::Uuid(u) => Value::String(u.to_string()),
        CqlValue::Timeuuid(t) => Value::String(t.as_ref().to_string()),
        CqlValue::Timestamp(ts) => Value::String(cql_timestamp_text(ts.0)),
        CqlValue::Date(d) => Value::String(cql_date_text(d.0)),
        CqlValue::Time(t) => Value::String(cql_time_text(t.0)),
        CqlValue::Duration(d) => Value::String(cql_duration_text(d.months, d.days, d.nanoseconds)),
        CqlValue::Inet(addr) => Value::String(addr.to_string()),
        CqlValue::Blob(b) => Value::String(format!("0x{}", crate::util::values::hex_encode(b))),
        CqlValue::Varint(v) => {
            exact_decimal_to_json(&signed_be_bytes_to_decimal(v.as_signed_bytes_be_slice()))
        }
        CqlValue::Decimal(d) => {
            let (bytes, scale) = d.as_signed_be_bytes_slice_and_exponent();
            exact_decimal_to_json(&scaled_decimal_text(
                &signed_be_bytes_to_decimal(bytes),
                scale,
            ))
        }
        CqlValue::Counter(c) => i64_to_json(c.0),
        CqlValue::Set(items) | CqlValue::List(items) | CqlValue::Vector(items) => {
            Value::Array(items.iter().map(|v| cql_value_to_json("", v)).collect())
        }
        CqlValue::Map(entries) => {
            let obj: serde_json::Map<String, Value> = entries
                .iter()
                .map(|(k, v)| (cql_map_key_text(k), cql_value_to_json("", v)))
                .collect();
            Value::Object(obj)
        }
        CqlValue::Tuple(fields) => Value::Array(
            fields
                .iter()
                .map(|f| match f {
                    Some(v) => cql_value_to_json("", v),
                    None => Value::Null,
                })
                .collect(),
        ),
        CqlValue::UserDefinedType { fields, .. } => {
            let obj: serde_json::Map<String, Value> = fields
                .iter()
                .map(|(name, val)| {
                    let v = match val {
                        Some(v) => cql_value_to_json("", v),
                        None => Value::Null,
                    };
                    (name.clone(), v)
                })
                .collect();
            Value::Object(obj)
        }
        CqlValue::Empty => Value::Null,
        _ => Value::String(format!("{:?}", raw)),
    }
}

/// JSON object key for a map entry: the key's own text (`1` for an int
/// key, the uuid for a uuid key) rather than its Rust `Debug` form.
fn cql_map_key_text(k: &scylla::value::CqlValue) -> String {
    match cql_value_to_json("", k) {
        serde_json::Value::String(s) => s,
        other => other.to_string(),
    }
}

/// `timestamp` (ms since the epoch) as `YYYY-MM-DD HH:MM:SS.mmmZ`, a form
/// CQL accepts back as a timestamp literal. Out-of-range values keep the
/// raw millisecond count, which CQL also accepts.
fn cql_timestamp_text(ms: i64) -> String {
    match chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ms) {
        Some(dt) => dt.format("%Y-%m-%d %H:%M:%S%.3fZ").to_string(),
        None => ms.to_string(),
    }
}

/// `date` (days since the epoch, offset by 2^31) as `YYYY-MM-DD`.
fn cql_date_text(raw: u32) -> String {
    let days = i64::from(raw) - (1i64 << 31);
    chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
        .and_then(|epoch| epoch.checked_add_signed(chrono::Duration::days(days)))
        .map(|d| d.format("%Y-%m-%d").to_string())
        .unwrap_or_else(|| raw.to_string())
}

/// `time` (ns since midnight) as `HH:MM:SS[.fraction]`.
fn cql_time_text(ns: i64) -> String {
    if !(0..86_400_000_000_000).contains(&ns) {
        return ns.to_string();
    }
    let secs = ns / 1_000_000_000;
    let frac = ns % 1_000_000_000;
    let base = format!(
        "{:02}:{:02}:{:02}",
        secs / 3600,
        (secs / 60) % 60,
        secs % 60
    );
    if frac == 0 {
        base
    } else {
        let f = format!("{:09}", frac);
        format!("{}.{}", base, f.trim_end_matches('0'))
    }
}

/// `duration` in CQL's own literal syntax (`1mo2d3h4m5s6ms7us8ns`), which
/// is also what cqlsh prints and what CQL accepts back unquoted.
fn cql_duration_text(months: i32, days: i32, nanos: i64) -> String {
    let negative = months < 0 || days < 0 || nanos < 0;
    let months = i64::from(months).unsigned_abs();
    let days = i64::from(days).unsigned_abs();
    let mut ns = nanos.unsigned_abs();
    let mut out = String::new();
    if negative {
        out.push('-');
    }
    let mut push = |v: u64, unit: &str| {
        if v > 0 {
            out.push_str(&format!("{}{}", v, unit));
        }
    };
    push(months / 12, "y");
    push(months % 12, "mo");
    push(days, "d");
    const UNITS: [(u64, &str); 7] = [
        (3_600_000_000_000, "h"),
        (60_000_000_000, "m"),
        (1_000_000_000, "s"),
        (1_000_000, "ms"),
        (1_000, "us"),
        (1, "ns"),
        (0, ""),
    ];
    for (size, unit) in UNITS.iter().take(6) {
        push(ns / size, unit);
        ns %= size;
    }
    if out.is_empty() || out == "-" {
        return "0s".to_string();
    }
    out
}

/// True when `s` is a duration literal CQL accepts unquoted: the
/// `1h30m` unit form or ISO 8601 (`P1DT2H`, `P2W`).
fn is_cql_duration_literal(s: &str) -> bool {
    let t = s.strip_prefix('-').unwrap_or(s);
    if t.is_empty() {
        return false;
    }
    if let Some(iso) = t.strip_prefix('P') {
        return !iso.is_empty()
            && iso
                .bytes()
                .all(|b| b.is_ascii_digit() || b"YMWDTHS.-:".contains(&b))
            && iso.bytes().any(|b| b.is_ascii_digit());
    }
    let b = t.as_bytes();
    let mut i = 0;
    while i < b.len() {
        let start = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == start {
            return false;
        }
        let rest = &t[i..];
        let unit = ["mo", "ms", "us", "µs", "ns", "y", "w", "d", "h", "m", "s"]
            .iter()
            .find(|u| rest.to_ascii_lowercase().starts_with(*u));
        match unit {
            Some(u) => i += u.len(),
            None => return false,
        }
    }
    true
}

fn json_to_cql_literal(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(json_to_cql_literal).collect();
            format!("[{}]", items.join(", "))
        }
        serde_json::Value::Object(obj) => {
            let items: Vec<String> = obj
                .iter()
                .map(|(k, v)| format!("'{}': {}", k.replace('\'', "''"), json_to_cql_literal(v)))
                .collect();
            format!("{{{}}}", items.join(", "))
        }
    }
}

/// A parsed CQL column type, as `system_schema` spells it.
#[derive(Debug, Clone, PartialEq)]
enum CqlType {
    /// A native type name, lowercased (`int`, `uuid`, `blob`, ...).
    Native(String),
    List(Box<CqlType>),
    Set(Box<CqlType>),
    Map(Box<CqlType>, Box<CqlType>),
    Tuple(Vec<CqlType>),
    /// A user-defined type, by (unquoted) name.
    Udt(String),
}

const CQL_NATIVE_TYPES: &[&str] = &[
    "ascii",
    "bigint",
    "blob",
    "boolean",
    "counter",
    "date",
    "decimal",
    "double",
    "duration",
    "float",
    "inet",
    "int",
    "smallint",
    "text",
    "time",
    "timestamp",
    "timeuuid",
    "tinyint",
    "uuid",
    "varchar",
    "varint",
];

/// Split `a, b<c, d>, e` at top-level commas.
fn split_type_args(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut in_quotes = false;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '"' => in_quotes = !in_quotes,
            '<' if !in_quotes => depth += 1,
            '>' if !in_quotes => depth -= 1,
            ',' if !in_quotes && depth == 0 => {
                parts.push(s[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(s[start..].trim());
    parts
}

fn parse_cql_type(t: &str) -> CqlType {
    let t = t.trim();
    if let Some(open) = t.find('<') {
        if t.ends_with('>') {
            let name = t[..open].trim().to_ascii_lowercase();
            let inner = &t[open + 1..t.len() - 1];
            let args = split_type_args(inner);
            match (name.as_str(), args.as_slice()) {
                ("frozen", [one]) => return parse_cql_type(one),
                ("list", [one]) => return CqlType::List(Box::new(parse_cql_type(one))),
                ("set", [one]) => return CqlType::Set(Box::new(parse_cql_type(one))),
                ("map", [k, v]) => {
                    return CqlType::Map(Box::new(parse_cql_type(k)), Box::new(parse_cql_type(v)))
                }
                ("tuple", items) => {
                    return CqlType::Tuple(items.iter().map(|i| parse_cql_type(i)).collect())
                }
                // vector<float, 3>: written like a list.
                ("vector", [elem, _]) => return CqlType::List(Box::new(parse_cql_type(elem))),
                _ => {}
            }
        }
    }
    let lower = t.to_ascii_lowercase();
    if CQL_NATIVE_TYPES.contains(&lower.as_str()) || t.starts_with('\'') {
        CqlType::Native(lower)
    } else {
        CqlType::Udt(t.trim_matches('"').to_string())
    }
}

/// Field names and types of the user-defined types in a keyspace.
type UdtDefs = std::collections::HashMap<String, Vec<(String, CqlType)>>;

/// Reads a JSON string as the JSON document it holds (an edited
/// collection arrives as the text the user typed).
fn json_from_text(val: &serde_json::Value) -> Option<serde_json::Value> {
    match val {
        serde_json::Value::String(s) => serde_json::from_str(s.trim()).ok(),
        _ => None,
    }
}

fn is_canonical_uuid(s: &str) -> bool {
    s.len() == 36 && uuid::Uuid::try_parse(s).is_ok()
}

/// CQL literal for `val` written to a column of type `ty`.
///
/// Values arrive in the shapes `cql_value_to_json` produces (or as the
/// edited text of them): uuids, blobs (`0x…`) and durations are written
/// unquoted after validation; sets as `{…}`, tuples as `(…)`, UDTs with
/// unquoted field names and map keys converted to the key type.
fn cql_literal_for(ty: &CqlType, val: &serde_json::Value, udts: &UdtDefs) -> Result<String> {
    use serde_json::Value;
    if val.is_null() {
        return Ok("NULL".to_string());
    }
    let wrong = |what: &str| {
        anyhow!(
            "Invalid value {} for a {} column: expected {}",
            val,
            cql_type_name(ty),
            what
        )
    };
    match ty {
        CqlType::Native(name) => Ok(native_cql_literal(name, val)?),
        CqlType::List(elem) | CqlType::Set(elem) => {
            let owned;
            let items = match val {
                Value::Array(a) => a,
                _ => {
                    owned = json_from_text(val).ok_or_else(|| wrong("a JSON array"))?;
                    owned.as_array().ok_or_else(|| wrong("a JSON array"))?
                }
            };
            let parts = items
                .iter()
                .map(|v| cql_literal_for(elem, v, udts))
                .collect::<Result<Vec<_>>>()?;
            Ok(if matches!(ty, CqlType::Set(_)) {
                format!("{{{}}}", parts.join(", "))
            } else {
                format!("[{}]", parts.join(", "))
            })
        }
        CqlType::Tuple(types) => {
            let owned;
            let items = match val {
                Value::Array(a) => a,
                _ => {
                    owned = json_from_text(val).ok_or_else(|| wrong("a JSON array"))?;
                    owned.as_array().ok_or_else(|| wrong("a JSON array"))?
                }
            };
            if items.len() > types.len() {
                return Err(wrong(&format!("at most {} items", types.len())));
            }
            let parts = items
                .iter()
                .zip(types)
                .map(|(v, t)| cql_literal_for(t, v, udts))
                .collect::<Result<Vec<_>>>()?;
            Ok(format!("({})", parts.join(", ")))
        }
        CqlType::Map(kt, vt) => {
            let owned;
            let obj = match val {
                Value::Object(o) => o,
                _ => {
                    owned = json_from_text(val).ok_or_else(|| wrong("a JSON object"))?;
                    owned.as_object().ok_or_else(|| wrong("a JSON object"))?
                }
            };
            let parts = obj
                .iter()
                .map(|(k, v)| {
                    Ok(format!(
                        "{}: {}",
                        cql_literal_for(kt, &Value::String(k.clone()), udts)?,
                        cql_literal_for(vt, v, udts)?
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(format!("{{{}}}", parts.join(", ")))
        }
        CqlType::Udt(name) => {
            let owned;
            let obj = match val {
                Value::Object(o) => o,
                _ => {
                    owned = json_from_text(val).ok_or_else(|| wrong("a JSON object"))?;
                    owned.as_object().ok_or_else(|| wrong("a JSON object"))?
                }
            };
            let fields = udts.get(name);
            let parts = obj
                .iter()
                .map(|(k, v)| {
                    let field_ty = fields
                        .and_then(|f| f.iter().find(|(n, _)| n == k))
                        .map(|(_, t)| t);
                    let lit = match field_ty {
                        Some(t) => cql_literal_for(t, v, udts)?,
                        None => json_to_cql_literal(v),
                    };
                    Ok(format!("{}: {}", quote_ident(k), lit))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(format!("{{{}}}", parts.join(", ")))
        }
    }
}

fn cql_type_name(ty: &CqlType) -> String {
    match ty {
        CqlType::Native(n) => n.clone(),
        CqlType::List(e) => format!("list<{}>", cql_type_name(e)),
        CqlType::Set(e) => format!("set<{}>", cql_type_name(e)),
        CqlType::Map(k, v) => format!("map<{}, {}>", cql_type_name(k), cql_type_name(v)),
        CqlType::Tuple(t) => format!(
            "tuple<{}>",
            t.iter().map(cql_type_name).collect::<Vec<_>>().join(", ")
        ),
        CqlType::Udt(n) => n.clone(),
    }
}

fn native_cql_literal(name: &str, val: &serde_json::Value) -> Result<String> {
    use serde_json::Value;
    let Value::String(s) = val else {
        // A number / boolean typed into a text-like column is text.
        if matches!(name, "text" | "varchar" | "ascii" | "inet")
            && !val.is_object()
            && !val.is_array()
        {
            return Ok(json_to_cql_literal(&Value::String(val.to_string())));
        }
        return Ok(json_to_cql_literal(val));
    };
    let t = s.trim();
    match name {
        "int" | "bigint" | "smallint" | "tinyint" | "varint" | "decimal" | "counter" => {
            if let Some(n) = crate::util::sql::numeric_literal(t) {
                return Ok(n.to_string());
            }
        }
        "float" | "double" => {
            if let Some(n) = crate::util::sql::numeric_literal(t) {
                return Ok(n.to_string());
            }
            match t {
                "NaN" => return Ok("NaN".to_string()),
                "Infinity" | "+Infinity" => return Ok("Infinity".to_string()),
                "-Infinity" => return Ok("-Infinity".to_string()),
                _ => {}
            }
        }
        "boolean" => {
            if t.eq_ignore_ascii_case("true") || t.eq_ignore_ascii_case("false") {
                return Ok(t.to_ascii_lowercase());
            }
        }
        "uuid" | "timeuuid" => {
            if is_canonical_uuid(t) {
                return Ok(t.to_ascii_lowercase());
            }
            anyhow::bail!("Invalid {} value {:?}", name, s);
        }
        "blob" => {
            let hex =
                crate::util::values::binary_value_hex(s, crate::util::values::HexDisplay::ZeroX)
                    .map_err(|e| anyhow!(e))?;
            return Ok(format!("0x{}", hex));
        }
        "duration" => {
            if is_cql_duration_literal(t) {
                return Ok(t.to_string());
            }
            anyhow::bail!(
                "Invalid duration {:?}: expected e.g. 1h30m, 2d or ISO 8601 (P1DT2H)",
                s
            );
        }
        _ => {}
    }
    Ok(json_to_cql_literal(val))
}

/// Literal for a value written to a column whose `system_schema` type is
/// `data_type` (unknown columns fall back to the untyped rendering).
fn typed_cql_literal(
    data_type: Option<&str>,
    val: &serde_json::Value,
    udts: &UdtDefs,
) -> Result<String> {
    match data_type {
        Some(t) => cql_literal_for(&parse_cql_type(t), val, udts),
        None => Ok(json_to_cql_literal(val)),
    }
}

/// True when a parsed type mentions a user-defined type anywhere.
fn mentions_udt(ty: &CqlType) -> bool {
    match ty {
        CqlType::Native(_) => false,
        CqlType::Udt(_) => true,
        CqlType::List(e) | CqlType::Set(e) => mentions_udt(e),
        CqlType::Map(k, v) => mentions_udt(k) || mentions_udt(v),
        CqlType::Tuple(t) => t.iter().any(mentions_udt),
    }
}

/// The signed change to apply to a counter edited from `old` to `new`
/// (CQL counters can only be incremented / decremented).
fn counter_delta(old: &serde_json::Value, new: &serde_json::Value) -> Result<i64> {
    let as_i64 = |v: &serde_json::Value| -> Option<i64> {
        match v {
            serde_json::Value::Null => Some(0),
            serde_json::Value::Number(n) => n.as_i64(),
            serde_json::Value::String(s) => s.trim().parse().ok(),
            _ => None,
        }
    };
    let (Some(o), Some(n)) = (as_i64(old), as_i64(new)) else {
        anyhow::bail!("Invalid counter value {}: expected an integer", new);
    };
    n.checked_sub(o)
        .ok_or_else(|| anyhow!("Counter change from {} to {} overflows", o, n))
}

/// `"c" = "c" + 5` / `"c" = "c" - 5`.
fn counter_assignment(col: &str, delta: i64) -> String {
    let q = quote_ident(col);
    if delta < 0 {
        format!("{q} = {q} - {}", delta.unsigned_abs())
    } else {
        format!("{q} = {q} + {delta}")
    }
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Reorder (and if necessary extend) the catalog column list so it
/// follows `result_names`, the column order of the cells actually
/// returned by the server. Columns missing from the catalog get a
/// minimal placeholder entry so headers and cells always line up.
fn align_columns_to_result(columns: Vec<ColumnInfo>, result_names: Vec<String>) -> Vec<ColumnInfo> {
    if result_names.is_empty() {
        return columns;
    }
    let mut by_name: std::collections::HashMap<String, ColumnInfo> =
        columns.into_iter().map(|c| (c.name.clone(), c)).collect();
    result_names
        .into_iter()
        .enumerate()
        .map(|(i, name)| {
            let mut col = by_name.remove(&name).unwrap_or_else(|| ColumnInfo {
                name: name.clone(),
                data_type: "unknown".to_string(),
                is_nullable: true,
                is_primary_key: false,
                default_value: None,
                ordinal_position: 0,
                is_auto_generated: false,
            });
            col.ordinal_position = i as i32 + 1;
            col
        })
        .collect()
}

/// Decimal text of a big-endian two's-complement integer.
fn signed_be_bytes_to_decimal(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return "0".to_string();
    }
    let negative = bytes[0] & 0x80 != 0;
    let mut mag: Vec<u8> = if negative {
        // Two's complement negate: invert and add one.
        let mut v: Vec<u8> = bytes.iter().map(|b| !b).collect();
        for b in v.iter_mut().rev() {
            let (n, overflow) = b.overflowing_add(1);
            *b = n;
            if !overflow {
                break;
            }
        }
        v
    } else {
        bytes.to_vec()
    };
    // Repeated division by 10 over the base-256 digits.
    let mut digits = Vec::new();
    while mag.iter().any(|&b| b != 0) {
        let mut rem: u32 = 0;
        for b in mag.iter_mut() {
            let cur = (rem << 8) | u32::from(*b);
            *b = (cur / 10) as u8;
            rem = cur % 10;
        }
        digits.push(char::from(b'0' + rem as u8));
    }
    if digits.is_empty() {
        digits.push('0');
    }
    if negative {
        digits.push('-');
    }
    digits.iter().rev().collect()
}

/// Decimal text of `unscaled * 10^-scale`.
fn scaled_decimal_text(unscaled: &str, scale: i32) -> String {
    let (sign, digits) = match unscaled.strip_prefix('-') {
        Some(d) => ("-", d),
        None => ("", unscaled),
    };
    if scale <= 0 {
        if digits == "0" {
            return "0".to_string();
        }
        return format!("{}{}{}", sign, digits, "0".repeat((-scale) as usize));
    }
    let scale = scale as usize;
    let padded = if digits.len() <= scale {
        format!("{}{}", "0".repeat(scale - digits.len() + 1), digits)
    } else {
        digits.to_string()
    };
    let (int_part, frac_part) = padded.split_at(padded.len() - scale);
    format!("{}{}.{}", sign, int_part, frac_part)
}

/// JSON for an exact decimal string: a number when it survives the
/// f64 round trip unchanged, otherwise the exact text.
fn exact_decimal_to_json(text: &str) -> serde_json::Value {
    crate::util::sql::exact_numeric_text_to_json(text)
}

fn rows_from_result(result: &QueryRowsResult) -> (Vec<String>, Vec<Vec<serde_json::Value>>) {
    let col_specs = result.column_specs();
    let columns: Vec<String> = col_specs.iter().map(|c| c.name().to_string()).collect();
    let mut rows = Vec::new();

    if let Ok(rows_iter) = result.rows::<scylla::value::Row>() {
        for row in rows_iter.flatten() {
            let json_row: Vec<serde_json::Value> = row
                .columns
                .iter()
                .enumerate()
                .map(|(i, opt)| match opt {
                    Some(v) => {
                        let col_name = if i < columns.len() { &columns[i] } else { "" };
                        cql_value_to_json(col_name, v)
                    }
                    None => serde_json::Value::Null,
                })
                .collect();
            rows.push(json_row);
        }
    }

    (columns, rows)
}

#[async_trait]
impl DatabaseDriver for CassandraDriver {
    async fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        let result = self
            .session
            .query_unpaged("SELECT keyspace_name FROM system_schema.keyspaces", &[])
            .await
            .map_err(|e| anyhow!("{}", e))?
            .into_rows_result()
            .map_err(|e| anyhow!("{}", e))?;

        let mut databases = Vec::new();
        if let Ok(rows) = result.rows::<(String,)>() {
            for (name,) in rows.flatten() {
                if !SYSTEM_KEYSPACES.contains(&name.as_str()) {
                    databases.push(DatabaseInfo { name });
                }
            }
        }
        databases.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(databases)
    }

    async fn list_schemas(&self, database: &str) -> Result<Vec<SchemaInfo>> {
        Ok(vec![SchemaInfo {
            name: database.to_string(),
        }])
    }

    async fn list_tables(&self, database: &str, _schema: &str) -> Result<Vec<TableInfo>> {
        // Base tables live in `system_schema.tables`; materialized
        // views live in `system_schema.views`. The sidebar splits
        // the result by `table_type`:
        //   - "BASE TABLE" → "Tables" group
        //   - "VIEW"       → "Views" group
        // The frontend's filter is an exact `===` match (see
        // ObjectTree.tsx), so we MUST emit "BASE TABLE" here, not
        // "TABLE". The mismatch caused every keyspace's Tables
        // folder to render as "(empty)" despite the data really
        // being there in `system_schema.tables`.

        let tables_result = self
            .session
            .query_unpaged(
                "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?",
                (database,),
            )
            .await
            .map_err(|e| anyhow!("{}", e))?
            .into_rows_result()
            .map_err(|e| anyhow!("{}", e))?;

        let mut tables = Vec::new();
        if let Ok(rows) = tables_result.rows::<(String,)>() {
            for (name,) in rows.flatten() {
                tables.push(TableInfo {
                    name: name.clone(),
                    schema: database.to_string(),
                    table_type: "BASE TABLE".to_string(),
                    row_count_estimate: None,
                    ..Default::default()
                });
            }
        }

        // Materialized views. Cassandra has had MVs since 3.0;
        // `system_schema.views` exists on every supported version.
        // We surface them so the sidebar's "Views" group reflects
        // what cqlsh's `DESCRIBE` would show.
        let views_result = self
            .session
            .query_unpaged(
                "SELECT view_name FROM system_schema.views WHERE keyspace_name = ?",
                (database,),
            )
            .await
            .map_err(|e| anyhow!("{}", e))?
            .into_rows_result()
            .map_err(|e| anyhow!("{}", e))?;

        if let Ok(rows) = views_result.rows::<(String,)>() {
            for (name,) in rows.flatten() {
                tables.push(TableInfo {
                    name: name.clone(),
                    schema: database.to_string(),
                    table_type: "VIEW".to_string(),
                    row_count_estimate: None,
                    ..Default::default()
                });
            }
        }

        tables.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(tables)
    }

    async fn list_columns(
        &self,
        database: &str,
        _schema: &str,
        table: &str,
    ) -> Result<Vec<ColumnInfo>> {
        let result = self
            .session
            .query_unpaged(
                "SELECT column_name, type, kind, position FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?",
                (database, table),
            )
            .await
            .map_err(|e| anyhow!("{}", e))?
            .into_rows_result()
            .map_err(|e| anyhow!("{}", e))?;

        // `position` is only meaningful within a kind (0..n for the
        // partition and clustering keys, -1 for every static/regular
        // column), so sort by kind first. This matches the order in
        // which `SELECT *` returns cells: partition keys, clustering
        // keys, then the remaining columns by name.
        let mut keyed: Vec<(u8, i32, ColumnInfo)> = Vec::new();
        if let Ok(rows) = result.rows::<(String, String, String, i32)>() {
            for (col_name, col_type, kind, position) in rows.flatten() {
                let is_pk = kind == "partition_key" || kind == "clustering";
                let rank = match kind.as_str() {
                    "partition_key" => 0,
                    "clustering" => 1,
                    "static" => 2,
                    _ => 3,
                };
                keyed.push((
                    rank,
                    position,
                    ColumnInfo {
                        name: col_name,
                        data_type: col_type,
                        is_nullable: !is_pk,
                        is_primary_key: is_pk,
                        default_value: None,
                        ordinal_position: position,
                        is_auto_generated: false,
                    },
                ));
            }
        }
        keyed.sort_by(|a, b| {
            (a.0, a.1)
                .cmp(&(b.0, b.1))
                .then_with(|| a.2.name.cmp(&b.2.name))
        });
        let columns = keyed
            .into_iter()
            .enumerate()
            .map(|(i, (_, _, mut c))| {
                c.ordinal_position = i as i32 + 1;
                c
            })
            .collect();
        Ok(columns)
    }

    async fn list_indexes(
        &self,
        database: &str,
        _schema: &str,
        table: &str,
    ) -> Result<Vec<IndexInfo>> {
        let result = self
            .session
            .query_unpaged(
                "SELECT index_name, options FROM system_schema.indexes WHERE keyspace_name = ? AND table_name = ?",
                (database, table),
            )
            .await
            .map_err(|e| anyhow!("{}", e))?
            .into_rows_result()
            .map_err(|e| anyhow!("{}", e))?;

        let mut indexes = Vec::new();
        if let Ok(rows) = result.rows::<(String, std::collections::HashMap<String, String>)>() {
            for (name, options) in rows.flatten() {
                let target = options.get("target").cloned().unwrap_or_default();
                indexes.push(IndexInfo {
                    name,
                    columns: vec![target],
                    is_unique: false,
                    index_type: "SECONDARY".to_string(),
                });
            }
        }
        Ok(indexes)
    }

    async fn list_foreign_keys(
        &self,
        _database: &str,
        _schema: &str,
        _table: &str,
    ) -> Result<Vec<ForeignKeyInfo>> {
        Ok(vec![])
    }

    async fn list_functions(&self, _database: &str, _schema: &str) -> Result<Vec<FunctionInfo>> {
        Ok(vec![])
    }

    async fn list_triggers(
        &self,
        _database: &str,
        _schema: &str,
        _table: &str,
    ) -> Result<Vec<TriggerInfo>> {
        Ok(vec![])
    }

    async fn get_table_stats(
        &self,
        database: &str,
        _schema: &str,
        table: &str,
    ) -> Result<TableStats> {
        Ok(TableStats {
            table_name: format!("{}.{}", database, table),
            row_count: -1,
            total_size: "N/A".to_string(),
            index_size: "N/A".to_string(),
            data_size: "N/A".to_string(),
            last_vacuum: None,
            last_analyze: None,
            dead_tuples: None,
            live_tuples: None,
        })
    }

    async fn fetch_rows(
        &self,
        database: &str,
        _schema: &str,
        table: &str,
        offset: u64,
        limit: u64,
        sort: Vec<SortSpec>,
        filter: Option<String>,
    ) -> Result<TableData> {
        let columns = self.list_columns(database, database, table).await?;

        let qualified = format!("{}.{}", quote_ident(database), quote_ident(table));

        // Build the optional WHERE fragment once and reuse it for both
        // the count query and the data query so the two stay in sync.
        let where_clause = match filter.as_deref().map(str::trim).filter(|f| !f.is_empty()) {
            Some(f) => {
                if let Some(reason) = filter_unsafe_reason(f) {
                    anyhow::bail!("Filter rejected: {}", reason);
                }
                debug_assert!(!filter_is_unsafe(f));
                format!(" WHERE {}", f)
            }
            None => String::new(),
        };

        let mut cql = format!("SELECT * FROM {}{}", qualified, where_clause);

        if !sort.is_empty() {
            // CQL `ORDER BY` is only valid on a partition's
            // clustering columns and only in the table's defined
            // clustering order (or its strict reverse). Tablio
            // can't statically know which columns are clustering
            // keys at this layer, so we just pass the user-picked
            // sort vec through and let the Cassandra server be
            // the gatekeeper: legal orderings work, illegal ones
            // surface as an execution error. Multi-column sort
            // (issue #57) follows the same rule — typically only
            // useful when the user is sorting by the clustering
            // columns in clustering order.
            let parts: Vec<String> = sort
                .iter()
                .map(|s| {
                    format!(
                        "{} {}",
                        quote_ident(&s.column),
                        match s.direction {
                            SortDirection::Asc => "ASC",
                            SortDirection::Desc => "DESC",
                        }
                    )
                })
                .collect();
            cql.push_str(&format!(" ORDER BY {}", parts.join(", ")));
        }

        // CQL has no OFFSET, so the first `offset` rows still have to
        // be read and skipped. Fetch them page by page (instead of one
        // unpaged response holding offset+limit rows), keep only the
        // requested window plus one extra row to know whether another
        // page exists, and stop as soon as that is reached. Saturating
        // arithmetic keeps hostile pagination parameters from
        // overflowing.
        let wanted = offset.saturating_add(limit).saturating_add(1);
        cql.push_str(&format!(
            " LIMIT {} ALLOW FILTERING",
            wanted.min(i32::MAX as u64)
        ));
        let page_size = limit.saturating_add(1).clamp(1, 5000) as i32;
        let mut statement = scylla::statement::Statement::new(cql);
        statement.set_page_size(page_size);

        let mut paging_state = PagingState::start();
        let mut result_columns: Option<Vec<String>> = None;
        let mut seen: u64 = 0;
        let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();
        loop {
            let (page, paging_response) = self
                .session
                .query_single_page(statement.clone(), &[], paging_state)
                .await
                .map_err(|e| anyhow!("{}", e))?;
            let page = page.into_rows_result().map_err(|e| anyhow!("{}", e))?;
            let (names, page_rows) = rows_from_result(&page);
            if result_columns.is_none() {
                result_columns = Some(names);
            }
            for row in page_rows {
                if seen >= offset && (rows.len() as u64) <= limit {
                    rows.push(row);
                }
                seen += 1;
            }
            if (rows.len() as u64) > limit || seen >= wanted {
                break;
            }
            match paging_response {
                PagingStateResponse::HasMorePages { state } => paging_state = state,
                PagingStateResponse::NoMorePages => break,
            }
        }
        let has_more = (rows.len() as u64) > limit;
        rows.truncate(limit as usize);

        // Header order must match cell order exactly: the grid looks up
        // primary-key values by header index, so a mismatch makes edits
        // target the wrong row. Use the result's own column specs as
        // the source of truth and attach catalog metadata by name.
        let columns = align_columns_to_result(columns, result_columns.unwrap_or_default());

        // An exact `COUNT(*)` needs a full scan in Cassandra and times
        // out (or is refused) on large tables. It is best-effort: when
        // it fails, report a lower bound that still lets the grid page
        // forward (one past the current window when more rows exist).
        let fetched_through = offset + rows.len() as u64 + u64::from(has_more);
        let count_cql = format!(
            "SELECT COUNT(*) FROM {}{} ALLOW FILTERING",
            qualified, where_clause
        );
        let counted = match self.session.query_unpaged(count_cql.as_str(), &[]).await {
            Ok(res) => res
                .into_rows_result()
                .ok()
                .and_then(|r| r.first_row::<(i64,)>().ok())
                .map(|(c,)| c),
            Err(e) => {
                log::warn!("Cassandra COUNT(*) failed, using approximate total: {}", e);
                None
            }
        };
        let total_rows = match counted {
            Some(c) => c.max(fetched_through as i64),
            None => fetched_through as i64,
        };

        Ok(TableData {
            columns,
            rows,
            total_rows,
            offset,
            limit,
        })
    }

    async fn execute_query(&self, _database: &str, sql: &str) -> Result<QueryResult> {
        let start = Instant::now();
        let trimmed = sql.trim().to_uppercase();

        let is_select = trimmed.starts_with("SELECT")
            || trimmed.starts_with("DESCRIBE")
            || trimmed.starts_with("LIST");

        if is_select {
            let result = self
                .session
                .query_unpaged(sql, &[])
                .await
                .map_err(|e| anyhow!("{}", e))?
                .into_rows_result()
                .map_err(|e| anyhow!("{}", e))?;

            let (columns, rows) = rows_from_result(&result);
            let elapsed = start.elapsed().as_millis() as u64;

            Ok(QueryResult {
                columns,
                rows_affected: rows.len() as u64,
                rows,
                execution_time_ms: elapsed,
                is_select: true,
            })
        } else {
            self.session
                .query_unpaged(sql, &[])
                .await
                .map_err(|e| anyhow!("{}", e))?;
            let elapsed = start.elapsed().as_millis() as u64;

            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: 0,
                execution_time_ms: elapsed,
                is_select: false,
            })
        }
    }

    async fn explain_query(&self, _database: &str, sql: &str) -> Result<ExplainResult> {
        let start = Instant::now();
        let raw_text = format!("CQL does not support EXPLAIN. The query was: {}", sql);
        let elapsed = start.elapsed().as_millis() as u64;

        Ok(ExplainResult {
            plan: ExplainNode {
                node_type: "CQL Query".to_string(),
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
            raw_text,
            execution_time_ms: elapsed,
        })
    }

    async fn validate_query(&self, _database: &str, sql: &str) -> Result<Option<ValidationError>> {
        if sql.trim().is_empty() {
            return Ok(Some(ValidationError {
                message: "Empty query".to_string(),
                position: None,
            }));
        }
        match self.session.prepare(sql).await {
            Ok(_) => Ok(None),
            Err(e) => Ok(Some(ValidationError {
                message: e.to_string(),
                position: None,
            })),
        }
    }

    async fn get_ddl(
        &self,
        database: &str,
        _schema: &str,
        object_name: &str,
        object_type: &str,
    ) -> Result<String> {
        match object_type.to_uppercase().as_str() {
            "TABLE" => {
                let cols = self.list_columns(database, database, object_name).await?;
                let pk_cols: Vec<&str> = cols
                    .iter()
                    .filter(|c| c.is_primary_key)
                    .map(|c| c.name.as_str())
                    .collect();
                let col_defs: Vec<String> = cols
                    .iter()
                    .map(|c| format!("  {} {}", c.name, c.data_type))
                    .collect();

                let pk = if pk_cols.is_empty() {
                    String::new()
                } else {
                    format!(",\n  PRIMARY KEY ({})", pk_cols.join(", "))
                };

                Ok(format!(
                    "CREATE TABLE {}.{} (\n{}{}\n);",
                    database,
                    object_name,
                    col_defs.join(",\n"),
                    pk
                ))
            }
            _ => Err(anyhow!("DDL generation not supported for {}", object_type)),
        }
    }

    async fn apply_changes(&self, changes: &DataChanges) -> Result<()> {
        let ks = &changes.database;
        let tbl = &changes.table;
        let qualified = format!("{}.{}", quote_ident(ks), quote_ident(tbl));

        // Column types let numeric values that arrive as strings (big
        // integers / exact decimals the frontend cannot hold as JS
        // numbers) be written as bare numeric literals: CQL rejects a
        // quoted string for bigint/varint/decimal columns.
        let columns = self.list_columns(ks, ks, tbl).await.unwrap_or_default();
        let col_types: std::collections::HashMap<String, String> = columns
            .iter()
            .map(|c| (c.name.clone(), c.data_type.clone()))
            .collect();
        let udts = if col_types.values().any(|t| mentions_udt(&parse_cql_type(t))) {
            self.load_udt_defs(ks).await.unwrap_or_default()
        } else {
            UdtDefs::new()
        };
        let lit = |col: &str, val: &serde_json::Value| -> Result<String> {
            typed_cql_literal(col_types.get(col).map(String::as_str), val, &udts)
        };
        // Counter columns can only be incremented, so an edit becomes
        // `c = c + (new - old)`; a counter table's statements cannot
        // share a batch with anything else, so they run one by one.
        let is_counter = |col: &str| col_types.get(col).map(String::as_str) == Some("counter");
        let counter_table = col_types.values().any(|t| t == "counter");

        if let Some(row) = single_row_edit(changes, &columns) {
            return self
                .apply_single_row_edit(&qualified, changes, row, &lit)
                .await;
        }

        let mut stmts = Vec::new();

        for update in &changes.updates {
            if update.primary_key_values.is_empty() {
                anyhow::bail!("Cannot update row: no primary key values provided");
            }
            let pk_clause: Vec<String> = update
                .primary_key_values
                .iter()
                .map(|(col, val)| Ok(format!("{} = {}", quote_ident(col), lit(col, val)?)))
                .collect::<Result<_>>()?;
            let assignment = if is_counter(&update.column_name) {
                counter_assignment(
                    &update.column_name,
                    counter_delta(&update.old_value, &update.new_value)?,
                )
            } else {
                format!(
                    "{} = {}",
                    quote_ident(&update.column_name),
                    lit(&update.column_name, &update.new_value)?
                )
            };
            stmts.push(format!(
                "UPDATE {} SET {} WHERE {}",
                qualified,
                assignment,
                pk_clause.join(" AND ")
            ));
        }

        for insert in &changes.inserts {
            if counter_table {
                // A counter row is created by incrementing it.
                let (counters, keys): (Vec<_>, Vec<_>) =
                    insert.values.iter().partition(|(c, _)| is_counter(c));
                let sets: Vec<String> = counters
                    .iter()
                    .filter(|(_, v)| !v.is_null())
                    .map(|(c, v)| {
                        Ok(counter_assignment(
                            c,
                            counter_delta(&serde_json::Value::Null, v)?,
                        ))
                    })
                    .collect::<Result<_>>()?;
                if sets.is_empty() {
                    anyhow::bail!("Cannot insert into a counter table without a counter value");
                }
                let pk_clause: Vec<String> = keys
                    .iter()
                    .map(|(col, val)| Ok(format!("{} = {}", quote_ident(col), lit(col, val)?)))
                    .collect::<Result<_>>()?;
                stmts.push(format!(
                    "UPDATE {} SET {} WHERE {}",
                    qualified,
                    sets.join(", "),
                    pk_clause.join(" AND ")
                ));
                continue;
            }
            let col_names: Vec<String> =
                insert.values.iter().map(|(c, _)| quote_ident(c)).collect();
            let vals: Vec<String> = insert
                .values
                .iter()
                .map(|(c, v)| lit(c, v))
                .collect::<Result<_>>()?;
            stmts.push(format!(
                "INSERT INTO {} ({}) VALUES ({})",
                qualified,
                col_names.join(", "),
                vals.join(", ")
            ));
        }

        for delete in &changes.deletes {
            if delete.primary_key_values.is_empty() {
                anyhow::bail!("Cannot delete row: no primary key values provided");
            }
            let pk_clause: Vec<String> = delete
                .primary_key_values
                .iter()
                .map(|(col, val)| Ok(format!("{} = {}", quote_ident(col), lit(col, val)?)))
                .collect::<Result<_>>()?;
            stmts.push(format!(
                "DELETE FROM {} WHERE {}",
                qualified,
                pk_clause.join(" AND ")
            ));
        }

        if stmts.is_empty() {
            return Ok(());
        }

        if counter_table {
            for stmt in &stmts {
                self.session
                    .query_unpaged(stmt.as_str(), &[])
                    .await
                    .map_err(|e| anyhow!("{}", e))?;
            }
        } else if stmts.len() == 1 {
            self.session
                .query_unpaged(stmts[0].as_str(), &[])
                .await
                .map_err(|e| anyhow!("{}", e))?;
        } else {
            let batch_cql = format!("BEGIN BATCH\n{}\nAPPLY BATCH", stmts.join(";\n"));
            self.session
                .query_unpaged(batch_cql.as_str(), &[])
                .await
                .map_err(|e| anyhow!("{}", e))?;
        }

        Ok(())
    }

    async fn create_table(
        &self,
        database: &str,
        _schema: &str,
        table_name: &str,
        columns: &[ColumnDefinition],
    ) -> Result<()> {
        for col in columns {
            if col.data_type.contains(';')
                || col.data_type.contains("--")
                || col.data_type.contains("/*")
            {
                anyhow::bail!("Invalid character in data type for column {}", col.name);
            }
        }

        let col_defs: Vec<String> = columns
            .iter()
            .map(|c| format!("{} {}", quote_ident(&c.name), c.data_type))
            .collect();
        let pk_cols: Vec<String> = columns
            .iter()
            .filter(|c| c.is_primary_key)
            .map(|c| quote_ident(&c.name))
            .collect();

        if pk_cols.is_empty() {
            return Err(anyhow!(
                "Cassandra tables require at least one primary key column"
            ));
        }

        let cql = format!(
            "CREATE TABLE {}.{} ({}, PRIMARY KEY ({}))",
            quote_ident(database),
            quote_ident(table_name),
            col_defs.join(", "),
            pk_cols.join(", ")
        );
        self.session
            .query_unpaged(cql.as_str(), &[])
            .await
            .map_err(|e| anyhow!("{}", e))?;
        Ok(())
    }

    #[allow(clippy::collapsible_match)]
    async fn alter_table(
        &self,
        database: &str,
        _schema: &str,
        table_name: &str,
        operations: &[AlterTableOperation],
    ) -> Result<()> {
        let qualified = format!("{}.{}", quote_ident(database), quote_ident(table_name));
        for op in operations {
            match op {
                AlterTableOperation::AddColumn { column } => {
                    if column.data_type.contains(';')
                        || column.data_type.contains("--")
                        || column.data_type.contains("/*")
                    {
                        anyhow::bail!("Invalid character in data type for column {}", column.name);
                    }
                }
                AlterTableOperation::ChangeColumnType {
                    column_name,
                    new_type,
                } => {
                    if new_type.contains(';') || new_type.contains("--") || new_type.contains("/*")
                    {
                        anyhow::bail!("Invalid character in data type for column {}", column_name);
                    }
                }
                _ => {}
            }
            let cql = match op {
                AlterTableOperation::AddColumn { column } => {
                    format!(
                        "ALTER TABLE {} ADD {} {}",
                        qualified,
                        quote_ident(&column.name),
                        column.data_type
                    )
                }
                AlterTableOperation::DropColumn { column_name } => {
                    format!(
                        "ALTER TABLE {} DROP {}",
                        qualified,
                        quote_ident(column_name)
                    )
                }
                AlterTableOperation::RenameColumn { old_name, new_name } => {
                    format!(
                        "ALTER TABLE {} RENAME {} TO {}",
                        qualified,
                        quote_ident(old_name),
                        quote_ident(new_name)
                    )
                }
                AlterTableOperation::ChangeColumnType {
                    column_name,
                    new_type,
                } => {
                    format!(
                        "ALTER TABLE {} ALTER {} TYPE {}",
                        qualified,
                        quote_ident(column_name),
                        new_type
                    )
                }
                AlterTableOperation::RenameTable { new_name } => {
                    return Err(anyhow!(
                        "Cassandra does not support renaming tables (attempted rename to '{}')",
                        new_name
                    ));
                }
                AlterTableOperation::SetNullable { .. }
                | AlterTableOperation::SetDefault { .. } => {
                    continue;
                }
            };
            self.session
                .query_unpaged(cql.as_str(), &[])
                .await
                .map_err(|e| anyhow!("{}", e))?;
        }
        Ok(())
    }

    async fn drop_object(
        &self,
        database: &str,
        _schema: &str,
        object_name: &str,
        object_type: &str,
    ) -> Result<()> {
        let cql = match object_type.to_uppercase().as_str() {
            "TABLE" => format!(
                "DROP TABLE {}.{}",
                quote_ident(database),
                quote_ident(object_name)
            ),
            "INDEX" => format!(
                "DROP INDEX {}.{}",
                quote_ident(database),
                quote_ident(object_name)
            ),
            _ => return Err(anyhow!("Cannot drop object of type '{}'", object_type)),
        };
        self.session
            .query_unpaged(cql.as_str(), &[])
            .await
            .map_err(|e| anyhow!("{}", e))?;
        Ok(())
    }

    async fn truncate_table(&self, database: &str, _schema: &str, table_name: &str) -> Result<()> {
        let cql = format!(
            "TRUNCATE {}.{}",
            quote_ident(database),
            quote_ident(table_name)
        );
        self.session
            .query_unpaged(cql.as_str(), &[])
            .await
            .map_err(|e| anyhow!("{}", e))?;
        Ok(())
    }

    async fn import_data(
        &self,
        database: &str,
        _schema: &str,
        table: &str,
        columns: &[String],
        rows: &[Vec<serde_json::Value>],
    ) -> Result<u64> {
        let qualified = format!("{}.{}", quote_ident(database), quote_ident(table));
        let col_list: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
        let col_str = col_list.join(", ");
        let mut count = 0u64;

        const BATCH_SIZE: usize = 50;
        for chunk in rows.chunks(BATCH_SIZE) {
            if chunk.len() == 1 {
                let vals: Vec<String> = chunk[0].iter().map(json_to_cql_literal).collect();
                let cql = format!(
                    "INSERT INTO {} ({}) VALUES ({})",
                    qualified,
                    col_str,
                    vals.join(", ")
                );
                self.session
                    .query_unpaged(cql.as_str(), &[])
                    .await
                    .map_err(|e| anyhow!("{}", e))?;
                count += 1;
            } else {
                let stmts: Vec<String> = chunk
                    .iter()
                    .map(|row| {
                        let vals: Vec<String> = row.iter().map(json_to_cql_literal).collect();
                        format!(
                            "INSERT INTO {} ({}) VALUES ({})",
                            qualified,
                            col_str,
                            vals.join(", ")
                        )
                    })
                    .collect();
                let batch_cql = format!("BEGIN BATCH\n{}\nAPPLY BATCH", stmts.join(";\n"));
                self.session
                    .query_unpaged(batch_cql.as_str(), &[])
                    .await
                    .map_err(|e| anyhow!("{}", e))?;
                count += chunk.len() as u64;
            }
        }
        Ok(count)
    }

    async fn get_server_activity(&self) -> Result<Vec<ServerActivity>> {
        Ok(vec![])
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
        Ok(QueryStatsResponse {
            available: false,
            kind: QueryStatsKind::EngineUnsupported,
            message: Some("Query statistics are not available for Cassandra/ScyllaDB".to_string()),
            entries: vec![],
        })
    }

    async fn cancel_query(&self, _pid: &str) -> Result<()> {
        Err(anyhow!(
            "Query cancellation is not supported for Cassandra/ScyllaDB"
        ))
    }

    async fn list_roles(&self) -> Result<Vec<RoleInfo>> {
        let result = self
            .session
            .query_unpaged(
                "SELECT role, is_superuser, can_login FROM system_auth.roles",
                &[],
            )
            .await;

        match result {
            Ok(qr) => {
                let qr = qr.into_rows_result().map_err(|e| anyhow!("{}", e))?;
                let mut roles = Vec::new();
                if let Ok(rows) = qr.rows::<(String, bool, bool)>() {
                    for (name, is_superuser, can_login) in rows.flatten() {
                        roles.push(RoleInfo {
                            name,
                            is_superuser,
                            can_login,
                            can_create_db: false,
                            can_create_role: is_superuser,
                            is_replication: false,
                            connection_limit: -1,
                            valid_until: None,
                            member_of: vec![],
                        });
                    }
                }
                Ok(roles)
            }
            Err(_) => Ok(vec![]),
        }
    }

    async fn create_role(&self, req: &CreateRoleRequest) -> Result<()> {
        let mut cql = format!("CREATE ROLE '{}'", req.name.replace('\'', "''"));
        let mut with_parts = Vec::new();

        if let Some(ref pw) = req.password {
            with_parts.push(format!("PASSWORD = '{}'", pw.replace('\'', "''")));
        }
        with_parts.push(format!("LOGIN = {}", req.can_login));
        with_parts.push(format!("SUPERUSER = {}", req.is_superuser));

        if !with_parts.is_empty() {
            cql.push_str(&format!(" WITH {}", with_parts.join(" AND ")));
        }

        self.session
            .query_unpaged(cql.as_str(), &[])
            .await
            .map_err(|e| anyhow!("{}", e))?;
        Ok(())
    }

    async fn drop_role(&self, name: &str) -> Result<()> {
        let cql = format!("DROP ROLE '{}'", name.replace('\'', "''"));
        self.session
            .query_unpaged(cql.as_str(), &[])
            .await
            .map_err(|e| anyhow!("{}", e))?;
        Ok(())
    }

    async fn alter_role(&self, req: &AlterRoleRequest) -> Result<()> {
        let mut with_parts = Vec::new();

        if let Some(ref pw) = req.password {
            with_parts.push(format!("PASSWORD = '{}'", pw.replace('\'', "''")));
        }
        if let Some(login) = req.can_login {
            with_parts.push(format!("LOGIN = {}", login));
        }
        if let Some(su) = req.is_superuser {
            with_parts.push(format!("SUPERUSER = {}", su));
        }

        if with_parts.is_empty() {
            return Ok(());
        }

        let cql = format!(
            "ALTER ROLE '{}' WITH {}",
            req.name.replace('\'', "''"),
            with_parts.join(" AND ")
        );
        self.session
            .query_unpaged(cql.as_str(), &[])
            .await
            .map_err(|e| anyhow!("{}", e))?;
        Ok(())
    }

    async fn test_connection(&self) -> Result<bool> {
        self.session
            .query_unpaged("SELECT now() FROM system.local", &[])
            .await
            .map_err(|e| anyhow!("{}", e))?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_ident_simple() {
        assert_eq!(quote_ident("col"), r#""col""#);
    }

    #[test]
    fn quote_ident_with_double_quote() {
        assert_eq!(quote_ident(r#"col"umn"#), r#""col""umn""#);
    }

    #[test]
    fn quote_ident_empty() {
        assert_eq!(quote_ident(""), r#""""#);
    }

    #[test]
    fn quote_ident_prevents_injection() {
        let evil = r#""; DROP KEYSPACE test; --"#;
        let quoted = quote_ident(evil);
        assert!(quoted.starts_with('"'));
        assert!(quoted.ends_with('"'));
    }

    #[test]
    fn json_to_cql_literal_null() {
        assert_eq!(json_to_cql_literal(&serde_json::Value::Null), "NULL");
    }

    #[test]
    fn json_to_cql_literal_bool() {
        assert_eq!(json_to_cql_literal(&serde_json::Value::Bool(true)), "true");
        assert_eq!(
            json_to_cql_literal(&serde_json::Value::Bool(false)),
            "false"
        );
    }

    #[test]
    fn json_to_cql_literal_number() {
        assert_eq!(
            json_to_cql_literal(&serde_json::Value::Number(42i64.into())),
            "42"
        );
    }

    #[test]
    fn json_to_cql_literal_string() {
        assert_eq!(
            json_to_cql_literal(&serde_json::Value::String("hello".into())),
            "'hello'"
        );
    }

    #[test]
    fn json_to_cql_literal_string_escapes_quotes() {
        assert_eq!(
            json_to_cql_literal(&serde_json::Value::String("O'Brien".into())),
            "'O''Brien'"
        );
    }

    #[test]
    fn json_to_cql_literal_string_injection_attempt() {
        let val = json_to_cql_literal(&serde_json::Value::String("'; DROP TABLE t; --".into()));
        assert_eq!(val, "'''; DROP TABLE t; --'");
    }

    #[test]
    fn json_to_cql_literal_array() {
        let val = serde_json::json!([1, 2, 3]);
        let lit = json_to_cql_literal(&val);
        assert_eq!(lit, "[1, 2, 3]");
    }

    #[test]
    fn json_to_cql_literal_map() {
        let val = serde_json::json!({"key": "val"});
        let lit = json_to_cql_literal(&val);
        assert!(lit.starts_with('{'));
        assert!(lit.ends_with('}'));
        assert!(lit.contains("'key'"));
        assert!(lit.contains("'val'"));
    }

    #[test]
    fn system_keyspaces_filtered() {
        for ks in SYSTEM_KEYSPACES {
            assert!([
                "system",
                "system_auth",
                "system_distributed",
                "system_distributed_everywhere",
                "system_schema",
                "system_traces",
                "system_views",
                "system_virtual_schema"
            ]
            .contains(ks));
        }
    }

    #[test]
    fn cql_value_to_json_text() {
        use scylla::value::CqlValue;
        let val = cql_value_to_json("", &CqlValue::Text("hello".to_string()));
        assert_eq!(val, serde_json::json!("hello"));
    }

    #[test]
    fn cql_value_to_json_boolean() {
        use scylla::value::CqlValue;
        assert_eq!(
            cql_value_to_json("", &CqlValue::Boolean(true)),
            serde_json::json!(true)
        );
        assert_eq!(
            cql_value_to_json("", &CqlValue::Boolean(false)),
            serde_json::json!(false)
        );
    }

    #[test]
    fn cql_value_to_json_int() {
        use scylla::value::CqlValue;
        assert_eq!(
            cql_value_to_json("", &CqlValue::Int(42)),
            serde_json::json!(42)
        );
    }

    #[test]
    fn cql_value_to_json_bigint() {
        use scylla::value::CqlValue;
        assert_eq!(
            cql_value_to_json("", &CqlValue::BigInt(9999999999i64)),
            serde_json::json!(9999999999i64)
        );
    }

    #[test]
    fn cql_value_to_json_float() {
        use scylla::value::CqlValue;
        let val = cql_value_to_json("", &CqlValue::Float(1.5));
        assert!((val.as_f64().unwrap() - 1.5).abs() < 0.01);
    }

    #[test]
    fn cql_value_to_json_double() {
        use scylla::value::CqlValue;
        let val = cql_value_to_json("", &CqlValue::Double(7.25));
        assert!((val.as_f64().unwrap() - 7.25).abs() < 0.001);
    }

    #[test]
    fn cql_value_to_json_empty() {
        use scylla::value::CqlValue;
        assert_eq!(
            cql_value_to_json("", &CqlValue::Empty),
            serde_json::Value::Null
        );
    }

    #[test]
    fn cql_value_to_json_blob() {
        use scylla::value::CqlValue;
        let val = cql_value_to_json("", &CqlValue::Blob(vec![0xDE, 0xAD]));
        assert_eq!(val, serde_json::json!("0xdead"));
    }

    #[test]
    fn cql_value_to_json_list() {
        use scylla::value::CqlValue;
        let val = cql_value_to_json(
            "",
            &CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2)]),
        );
        assert_eq!(val, serde_json::json!([1, 2]));
    }
}

#[cfg(test)]
mod value_tests {
    use super::*;
    use serde_json::json;

    fn lit(t: &str, v: serde_json::Value) -> String {
        typed_cql_literal(Some(t), &v, &UdtDefs::new()).unwrap()
    }

    #[test]
    fn parses_cql_types() {
        assert_eq!(
            parse_cql_type("frozen<map<int, frozen<set<uuid>>>>"),
            CqlType::Map(
                Box::new(CqlType::Native("int".into())),
                Box::new(CqlType::Set(Box::new(CqlType::Native("uuid".into()))))
            )
        );
        assert_eq!(
            parse_cql_type("frozen<tuple<int, text>>"),
            CqlType::Tuple(vec![
                CqlType::Native("int".into()),
                CqlType::Native("text".into())
            ])
        );
        assert_eq!(parse_cql_type("frozen<addr>"), CqlType::Udt("addr".into()));
        assert_eq!(
            parse_cql_type("vector<float, 3>"),
            CqlType::List(Box::new(CqlType::Native("float".into())))
        );
    }

    #[test]
    fn literals_follow_the_column_type() {
        let u = "6f1c0f3e-3b8a-4e4f-9a51-0c9d2b7a1e11";
        assert_eq!(lit("uuid", json!(u)), u);
        assert_eq!(lit("timeuuid", json!(u)), u);
        assert!(typed_cql_literal(Some("uuid"), &json!("x'); DROP"), &UdtDefs::new()).is_err());
        assert_eq!(lit("blob", json!("0x00FF")), "0x00ff");
        assert_eq!(lit("set<int>", json!([1, 2])), "{1, 2}");
        assert_eq!(lit("set<int>", json!("[3]")), "{3}");
        assert_eq!(lit("list<text>", json!(["a'b"])), "['a''b']");
        assert_eq!(lit("map<int, text>", json!({"1": "one"})), "{1: 'one'}");
        assert_eq!(lit("frozen<tuple<int, text>>", json!([7, "x"])), "(7, 'x')");
        assert_eq!(lit("duration", json!("1mo2d3h")), "1mo2d3h");
        assert!(typed_cql_literal(Some("duration"), &json!("1 hour"), &UdtDefs::new()).is_err());
        assert_eq!(lit("double", json!("NaN")), "NaN");
        assert_eq!(lit("float", json!("-Infinity")), "-Infinity");
        assert_eq!(lit("text", json!(5)), "'5'");
        assert!(typed_cql_literal(Some("set<int>"), &json!("nope"), &UdtDefs::new()).is_err());
    }

    #[test]
    fn udt_literals_use_field_types() {
        let mut udts = UdtDefs::new();
        udts.insert(
            "addr".into(),
            vec![
                ("street".into(), CqlType::Native("text".into())),
                ("id".into(), CqlType::Native("uuid".into())),
            ],
        );
        let u = "6f1c0f3e-3b8a-4e4f-9a51-0c9d2b7a1e11";
        assert_eq!(
            typed_cql_literal(
                Some("frozen<addr>"),
                &json!({"id": u, "street": "Main"}),
                &udts
            )
            .unwrap(),
            format!("{{\"id\": {u}, \"street\": 'Main'}}")
        );
    }

    #[test]
    fn temporal_and_duration_text() {
        assert_eq!(cql_date_text(1u32 << 31), "1970-01-01");
        assert_eq!(cql_date_text((1u32 << 31) + 19_782), "2024-02-29");
        assert_eq!(cql_time_text(0), "00:00:00");
        assert_eq!(cql_time_text(29_574_123_000_000), "08:12:54.123");
        assert_eq!(cql_timestamp_text(0), "1970-01-01 00:00:00.000Z");
        assert_eq!(
            cql_duration_text(14, 2, 3_600_000_000_000 + 6_000_000),
            "1y2mo2d1h6ms"
        );
        assert_eq!(cql_duration_text(0, 0, -7_200_000_000_000), "-2h");
        assert_eq!(cql_duration_text(0, 0, 0), "0s");
        assert!(is_cql_duration_literal("1y2mo3w4d5h6m7s8ms9us10ns"));
        assert!(is_cql_duration_literal("P1DT2H"));
        assert!(!is_cql_duration_literal("1h; DROP"));
        assert!(!is_cql_duration_literal(""));
    }

    #[test]
    fn non_finite_and_float4_values() {
        use scylla::value::CqlValue;
        assert_eq!(cql_value_to_json("", &CqlValue::Float(0.1)), json!(0.1));
        assert_eq!(
            cql_value_to_json("", &CqlValue::Double(f64::NAN)),
            json!("NaN")
        );
        assert_eq!(
            cql_value_to_json(
                "",
                &CqlValue::Map(vec![(CqlValue::Int(1), CqlValue::Int(2))])
            ),
            json!({"1": 2})
        );
    }

    #[test]
    fn counter_deltas() {
        assert_eq!(counter_delta(&json!(5), &json!(12)).unwrap(), 7);
        assert_eq!(counter_delta(&json!(null), &json!("3")).unwrap(), 3);
        assert!(counter_delta(&json!(1), &json!("x")).is_err());
        assert_eq!(counter_assignment("c", -2), "\"c\" = \"c\" - 2");
    }
}
