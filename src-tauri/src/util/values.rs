//! Cell value helpers shared by the drivers: JSON encoding of floats
//! and binary values, and parsing the binary display formats back into
//! bytes for write-back.

use serde_json::Value;

/// Text sent for a non-finite float, matching PostgreSQL's spelling
/// (`NaN`, `Infinity`, `-Infinity`), which every engine that can store
/// these values also accepts back as a quoted literal.
pub fn non_finite_float_text(v: f64) -> &'static str {
    if v.is_nan() {
        "NaN"
    } else if v > 0.0 {
        "Infinity"
    } else {
        "-Infinity"
    }
}

/// JSON for a double-precision cell: a number when finite, otherwise
/// the string from [`non_finite_float_text`] (JSON has no NaN/Inf, and
/// `serde_json` would silently turn them into `null`).
pub fn f64_to_json(v: f64) -> Value {
    if v.is_finite() {
        if let Some(n) = serde_json::Number::from_f64(v) {
            return Value::Number(n);
        }
    }
    Value::String(non_finite_float_text(v).to_string())
}

/// JSON for a single-precision cell. Widening `0.1f32` to f64 gives
/// `0.10000000149011612`; the shortest decimal that round-trips the f32
/// (`0.1`) is what the database itself prints, so use that.
pub fn f32_to_json(v: f32) -> Value {
    if !v.is_finite() {
        return Value::String(non_finite_float_text(f64::from(v)).to_string());
    }
    match v.to_string().parse::<f64>() {
        Ok(f) => f64_to_json(f),
        Err(_) => f64_to_json(f64::from(v)),
    }
}

/// Lowercase hex digits of `bytes`, no prefix.
pub fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{:02x}", b);
    }
    s
}

/// Display formats a binary cell can arrive in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HexDisplay {
    /// `0x00ff` (MySQL family, SQL Server, Cassandra).
    ZeroX,
    /// `X'00ff'` (SQLite).
    SqlQuoted,
}

/// Hex digits (validated, lowercase, even length) of a value written in
/// a binary display format, or `None` when `s` is not in that format.
///
/// An odd number of hex digits is reported as an error rather than
/// treated as text, so a typo such as `0x0ff` is not silently stored as
/// the five characters `0`, `x`, `0`, `f`, `f`.
pub fn parse_hex_display(s: &str, format: HexDisplay) -> Result<Option<String>, String> {
    let t = s.trim();
    let digits = match format {
        HexDisplay::ZeroX => t.strip_prefix("0x").or_else(|| t.strip_prefix("0X")),
        HexDisplay::SqlQuoted => t
            .strip_prefix("X'")
            .or_else(|| t.strip_prefix("x'"))
            .and_then(|r| r.strip_suffix('\'')),
    };
    let Some(digits) = digits else {
        return Ok(None);
    };
    if !digits.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Ok(None);
    }
    if digits.len() % 2 != 0 {
        return Err(format!(
            "Invalid binary value {:?}: a hex literal needs an even number of digits",
            s
        ));
    }
    Ok(Some(digits.to_ascii_lowercase()))
}

/// Hex digits for a value written to a binary column: the bytes of a
/// value in the display format, or else the UTF-8 bytes of the text the
/// user typed.
pub fn binary_value_hex(s: &str, format: HexDisplay) -> Result<String, String> {
    Ok(match parse_hex_display(s, format)? {
        Some(hex) => hex,
        None => hex_encode(s.as_bytes()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn f64_non_finite_are_strings() {
        assert_eq!(f64_to_json(f64::NAN), json!("NaN"));
        assert_eq!(f64_to_json(f64::INFINITY), json!("Infinity"));
        assert_eq!(f64_to_json(f64::NEG_INFINITY), json!("-Infinity"));
        assert_eq!(f64_to_json(1.5), json!(1.5));
    }

    #[test]
    fn f32_uses_shortest_round_trip_repr() {
        assert_eq!(f32_to_json(0.1), json!(0.1));
        assert_eq!(f32_to_json(3.4028235e38), json!(3.4028235e38));
        assert_eq!(f32_to_json(-1.25), json!(-1.25));
        assert_eq!(f32_to_json(f32::NAN), json!("NaN"));
        assert_eq!(f32_to_json(f32::NEG_INFINITY), json!("-Infinity"));
    }

    #[test]
    fn hex_display_parsing() {
        use HexDisplay::*;
        assert_eq!(
            parse_hex_display("0x00FF80", ZeroX),
            Ok(Some("00ff80".into()))
        );
        assert_eq!(parse_hex_display("0x", ZeroX), Ok(Some(String::new())));
        assert_eq!(
            parse_hex_display("X'00ff'", SqlQuoted),
            Ok(Some("00ff".into()))
        );
        assert_eq!(parse_hex_display("hello", ZeroX), Ok(None));
        assert_eq!(parse_hex_display("0xzz", ZeroX), Ok(None));
        assert_eq!(parse_hex_display("X'00ff", SqlQuoted), Ok(None));
        assert!(parse_hex_display("0x0ff", ZeroX).is_err());
        // Display format of another engine is just text.
        assert_eq!(parse_hex_display("0x00", SqlQuoted), Ok(None));
    }

    #[test]
    fn binary_value_falls_back_to_text_bytes() {
        assert_eq!(
            binary_value_hex("0x00ff80", HexDisplay::ZeroX).unwrap(),
            "00ff80"
        );
        assert_eq!(binary_value_hex("Ab", HexDisplay::ZeroX).unwrap(), "4162");
    }
}
