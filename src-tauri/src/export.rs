use serde_json::Value;

pub fn to_csv(columns: &[String], rows: &[Vec<Value>]) -> String {
    let mut out = String::new();

    out.push_str(
        &columns
            .iter()
            .map(|c| escape_csv(c))
            .collect::<Vec<_>>()
            .join(","),
    );
    out.push('\n');

    for row in rows {
        let line: Vec<String> = row
            .iter()
            .map(|v| match v {
                Value::Null => String::new(),
                Value::String(s) => escape_csv(s),
                Value::Bool(b) => b.to_string(),
                Value::Number(n) => n.to_string(),
                _ => escape_csv(&v.to_string()),
            })
            .collect();
        out.push_str(&line.join(","));
        out.push('\n');
    }
    out
}

pub fn to_json(columns: &[String], rows: &[Vec<Value>]) -> String {
    let objects: Vec<serde_json::Map<String, Value>> = rows
        .iter()
        .map(|row| {
            columns
                .iter()
                .zip(row.iter())
                .map(|(col, val)| (col.clone(), val.clone()))
                .collect()
        })
        .collect();

    serde_json::to_string_pretty(&objects).unwrap_or_else(|_| "[]".to_string())
}

pub fn to_sql_inserts(table_name: &str, columns: &[String], rows: &[Vec<Value>]) -> String {
    let mut out = String::new();
    let cols_str = columns
        .iter()
        .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");

    for row in rows {
        let vals: Vec<String> = row
            .iter()
            .map(|v| match v {
                Value::Null => "NULL".to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Number(n) => n.to_string(),
                Value::String(s) => format!("'{}'", s.replace('\'', "''")),
                _ => format!("'{}'", v.to_string().replace('\'', "''")),
            })
            .collect();

        out.push_str(&format!(
            "INSERT INTO \"{}\" ({}) VALUES ({});\n",
            table_name.replace('"', "\"\""),
            cols_str,
            vals.join(", ")
        ));
    }
    out
}

fn escape_csv(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn csv_basic() {
        let cols = ["a".to_string(), "b".to_string()];
        let rows = vec![
            vec![Value::Number(1i64.into()), Value::String("x".into())],
            vec![Value::Null, Value::String("with,comma".into())],
        ];
        let out = to_csv(&cols, &rows);
        assert!(out.contains("a,b"));
        assert!(out.contains("1,x"));
        assert!(out.contains(r#""with,comma""#));
    }

    #[test]
    fn csv_empty_rows() {
        let cols = ["id".to_string()];
        let out = to_csv(&cols, &[]);
        assert_eq!(out, "id\n");
    }

    #[test]
    fn csv_empty_columns_and_rows() {
        let out = to_csv(&[], &[]);
        assert_eq!(out, "\n");
    }

    #[test]
    fn csv_quotes_in_value() {
        let cols = ["v".to_string()];
        let rows = vec![vec![Value::String("say \"hello\"".into())]];
        let out = to_csv(&cols, &rows);
        assert!(out.contains(r#""say ""hello"""#));
    }

    #[test]
    fn csv_newline_in_value() {
        let cols = ["v".to_string()];
        let rows = vec![vec![Value::String("line1\nline2".into())]];
        let out = to_csv(&cols, &rows);
        assert!(out.contains(r#""line1"#));
    }

    #[test]
    fn csv_boolean_value() {
        let cols = ["flag".to_string()];
        let rows = vec![vec![Value::Bool(true)], vec![Value::Bool(false)]];
        let out = to_csv(&cols, &rows);
        assert!(out.contains("true"));
        assert!(out.contains("false"));
    }

    #[test]
    fn csv_array_value() {
        let cols = ["arr".to_string()];
        let rows = vec![vec![Value::Array(vec![Value::Number(1i64.into())])]];
        let out = to_csv(&cols, &rows);
        assert!(out.contains("[1]"));
    }

    #[test]
    fn csv_null_renders_empty() {
        let cols = ["a".to_string(), "b".to_string()];
        let rows = vec![vec![Value::Null, Value::String("x".into())]];
        let out = to_csv(&cols, &rows);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines[1], ",x");
    }

    #[test]
    fn json_basic() {
        let cols = ["id".to_string()];
        let rows = vec![vec![Value::Number(1i64.into())]];
        let out = to_json(&cols, &rows);
        assert!(out.contains("\"id\""));
        assert!(out.contains("1"));
    }

    #[test]
    fn json_empty_rows() {
        let cols = ["id".to_string()];
        let out = to_json(&cols, &[]);
        assert_eq!(out.trim(), "[]");
    }

    #[test]
    fn json_multiple_columns() {
        let cols = ["a".to_string(), "b".to_string()];
        let rows = vec![vec![
            Value::Number(1i64.into()),
            Value::String("hello".into()),
        ]];
        let out = to_json(&cols, &rows);
        let parsed: Vec<serde_json::Map<String, Value>> = serde_json::from_str(&out).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0]["a"], Value::Number(1i64.into()));
        assert_eq!(parsed[0]["b"], Value::String("hello".into()));
    }

    #[test]
    fn json_null_and_bool() {
        let cols = ["x".to_string(), "y".to_string()];
        let rows = vec![vec![Value::Null, Value::Bool(false)]];
        let out = to_json(&cols, &rows);
        let parsed: Vec<serde_json::Map<String, Value>> = serde_json::from_str(&out).unwrap();
        assert_eq!(parsed[0]["x"], Value::Null);
        assert_eq!(parsed[0]["y"], Value::Bool(false));
    }

    #[test]
    fn sql_inserts_basic() {
        let cols = ["name".to_string()];
        let rows = vec![vec![Value::String("O'Brien".into())]];
        let out = to_sql_inserts("users", &cols, &rows);
        assert!(out.contains("INSERT INTO \"users\""));
        assert!(out.contains("'O''Brien'"));
    }

    #[test]
    fn sql_inserts_empty_rows() {
        let cols = ["id".to_string()];
        let out = to_sql_inserts("t", &cols, &[]);
        assert_eq!(out, "");
    }

    #[test]
    fn sql_inserts_null_and_number() {
        let cols = ["a".to_string(), "b".to_string()];
        let rows = vec![vec![Value::Null, Value::Number(42i64.into())]];
        let out = to_sql_inserts("t", &cols, &rows);
        assert!(out.contains("NULL, 42"));
    }

    #[test]
    fn sql_inserts_bool() {
        let cols = ["flag".to_string()];
        let rows = vec![vec![Value::Bool(true)]];
        let out = to_sql_inserts("t", &cols, &rows);
        assert!(out.contains("true"));
    }

    #[test]
    fn sql_inserts_table_name_with_quote() {
        let cols = ["v".to_string()];
        let rows = vec![vec![Value::Number(1i64.into())]];
        let out = to_sql_inserts(r#"my"table"#, &cols, &rows);
        assert!(out.contains(r#""my""table""#));
    }

    #[test]
    fn sql_inserts_column_name_with_quote() {
        let cols = [r#"col"umn"#.to_string()];
        let rows = vec![vec![Value::Number(1i64.into())]];
        let out = to_sql_inserts("t", &cols, &rows);
        assert!(out.contains(r#""col""umn""#));
    }

    #[test]
    fn sql_inserts_multiple_rows() {
        let cols = ["id".to_string()];
        let rows = vec![
            vec![Value::Number(1i64.into())],
            vec![Value::Number(2i64.into())],
            vec![Value::Number(3i64.into())],
        ];
        let out = to_sql_inserts("t", &cols, &rows);
        assert_eq!(out.matches("INSERT INTO").count(), 3);
    }

    #[test]
    fn escape_csv_no_special() {
        assert_eq!(escape_csv("hello"), "hello");
    }

    #[test]
    fn escape_csv_with_comma() {
        assert_eq!(escape_csv("a,b"), r#""a,b""#);
    }

    #[test]
    fn escape_csv_with_double_quote() {
        assert_eq!(escape_csv(r#"say "hi""#), r#""say ""hi""""#);
    }

    #[test]
    fn escape_csv_with_newline() {
        assert_eq!(escape_csv("a\nb"), "\"a\nb\"");
    }

    #[test]
    fn escape_csv_with_carriage_return() {
        assert_eq!(escape_csv("a\rb"), "\"a\rb\"");
    }

    #[test]
    fn escape_csv_empty_string() {
        assert_eq!(escape_csv(""), "");
    }

    #[test]
    fn escape_csv_all_specials_combined() {
        let input = "a,b\n\"c\r";
        let escaped = escape_csv(input);
        assert!(escaped.starts_with('"'));
        assert!(escaped.ends_with('"'));
    }

    #[test]
    fn csv_object_value() {
        let cols = ["data".to_string()];
        let rows = vec![vec![Value::Object(serde_json::Map::from_iter(vec![(
            "key".to_string(),
            Value::String("val".into()),
        )]))]];
        let out = to_csv(&cols, &rows);
        assert!(out.contains("key"));
        assert!(out.contains("val"));
    }

    #[test]
    fn json_array_value() {
        let cols = ["arr".to_string()];
        let rows = vec![vec![Value::Array(vec![
            Value::Number(1i64.into()),
            Value::Number(2i64.into()),
        ])]];
        let out = to_json(&cols, &rows);
        let parsed: Vec<serde_json::Map<String, Value>> = serde_json::from_str(&out).unwrap();
        assert_eq!(
            parsed[0]["arr"],
            Value::Array(vec![Value::Number(1i64.into()), Value::Number(2i64.into())])
        );
    }

    #[test]
    fn json_multiple_rows() {
        let cols = ["id".to_string(), "name".to_string()];
        let rows = vec![
            vec![Value::Number(1i64.into()), Value::String("Alice".into())],
            vec![Value::Number(2i64.into()), Value::String("Bob".into())],
        ];
        let out = to_json(&cols, &rows);
        let parsed: Vec<serde_json::Map<String, Value>> = serde_json::from_str(&out).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0]["name"], Value::String("Alice".into()));
        assert_eq!(parsed[1]["name"], Value::String("Bob".into()));
    }

    #[test]
    fn sql_inserts_array_value() {
        let cols = ["arr".to_string()];
        let rows = vec![vec![Value::Array(vec![Value::Number(1i64.into())])]];
        let out = to_sql_inserts("t", &cols, &rows);
        assert!(out.contains("[1]"));
        assert!(out.contains("'"));
    }

    #[test]
    fn sql_inserts_object_value() {
        let cols = ["data".to_string()];
        let rows = vec![vec![Value::Object(serde_json::Map::from_iter(vec![(
            "k".to_string(),
            Value::String("v".into()),
        )]))]];
        let out = to_sql_inserts("t", &cols, &rows);
        assert!(out.contains("INSERT INTO"));
    }

    #[test]
    fn csv_number_types() {
        let cols = ["int".to_string(), "float".to_string()];
        let n = serde_json::Number::from_f64(3.14).unwrap();
        let rows = vec![vec![Value::Number(42i64.into()), Value::Number(n)]];
        let out = to_csv(&cols, &rows);
        assert!(out.contains("42"));
        assert!(out.contains("3.14"));
    }
}
