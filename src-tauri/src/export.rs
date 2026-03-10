use serde_json::Value;

pub fn to_csv(columns: &[String], rows: &[Vec<Value>]) -> String {
    let mut out = String::new();

    out.push_str(&columns.iter().map(|c| escape_csv(c)).collect::<Vec<_>>().join(","));
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
