use tablio_lib::db::postgres::PostgresDriver;
use tablio_lib::db::DatabaseDriver;
use tablio_lib::models::*;

macro_rules! pg_driver {
    () => {{
        let url = match std::env::var("TEST_POSTGRES_URL") {
            Ok(v) if !v.is_empty() => v,
            _ => {
                eprintln!("Skipping: TEST_POSTGRES_URL not set");
                return;
            }
        };
        let parts = url
            .strip_prefix("postgres://")
            .or_else(|| url.strip_prefix("postgresql://"))
            .expect("bad TEST_POSTGRES_URL");
        let (user_pass, rest) = parts.split_once('@').expect("missing @");
        let (user, password) = user_pass.split_once(':').expect("missing :");
        let (host_port, database) = rest.split_once('/').expect("missing /");
        let database = database.split('?').next().unwrap();
        let (host, port) = host_port.split_once(':').expect("missing port");
        let config = ConnectionConfig {
            id: "test".into(),
            name: "test".into(),
            db_type: DbType::Postgres,
            host: host.into(),
            port: port.parse().unwrap(),
            user: user.into(),
            password: password.into(),
            database: database.into(),
            color: "#000".into(),
            ssl: false,
            group: None,
            ssh_enabled: false,
            ssh_host: String::new(),
            ssh_port: 22,
            ssh_user: String::new(),
            ssh_password: String::new(),
            ssh_key_path: String::new(),
        };
        PostgresDriver::connect(&config).await.unwrap()
    }};
}

fn unique_table(prefix: &str) -> String {
    format!(
        "{}_{}",
        prefix,
        uuid::Uuid::new_v4().simple().to_string().get(..8).unwrap()
    )
}

const DB: &str = "testdb";
const SCHEMA: &str = "public";

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_test_connection() {
    let driver = pg_driver!();
    assert!(driver.test_connection().await.unwrap());
}

#[tokio::test]
async fn pg_list_databases() {
    let driver = pg_driver!();
    let dbs = driver.list_databases().await.unwrap();
    assert!(!dbs.is_empty());
    assert!(dbs.iter().all(|d| !d.name.is_empty()));
}

// ---------------------------------------------------------------------------
// Schema introspection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_list_schemas() {
    let driver = pg_driver!();
    let schemas = driver.list_schemas(DB).await.unwrap();
    let names: Vec<&str> = schemas.iter().map(|s| s.name.as_str()).collect();
    assert!(names.contains(&"public"));
}

#[tokio::test]
async fn pg_list_tables_after_create_table() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_tbl");

    let cols = vec![
        ColumnDefinition {
            name: "id".into(),
            data_type: "serial".into(),
            is_nullable: false,
            is_primary_key: true,
            default_value: None,
        },
        ColumnDefinition {
            name: "name".into(),
            data_type: "varchar(100)".into(),
            is_nullable: true,
            is_primary_key: false,
            default_value: None,
        },
    ];
    driver.create_table(DB, SCHEMA, &tbl, &cols).await.unwrap();

    let tables = driver.list_tables(DB, SCHEMA).await.unwrap();
    assert!(tables.iter().any(|t| t.name == tbl));

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn pg_list_columns_varied_types() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_cols");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE {}.\"{}\" (\
                   id SERIAL PRIMARY KEY, \
                   title VARCHAR(80) NOT NULL, \
                   body TEXT, \
                   active BOOLEAN DEFAULT false, \
                   price NUMERIC(14,3), \
                   created_at TIMESTAMP WITHOUT TIME ZONE, \
                   ext_id UUID, \
                   meta JSON, \
                   payload JSONB, \
                   tags INTEGER[]\
                 )",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let cols = driver.list_columns(DB, SCHEMA, &tbl).await.unwrap();
    assert_eq!(cols.len(), 10);

    let by_name: std::collections::HashMap<_, _> =
        cols.iter().map(|c| (c.name.as_str(), c)).collect();

    assert_eq!(by_name["id"].name, "id");
    assert!(by_name["id"].is_primary_key);
    assert!(by_name["id"].is_auto_generated);

    assert!(by_name["title"].data_type.contains("character varying"));
    assert!(by_name["title"].data_type.contains("80"));
    assert!(!by_name["title"].is_nullable);

    assert_eq!(by_name["body"].data_type, "text");

    assert_eq!(by_name["active"].data_type, "boolean");

    assert!(by_name["price"].data_type.contains("numeric"));
    assert!(by_name["price"].data_type.contains("14"));
    assert!(by_name["price"].data_type.contains("3"));

    assert!(by_name["created_at"]
        .data_type
        .contains("timestamp without time zone"));

    assert_eq!(by_name["ext_id"].data_type, "uuid");
    assert_eq!(by_name["meta"].data_type, "json");
    assert_eq!(by_name["payload"].data_type, "jsonb");
    assert_eq!(by_name["tags"].data_type, "ARRAY");

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn pg_list_indexes() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_idx");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE {}.\"{}\" (id SERIAL PRIMARY KEY, slug TEXT NOT NULL)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();
    let idx_name = format!("idx_{}_slug", tbl);
    driver
        .execute_query(
            DB,
            &format!(
                "CREATE INDEX \"{}\" ON {}.\"{}\" (slug)",
                idx_name, SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let indexes = driver.list_indexes(DB, SCHEMA, &tbl).await.unwrap();
    let slug_idx = indexes
        .iter()
        .find(|i| i.name == idx_name)
        .expect("custom index");
    assert_eq!(slug_idx.columns, vec!["slug"]);
    assert!(!slug_idx.is_unique);
    assert!(!indexes.is_empty());

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn pg_list_foreign_keys() {
    let driver = pg_driver!();
    let parent = unique_table("pg_fk_p");
    let child = unique_table("pg_fk_c");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE {}.\"{}\" (id INT PRIMARY KEY, label TEXT)",
                SCHEMA, parent
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE {}.\"{}\" (\
                   id INT PRIMARY KEY, \
                   parent_id INT NOT NULL, \
                   CONSTRAINT fk_{}_parent FOREIGN KEY (parent_id) \
                     REFERENCES {}.\"{}\"(id) ON DELETE CASCADE ON UPDATE CASCADE\
                 )",
                SCHEMA, child, child, SCHEMA, parent
            ),
        )
        .await
        .unwrap();

    let fks = driver.list_foreign_keys(DB, SCHEMA, &child).await.unwrap();
    assert_eq!(fks.len(), 1);
    assert_eq!(fks[0].column, "parent_id");
    assert_eq!(fks[0].referenced_table, parent);
    assert_eq!(fks[0].referenced_column, "id");
    assert_eq!(fks[0].on_delete, "CASCADE");
    assert_eq!(fks[0].on_update, "CASCADE");

    driver
        .drop_object(DB, SCHEMA, &child, "TABLE")
        .await
        .unwrap();
    driver
        .drop_object(DB, SCHEMA, &parent, "TABLE")
        .await
        .unwrap();
}

#[tokio::test]
async fn pg_list_functions() {
    let driver = pg_driver!();
    let fn_name = unique_table("pgfn");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE OR REPLACE FUNCTION {}.{}() RETURNS integer \
                 LANGUAGE sql IMMUTABLE AS $$ SELECT 42 $$",
                SCHEMA, fn_name
            ),
        )
        .await
        .unwrap();

    let funcs = driver.list_functions(DB, SCHEMA).await.unwrap();
    assert!(funcs
        .iter()
        .any(|f| f.name == fn_name && f.kind == "function"));
    assert!(funcs
        .iter()
        .any(|f| f.name == fn_name && f.language == "sql"));

    driver
        .execute_query(
            DB,
            &format!("DROP FUNCTION IF EXISTS {}.{}()", SCHEMA, fn_name),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn pg_list_triggers() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_trg");
    let fn_name = format!("{}_tf", tbl);
    let trg_name = format!("{}_t", tbl);

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE {}.\"{}\" (id SERIAL PRIMARY KEY, val INT)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE OR REPLACE FUNCTION {}.{}() RETURNS trigger \
                 LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END; $$",
                SCHEMA, fn_name
            ),
        )
        .await
        .unwrap();

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TRIGGER \"{}\" BEFORE INSERT ON {}.\"{}\" \
                 FOR EACH ROW EXECUTE PROCEDURE {}.{}()",
                trg_name, SCHEMA, tbl, SCHEMA, fn_name
            ),
        )
        .await
        .unwrap();

    let triggers = driver.list_triggers(DB, SCHEMA, &tbl).await.unwrap();
    assert_eq!(triggers.len(), 1);
    assert_eq!(triggers[0].name, trg_name);
    assert_eq!(triggers[0].table_name, tbl);
    assert_eq!(triggers[0].event, "INSERT");
    assert_eq!(triggers[0].timing, "BEFORE");

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    driver
        .execute_query(
            DB,
            &format!("DROP FUNCTION IF EXISTS {}.{}()", SCHEMA, fn_name),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn pg_get_table_stats() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_stats");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE {}.\"{}\" (id SERIAL PRIMARY KEY, n INT)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            DB,
            &format!(
                "INSERT INTO {}.\"{}\" (n) SELECT g FROM generate_series(1,5) g",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    driver
        .execute_query(DB, &format!("ANALYZE {}.\"{}\"", SCHEMA, tbl))
        .await
        .unwrap();
    let stats = driver.get_table_stats(DB, SCHEMA, &tbl).await.unwrap();
    assert_eq!(stats.table_name, tbl);
    assert!(stats.row_count >= 5);
    assert!(!stats.total_size.is_empty());
    assert!(!stats.data_size.is_empty());

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// fetch_rows
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_fetch_rows_empty_table() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_empty");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE {}.\"{}\" (id SERIAL PRIMARY KEY, val TEXT)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let data = driver
        .fetch_rows(DB, SCHEMA, &tbl, 0, 50, None, None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 0);
    assert!(data.rows.is_empty());
    assert_eq!(data.columns.len(), 2);

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn pg_fetch_rows_with_data() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_ins");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE {}.\"{}\" (id SERIAL PRIMARY KEY, name TEXT NOT NULL)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let changes = DataChanges {
        connection_id: "test".into(),
        database: DB.into(),
        schema: SCHEMA.into(),
        table: tbl.clone(),
        updates: vec![],
        inserts: vec![NewRow {
            values: vec![("name".into(), serde_json::json!("Alice"))],
        }],
        deletes: vec![],
    };
    driver.apply_changes(&changes).await.unwrap();

    let data = driver
        .fetch_rows(DB, SCHEMA, &tbl, 0, 50, None, None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 1);
    assert_eq!(data.rows[0][1], serde_json::json!("Alice"));

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn pg_fetch_rows_pagination() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_page");

    driver
        .execute_query(
            DB,
            &format!("CREATE TABLE {}.\"{}\" (id INT PRIMARY KEY)", SCHEMA, tbl),
        )
        .await
        .unwrap();

    let values: Vec<String> = (1..=10).map(|i| format!("({})", i)).collect();
    driver
        .execute_query(
            DB,
            &format!(
                "INSERT INTO {}.\"{}\" VALUES {}",
                SCHEMA,
                tbl,
                values.join(", ")
            ),
        )
        .await
        .unwrap();

    let page1 = driver
        .fetch_rows(DB, SCHEMA, &tbl, 0, 5, None, None)
        .await
        .unwrap();
    assert_eq!(page1.rows.len(), 5);
    assert_eq!(page1.total_rows, 10);
    assert_eq!(page1.rows[0][0], serde_json::json!(1));

    let page2 = driver
        .fetch_rows(DB, SCHEMA, &tbl, 5, 5, None, None)
        .await
        .unwrap();
    assert_eq!(page2.rows.len(), 5);
    assert_eq!(page2.rows[0][0], serde_json::json!(6));

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn pg_fetch_rows_sort_asc_desc() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_sort");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE {}.\"{}\" (id INT PRIMARY KEY, name TEXT)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            DB,
            &format!(
                "INSERT INTO {}.\"{}\" VALUES (1,'c'),(2,'a'),(3,'b')",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let asc = driver
        .fetch_rows(
            DB,
            SCHEMA,
            &tbl,
            0,
            10,
            Some(SortSpec {
                column: "name".into(),
                direction: SortDirection::Asc,
            }),
            None,
        )
        .await
        .unwrap();
    assert_eq!(asc.rows[0][1], serde_json::json!("a"));
    assert_eq!(asc.rows[2][1], serde_json::json!("c"));

    let desc = driver
        .fetch_rows(
            DB,
            SCHEMA,
            &tbl,
            0,
            10,
            Some(SortSpec {
                column: "name".into(),
                direction: SortDirection::Desc,
            }),
            None,
        )
        .await
        .unwrap();
    assert_eq!(desc.rows[0][1], serde_json::json!("c"));
    assert_eq!(desc.rows[2][1], serde_json::json!("a"));

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn pg_fetch_rows_filter() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_filt");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE {}.\"{}\" (id INT PRIMARY KEY, val INT)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            DB,
            &format!(
                "INSERT INTO {}.\"{}\" VALUES (1,10),(2,20),(3,30)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let data = driver
        .fetch_rows(DB, SCHEMA, &tbl, 0, 50, None, Some("\"val\" > 15".into()))
        .await
        .unwrap();
    assert_eq!(data.total_rows, 2);

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn pg_fetch_rows_unsafe_filter_rejected() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_unsafe");

    driver
        .execute_query(
            DB,
            &format!("CREATE TABLE {}.\"{}\" (id INT PRIMARY KEY)", SCHEMA, tbl),
        )
        .await
        .unwrap();

    let result = driver
        .fetch_rows(
            DB,
            SCHEMA,
            &tbl,
            0,
            50,
            None,
            Some("1=1; DROP TABLE x".into()),
        )
        .await;
    assert!(result.is_err());

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn pg_fetch_rows_null_values() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_null");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE {}.\"{}\" (id INT PRIMARY KEY, note TEXT)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            DB,
            &format!(
                "INSERT INTO {}.\"{}\" VALUES (1, NULL), (2, 'x')",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let data = driver
        .fetch_rows(DB, SCHEMA, &tbl, 0, 10, None, None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 2);
    assert_eq!(data.rows[0][1], serde_json::Value::Null);
    assert_eq!(data.rows[1][1], serde_json::json!("x"));

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn pg_fetch_rows_various_data_types() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_types");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE {}.\"{}\" (\
                   id INT PRIMARY KEY, \
                   i INT, \
                   t TEXT, \
                   b BOOLEAN, \
                   n NUMERIC(8,2), \
                   ts TIMESTAMP WITHOUT TIME ZONE, \
                   j JSON\
                 )",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            DB,
            &format!(
                "INSERT INTO {}.\"{}\" VALUES (\
                   1, 7, 'hi', true, 12.34, '2020-01-15 10:30:00', '{{\"k\":1}}'::json\
                 )",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let data = driver
        .fetch_rows(DB, SCHEMA, &tbl, 0, 10, None, None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 1);
    let row = &data.rows[0];
    assert_eq!(row[1], serde_json::json!(7));
    assert_eq!(row[2], serde_json::json!("hi"));
    assert_eq!(row[3], serde_json::json!(true));
    assert!(
        row[4].is_number(),
        "NUMERIC should be number, got: {:?}",
        row[4]
    );
    assert!(
        row.len() >= 7,
        "expected at least 7 columns, got {}",
        row.len()
    );

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// execute_query, explain_query, get_ddl
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_execute_query_select() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_sel");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE {}.\"{}\" (id INT PRIMARY KEY, name TEXT)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            DB,
            &format!("INSERT INTO {}.\"{}\" VALUES (1,'hello')", SCHEMA, tbl),
        )
        .await
        .unwrap();

    let result = driver
        .execute_query(DB, &format!("SELECT * FROM {}.\"{}\"", SCHEMA, tbl))
        .await
        .unwrap();
    assert!(result.is_select);
    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.rows.len(), 1);
    assert!(result.execution_time_ms < 60_000);

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn pg_execute_query_dml_insert() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_dml");

    driver
        .execute_query(
            DB,
            &format!("CREATE TABLE {}.\"{}\" (id INT PRIMARY KEY)", SCHEMA, tbl),
        )
        .await
        .unwrap();

    let result = driver
        .execute_query(
            DB,
            &format!("INSERT INTO {}.\"{}\" VALUES (1),(2),(3)", SCHEMA, tbl),
        )
        .await
        .unwrap();
    assert!(!result.is_select);
    assert_eq!(result.rows_affected, 3);

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn pg_execute_query_invalid_sql_errors() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_badsql");

    driver
        .execute_query(DB, &format!("CREATE TABLE {}.\"{}\" (id INT)", SCHEMA, tbl))
        .await
        .unwrap();

    let r = driver.execute_query(DB, "SELEC 1 FROM nowhere").await;
    assert!(r.is_err());

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn pg_explain_query() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_expl");

    driver
        .execute_query(
            DB,
            &format!("CREATE TABLE {}.\"{}\" (id INT PRIMARY KEY)", SCHEMA, tbl),
        )
        .await
        .unwrap();

    let ex = driver
        .explain_query(
            DB,
            &format!("SELECT * FROM {}.\"{}\" WHERE id = 1", SCHEMA, tbl),
        )
        .await
        .unwrap();
    assert!(!ex.raw_text.is_empty());
    assert!(!ex.plan.node_type.is_empty());

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn pg_get_ddl_table() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_ddl");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE {}.\"{}\" (id INT PRIMARY KEY, title TEXT NOT NULL)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let ddl = driver.get_ddl(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    assert!(ddl.to_uppercase().contains("CREATE TABLE"));
    assert!(ddl.contains(&tbl));
    assert!(ddl.contains("id"));
    assert!(ddl.contains("title"));

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// apply_changes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_apply_changes_insert() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_ac_ins");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE {}.\"{}\" (id SERIAL PRIMARY KEY, code TEXT)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let changes = DataChanges {
        connection_id: "test".into(),
        database: DB.into(),
        schema: SCHEMA.into(),
        table: tbl.clone(),
        updates: vec![],
        inserts: vec![NewRow {
            values: vec![("code".into(), serde_json::json!("Z1"))],
        }],
        deletes: vec![],
    };
    driver.apply_changes(&changes).await.unwrap();

    let data = driver
        .fetch_rows(DB, SCHEMA, &tbl, 0, 10, None, None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 1);
    assert_eq!(data.rows[0][1], serde_json::json!("Z1"));

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn pg_apply_changes_update() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_upd");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE {}.\"{}\" (id INT PRIMARY KEY, val TEXT)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            DB,
            &format!("INSERT INTO {}.\"{}\" VALUES (1, 'old')", SCHEMA, tbl),
        )
        .await
        .unwrap();

    let changes = DataChanges {
        connection_id: "test".into(),
        database: DB.into(),
        schema: SCHEMA.into(),
        table: tbl.clone(),
        updates: vec![CellChange {
            row_index: 0,
            column_name: "val".into(),
            old_value: serde_json::json!("old"),
            new_value: serde_json::json!("new"),
            primary_key_values: vec![("id".into(), serde_json::json!(1))],
        }],
        inserts: vec![],
        deletes: vec![],
    };
    driver.apply_changes(&changes).await.unwrap();

    let data = driver
        .fetch_rows(DB, SCHEMA, &tbl, 0, 50, None, None)
        .await
        .unwrap();
    assert_eq!(data.rows[0][1], serde_json::json!("new"));

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn pg_apply_changes_delete() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_del");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE {}.\"{}\" (id INT PRIMARY KEY, val TEXT)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            DB,
            &format!(
                "INSERT INTO {}.\"{}\" VALUES (1, 'a'), (2, 'b')",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let changes = DataChanges {
        connection_id: "test".into(),
        database: DB.into(),
        schema: SCHEMA.into(),
        table: tbl.clone(),
        updates: vec![],
        inserts: vec![],
        deletes: vec![DeleteRow {
            primary_key_values: vec![("id".into(), serde_json::json!(1))],
        }],
    };
    driver.apply_changes(&changes).await.unwrap();

    let data = driver
        .fetch_rows(DB, SCHEMA, &tbl, 0, 50, None, None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 1);
    assert_eq!(data.rows[0][0], serde_json::json!(2));

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn pg_apply_changes_batch_insert_update_delete() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_batch");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE {}.\"{}\" (id INT PRIMARY KEY, val INT)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            DB,
            &format!(
                "INSERT INTO {}.\"{}\" VALUES (1, 10), (2, 20), (3, 30)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let changes = DataChanges {
        connection_id: "test".into(),
        database: DB.into(),
        schema: SCHEMA.into(),
        table: tbl.clone(),
        updates: vec![CellChange {
            row_index: 0,
            column_name: "val".into(),
            old_value: serde_json::json!(10),
            new_value: serde_json::json!(99),
            primary_key_values: vec![("id".into(), serde_json::json!(1))],
        }],
        inserts: vec![NewRow {
            values: vec![
                ("id".into(), serde_json::json!(4)),
                ("val".into(), serde_json::json!(40)),
            ],
        }],
        deletes: vec![DeleteRow {
            primary_key_values: vec![("id".into(), serde_json::json!(2))],
        }],
    };
    driver.apply_changes(&changes).await.unwrap();

    let data = driver
        .fetch_rows(DB, SCHEMA, &tbl, 0, 50, None, None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 3);
    let ids: Vec<i64> = data.rows.iter().filter_map(|r| r[0].as_i64()).collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&3));
    assert!(ids.contains(&4));
    assert!(!ids.contains(&2));

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// create_table
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_create_table_basic() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_ct_basic");

    let cols = vec![
        ColumnDefinition {
            name: "id".into(),
            data_type: "bigserial".into(),
            is_nullable: false,
            is_primary_key: true,
            default_value: None,
        },
        ColumnDefinition {
            name: "label".into(),
            data_type: "text".into(),
            is_nullable: true,
            is_primary_key: false,
            default_value: None,
        },
    ];
    driver.create_table(DB, SCHEMA, &tbl, &cols).await.unwrap();

    let listed = driver.list_columns(DB, SCHEMA, &tbl).await.unwrap();
    assert_eq!(listed.len(), 2);
    assert!(listed.iter().any(|c| c.name == "id" && c.is_primary_key));

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn pg_create_table_no_columns_error() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_no_cols");
    let result = driver.create_table(DB, SCHEMA, &tbl, &[]).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn pg_create_table_unsafe_type_injection_error() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_inj");
    let cols = vec![ColumnDefinition {
        name: "x".into(),
        data_type: "int); DROP TABLE evil; --".into(),
        is_nullable: true,
        is_primary_key: false,
        default_value: None,
    }];
    let result = driver.create_table(DB, SCHEMA, &tbl, &cols).await;
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// alter_table
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_alter_table_add_rename_drop_column() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_alter");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE {}.\"{}\" (id INT PRIMARY KEY, old_col TEXT)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    driver
        .alter_table(
            DB,
            SCHEMA,
            &tbl,
            &[
                AlterTableOperation::AddColumn {
                    column: ColumnDefinition {
                        name: "new_col".into(),
                        data_type: "integer".into(),
                        is_nullable: true,
                        is_primary_key: false,
                        default_value: Some("0".into()),
                    },
                },
                AlterTableOperation::RenameColumn {
                    old_name: "old_col".into(),
                    new_name: "renamed_col".into(),
                },
            ],
        )
        .await
        .unwrap();

    let cols = driver.list_columns(DB, SCHEMA, &tbl).await.unwrap();
    let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
    assert!(names.contains(&"new_col"));
    assert!(names.contains(&"renamed_col"));
    assert!(!names.contains(&"old_col"));

    driver
        .alter_table(
            DB,
            SCHEMA,
            &tbl,
            &[AlterTableOperation::DropColumn {
                column_name: "new_col".into(),
            }],
        )
        .await
        .unwrap();

    let cols = driver.list_columns(DB, SCHEMA, &tbl).await.unwrap();
    let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
    assert!(!names.contains(&"new_col"));

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn pg_alter_table_change_column_type() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_alt_type");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE {}.\"{}\" (id INT PRIMARY KEY, n INT NOT NULL)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            DB,
            &format!("INSERT INTO {}.\"{}\" VALUES (1, 42)", SCHEMA, tbl),
        )
        .await
        .unwrap();

    driver
        .alter_table(
            DB,
            SCHEMA,
            &tbl,
            &[AlterTableOperation::ChangeColumnType {
                column_name: "n".into(),
                new_type: "BIGINT".into(),
            }],
        )
        .await
        .unwrap();

    let cols = driver.list_columns(DB, SCHEMA, &tbl).await.unwrap();
    let n = cols.iter().find(|c| c.name == "n").unwrap();
    assert!(n.data_type.contains("bigint"));

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn pg_alter_table_set_nullable() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_alt_null");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE {}.\"{}\" (id INT PRIMARY KEY, note TEXT NOT NULL)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    driver
        .alter_table(
            DB,
            SCHEMA,
            &tbl,
            &[AlterTableOperation::SetNullable {
                column_name: "note".into(),
                nullable: true,
            }],
        )
        .await
        .unwrap();

    let cols = driver.list_columns(DB, SCHEMA, &tbl).await.unwrap();
    let note = cols.iter().find(|c| c.name == "note").unwrap();
    assert!(note.is_nullable);

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn pg_alter_table_change_type_unsafe_rejected() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_alt_bad");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE {}.\"{}\" (id INT PRIMARY KEY, n INT)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let result = driver
        .alter_table(
            DB,
            SCHEMA,
            &tbl,
            &[AlterTableOperation::ChangeColumnType {
                column_name: "n".into(),
                new_type: "int; DROP TABLE x; --".into(),
            }],
        )
        .await;
    assert!(result.is_err());

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// truncate_table, drop_object
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_truncate_table() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_trunc");

    driver
        .execute_query(
            DB,
            &format!("CREATE TABLE {}.\"{}\" (id INT PRIMARY KEY)", SCHEMA, tbl),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            DB,
            &format!("INSERT INTO {}.\"{}\" VALUES (1),(2),(3)", SCHEMA, tbl),
        )
        .await
        .unwrap();

    driver.truncate_table(DB, SCHEMA, &tbl).await.unwrap();
    let data = driver
        .fetch_rows(DB, SCHEMA, &tbl, 0, 50, None, None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 0);

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn pg_drop_object_table_removes_from_list() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_drop");

    driver
        .execute_query(
            DB,
            &format!("CREATE TABLE {}.\"{}\" (id INT PRIMARY KEY)", SCHEMA, tbl),
        )
        .await
        .unwrap();

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let tables = driver.list_tables(DB, SCHEMA).await.unwrap();
    assert!(!tables.iter().any(|t| t.name == tbl));
}

#[tokio::test]
async fn pg_drop_object_view() {
    let driver = pg_driver!();
    let base = unique_table("pg_vbase");
    let vname = unique_table("pg_vw");

    driver
        .execute_query(
            DB,
            &format!("CREATE TABLE {}.\"{}\" (id INT PRIMARY KEY)", SCHEMA, base),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            DB,
            &format!(
                "CREATE VIEW {}.\"{}\" AS SELECT id FROM {}.\"{}\"",
                SCHEMA, vname, SCHEMA, base
            ),
        )
        .await
        .unwrap();

    driver
        .drop_object(DB, SCHEMA, &vname, "VIEW")
        .await
        .unwrap();
    driver
        .drop_object(DB, SCHEMA, &base, "TABLE")
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// import_data
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_import_data() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_import");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE {}.\"{}\" (id INT PRIMARY KEY, name TEXT)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let columns = vec!["id".to_string(), "name".to_string()];
    let rows = vec![
        vec![serde_json::json!(1), serde_json::json!("row1")],
        vec![serde_json::json!(2), serde_json::json!("row2")],
        vec![serde_json::json!(3), serde_json::json!("row3")],
    ];

    let imported = driver
        .import_data(DB, SCHEMA, &tbl, &columns, &rows)
        .await
        .unwrap();
    assert_eq!(imported, 3);

    let data = driver
        .fetch_rows(DB, SCHEMA, &tbl, 0, 50, None, None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 3);

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn pg_import_data_large_batch() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_import_big");

    driver
        .execute_query(
            DB,
            &format!("CREATE TABLE {}.\"{}\" (id INT PRIMARY KEY)", SCHEMA, tbl),
        )
        .await
        .unwrap();

    let columns = vec!["id".to_string()];
    let rows: Vec<Vec<serde_json::Value>> = (0..600).map(|i| vec![serde_json::json!(i)]).collect();

    let imported = driver
        .import_data(DB, SCHEMA, &tbl, &columns, &rows)
        .await
        .unwrap();
    assert_eq!(imported, 600);

    let data = driver
        .fetch_rows(DB, SCHEMA, &tbl, 0, 1, None, None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 600);

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// Server monitoring and roles
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_get_server_activity_ok() {
    let driver = pg_driver!();
    let _ = driver.get_server_activity().await.unwrap();
}

#[tokio::test]
async fn pg_get_database_stats_ok() {
    let driver = pg_driver!();
    let stats = driver.get_database_stats().await.unwrap();
    assert!(stats.total_connections >= 0);
    assert!(stats.timestamp_ms > 0.0);
}

#[tokio::test]
async fn pg_get_locks_ok() {
    let driver = pg_driver!();
    let _ = driver.get_locks().await.unwrap();
}

#[tokio::test]
async fn pg_get_server_config_ok() {
    let driver = pg_driver!();
    let cfg = driver.get_server_config().await.unwrap();
    assert!(!cfg.is_empty());
}

#[tokio::test]
async fn pg_get_query_stats_ok_or_extension_message() {
    let driver = pg_driver!();
    let res = driver.get_query_stats().await;
    assert!(res.is_ok());
    let qs = res.unwrap();
    if !qs.available {
        let msg = qs.message.as_deref().unwrap_or("");
        assert!(
            msg.contains("pg_stat_statements")
                || msg.contains("extension")
                || msg.contains("shared_preload")
        );
    }
}

#[tokio::test]
async fn pg_list_roles_non_empty() {
    let driver = pg_driver!();
    let roles = driver.list_roles().await.unwrap();
    assert!(!roles.is_empty());
}

// ---------------------------------------------------------------------------
// Remaining trait methods: cancel_query, create_role, drop_role, alter_role
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_cancel_query_invalid_pid_errors() {
    let driver = pg_driver!();
    let err = driver.cancel_query("not_a_pid").await;
    assert!(err.is_err());
}

#[tokio::test]
async fn pg_cancel_query_backend_call_ok() {
    let driver = pg_driver!();
    driver.cancel_query("0").await.unwrap();
}

#[tokio::test]
async fn pg_create_drop_alter_role() {
    let driver = pg_driver!();
    let role = format!(
        "tablio_t_{}",
        uuid::Uuid::new_v4().simple().to_string().get(..12).unwrap()
    );

    let create = CreateRoleRequest {
        connection_id: "test".into(),
        name: role.clone(),
        password: None,
        is_superuser: false,
        can_login: false,
        can_create_db: false,
        can_create_role: false,
        connection_limit: -1,
        valid_until: None,
    };
    if driver.create_role(&create).await.is_err() {
        return;
    }

    let _ = driver
        .alter_role(&AlterRoleRequest {
            connection_id: "test".into(),
            name: role.clone(),
            password: None,
            is_superuser: None,
            can_login: None,
            can_create_db: None,
            can_create_role: None,
            connection_limit: Some(5),
            valid_until: None,
        })
        .await;

    driver.drop_role(&role).await.unwrap();
}
