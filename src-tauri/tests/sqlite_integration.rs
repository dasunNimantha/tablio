use tablio_lib::db::sqlite::SqliteDriver;
use tablio_lib::db::DatabaseDriver;
use tablio_lib::models::*;

async fn create_driver() -> (SqliteDriver, String) {
    let id = uuid::Uuid::new_v4().simple().to_string();
    let path = format!("/tmp/tablio_test_{}.db", &id[..8]);
    let config = ConnectionConfig {
        id: "test".into(),
        name: "test".into(),
        db_type: DbType::Sqlite,
        host: String::new(),
        port: 0,
        user: String::new(),
        password: String::new(),
        database: format!("sqlite:{}?mode=rwc", path),
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
    let driver = SqliteDriver::connect(&config).await.unwrap();
    (driver, path)
}

fn unique_table(prefix: &str) -> String {
    format!(
        "{}_{}",
        prefix,
        uuid::Uuid::new_v4().simple().to_string().get(..8).unwrap()
    )
}

const DB: &str = "main";
const SCHEMA: &str = "main";

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_test_connection() {
    let (driver, path) = create_driver().await;
    assert!(driver.test_connection().await.unwrap());
    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// Catalog
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_list_databases() {
    let (driver, path) = create_driver().await;
    let dbs = driver.list_databases().await.unwrap();
    assert_eq!(dbs.len(), 1);
    assert_eq!(dbs[0].name, "main");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_list_schemas() {
    let (driver, path) = create_driver().await;
    let schemas = driver.list_schemas(DB).await.unwrap();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].name, "main");
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_list_tables() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("lt");

    driver
        .execute_query(
            DB,
            &format!("CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY)", tbl),
        )
        .await
        .unwrap();

    let tables = driver.list_tables(DB, SCHEMA).await.unwrap();
    assert!(tables
        .iter()
        .any(|t| t.name == tbl && t.table_type == "BASE TABLE"));

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// Columns
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_list_columns() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("cols");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE \"{}\" (\
                    c_int INTEGER,\
                    c_text TEXT,\
                    c_real REAL,\
                    c_blob BLOB,\
                    c_numeric NUMERIC\
                )",
                tbl
            ),
        )
        .await
        .unwrap();

    let cols = driver.list_columns(DB, SCHEMA, &tbl).await.unwrap();
    assert_eq!(cols.len(), 5);

    let by_name: std::collections::HashMap<_, _> =
        cols.iter().map(|c| (c.name.as_str(), c)).collect();

    assert_eq!(by_name["c_int"].data_type.to_uppercase(), "INTEGER");
    assert_eq!(by_name["c_text"].data_type.to_uppercase(), "TEXT");
    assert_eq!(by_name["c_real"].data_type.to_uppercase(), "REAL");
    assert_eq!(by_name["c_blob"].data_type.to_uppercase(), "BLOB");
    assert_eq!(by_name["c_numeric"].data_type.to_uppercase(), "NUMERIC");

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// Indexes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_list_indexes() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("idx");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                tbl
            ),
        )
        .await
        .unwrap();
    let index_name = format!("ix_{}", &tbl[tbl.len().saturating_sub(8)..]);
    driver
        .execute_query(
            DB,
            &format!("CREATE INDEX \"{}\" ON \"{}\" (name)", index_name, tbl),
        )
        .await
        .unwrap();

    let indexes = driver.list_indexes(DB, SCHEMA, &tbl).await.unwrap();
    let named: Vec<_> = indexes.iter().filter(|i| i.name == index_name).collect();
    assert_eq!(named.len(), 1);
    assert_eq!(named[0].columns, vec!["name"]);
    assert!(!named[0].is_unique);

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// Foreign keys
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_list_foreign_keys() {
    let (driver, path) = create_driver().await;
    driver
        .execute_query(DB, "PRAGMA foreign_keys = ON")
        .await
        .unwrap();

    let parent = unique_table("par");
    let child = unique_table("chd");

    driver
        .execute_query(
            DB,
            &format!("CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY)", parent),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY, pid INTEGER NOT NULL REFERENCES \"{}\"(id))",
                child, parent
            ),
        )
        .await
        .unwrap();

    let fks = driver.list_foreign_keys(DB, SCHEMA, &child).await.unwrap();
    assert_eq!(fks.len(), 1);
    assert_eq!(fks[0].column, "pid");
    assert_eq!(fks[0].referenced_table, parent);
    assert_eq!(fks[0].referenced_column, "id");

    driver
        .drop_object(DB, SCHEMA, &child, "TABLE")
        .await
        .unwrap();
    driver
        .drop_object(DB, SCHEMA, &parent, "TABLE")
        .await
        .unwrap();
    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// Functions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_list_functions() {
    let (driver, path) = create_driver().await;
    let funcs = driver.list_functions(DB, SCHEMA).await.unwrap();
    assert!(funcs.is_empty());
    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// Triggers
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_list_triggers() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("trg");

    driver
        .execute_query(
            DB,
            &format!("CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY)", tbl),
        )
        .await
        .unwrap();

    let trg_name = format!("tr_{}", &tbl[tbl.len().saturating_sub(8)..]);
    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TRIGGER \"{}\" AFTER INSERT ON \"{}\" FOR EACH ROW BEGIN SELECT 1; END",
                trg_name, tbl
            ),
        )
        .await
        .unwrap();

    let triggers = driver.list_triggers(DB, SCHEMA, &tbl).await.unwrap();
    assert_eq!(triggers.len(), 1);
    assert_eq!(triggers[0].name, trg_name);
    assert_eq!(triggers[0].table_name, tbl);
    assert_eq!(triggers[0].event, "INSERT");
    assert_eq!(triggers[0].timing, "AFTER");

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// Table statistics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_get_table_stats() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("stats");

    driver
        .execute_query(
            DB,
            &format!("CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY)", tbl),
        )
        .await
        .unwrap();
    driver
        .execute_query(DB, &format!("INSERT INTO \"{}\" VALUES (1),(2),(3)", tbl))
        .await
        .unwrap();

    let stats = driver.get_table_stats(DB, SCHEMA, &tbl).await.unwrap();
    assert_eq!(stats.table_name, tbl);
    assert_eq!(stats.row_count, 3);
    assert_eq!(stats.live_tuples, Some(3));
    assert!(!stats.total_size.is_empty());

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// Fetch rows
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_fetch_rows_empty_table() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("empty");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY, val TEXT)",
                tbl
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
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_fetch_rows_with_data() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("data");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                tbl
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
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_fetch_rows_pagination() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("page");

    driver
        .execute_query(
            DB,
            &format!("CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY)", tbl),
        )
        .await
        .unwrap();

    let values: Vec<String> = (1..=10).map(|i| format!("({})", i)).collect();
    driver
        .execute_query(
            DB,
            &format!("INSERT INTO \"{}\" VALUES {}", tbl, values.join(", ")),
        )
        .await
        .unwrap();

    let page1 = driver
        .fetch_rows(DB, SCHEMA, &tbl, 0, 5, None, None)
        .await
        .unwrap();
    assert_eq!(page1.rows.len(), 5);
    assert_eq!(page1.total_rows, 10);

    let page2 = driver
        .fetch_rows(DB, SCHEMA, &tbl, 5, 5, None, None)
        .await
        .unwrap();
    assert_eq!(page2.rows.len(), 5);

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_fetch_rows_sort_asc_desc() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("sort");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY, name TEXT)",
                tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            DB,
            &format!("INSERT INTO \"{}\" VALUES (1,'c'),(2,'a'),(3,'b')", tbl),
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
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_fetch_rows_filter() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("filt");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY, val INTEGER)",
                tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            DB,
            &format!("INSERT INTO \"{}\" VALUES (1,10),(2,20),(3,30)", tbl),
        )
        .await
        .unwrap();

    let data = driver
        .fetch_rows(DB, SCHEMA, &tbl, 0, 50, None, Some("\"val\" > 15".into()))
        .await
        .unwrap();
    assert_eq!(data.total_rows, 2);

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_fetch_rows_unsafe_filter_rejected() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("unsafe");

    driver
        .execute_query(
            DB,
            &format!("CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY)", tbl),
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
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_fetch_rows_null_values() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("nulls");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY, payload BLOB)",
                tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            DB,
            &format!("INSERT INTO \"{}\" (id, payload) VALUES (1, NULL)", tbl),
        )
        .await
        .unwrap();

    let data = driver
        .fetch_rows(DB, SCHEMA, &tbl, 0, 10, None, None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 1);
    let v = &data.rows[0][1];
    assert!(v.is_null(), "NULL BLOB should decode as null, got: {:?}", v);

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_fetch_rows_various_data_types() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("types");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE \"{}\" (\
                    id INTEGER PRIMARY KEY,\
                    t TEXT,\
                    r REAL,\
                    b BLOB\
                )",
                tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            DB,
            &format!(
                "INSERT INTO \"{}\" (t, r, b) VALUES ('hi', 3.5, X'DEADBEEF')",
                tbl
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
    assert_eq!(row[1], serde_json::json!("hi"));
    assert!(row[2].as_f64().unwrap() - 3.5 < 1e-9);
    let b = &row[3];
    assert!(
        b.is_string(),
        "BLOB with data should decode as hex string, got: {:?}",
        b
    );
    assert!(
        b.as_str().unwrap().contains("deadbeef"),
        "BLOB hex should contain deadbeef, got: {:?}",
        b
    );

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// Execute query
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_execute_query_select() {
    let (driver, path) = create_driver().await;
    let result = driver
        .execute_query(DB, "SELECT 1 AS num, 'hello' AS greeting")
        .await
        .unwrap();
    assert!(result.is_select);
    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.rows.len(), 1);
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_execute_query_dml() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("dml");

    driver
        .execute_query(
            DB,
            &format!("CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY)", tbl),
        )
        .await
        .unwrap();

    let result = driver
        .execute_query(DB, &format!("INSERT INTO \"{}\" VALUES (1),(2),(3)", tbl))
        .await
        .unwrap();
    assert!(!result.is_select);
    assert_eq!(result.rows_affected, 3);

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_execute_query_invalid_sql() {
    let (driver, path) = create_driver().await;
    let err = driver.execute_query(DB, "SELECT FROM WHERE").await;
    assert!(err.is_err());
    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// Explain
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_explain_query() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("expl");

    driver
        .execute_query(
            DB,
            &format!("CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY)", tbl),
        )
        .await
        .unwrap();

    let ex = driver
        .explain_query(DB, &format!("SELECT * FROM \"{}\"", tbl))
        .await
        .unwrap();
    assert!(!ex.raw_text.is_empty());
    assert!(!ex.plan.node_type.is_empty());

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// DDL
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_get_ddl() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("ddl");

    let ddl_expected_fragment = format!("CREATE TABLE \"{}\"", tbl);
    driver
        .execute_query(
            DB,
            &format!("CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY, x TEXT)", tbl),
        )
        .await
        .unwrap();

    let ddl = driver.get_ddl(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    assert!(
        ddl.to_uppercase()
            .contains(&ddl_expected_fragment.to_uppercase())
            || ddl.to_uppercase().contains("CREATE TABLE")
    );
    assert!(ddl.contains("id"));

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// Apply changes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_apply_changes_insert() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("ains");

    driver
        .execute_query(
            DB,
            &format!("CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY, v TEXT)", tbl),
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
            values: vec![("v".into(), serde_json::json!("row"))],
        }],
        deletes: vec![],
    };
    driver.apply_changes(&changes).await.unwrap();

    let data = driver
        .fetch_rows(DB, SCHEMA, &tbl, 0, 10, None, None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 1);
    assert_eq!(data.rows[0][1], serde_json::json!("row"));

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_apply_changes_update() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("aupd");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY, val TEXT)",
                tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(DB, &format!("INSERT INTO \"{}\" VALUES (1, 'old')", tbl))
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
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_apply_changes_delete() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("adel");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY, val TEXT)",
                tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            DB,
            &format!("INSERT INTO \"{}\" VALUES (1,'a'),(2,'b')", tbl),
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
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_apply_changes_batch() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("abch");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY, val TEXT)",
                tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            DB,
            &format!("INSERT INTO \"{}\" VALUES (1,'a'),(2,'b')", tbl),
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
            old_value: serde_json::json!("a"),
            new_value: serde_json::json!("z"),
            primary_key_values: vec![("id".into(), serde_json::json!(1))],
        }],
        inserts: vec![NewRow {
            values: vec![
                ("id".into(), serde_json::json!(3)),
                ("val".into(), serde_json::json!("c")),
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
    assert_eq!(data.total_rows, 2);
    let ids: std::collections::HashSet<_> = data.rows.iter().map(|r| r[0].clone()).collect();
    assert!(ids.contains(&serde_json::json!(1)));
    assert!(ids.contains(&serde_json::json!(3)));

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// Create table
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_create_table_basic() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("ctb");

    let cols = vec![
        ColumnDefinition {
            name: "id".into(),
            data_type: "INTEGER".into(),
            is_nullable: false,
            is_primary_key: true,
            default_value: None,
        },
        ColumnDefinition {
            name: "title".into(),
            data_type: "TEXT".into(),
            is_nullable: true,
            is_primary_key: false,
            default_value: None,
        },
    ];
    driver.create_table(DB, SCHEMA, &tbl, &cols).await.unwrap();

    let listed = driver.list_tables(DB, SCHEMA).await.unwrap();
    assert!(listed.iter().any(|t| t.name == tbl));

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_create_table_no_columns_error() {
    let (driver, path) = create_driver().await;
    let result = driver.create_table(DB, SCHEMA, "should_fail", &[]).await;
    assert!(result.is_err());
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_create_table_unsafe_type_error() {
    let (driver, path) = create_driver().await;
    let cols = vec![ColumnDefinition {
        name: "x".into(),
        data_type: "INTEGER); DROP TABLE evil; --".into(),
        is_nullable: true,
        is_primary_key: false,
        default_value: None,
    }];
    let result = driver.create_table(DB, SCHEMA, "should_fail", &cols).await;
    assert!(result.is_err());
    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// Alter table
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_alter_table_add_column() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("altadd");

    driver
        .execute_query(
            DB,
            &format!("CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY)", tbl),
        )
        .await
        .unwrap();

    let ops = vec![AlterTableOperation::AddColumn {
        column: ColumnDefinition {
            name: "extra".into(),
            data_type: "TEXT".into(),
            is_nullable: true,
            is_primary_key: false,
            default_value: None,
        },
    }];
    driver.alter_table(DB, SCHEMA, &tbl, &ops).await.unwrap();

    let cols = driver.list_columns(DB, SCHEMA, &tbl).await.unwrap();
    assert!(cols.iter().any(|c| c.name == "extra"));

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_alter_table_rename_column() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("altrn");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY, old_name TEXT)",
                tbl
            ),
        )
        .await
        .unwrap();

    let ops = vec![AlterTableOperation::RenameColumn {
        old_name: "old_name".into(),
        new_name: "new_name".into(),
    }];
    driver.alter_table(DB, SCHEMA, &tbl, &ops).await.unwrap();

    let cols = driver.list_columns(DB, SCHEMA, &tbl).await.unwrap();
    assert!(cols.iter().any(|c| c.name == "new_name"));
    assert!(!cols.iter().any(|c| c.name == "old_name"));

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_alter_table_drop_column() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("altdrp");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY, extra TEXT, keep TEXT)",
                tbl
            ),
        )
        .await
        .unwrap();

    driver
        .alter_table(
            DB,
            SCHEMA,
            &tbl,
            &[AlterTableOperation::DropColumn {
                column_name: "extra".into(),
            }],
        )
        .await
        .unwrap();

    let cols = driver.list_columns(DB, SCHEMA, &tbl).await.unwrap();
    assert_eq!(cols.len(), 2);
    assert!(!cols.iter().any(|c| c.name == "extra"));
    assert!(cols.iter().any(|c| c.name == "keep"));

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_alter_table_rename_table() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("altrn_tbl");
    let new_name = unique_table("altrn_new");

    driver
        .execute_query(
            DB,
            &format!("CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY)", tbl),
        )
        .await
        .unwrap();

    driver
        .alter_table(
            DB,
            SCHEMA,
            &tbl,
            &[AlterTableOperation::RenameTable {
                new_name: new_name.clone(),
            }],
        )
        .await
        .unwrap();

    let tables = driver.list_tables(DB, SCHEMA).await.unwrap();
    assert!(!tables.iter().any(|t| t.name == tbl));
    assert!(tables.iter().any(|t| t.name == new_name));

    driver
        .drop_object(DB, SCHEMA, &new_name, "TABLE")
        .await
        .unwrap();
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_alter_table_change_type_unsupported() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("altchg");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY, n INTEGER)",
                tbl
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
                new_type: "TEXT".into(),
            }],
        )
        .await;
    assert!(result.is_err(), "SQLite should reject ChangeColumnType");

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_alter_table_set_nullable_unsupported() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("altnul");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY, note TEXT NOT NULL)",
                tbl
            ),
        )
        .await
        .unwrap();

    let result = driver
        .alter_table(
            DB,
            SCHEMA,
            &tbl,
            &[AlterTableOperation::SetNullable {
                column_name: "note".into(),
                nullable: true,
            }],
        )
        .await;
    assert!(result.is_err(), "SQLite should reject SetNullable");

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_alter_table_set_default_unsupported() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("altdef");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY, val TEXT)",
                tbl
            ),
        )
        .await
        .unwrap();

    let result = driver
        .alter_table(
            DB,
            SCHEMA,
            &tbl,
            &[AlterTableOperation::SetDefault {
                column_name: "val".into(),
                default_value: Some("hello".into()),
            }],
        )
        .await;
    assert!(result.is_err(), "SQLite should reject SetDefault");

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// Truncate and drop
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_truncate_table() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("trunc");

    driver
        .execute_query(
            DB,
            &format!("CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY)", tbl),
        )
        .await
        .unwrap();
    driver
        .execute_query(DB, &format!("INSERT INTO \"{}\" VALUES (1),(2),(3)", tbl))
        .await
        .unwrap();

    driver.truncate_table(DB, SCHEMA, &tbl).await.unwrap();
    let data = driver
        .fetch_rows(DB, SCHEMA, &tbl, 0, 50, None, None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 0);

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_drop_object() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("drop");

    driver
        .execute_query(
            DB,
            &format!("CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY)", tbl),
        )
        .await
        .unwrap();

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let tables = driver.list_tables(DB, SCHEMA).await.unwrap();
    assert!(!tables.iter().any(|t| t.name == tbl));
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_drop_object_view() {
    let (driver, path) = create_driver().await;
    let base = unique_table("vbase");
    let vname = unique_table("vw");

    driver
        .execute_query(
            DB,
            &format!("CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY)", base),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            DB,
            &format!("CREATE VIEW \"{}\" AS SELECT id FROM \"{}\"", vname, base),
        )
        .await
        .unwrap();

    driver
        .drop_object(DB, SCHEMA, &vname, "VIEW")
        .await
        .unwrap();

    let tables = driver.list_tables(DB, SCHEMA).await.unwrap();
    assert!(!tables.iter().any(|t| t.name == vname));

    driver
        .drop_object(DB, SCHEMA, &base, "TABLE")
        .await
        .unwrap();
    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// Import data
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_import_data() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("imp");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY, label TEXT NOT NULL)",
                tbl
            ),
        )
        .await
        .unwrap();

    let columns = vec!["id".into(), "label".into()];
    let rows = vec![
        vec![serde_json::json!(1), serde_json::json!("one")],
        vec![serde_json::json!(2), serde_json::json!("two")],
    ];
    let n = driver
        .import_data(DB, SCHEMA, &tbl, &columns, &rows)
        .await
        .unwrap();
    assert_eq!(n, 2);

    let data = driver
        .fetch_rows(DB, SCHEMA, &tbl, 0, 10, None, None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 2);

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_import_data_large_batch() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("implg");

    driver
        .execute_query(
            DB,
            &format!("CREATE TABLE \"{}\" (id INTEGER PRIMARY KEY)", tbl),
        )
        .await
        .unwrap();

    let columns = vec!["id".into()];
    let rows: Vec<Vec<serde_json::Value>> = (1..=55).map(|i| vec![serde_json::json!(i)]).collect();
    let n = driver
        .import_data(DB, SCHEMA, &tbl, &columns, &rows)
        .await
        .unwrap();
    assert_eq!(n, 55);

    let data = driver
        .fetch_rows(DB, SCHEMA, &tbl, 0, 200, None, None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 55);

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// Server introspection (SQLite stubs)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_get_server_activity() {
    let (driver, path) = create_driver().await;
    let act = driver.get_server_activity().await.unwrap();
    assert!(act.is_empty());
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_get_database_stats() {
    let (driver, path) = create_driver().await;
    let s = driver.get_database_stats().await.unwrap();
    assert_eq!(s.total_connections, 1);
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_get_locks() {
    let (driver, path) = create_driver().await;
    let locks = driver.get_locks().await.unwrap();
    assert!(locks.is_empty());
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_get_server_config() {
    let (driver, path) = create_driver().await;
    let cfg = driver.get_server_config().await.unwrap();
    assert!(cfg.is_empty());
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_get_query_stats() {
    let (driver, path) = create_driver().await;
    let qs = driver.get_query_stats().await.unwrap();
    assert!(!qs.available);
    assert!(qs.message.is_some());
    assert!(qs.entries.is_empty());
    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// Roles
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_list_roles() {
    let (driver, path) = create_driver().await;
    let roles = driver.list_roles().await.unwrap();
    assert!(roles.is_empty());
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_create_role_unsupported() {
    let (driver, path) = create_driver().await;
    let req = CreateRoleRequest {
        connection_id: "test".into(),
        name: "n".into(),
        password: None,
        is_superuser: false,
        can_login: true,
        can_create_db: false,
        can_create_role: false,
        connection_limit: -1,
        valid_until: None,
    };
    assert!(driver.create_role(&req).await.is_err());
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_drop_role_unsupported() {
    let (driver, path) = create_driver().await;
    assert!(driver.drop_role("any").await.is_err());
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_alter_role_unsupported() {
    let (driver, path) = create_driver().await;
    let req = AlterRoleRequest {
        connection_id: "test".into(),
        name: "n".into(),
        password: None,
        is_superuser: None,
        can_login: None,
        can_create_db: None,
        can_create_role: None,
        connection_limit: None,
        valid_until: None,
    };
    assert!(driver.alter_role(&req).await.is_err());
    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// Cancel query
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_cancel_query() {
    let (driver, path) = create_driver().await;
    driver.cancel_query("0").await.unwrap();
    let _ = std::fs::remove_file(&path);
}
