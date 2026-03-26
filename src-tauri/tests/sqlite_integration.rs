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
// Schema introspection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_create_table_and_list() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("sl_tbl");

    let cols = vec![
        ColumnDefinition {
            name: "id".into(),
            data_type: "INTEGER".into(),
            is_nullable: false,
            is_primary_key: true,
            default_value: None,
        },
        ColumnDefinition {
            name: "name".into(),
            data_type: "TEXT".into(),
            is_nullable: true,
            is_primary_key: false,
            default_value: None,
        },
    ];
    driver.create_table(DB, SCHEMA, &tbl, &cols).await.unwrap();

    let tables = driver.list_tables(DB, SCHEMA).await.unwrap();
    assert!(tables.iter().any(|t| t.name == tbl));

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn sqlite_list_columns_metadata() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("sl_cols");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE \"{}\" (\
                   id INTEGER PRIMARY KEY, \
                   name TEXT NOT NULL, \
                   active INTEGER DEFAULT 1, \
                   amount REAL\
                 )",
                tbl
            ),
        )
        .await
        .unwrap();

    let cols = driver.list_columns(DB, SCHEMA, &tbl).await.unwrap();
    assert_eq!(cols.len(), 4);

    assert_eq!(cols[0].name, "id");
    assert!(cols[0].is_primary_key);
    assert!(cols[0].is_auto_generated); // INTEGER PRIMARY KEY → rowid alias

    assert_eq!(cols[1].name, "name");
    assert!(!cols[1].is_nullable);
    assert_eq!(cols[1].data_type, "TEXT");

    assert_eq!(cols[2].name, "active");
    assert!(cols[2].is_nullable);

    assert_eq!(cols[3].name, "amount");
    assert_eq!(cols[3].data_type, "REAL");

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// Fetch rows
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_fetch_rows_empty_table() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("sl_empty");

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
async fn sqlite_insert_and_fetch() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("sl_ins");

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
async fn sqlite_update_via_apply_changes() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("sl_upd");

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
async fn sqlite_delete_via_apply_changes() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("sl_del");

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
async fn sqlite_fetch_rows_pagination() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("sl_page");

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
async fn sqlite_fetch_rows_sort() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("sl_sort");

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
    let tbl = unique_table("sl_filt");

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
    let tbl = unique_table("sl_unsafe");

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
    let tbl = unique_table("sl_dml");

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

// ---------------------------------------------------------------------------
// Truncate & drop
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sqlite_truncate_and_drop() {
    let (driver, path) = create_driver().await;
    let tbl = unique_table("sl_trunc");

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
    let tables = driver.list_tables(DB, SCHEMA).await.unwrap();
    assert!(!tables.iter().any(|t| t.name == tbl));
    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

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
