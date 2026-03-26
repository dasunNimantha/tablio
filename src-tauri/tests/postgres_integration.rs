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
async fn pg_create_table_and_list() {
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
async fn pg_list_columns_metadata() {
    let driver = pg_driver!();
    let tbl = unique_table("pg_cols");

    driver
        .execute_query(
            DB,
            &format!(
                "CREATE TABLE {}.\"{}\" (\
                   id SERIAL PRIMARY KEY, \
                   name VARCHAR(50) NOT NULL, \
                   active BOOLEAN DEFAULT true, \
                   amount NUMERIC(10,2)\
                 )",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let cols = driver.list_columns(DB, SCHEMA, &tbl).await.unwrap();
    assert_eq!(cols.len(), 4);

    assert_eq!(cols[0].name, "id");
    assert!(cols[0].is_primary_key);
    assert!(cols[0].is_auto_generated);

    assert_eq!(cols[1].name, "name");
    assert!(!cols[1].is_nullable);
    assert!(cols[1].data_type.contains("character varying"));
    assert!(cols[1].data_type.contains("50"));

    assert_eq!(cols[2].name, "active");
    assert_eq!(cols[2].data_type, "boolean");

    assert_eq!(cols[3].name, "amount");
    assert!(cols[3].data_type.contains("numeric"));

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// Fetch rows
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
async fn pg_insert_and_fetch() {
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
async fn pg_update_via_apply_changes() {
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
async fn pg_delete_via_apply_changes() {
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
async fn pg_fetch_rows_sort() {
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

// ---------------------------------------------------------------------------
// Execute query
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
    assert!(result.execution_time_ms < 5000);

    driver.drop_object(DB, SCHEMA, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn pg_execute_query_dml() {
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

// ---------------------------------------------------------------------------
// Alter table
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_alter_table_operations() {
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

// ---------------------------------------------------------------------------
// Truncate & drop
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_truncate_and_drop() {
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
    let tables = driver.list_tables(DB, SCHEMA).await.unwrap();
    assert!(!tables.iter().any(|t| t.name == tbl));
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pg_create_table_no_columns_error() {
    let driver = pg_driver!();
    let result = driver.create_table(DB, SCHEMA, "should_fail", &[]).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn pg_create_table_unsafe_type_error() {
    let driver = pg_driver!();
    let cols = vec![ColumnDefinition {
        name: "x".into(),
        data_type: "int); DROP TABLE evil; --".into(),
        is_nullable: true,
        is_primary_key: false,
        default_value: None,
    }];
    let result = driver.create_table(DB, SCHEMA, "should_fail", &cols).await;
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// Import data
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
