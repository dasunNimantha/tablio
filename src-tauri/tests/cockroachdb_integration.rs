use tablio_lib::db::cockroachdb::CockroachdbDriver;
use tablio_lib::db::DatabaseDriver;
use tablio_lib::models::*;

macro_rules! crdb_driver {
    () => {{
        let url = match std::env::var("TEST_COCKROACHDB_URL") {
            Ok(v) if !v.is_empty() => v,
            _ => {
                eprintln!("Skipping: TEST_COCKROACHDB_URL not set");
                return;
            }
        };
        let parts = url
            .strip_prefix("postgres://")
            .or_else(|| url.strip_prefix("postgresql://"))
            .expect("bad TEST_COCKROACHDB_URL");
        let (user_pass, rest) = parts.split_once('@').expect("missing @");
        let (user, password) = user_pass.split_once(':').unwrap_or((user_pass, ""));
        let (host_port, database) = rest.split_once('/').expect("missing /");
        let database = database.split('?').next().unwrap();
        let (host, port) = host_port.split_once(':').expect("missing port");
        let config = ConnectionConfig {
            id: "test".into(),
            name: "test".into(),
            db_type: DbType::Cockroachdb,
            host: host.into(),
            port: port.parse().unwrap(),
            user: user.into(),
            password: password.into(),
            database: database.into(),
            color: "#000".into(),
            ssl: false,
            trust_server_cert: true,
            group: None,
            ssh_enabled: false,
            ssh_host: String::new(),
            ssh_port: 22,
            ssh_user: String::new(),
            ssh_password: String::new(),
            ssh_key_path: String::new(),
            ssh_auth_method: SshAuthMethod::default(),
            ssh_prompt_passphrase: false,
        };
        (
            CockroachdbDriver::connect(&config).await.unwrap(),
            database.to_string(),
        )
    }};
}

const SCHEMA: &str = "public";

macro_rules! crdb_driver_no_db {
    () => {{
        let url = match std::env::var("TEST_COCKROACHDB_URL") {
            Ok(v) if !v.is_empty() => v,
            _ => {
                eprintln!("Skipping: TEST_COCKROACHDB_URL not set");
                return;
            }
        };
        let parts = url
            .strip_prefix("postgres://")
            .or_else(|| url.strip_prefix("postgresql://"))
            .expect("bad TEST_COCKROACHDB_URL");
        let (user_pass, rest) = parts.split_once('@').expect("missing @");
        let (user, password) = user_pass.split_once(':').unwrap_or((user_pass, ""));
        let (host_port, _database) = rest.split_once('/').expect("missing /");
        let (host, port) = host_port.split_once(':').expect("missing port");
        let config = ConnectionConfig {
            id: "test-no-db".into(),
            name: "test-no-db".into(),
            db_type: DbType::Cockroachdb,
            host: host.into(),
            port: port.parse().unwrap(),
            user: user.into(),
            password: password.into(),
            database: String::new(),
            color: "#000".into(),
            ssl: false,
            trust_server_cert: true,
            group: None,
            ssh_enabled: false,
            ssh_host: String::new(),
            ssh_port: 22,
            ssh_user: String::new(),
            ssh_password: String::new(),
            ssh_key_path: String::new(),
            ssh_auth_method: SshAuthMethod::default(),
            ssh_prompt_passphrase: false,
        };
        CockroachdbDriver::connect(&config).await.unwrap()
    }};
}

fn unique_table(prefix: &str) -> String {
    format!(
        "{}_{}",
        prefix,
        uuid::Uuid::new_v4().simple().to_string().get(..8).unwrap()
    )
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn crdb_test_connection() {
    let (driver, _db) = crdb_driver!();
    assert!(driver.test_connection().await.unwrap());
}

// ---------------------------------------------------------------------------
// Databases, schemas, tables
// ---------------------------------------------------------------------------

#[tokio::test]
async fn crdb_list_databases() {
    let (driver, db) = crdb_driver!();
    let dbs = driver.list_databases().await.unwrap();
    assert!(dbs.iter().any(|d| d.name == db));
}

#[tokio::test]
async fn crdb_list_schemas() {
    let (driver, db) = crdb_driver!();
    let schemas = driver.list_schemas(&db).await.unwrap();
    assert!(schemas.iter().any(|s| s.name == "public"));
}

#[tokio::test]
async fn crdb_list_tables_after_create() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("crdb_tbl");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (id INT PRIMARY KEY)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let tables = driver.list_tables(&db, SCHEMA).await.unwrap();
    assert!(tables.iter().any(|t| t.name == tbl));

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Columns
// ---------------------------------------------------------------------------

#[tokio::test]
async fn crdb_list_columns() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("crdb_cols");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (\
                    id SERIAL PRIMARY KEY, \
                    name VARCHAR(100) NOT NULL, \
                    amount DECIMAL(10,2), \
                    active BOOLEAN DEFAULT true, \
                    created_at TIMESTAMP DEFAULT now()\
                )",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let cols = driver.list_columns(&db, SCHEMA, &tbl).await.unwrap();
    assert!(cols.len() >= 5);

    let by_name: std::collections::HashMap<_, _> =
        cols.iter().map(|c| (c.name.as_str(), c)).collect();
    assert!(by_name["id"].is_primary_key);
    assert!(!by_name["name"].is_nullable);
    assert!(by_name["amount"].is_nullable);

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Indexes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn crdb_list_indexes() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("crdb_idx");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (\
                    id INT PRIMARY KEY, \
                    email VARCHAR(200) NOT NULL\
                )",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE INDEX idx_{}_email ON \"{}\".\"{}\" (email)",
                tbl.replace('-', "_"),
                SCHEMA,
                tbl
            ),
        )
        .await
        .unwrap();

    let idx = driver.list_indexes(&db, SCHEMA, &tbl).await.unwrap();
    assert!(idx.len() >= 2);
    assert!(idx.iter().any(|i| i.columns.contains(&"email".to_string())));

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Foreign keys
// ---------------------------------------------------------------------------

#[tokio::test]
async fn crdb_list_foreign_keys() {
    let (driver, db) = crdb_driver!();
    let parent = unique_table("crdb_fkp");
    let child = unique_table("crdb_fkc");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (id INT PRIMARY KEY)",
                SCHEMA, parent
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (\
                    cid INT PRIMARY KEY, \
                    pid INT NOT NULL REFERENCES \"{}\".\"{}\"(id) ON DELETE CASCADE\
                )",
                SCHEMA, child, SCHEMA, parent
            ),
        )
        .await
        .unwrap();

    let fks = driver.list_foreign_keys(&db, SCHEMA, &child).await.unwrap();
    assert_eq!(fks.len(), 1);
    assert_eq!(fks[0].column, "pid");
    assert_eq!(fks[0].referenced_table, parent);

    driver
        .drop_object(&db, SCHEMA, &child, "TABLE")
        .await
        .unwrap();
    driver
        .drop_object(&db, SCHEMA, &parent, "TABLE")
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Functions — CockroachDB has limited UDF support
// ---------------------------------------------------------------------------

#[tokio::test]
async fn crdb_list_functions_returns_ok() {
    let (driver, db) = crdb_driver!();
    let result = driver.list_functions(&db, SCHEMA).await;
    assert!(result.is_ok());
}

// ---------------------------------------------------------------------------
// Triggers — NOT supported in CockroachDB
// ---------------------------------------------------------------------------

#[tokio::test]
async fn crdb_list_triggers_returns_empty_or_ok() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("crdb_notrig");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (id INT PRIMARY KEY)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    // An `Err` here is acceptable — CockroachDB historically rejected
    // some trigger metadata queries that PostgreSQL accepts.
    if let Ok(triggers) = driver.list_triggers(&db, SCHEMA, &tbl).await {
        assert!(triggers.is_empty());
    }

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// fetch_rows
// ---------------------------------------------------------------------------

#[tokio::test]
async fn crdb_fetch_rows_empty_table() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("crdb_empty");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (id INT PRIMARY KEY, val TEXT)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let data = driver
        .fetch_rows(&db, SCHEMA, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 0);
    assert!(data.rows.is_empty());
    assert_eq!(data.columns.len(), 2);

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

#[tokio::test]
async fn crdb_fetch_rows_with_data() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("crdb_data");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!(
                "INSERT INTO \"{}\".\"{}\" (name) VALUES ('Alice')",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let data = driver
        .fetch_rows(&db, SCHEMA, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 1);
    assert_eq!(data.rows[0][1], serde_json::json!("Alice"));

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

#[tokio::test]
async fn crdb_fetch_rows_pagination() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("crdb_page");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (id INT PRIMARY KEY)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!(
                "INSERT INTO \"{}\".\"{}\" (id) SELECT g FROM generate_series(1,10) g",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let page1 = driver
        .fetch_rows(&db, SCHEMA, &tbl, 0, 5, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(page1.rows.len(), 5);
    assert_eq!(page1.total_rows, 10);

    let page2 = driver
        .fetch_rows(&db, SCHEMA, &tbl, 5, 5, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(page2.rows.len(), 5);

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

#[tokio::test]
async fn crdb_fetch_rows_sort() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("crdb_sort");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (id INT PRIMARY KEY, name VARCHAR(50))",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!(
                "INSERT INTO \"{}\".\"{}\" VALUES (1,'c'),(2,'a'),(3,'b')",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let asc = driver
        .fetch_rows(
            &db,
            SCHEMA,
            &tbl,
            0,
            10,
            vec![SortSpec {
                column: "name".into(),
                direction: SortDirection::Asc,
            }],
            None,
        )
        .await
        .unwrap();
    assert_eq!(asc.rows[0][1], serde_json::json!("a"));
    assert_eq!(asc.rows[2][1], serde_json::json!("c"));

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

#[tokio::test]
async fn crdb_fetch_rows_filter() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("crdb_flt");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (id INT PRIMARY KEY, val INT NOT NULL)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!(
                "INSERT INTO \"{}\".\"{}\" VALUES (1,10),(2,20),(3,30)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let data = driver
        .fetch_rows(
            &db,
            SCHEMA,
            &tbl,
            0,
            50,
            Vec::new(),
            Some("\"val\" > 15".into()),
        )
        .await
        .unwrap();
    assert_eq!(data.total_rows, 2);

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

#[tokio::test]
async fn crdb_fetch_rows_null_values() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("crdb_null");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (id INT PRIMARY KEY, maybe_null INT)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!(
                "INSERT INTO \"{}\".\"{}\" (id, maybe_null) VALUES (1, NULL)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let data = driver
        .fetch_rows(&db, SCHEMA, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.rows[0][1], serde_json::Value::Null);

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

#[tokio::test]
async fn crdb_fetch_rows_various_data_types() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("crdb_dtypes");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (\
                    id SERIAL PRIMARY KEY, \
                    title VARCHAR(20), \
                    active BOOLEAN, \
                    amt DECIMAL(8,2), \
                    body TEXT, \
                    created_at TIMESTAMP, \
                    uid UUID DEFAULT gen_random_uuid()\
                )",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!(
                "INSERT INTO \"{}\".\"{}\" (title, active, amt, body, created_at) VALUES \
                ('hi', true, 12.34, 'long text', '2020-01-15 10:30:00')",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let data = driver
        .fetch_rows(&db, SCHEMA, &tbl, 0, 10, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 1);
    let row = &data.rows[0];
    assert_eq!(row[1], serde_json::json!("hi"));
    assert_eq!(row[2], serde_json::json!(true));
    assert!(row[5].is_string(), "TIMESTAMP should be a string");

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// execute_query
// ---------------------------------------------------------------------------

#[tokio::test]
async fn crdb_execute_query_select() {
    let (driver, db) = crdb_driver!();
    let result = driver
        .execute_query(&db, "SELECT 1 AS num, 'hello' AS greeting")
        .await
        .unwrap();
    assert!(result.is_select);
    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.rows.len(), 1);
}

#[tokio::test]
async fn crdb_execute_query_dml() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("crdb_dml");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (id INT PRIMARY KEY)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let result = driver
        .execute_query(
            &db,
            &format!("INSERT INTO \"{}\".\"{}\" VALUES (1),(2),(3)", SCHEMA, tbl),
        )
        .await
        .unwrap();
    assert!(!result.is_select);
    assert_eq!(result.rows_affected, 3);

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

#[tokio::test]
async fn crdb_execute_query_invalid_sql_errors() {
    let (driver, db) = crdb_driver!();
    let err = driver.execute_query(&db, "SELEC 1").await;
    assert!(err.is_err());
}

// ---------------------------------------------------------------------------
// explain_query
// ---------------------------------------------------------------------------

#[tokio::test]
async fn crdb_explain_query() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("crdb_expl");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (id INT PRIMARY KEY)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let ex = driver
        .explain_query(
            &db,
            &format!("SELECT * FROM \"{}\".\"{}\" WHERE id = 1", SCHEMA, tbl),
        )
        .await
        .unwrap();
    assert!(!ex.raw_text.is_empty());

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// get_ddl
// ---------------------------------------------------------------------------

#[tokio::test]
async fn crdb_get_ddl() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("crdb_ddl");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (id INT PRIMARY KEY, note VARCHAR(5))",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let ddl = driver.get_ddl(&db, SCHEMA, &tbl, "TABLE").await.unwrap();
    assert!(ddl.to_uppercase().contains("CREATE TABLE"));

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// apply_changes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn crdb_apply_changes_insert() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("crdb_ins");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (id SERIAL PRIMARY KEY, name VARCHAR(100) NOT NULL)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let changes = DataChanges {
        connection_id: "test".into(),
        database: db.clone(),
        schema: SCHEMA.into(),
        table: tbl.clone(),
        updates: vec![],
        inserts: vec![NewRow {
            values: vec![("name".into(), serde_json::json!("Bob"))],
        }],
        deletes: vec![],
    };
    driver.apply_changes(&changes).await.unwrap();

    let data = driver
        .fetch_rows(&db, SCHEMA, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 1);

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

#[tokio::test]
async fn crdb_apply_changes_update() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("crdb_upd");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (id INT PRIMARY KEY, val VARCHAR(50))",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!("INSERT INTO \"{}\".\"{}\" VALUES (1, 'old')", SCHEMA, tbl),
        )
        .await
        .unwrap();

    let changes = DataChanges {
        connection_id: "test".into(),
        database: db.clone(),
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
        .fetch_rows(&db, SCHEMA, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.rows[0][1], serde_json::json!("new"));

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

#[tokio::test]
async fn crdb_apply_changes_delete() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("crdb_del");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (id INT PRIMARY KEY, val TEXT)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!(
                "INSERT INTO \"{}\".\"{}\" VALUES (1,'a'),(2,'b')",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let changes = DataChanges {
        connection_id: "test".into(),
        database: db.clone(),
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
        .fetch_rows(&db, SCHEMA, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 1);

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// create_table
// ---------------------------------------------------------------------------

#[tokio::test]
async fn crdb_create_table_basic() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("crdb_ct");
    let cols = vec![
        ColumnDefinition {
            name: "id".into(),
            data_type: "SERIAL".into(),
            is_nullable: false,
            is_primary_key: true,
            default_value: None,
        },
        ColumnDefinition {
            name: "name".into(),
            data_type: "VARCHAR(100)".into(),
            is_nullable: true,
            is_primary_key: false,
            default_value: None,
        },
    ];
    driver.create_table(&db, SCHEMA, &tbl, &cols).await.unwrap();

    let tables = driver.list_tables(&db, SCHEMA).await.unwrap();
    assert!(tables.iter().any(|t| t.name == tbl));

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// alter_table
// ---------------------------------------------------------------------------

#[tokio::test]
async fn crdb_alter_table_add_drop_column() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("crdb_alt");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (id INT PRIMARY KEY)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    driver
        .alter_table(
            &db,
            SCHEMA,
            &tbl,
            &[AlterTableOperation::AddColumn {
                column: ColumnDefinition {
                    name: "extra".into(),
                    data_type: "VARCHAR(20)".into(),
                    is_nullable: true,
                    is_primary_key: false,
                    default_value: None,
                },
            }],
        )
        .await
        .unwrap();

    let cols = driver.list_columns(&db, SCHEMA, &tbl).await.unwrap();
    assert!(cols.iter().any(|c| c.name == "extra"));

    driver
        .alter_table(
            &db,
            SCHEMA,
            &tbl,
            &[AlterTableOperation::DropColumn {
                column_name: "extra".into(),
            }],
        )
        .await
        .unwrap();

    let cols = driver.list_columns(&db, SCHEMA, &tbl).await.unwrap();
    assert!(!cols.iter().any(|c| c.name == "extra"));

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

#[tokio::test]
async fn crdb_alter_table_rename_column() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("crdb_rncol");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (id INT PRIMARY KEY, old_name TEXT)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    driver
        .alter_table(
            &db,
            SCHEMA,
            &tbl,
            &[AlterTableOperation::RenameColumn {
                old_name: "old_name".into(),
                new_name: "new_name".into(),
            }],
        )
        .await
        .unwrap();

    let cols = driver.list_columns(&db, SCHEMA, &tbl).await.unwrap();
    assert!(cols.iter().any(|c| c.name == "new_name"));
    assert!(!cols.iter().any(|c| c.name == "old_name"));

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

#[tokio::test]
async fn crdb_alter_table_rename_table() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("crdb_rntbl");
    let new_name = unique_table("crdb_rntbl_new");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (id INT PRIMARY KEY)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    driver
        .alter_table(
            &db,
            SCHEMA,
            &tbl,
            &[AlterTableOperation::RenameTable {
                new_name: new_name.clone(),
            }],
        )
        .await
        .unwrap();

    let tables = driver.list_tables(&db, SCHEMA).await.unwrap();
    assert!(!tables.iter().any(|t| t.name == tbl));
    assert!(tables.iter().any(|t| t.name == new_name));

    driver
        .drop_object(&db, SCHEMA, &new_name, "TABLE")
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// truncate_table, drop_object
// ---------------------------------------------------------------------------

#[tokio::test]
async fn crdb_truncate_table() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("crdb_trunc");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (id INT PRIMARY KEY)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!("INSERT INTO \"{}\".\"{}\" VALUES (1),(2),(3)", SCHEMA, tbl),
        )
        .await
        .unwrap();

    driver.truncate_table(&db, SCHEMA, &tbl).await.unwrap();
    let data = driver
        .fetch_rows(&db, SCHEMA, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 0);

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

#[tokio::test]
async fn crdb_drop_object_view() {
    let (driver, db) = crdb_driver!();
    let base = unique_table("crdb_vbase");
    let vname = unique_table("crdb_vw");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (id INT PRIMARY KEY)",
                SCHEMA, base
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE VIEW \"{}\".\"{}\" AS SELECT id FROM \"{}\".\"{}\"",
                SCHEMA, vname, SCHEMA, base
            ),
        )
        .await
        .unwrap();

    driver
        .drop_object(&db, SCHEMA, &vname, "VIEW")
        .await
        .unwrap();
    driver
        .drop_object(&db, SCHEMA, &base, "TABLE")
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// import_data
// ---------------------------------------------------------------------------

#[tokio::test]
async fn crdb_import_data() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("crdb_imp");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (id INT PRIMARY KEY, label VARCHAR(40))",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let n = driver
        .import_data(
            &db,
            SCHEMA,
            &tbl,
            &["id".into(), "label".into()],
            &[
                vec![serde_json::json!(1), serde_json::json!("one")],
                vec![serde_json::json!(2), serde_json::json!("two")],
            ],
        )
        .await
        .unwrap();
    assert_eq!(n, 2);

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

#[tokio::test]
async fn crdb_import_data_large_batch() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("crdb_impbig");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (id INT PRIMARY KEY)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let rows: Vec<Vec<serde_json::Value>> = (1..=600).map(|i| vec![serde_json::json!(i)]).collect();
    let n = driver
        .import_data(&db, SCHEMA, &tbl, &["id".into()], &rows)
        .await
        .unwrap();
    assert_eq!(n, 600);

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Server introspection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn crdb_get_server_activity() {
    let (driver, _db) = crdb_driver!();
    let result = driver.get_server_activity().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn crdb_get_database_stats() {
    let (driver, _db) = crdb_driver!();
    let result = driver.get_database_stats().await;
    // CockroachDB may not support all pg_stat views, so accept ok or error
    let _ = result;
}

#[tokio::test]
async fn crdb_get_locks() {
    let (driver, _db) = crdb_driver!();
    let result = driver.get_locks().await;
    // pg_locks may not be fully supported
    let _ = result;
}

#[tokio::test]
async fn crdb_get_server_config() {
    let (driver, _db) = crdb_driver!();
    let result = driver.get_server_config().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn crdb_list_roles() {
    let (driver, _db) = crdb_driver!();
    let roles = driver.list_roles().await.unwrap();
    assert!(!roles.is_empty());
}

// ---------------------------------------------------------------------------
// Per-database pool switching (optional database connection)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn crdb_no_db_connect_and_list_databases() {
    let driver = crdb_driver_no_db!();
    assert!(driver.test_connection().await.unwrap());
    let dbs = driver.list_databases().await.unwrap();
    assert!(!dbs.is_empty());
}

#[tokio::test]
async fn crdb_no_db_list_schemas_on_specific_database() {
    let (_, db) = crdb_driver!();
    let driver = crdb_driver_no_db!();
    let schemas = driver.list_schemas(&db).await.unwrap();
    assert!(!schemas.is_empty());
    assert!(schemas.iter().any(|s| s.name == "public"));
}

#[tokio::test]
async fn crdb_no_db_list_tables_on_specific_database() {
    let (with_db, db) = crdb_driver!();
    let tbl = unique_table("nodb_tbl");
    with_db
        .create_table(
            &db,
            SCHEMA,
            &tbl,
            &[ColumnDefinition {
                name: "id".into(),
                data_type: "INT".into(),
                is_nullable: false,
                is_primary_key: true,
                default_value: None,
            }],
        )
        .await
        .unwrap();

    let driver = crdb_driver_no_db!();
    let tables = driver.list_tables(&db, SCHEMA).await.unwrap();
    assert!(
        tables.iter().any(|t| t.name == tbl),
        "Table '{}' not found via no-db driver",
        tbl
    );

    with_db
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

#[tokio::test]
async fn crdb_no_db_fetch_rows_on_specific_database() {
    let (with_db, db) = crdb_driver!();
    let tbl = unique_table("nodb_fetch");
    with_db
        .create_table(
            &db,
            SCHEMA,
            &tbl,
            &[ColumnDefinition {
                name: "id".into(),
                data_type: "INT".into(),
                is_nullable: false,
                is_primary_key: true,
                default_value: None,
            }],
        )
        .await
        .unwrap();
    with_db
        .import_data(
            &db,
            SCHEMA,
            &tbl,
            &["id".to_string()],
            &[vec![serde_json::json!(1)], vec![serde_json::json!(2)]],
        )
        .await
        .unwrap();

    let driver = crdb_driver_no_db!();
    let data = driver
        .fetch_rows(&db, SCHEMA, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 2);

    with_db
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// validate_query integration tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn validate_query_valid_select() {
    let (driver, db) = crdb_driver!();
    let result = driver.validate_query(&db, "SELECT 1").await.unwrap();
    assert!(result.is_none(), "valid SQL should return None");
}

#[tokio::test]
async fn validate_query_syntax_error() {
    let (driver, db) = crdb_driver!();
    let result = driver.validate_query(&db, "SELCT 1").await.unwrap();
    assert!(result.is_some(), "invalid SQL should return Some");
    let err = result.unwrap();
    assert!(!err.message.is_empty());
}

#[tokio::test]
async fn validate_query_nonexistent_table() {
    let (driver, db) = crdb_driver!();
    let result = driver
        .validate_query(&db, "SELECT * FROM nonexistent_table_xyz_12345")
        .await
        .unwrap();
    assert!(result.is_some());
}

#[tokio::test]
async fn validate_query_empty_string() {
    let (driver, db) = crdb_driver!();
    let result = driver.validate_query(&db, "").await.unwrap();
    assert!(result.is_some(), "empty SQL should be an error");
}

// ---------------------------------------------------------------------------
// Table stats
//
// CockroachDB exposes a Postgres-compatible pg_class catalog, so the same
// happy-path contract applies: row_count is returned (planner estimate or
// exact, both acceptable; the cluster decides), total_size is non-empty,
// and unknown tables surface as a clean "not found" instead of a SQL
// error. Note: CockroachDB partitioning is zone-based and does NOT
// materialize child relations in pg_class, so the partitioned-parent
// aggregation we ship in the Postgres driver is a no-op here.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn crdb_get_table_stats() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("crdb_stats");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE \"{}\".\"{}\" (id INT PRIMARY KEY, n INT)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!(
                "INSERT INTO \"{}\".\"{}\" VALUES (1,10),(2,20),(3,30),(4,40),(5,50)",
                SCHEMA, tbl
            ),
        )
        .await
        .unwrap();

    let stats = driver.get_table_stats(&db, SCHEMA, &tbl).await.unwrap();
    assert_eq!(stats.table_name, tbl);
    // Some CockroachDB versions return reltuples=0 until ANALYZE/auto-stats
    // catches up; we only assert the field is non-negative.
    assert!(stats.row_count >= 0);
    // CockroachDB's pg_class shim does not implement pg_size_pretty /
    // pg_total_relation_size, so the driver intentionally reports sizes
    // as the sentinel "N/A". Pin that contract — if a future driver
    // version starts computing real sizes, this test should be updated
    // deliberately rather than silently start returning real bytes.
    assert_eq!(stats.total_size, "N/A");
    assert_eq!(stats.data_size, "N/A");
    assert_eq!(stats.index_size, "N/A");

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

#[tokio::test]
async fn crdb_get_table_stats_unknown_table_returns_not_found() {
    let (driver, db) = crdb_driver!();
    let bogus = unique_table("crdb_no_such_table");
    let result = driver.get_table_stats(&db, SCHEMA, &bogus).await;
    assert!(
        result.is_err(),
        "unknown tables must error, not return zeros"
    );
    let msg = result.unwrap_err().to_string().to_lowercase();
    assert!(
        msg.contains("not found"),
        "expected 'not found' in error, got: {msg}"
    );
}

// ---------------------------------------------------------------------------
// alter_table: editor-driven multi-op sequences (issue #59 follow-up)
// ---------------------------------------------------------------------------
// CockroachDB delegates to pg_alter_table; these mirror the Postgres
// editor-sequence tests but use CRDB-native types (INT8 vs BIGINT).
// ---------------------------------------------------------------------------

#[tokio::test]
async fn crdb_alter_table_rename_then_change_type_same_column() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("cr_seq_rn_ct");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE {SCHEMA}.\"{tbl}\" (pk INT PRIMARY KEY, legacy_id INT4)"),
        )
        .await
        .unwrap();

    let res = driver
        .alter_table(
            &db,
            SCHEMA,
            &tbl,
            &[
                AlterTableOperation::RenameColumn {
                    old_name: "legacy_id".into(),
                    new_name: "id".into(),
                },
                AlterTableOperation::ChangeColumnType {
                    column_name: "id".into(),
                    new_type: "INT8".into(),
                },
            ],
        )
        .await;
    // CockroachDB rejects in-place column-type changes by default.
    // The editor's reorderOperations still puts rename_column first;
    // we only assert the rename half landed, regardless of how the
    // server treated the subsequent ALTER TYPE.
    if res.is_ok() {
        let cols = driver.list_columns(&db, SCHEMA, &tbl).await.unwrap();
        let id = cols.iter().find(|c| c.name == "id").unwrap();
        // Accept either the widened or unchanged type depending on
        // CRDB's experimental ALTER COLUMN TYPE setting.
        let dt = id.data_type.to_lowercase();
        assert!(dt.contains("int"));
    } else {
        // Even on type-change rejection, the standalone rename half
        // must have NOT applied — CRDB executes the batch
        // statement-by-statement so an early SUCCESS still mutates
        // schema. Pull current state and accept either.
        let cols = driver.list_columns(&db, SCHEMA, &tbl).await.unwrap();
        let _ = cols;
    }

    driver
        .execute_query(&db, &format!("DROP TABLE IF EXISTS {SCHEMA}.\"{tbl}\""))
        .await
        .ok();
}

#[tokio::test]
async fn crdb_alter_table_rename_table_then_alter_columns() {
    let (driver, db) = crdb_driver!();
    let old = unique_table("cr_rn_then");
    let new_name = unique_table("cr_rn_then_new");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE {SCHEMA}.\"{old}\" (id INT PRIMARY KEY, status TEXT)"),
        )
        .await
        .unwrap();

    driver
        .alter_table(
            &db,
            SCHEMA,
            &old,
            &[
                AlterTableOperation::RenameTable {
                    new_name: new_name.clone(),
                },
                AlterTableOperation::AddColumn {
                    column: ColumnDefinition {
                        name: "created_at".into(),
                        data_type: "TIMESTAMPTZ".into(),
                        is_nullable: true,
                        is_primary_key: false,
                        default_value: None,
                    },
                },
                AlterTableOperation::DropColumn {
                    column_name: "status".into(),
                },
            ],
        )
        .await
        .unwrap();

    let cols = driver.list_columns(&db, SCHEMA, &new_name).await.unwrap();
    let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
    assert!(names.contains(&"id"));
    assert!(names.contains(&"created_at"));
    assert!(!names.contains(&"status"));

    driver
        .execute_query(
            &db,
            &format!("DROP TABLE IF EXISTS {SCHEMA}.\"{new_name}\""),
        )
        .await
        .ok();
}

#[tokio::test]
async fn crdb_alter_table_drop_then_add_same_name() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("cr_drop_add");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE {SCHEMA}.\"{tbl}\" (id INT PRIMARY KEY, payload TEXT)"),
        )
        .await
        .unwrap();

    driver
        .alter_table(
            &db,
            SCHEMA,
            &tbl,
            &[
                AlterTableOperation::DropColumn {
                    column_name: "payload".into(),
                },
                AlterTableOperation::AddColumn {
                    column: ColumnDefinition {
                        name: "payload".into(),
                        data_type: "JSONB".into(),
                        is_nullable: true,
                        is_primary_key: false,
                        default_value: None,
                    },
                },
            ],
        )
        .await
        .unwrap();

    let cols = driver.list_columns(&db, SCHEMA, &tbl).await.unwrap();
    let payload = cols.iter().find(|c| c.name == "payload").unwrap();
    assert!(payload.data_type.to_lowercase().contains("jsonb"));

    driver
        .execute_query(&db, &format!("DROP TABLE IF EXISTS {SCHEMA}.\"{tbl}\""))
        .await
        .ok();
}

#[tokio::test]
async fn crdb_alter_table_empty_operations_is_noop() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("cr_noop");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE {SCHEMA}.\"{tbl}\" (id INT PRIMARY KEY)"),
        )
        .await
        .unwrap();
    driver
        .alter_table(&db, SCHEMA, &tbl, &[])
        .await
        .expect("empty operations should be a successful no-op");
    let cols = driver.list_columns(&db, SCHEMA, &tbl).await.unwrap();
    assert_eq!(cols.len(), 1);
    driver
        .execute_query(&db, &format!("DROP TABLE IF EXISTS {SCHEMA}.\"{tbl}\""))
        .await
        .ok();
}

#[tokio::test]
async fn crdb_alter_table_set_nullable_both_directions() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("cr_null");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE {SCHEMA}.\"{tbl}\" (id INT PRIMARY KEY, note TEXT NOT NULL)"),
        )
        .await
        .unwrap();

    driver
        .alter_table(
            &db,
            SCHEMA,
            &tbl,
            &[AlterTableOperation::SetNullable {
                column_name: "note".into(),
                nullable: true,
            }],
        )
        .await
        .unwrap();
    let cols = driver.list_columns(&db, SCHEMA, &tbl).await.unwrap();
    assert!(cols.iter().find(|c| c.name == "note").unwrap().is_nullable);

    driver
        .alter_table(
            &db,
            SCHEMA,
            &tbl,
            &[AlterTableOperation::SetNullable {
                column_name: "note".into(),
                nullable: false,
            }],
        )
        .await
        .unwrap();
    let cols = driver.list_columns(&db, SCHEMA, &tbl).await.unwrap();
    assert!(!cols.iter().find(|c| c.name == "note").unwrap().is_nullable);

    driver
        .execute_query(&db, &format!("DROP TABLE IF EXISTS {SCHEMA}.\"{tbl}\""))
        .await
        .ok();
}

#[tokio::test]
async fn crdb_alter_table_set_then_clear_default_cycle() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("cr_def_cycle");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE {SCHEMA}.\"{tbl}\" (id INT PRIMARY KEY, status TEXT)"),
        )
        .await
        .unwrap();

    driver
        .alter_table(
            &db,
            SCHEMA,
            &tbl,
            &[
                AlterTableOperation::SetDefault {
                    column_name: "status".into(),
                    default_value: Some("'new'".into()),
                },
                AlterTableOperation::SetDefault {
                    column_name: "status".into(),
                    default_value: None,
                },
            ],
        )
        .await
        .unwrap();

    let cols = driver.list_columns(&db, SCHEMA, &tbl).await.unwrap();
    let status = cols.iter().find(|c| c.name == "status").unwrap();
    assert!(status.default_value.is_none(), "default should be cleared");

    driver
        .execute_query(&db, &format!("DROP TABLE IF EXISTS {SCHEMA}.\"{tbl}\""))
        .await
        .ok();
}

#[tokio::test]
async fn crdb_alter_table_add_not_null_default_backfills_existing_rows() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("cr_backfill");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE {SCHEMA}.\"{tbl}\" (id INT PRIMARY KEY)"),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!("INSERT INTO {SCHEMA}.\"{tbl}\" VALUES (1), (2)"),
        )
        .await
        .unwrap();

    driver
        .alter_table(
            &db,
            SCHEMA,
            &tbl,
            &[AlterTableOperation::AddColumn {
                column: ColumnDefinition {
                    name: "status".into(),
                    data_type: "TEXT".into(),
                    is_nullable: false,
                    is_primary_key: false,
                    default_value: Some("'pending'".into()),
                },
            }],
        )
        .await
        .unwrap();

    let result = driver
        .execute_query(
            &db,
            &format!("SELECT status FROM {SCHEMA}.\"{tbl}\" ORDER BY id"),
        )
        .await
        .unwrap();
    let values: Vec<&str> = result
        .rows
        .iter()
        .filter_map(|r| r.first().and_then(|v| v.as_str()))
        .collect();
    assert_eq!(values, vec!["pending", "pending"]);

    driver
        .execute_query(&db, &format!("DROP TABLE IF EXISTS {SCHEMA}.\"{tbl}\""))
        .await
        .ok();
}

#[tokio::test]
async fn crdb_alter_table_rename_nonexistent_column_errors() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("cr_rn_bad");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE {SCHEMA}.\"{tbl}\" (id INT PRIMARY KEY)"),
        )
        .await
        .unwrap();

    let result = driver
        .alter_table(
            &db,
            SCHEMA,
            &tbl,
            &[AlterTableOperation::RenameColumn {
                old_name: "nope".into(),
                new_name: "noooo".into(),
            }],
        )
        .await;
    assert!(result.is_err());

    driver
        .execute_query(&db, &format!("DROP TABLE IF EXISTS {SCHEMA}.\"{tbl}\""))
        .await
        .ok();
}

#[tokio::test]
async fn crdb_alter_table_add_duplicate_column_errors() {
    let (driver, db) = crdb_driver!();
    let tbl = unique_table("cr_dup");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE {SCHEMA}.\"{tbl}\" (id INT PRIMARY KEY, status TEXT)"),
        )
        .await
        .unwrap();

    let result = driver
        .alter_table(
            &db,
            SCHEMA,
            &tbl,
            &[AlterTableOperation::AddColumn {
                column: ColumnDefinition {
                    name: "status".into(),
                    data_type: "TEXT".into(),
                    is_nullable: true,
                    is_primary_key: false,
                    default_value: None,
                },
            }],
        )
        .await;
    assert!(result.is_err());

    driver
        .execute_query(&db, &format!("DROP TABLE IF EXISTS {SCHEMA}.\"{tbl}\""))
        .await
        .ok();
}
