use tablio_lib::db::tidb::TidbDriver;
use tablio_lib::db::DatabaseDriver;
use tablio_lib::models::*;

macro_rules! tidb_driver {
    () => {{
        let url = match std::env::var("TEST_TIDB_URL") {
            Ok(v) if !v.is_empty() => v,
            _ => {
                eprintln!("Skipping: TEST_TIDB_URL not set");
                return;
            }
        };
        let parts = url.strip_prefix("mysql://").expect("bad TEST_TIDB_URL");
        let (user_pass, rest) = parts.split_once('@').expect("missing @");
        let (user, password) = user_pass.split_once(':').unwrap_or((user_pass, ""));
        let (host_port, database) = rest.split_once('/').expect("missing /");
        let database = database.split('?').next().unwrap();
        let (host, port) = host_port.split_once(':').expect("missing port");
        let config = ConnectionConfig {
            id: "test".into(),
            name: "test".into(),
            db_type: DbType::Tidb,
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
            TidbDriver::connect(&config).await.unwrap(),
            database.to_string(),
        )
    }};
}

macro_rules! tidb_driver_no_db {
    () => {{
        let url = match std::env::var("TEST_TIDB_URL") {
            Ok(v) if !v.is_empty() => v,
            _ => {
                eprintln!("Skipping: TEST_TIDB_URL not set");
                return;
            }
        };
        let parts = url.strip_prefix("mysql://").expect("bad TEST_TIDB_URL");
        let (user_pass, rest) = parts.split_once('@').expect("missing @");
        let (user, password) = user_pass.split_once(':').unwrap_or((user_pass, ""));
        let (host_port, _database) = rest.split_once('/').expect("missing /");
        let (host, port) = host_port.split_once(':').expect("missing port");
        let config = ConnectionConfig {
            id: "test-no-db".into(),
            name: "test-no-db".into(),
            db_type: DbType::Tidb,
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
        TidbDriver::connect(&config).await.unwrap()
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
async fn tidb_test_connection() {
    let (driver, _db) = tidb_driver!();
    assert!(driver.test_connection().await.unwrap());
}

// ---------------------------------------------------------------------------
// Databases, schemas, tables
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tidb_list_databases() {
    let (driver, db) = tidb_driver!();
    let dbs = driver.list_databases().await.unwrap();
    assert!(dbs.iter().any(|d| d.name == db));
}

#[tokio::test]
async fn tidb_list_schemas() {
    let (driver, db) = tidb_driver!();
    let schemas = driver.list_schemas(&db).await.unwrap();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].name, db);
}

#[tokio::test]
async fn tidb_list_tables() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_lst");
    driver
        .execute_query(&db, &format!("CREATE TABLE `{}` (id INT PRIMARY KEY)", tbl))
        .await
        .unwrap();
    let tables = driver.list_tables(&db, &db).await.unwrap();
    assert!(tables.iter().any(|t| t.name == tbl));
    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// Columns
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tidb_list_columns_various_types() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_types");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (\
                    id INT AUTO_INCREMENT PRIMARY KEY, \
                    title VARCHAR(100) NOT NULL, \
                    flag TINYINT(1) DEFAULT 0, \
                    amt DECIMAL(10,2), \
                    body TEXT, \
                    dtd DATETIME NULL, \
                    ts TIMESTAMP NULL DEFAULT NULL, \
                    jdoc JSON NULL\
                )",
                tbl
            ),
        )
        .await
        .unwrap();

    let cols = driver.list_columns(&db, &db, &tbl).await.unwrap();
    assert_eq!(cols.len(), 8);

    let by_name: std::collections::HashMap<_, _> =
        cols.iter().map(|c| (c.name.as_str(), c)).collect();
    assert!(by_name["id"].is_primary_key);
    assert!(by_name["id"].is_auto_generated);
    assert!(!by_name["title"].is_nullable);
    assert_eq!(by_name["amt"].data_type, "decimal");
    assert_eq!(by_name["body"].data_type, "text");

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// Indexes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tidb_list_indexes() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_idx");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (\
                    id INT AUTO_INCREMENT PRIMARY KEY, \
                    name VARCHAR(80) NOT NULL, \
                    KEY idx_name (name), \
                    UNIQUE KEY uq_id_name (id, name)\
                )",
                tbl
            ),
        )
        .await
        .unwrap();

    let idx = driver.list_indexes(&db, &db, &tbl).await.unwrap();
    let names: Vec<&str> = idx.iter().map(|i| i.name.as_str()).collect();
    assert!(names.contains(&"PRIMARY"));
    assert!(names.contains(&"idx_name"));
    assert!(names.contains(&"uq_id_name"));

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// Foreign keys
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tidb_list_foreign_keys() {
    let (driver, db) = tidb_driver!();
    let parent = unique_table("ti_fkp");
    let child = unique_table("ti_fkc");
    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{}` (id INT PRIMARY KEY)", parent),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (\
                    cid INT PRIMARY KEY, \
                    pid INT NOT NULL, \
                    CONSTRAINT fk_{}_ref FOREIGN KEY (pid) REFERENCES `{}`(id)\
                        ON DELETE CASCADE ON UPDATE CASCADE\
                )",
                child,
                child.replace('-', "_"),
                parent
            ),
        )
        .await
        .unwrap();

    let fks = driver.list_foreign_keys(&db, &db, &child).await.unwrap();
    assert_eq!(fks.len(), 1);
    assert_eq!(fks[0].column, "pid");
    assert_eq!(fks[0].referenced_table, parent);

    driver.drop_object(&db, &db, &child, "TABLE").await.unwrap();
    driver
        .drop_object(&db, &db, &parent, "TABLE")
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Functions — TiDB does NOT support stored functions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tidb_list_functions_empty() {
    let (driver, db) = tidb_driver!();
    let funcs = driver.list_functions(&db, &db).await.unwrap();
    let _ = funcs;
}

// ---------------------------------------------------------------------------
// Triggers — TiDB does NOT support triggers
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tidb_list_triggers_empty() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_notrig");
    driver
        .execute_query(&db, &format!("CREATE TABLE `{}` (id INT PRIMARY KEY)", tbl))
        .await
        .unwrap();

    let triggers = driver.list_triggers(&db, &db, &tbl).await.unwrap();
    assert!(triggers.is_empty());

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// Table stats
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tidb_get_table_stats() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_stats");
    driver
        .execute_query(&db, &format!("CREATE TABLE `{}` (id INT PRIMARY KEY)", tbl))
        .await
        .unwrap();
    driver
        .execute_query(&db, &format!("INSERT INTO `{}` VALUES (1),(2),(3)", tbl))
        .await
        .unwrap();

    let stats = driver.get_table_stats(&db, &db, &tbl).await.unwrap();
    assert_eq!(stats.table_name, tbl);
    assert_eq!(stats.row_count, 3);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// TiDB supports MySQL-compatible `PARTITION BY RANGE/LIST/HASH`. The
// driver counts rows with a plain `SELECT COUNT(*)`, which the TiDB
// optimizer fans out across partitions automatically. This test pins
// the aggregate behavior end-to-end.
#[tokio::test]
async fn tidb_get_table_stats_partitioned_table_aggregates_partitions() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_stats_part");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (
                    id INT NOT NULL,
                    yr INT NOT NULL,
                    PRIMARY KEY (id, yr)
                )
                PARTITION BY RANGE (yr) (
                    PARTITION p2024 VALUES LESS THAN (2025),
                    PARTITION p2025 VALUES LESS THAN (2026),
                    PARTITION p2026 VALUES LESS THAN (2027)
                )",
                tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!(
                "INSERT INTO `{}` VALUES \
                 (1,2024),(2,2024),(3,2025),(4,2025),(5,2026),(6,2026)",
                tbl
            ),
        )
        .await
        .unwrap();

    let stats = driver.get_table_stats(&db, &db, &tbl).await.unwrap();
    assert_eq!(stats.table_name, tbl);
    assert_eq!(
        stats.row_count, 6,
        "TiDB COUNT(*) must aggregate across all RANGE partitions"
    );

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_get_table_stats_unknown_table_returns_not_found() {
    let (driver, db) = tidb_driver!();
    let bogus = unique_table("ti_no_such_table");
    let result = driver.get_table_stats(&db, &db, &bogus).await;
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
// fetch_rows
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tidb_fetch_rows_empty_table() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_empty");
    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{}` (id INT PRIMARY KEY, val TEXT)", tbl),
        )
        .await
        .unwrap();

    let data = driver
        .fetch_rows(&db, &db, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 0);
    assert!(data.rows.is_empty());
    assert_eq!(data.columns.len(), 2);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_fetch_rows_with_data() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_data");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100) NOT NULL)",
                tbl
            ),
        )
        .await
        .unwrap();

    let changes = DataChanges {
        connection_id: "test".into(),
        database: db.clone(),
        schema: db.clone(),
        table: tbl.clone(),
        updates: vec![],
        inserts: vec![NewRow {
            values: vec![("name".into(), serde_json::json!("Alice"))],
        }],
        deletes: vec![],
    };
    driver.apply_changes(&changes).await.unwrap();

    let data = driver
        .fetch_rows(&db, &db, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 1);
    assert_eq!(data.rows[0][1], serde_json::json!("Alice"));

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_fetch_rows_pagination() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_page");
    driver
        .execute_query(&db, &format!("CREATE TABLE `{}` (id INT PRIMARY KEY)", tbl))
        .await
        .unwrap();

    let values: Vec<String> = (1..=10).map(|i| format!("({})", i)).collect();
    driver
        .execute_query(
            &db,
            &format!("INSERT INTO `{}` VALUES {}", tbl, values.join(", ")),
        )
        .await
        .unwrap();

    let page1 = driver
        .fetch_rows(&db, &db, &tbl, 0, 5, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(page1.rows.len(), 5);
    assert_eq!(page1.total_rows, 10);

    let page2 = driver
        .fetch_rows(&db, &db, &tbl, 5, 5, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(page2.rows.len(), 5);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_fetch_rows_sort_asc_desc() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_sort");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT PRIMARY KEY, name VARCHAR(50))",
                tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!("INSERT INTO `{}` VALUES (1,'c'),(2,'a'),(3,'b')", tbl),
        )
        .await
        .unwrap();

    let asc = driver
        .fetch_rows(
            &db,
            &db,
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

    let desc = driver
        .fetch_rows(
            &db,
            &db,
            &tbl,
            0,
            10,
            vec![SortSpec {
                column: "name".into(),
                direction: SortDirection::Desc,
            }],
            None,
        )
        .await
        .unwrap();
    assert_eq!(desc.rows[0][1], serde_json::json!("c"));
    assert_eq!(desc.rows[2][1], serde_json::json!("a"));

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_fetch_rows_filter() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_flt");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT PRIMARY KEY, val INT NOT NULL)",
                tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!("INSERT INTO `{}` VALUES (1,10),(2,20),(3,30)", tbl),
        )
        .await
        .unwrap();

    let data = driver
        .fetch_rows(&db, &db, &tbl, 0, 50, Vec::new(), Some("`val` > 15".into()))
        .await
        .unwrap();
    assert_eq!(data.total_rows, 2);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_fetch_rows_null_values() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_null");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT PRIMARY KEY, maybe_null INT NULL)",
                tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!("INSERT INTO `{}` (id, maybe_null) VALUES (1, NULL)", tbl),
        )
        .await
        .unwrap();

    let data = driver
        .fetch_rows(&db, &db, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.rows[0][1], serde_json::Value::Null);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_fetch_rows_various_data_types() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_ftypes");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (\
                    id INT AUTO_INCREMENT PRIMARY KEY, \
                    title VARCHAR(20), \
                    flag TINYINT(1), \
                    amt DECIMAL(8,2), \
                    body TEXT, \
                    jdoc JSON, \
                    created_at DATETIME NULL\
                )",
                tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!(
                "INSERT INTO `{}` (title, flag, amt, body, jdoc, created_at) VALUES \
                ('hi', 1, 12.34, 'long', '{{\"k\":1}}', '2020-01-15 10:30:00')",
                tbl
            ),
        )
        .await
        .unwrap();

    let data = driver
        .fetch_rows(&db, &db, &tbl, 0, 10, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 1);
    let row = &data.rows[0];
    assert_eq!(row[1], serde_json::json!("hi"));
    assert!(row[6].is_string(), "DATETIME should be a string");
    assert!(row[6].as_str().unwrap().contains("2020-01-15"));

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// execute_query
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tidb_execute_query_select() {
    let (driver, db) = tidb_driver!();
    let result = driver
        .execute_query(&db, "SELECT 1 AS num, 'hello' AS greeting")
        .await
        .unwrap();
    assert!(result.is_select);
    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.rows.len(), 1);
}

#[tokio::test]
async fn tidb_execute_query_dml() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_dml");
    driver
        .execute_query(&db, &format!("CREATE TABLE `{}` (id INT PRIMARY KEY)", tbl))
        .await
        .unwrap();

    let result = driver
        .execute_query(&db, &format!("INSERT INTO `{}` VALUES (1),(2),(3)", tbl))
        .await
        .unwrap();
    assert!(!result.is_select);
    assert_eq!(result.rows_affected, 3);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_execute_query_invalid_sql_errors() {
    let (driver, db) = tidb_driver!();
    let err = driver.execute_query(&db, "SELEC 1").await;
    assert!(err.is_err());
}

// ---------------------------------------------------------------------------
// explain_query
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tidb_explain_query() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_expl");
    driver
        .execute_query(&db, &format!("CREATE TABLE `{}` (id INT PRIMARY KEY)", tbl))
        .await
        .unwrap();

    let ex = driver
        .explain_query(&db, &format!("SELECT * FROM `{}` WHERE id = 1", tbl))
        .await
        .unwrap();
    assert!(!ex.raw_text.is_empty());

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// get_ddl
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tidb_get_ddl() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_ddl");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT PRIMARY KEY, note VARCHAR(5))",
                tbl
            ),
        )
        .await
        .unwrap();

    let ddl = driver.get_ddl(&db, &db, &tbl, "TABLE").await.unwrap();
    assert!(ddl.to_uppercase().contains("CREATE TABLE"));
    assert!(ddl.contains(&tbl));

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// apply_changes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tidb_apply_changes_insert() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_ins");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100) NOT NULL)",
                tbl
            ),
        )
        .await
        .unwrap();

    let changes = DataChanges {
        connection_id: "test".into(),
        database: db.clone(),
        schema: db.clone(),
        table: tbl.clone(),
        updates: vec![],
        inserts: vec![NewRow {
            values: vec![("name".into(), serde_json::json!("Bob"))],
        }],
        deletes: vec![],
    };
    driver.apply_changes(&changes).await.unwrap();

    let data = driver
        .fetch_rows(&db, &db, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 1);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_apply_changes_update() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_upd");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT PRIMARY KEY, val VARCHAR(50))",
                tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(&db, &format!("INSERT INTO `{}` VALUES (1, 'old')", tbl))
        .await
        .unwrap();

    let changes = DataChanges {
        connection_id: "test".into(),
        database: db.clone(),
        schema: db.clone(),
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
        .fetch_rows(&db, &db, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.rows[0][1], serde_json::json!("new"));

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_apply_changes_delete() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_del");
    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{}` (id INT PRIMARY KEY, val TEXT)", tbl),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!("INSERT INTO `{}` VALUES (1,'a'),(2,'b')", tbl),
        )
        .await
        .unwrap();

    let changes = DataChanges {
        connection_id: "test".into(),
        database: db.clone(),
        schema: db.clone(),
        table: tbl.clone(),
        updates: vec![],
        inserts: vec![],
        deletes: vec![DeleteRow {
            primary_key_values: vec![("id".into(), serde_json::json!(1))],
        }],
    };
    driver.apply_changes(&changes).await.unwrap();

    let data = driver
        .fetch_rows(&db, &db, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 1);
    assert_eq!(data.rows[0][0], serde_json::json!(2));

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// create_table
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tidb_create_table_basic() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_ct");
    let cols = vec![
        ColumnDefinition {
            name: "id".into(),
            data_type: "INT AUTO_INCREMENT".into(),
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
    driver.create_table(&db, &db, &tbl, &cols).await.unwrap();

    let tables = driver.list_tables(&db, &db).await.unwrap();
    assert!(tables.iter().any(|t| t.name == tbl));

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// alter_table
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tidb_alter_table_add_rename_drop() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_alt");
    driver
        .execute_query(&db, &format!("CREATE TABLE `{}` (id INT PRIMARY KEY)", tbl))
        .await
        .unwrap();

    let ops = vec![
        AlterTableOperation::AddColumn {
            column: ColumnDefinition {
                name: "extra".into(),
                data_type: "VARCHAR(20)".into(),
                is_nullable: true,
                is_primary_key: false,
                default_value: None,
            },
        },
        AlterTableOperation::RenameColumn {
            old_name: "extra".into(),
            new_name: "renamed_extra".into(),
        },
        AlterTableOperation::DropColumn {
            column_name: "renamed_extra".into(),
        },
    ];
    driver.alter_table(&db, &db, &tbl, &ops).await.unwrap();

    let cols = driver.list_columns(&db, &db, &tbl).await.unwrap();
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0].name, "id");

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_alter_table_change_column_type() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_chtype");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT PRIMARY KEY, n INT NOT NULL)",
                tbl
            ),
        )
        .await
        .unwrap();

    driver
        .alter_table(
            &db,
            &db,
            &tbl,
            &[AlterTableOperation::ChangeColumnType {
                column_name: "n".into(),
                new_type: "BIGINT".into(),
            }],
        )
        .await
        .unwrap();

    let cols = driver.list_columns(&db, &db, &tbl).await.unwrap();
    let n = cols.iter().find(|c| c.name == "n").unwrap();
    assert!(n.data_type.contains("bigint"));

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_alter_table_set_nullable() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_setnull");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT PRIMARY KEY, note VARCHAR(100) NOT NULL)",
                tbl
            ),
        )
        .await
        .unwrap();

    driver
        .alter_table(
            &db,
            &db,
            &tbl,
            &[AlterTableOperation::SetNullable {
                column_name: "note".into(),
                nullable: true,
            }],
        )
        .await
        .unwrap();

    let cols = driver.list_columns(&db, &db, &tbl).await.unwrap();
    let note = cols.iter().find(|c| c.name == "note").unwrap();
    assert!(note.is_nullable);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_alter_table_set_default() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_setdef");
    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{}` (id INT PRIMARY KEY, priority INT)", tbl),
        )
        .await
        .unwrap();

    driver
        .alter_table(
            &db,
            &db,
            &tbl,
            &[AlterTableOperation::SetDefault {
                column_name: "priority".into(),
                default_value: Some("5".into()),
            }],
        )
        .await
        .unwrap();

    driver
        .execute_query(&db, &format!("INSERT INTO `{}` (id) VALUES (1)", tbl))
        .await
        .unwrap();

    let data = driver
        .fetch_rows(&db, &db, &tbl, 0, 10, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.rows[0][1], serde_json::json!(5));

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_alter_table_rename_table() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_rn");
    let new_name = unique_table("ti_rn_new");
    driver
        .execute_query(&db, &format!("CREATE TABLE `{}` (id INT PRIMARY KEY)", tbl))
        .await
        .unwrap();

    driver
        .alter_table(
            &db,
            &db,
            &tbl,
            &[AlterTableOperation::RenameTable {
                new_name: new_name.clone(),
            }],
        )
        .await
        .unwrap();

    let tables = driver.list_tables(&db, &db).await.unwrap();
    assert!(!tables.iter().any(|t| t.name == tbl));
    assert!(tables.iter().any(|t| t.name == new_name));

    driver
        .drop_object(&db, &db, &new_name, "TABLE")
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// truncate_table, drop_object
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tidb_truncate_table() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_trunc");
    driver
        .execute_query(&db, &format!("CREATE TABLE `{}` (id INT PRIMARY KEY)", tbl))
        .await
        .unwrap();
    driver
        .execute_query(&db, &format!("INSERT INTO `{}` VALUES (1),(2),(3)", tbl))
        .await
        .unwrap();

    driver.truncate_table(&db, &db, &tbl).await.unwrap();
    let data = driver
        .fetch_rows(&db, &db, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 0);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_drop_object_view() {
    let (driver, db) = tidb_driver!();
    let base = unique_table("ti_vbase");
    let vname = unique_table("ti_vw");
    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{}` (id INT PRIMARY KEY)", base),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!("CREATE VIEW `{}` AS SELECT id FROM `{}`", vname, base),
        )
        .await
        .unwrap();

    driver.drop_object(&db, &db, &vname, "VIEW").await.unwrap();
    driver.drop_object(&db, &db, &base, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// import_data
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tidb_import_data() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_imp");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT PRIMARY KEY, label VARCHAR(40))",
                tbl
            ),
        )
        .await
        .unwrap();

    let n = driver
        .import_data(
            &db,
            &db,
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

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_import_data_large_batch() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_impbig");
    driver
        .execute_query(&db, &format!("CREATE TABLE `{}` (id INT PRIMARY KEY)", tbl))
        .await
        .unwrap();

    let rows: Vec<Vec<serde_json::Value>> = (1..=600).map(|i| vec![serde_json::json!(i)]).collect();
    let n = driver
        .import_data(&db, &db, &tbl, &["id".into()], &rows)
        .await
        .unwrap();
    assert_eq!(n, 600);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// Server introspection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tidb_get_server_activity() {
    let (driver, _db) = tidb_driver!();
    let activity = driver.get_server_activity().await.unwrap();
    assert!(!activity.is_empty());
}

#[tokio::test]
async fn tidb_get_database_stats() {
    let (driver, _db) = tidb_driver!();
    let stats = driver.get_database_stats().await.unwrap();
    assert!(stats.total_connections >= 1);
    assert!(stats.timestamp_ms > 0.0);
}

#[tokio::test]
async fn tidb_get_locks() {
    let (driver, _db) = tidb_driver!();
    let result = driver.get_locks().await;
    let _ = result;
}

#[tokio::test]
async fn tidb_get_server_config() {
    let (driver, _db) = tidb_driver!();
    let cfg = driver.get_server_config().await.unwrap();
    assert!(!cfg.is_empty());
}

#[tokio::test]
async fn tidb_list_roles() {
    let (driver, _db) = tidb_driver!();
    let roles = driver.list_roles().await;
    let _ = roles;
}

#[tokio::test]
async fn tidb_cancel_query_invalid_pid() {
    let (driver, _db) = tidb_driver!();
    let r = driver.cancel_query("not_a_number").await;
    assert!(r.is_err());
}

// ---------------------------------------------------------------------------
// Optional database connection (connect without specifying a database)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tidb_no_db_connect_and_list_databases() {
    let driver = tidb_driver_no_db!();
    assert!(driver.test_connection().await.unwrap());
    let dbs = driver.list_databases().await.unwrap();
    assert!(!dbs.is_empty());
}

#[tokio::test]
async fn tidb_no_db_list_tables_on_specific_database() {
    let (with_db, db) = tidb_driver!();
    let tbl = unique_table("nodb_tbl");
    with_db
        .create_table(
            &db,
            &db,
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

    let driver = tidb_driver_no_db!();
    let tables = driver.list_tables(&db, &db).await.unwrap();
    assert!(
        tables.iter().any(|t| t.name == tbl),
        "Table '{}' not found via no-db driver",
        tbl
    );

    with_db.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_no_db_fetch_rows_on_specific_database() {
    let (with_db, db) = tidb_driver!();
    let tbl = unique_table("nodb_fetch");
    with_db
        .create_table(
            &db,
            &db,
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
            &db,
            &tbl,
            &["id".to_string()],
            &[vec![serde_json::json!(1)], vec![serde_json::json!(2)]],
        )
        .await
        .unwrap();

    let driver = tidb_driver_no_db!();
    let data = driver
        .fetch_rows(&db, &db, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 2);

    with_db.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// validate_query integration tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn validate_query_valid_select() {
    let (driver, db) = tidb_driver!();
    let result = driver.validate_query(&db, "SELECT 1").await.unwrap();
    assert!(result.is_none(), "valid SQL should return None");
}

#[tokio::test]
async fn validate_query_syntax_error() {
    let (driver, db) = tidb_driver!();
    let result = driver.validate_query(&db, "SELCT 1").await.unwrap();
    assert!(result.is_some(), "invalid SQL should return Some");
    let err = result.unwrap();
    assert!(!err.message.is_empty());
}

#[tokio::test]
async fn validate_query_nonexistent_table() {
    let (driver, db) = tidb_driver!();
    let result = driver
        .validate_query(&db, "SELECT * FROM nonexistent_table_xyz_12345")
        .await
        .unwrap();
    assert!(result.is_some());
}

#[tokio::test]
async fn validate_query_empty_string() {
    let (driver, db) = tidb_driver!();
    let result = driver.validate_query(&db, "").await.unwrap();
    assert!(result.is_some(), "empty SQL should be an error");
}

#[tokio::test]
async fn tidb_get_query_stats_ok_or_stmt_summary_message() {
    let (driver, _db) = tidb_driver!();
    let res = driver.get_query_stats().await;
    assert!(res.is_ok(), "get_query_stats returned: {:?}", res.err());
    let qs = res.unwrap();
    if qs.available {
        assert_eq!(qs.kind, QueryStatsKind::Available);
        assert!(qs.message.is_none());
    } else {
        assert_eq!(qs.kind, QueryStatsKind::TidbStmtSummaryDisabled);
        let msg = qs.message.as_deref().unwrap_or("");
        assert!(
            msg.contains("tidb_enable_stmt_summary") || msg.contains("Statement summary"),
            "unexpected unavailable message: {msg}"
        );
    }
}

// ---------------------------------------------------------------------------
// alter_table: editor-driven multi-op sequences (issue #59 follow-up)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tidb_alter_table_rename_then_change_type_same_column() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_seq_rn_ct");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{tbl}` (pk INT PRIMARY KEY, legacy_id INT)"),
        )
        .await
        .unwrap();
    driver
        .execute_query(&db, &format!("INSERT INTO `{tbl}` VALUES (1, 42)"))
        .await
        .unwrap();

    driver
        .alter_table(
            &db,
            &db,
            &tbl,
            &[
                AlterTableOperation::RenameColumn {
                    old_name: "legacy_id".into(),
                    new_name: "id".into(),
                },
                AlterTableOperation::ChangeColumnType {
                    column_name: "id".into(),
                    new_type: "BIGINT".into(),
                },
            ],
        )
        .await
        .unwrap();

    let cols = driver.list_columns(&db, &db, &tbl).await.unwrap();
    let id = cols.iter().find(|c| c.name == "id").unwrap();
    assert!(id.data_type.to_lowercase().contains("bigint"));

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_alter_table_rename_table_then_alter_columns() {
    let (driver, db) = tidb_driver!();
    let old = unique_table("ti_rn_then");
    let new_name = unique_table("ti_rn_then_new");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{old}` (id INT PRIMARY KEY, status TEXT)"),
        )
        .await
        .unwrap();

    driver
        .alter_table(
            &db,
            &db,
            &old,
            &[
                AlterTableOperation::RenameTable {
                    new_name: new_name.clone(),
                },
                AlterTableOperation::AddColumn {
                    column: ColumnDefinition {
                        name: "created_at".into(),
                        data_type: "DATETIME".into(),
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

    let cols = driver.list_columns(&db, &db, &new_name).await.unwrap();
    let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
    assert!(names.contains(&"id"));
    assert!(names.contains(&"created_at"));
    assert!(!names.contains(&"status"));

    driver
        .drop_object(&db, &db, &new_name, "TABLE")
        .await
        .unwrap();
}

#[tokio::test]
async fn tidb_alter_table_drop_then_add_same_name() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_drop_add");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{tbl}` (id INT PRIMARY KEY, payload TEXT)"),
        )
        .await
        .unwrap();

    driver
        .alter_table(
            &db,
            &db,
            &tbl,
            &[
                AlterTableOperation::DropColumn {
                    column_name: "payload".into(),
                },
                AlterTableOperation::AddColumn {
                    column: ColumnDefinition {
                        name: "payload".into(),
                        data_type: "JSON".into(),
                        is_nullable: true,
                        is_primary_key: false,
                        default_value: None,
                    },
                },
            ],
        )
        .await
        .unwrap();

    let cols = driver.list_columns(&db, &db, &tbl).await.unwrap();
    let payload = cols.iter().find(|c| c.name == "payload").unwrap();
    assert!(payload.data_type.to_lowercase().contains("json"));

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_alter_table_empty_operations_is_noop() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_noop");

    driver
        .execute_query(&db, &format!("CREATE TABLE `{tbl}` (id INT PRIMARY KEY)"))
        .await
        .unwrap();
    driver
        .alter_table(&db, &db, &tbl, &[])
        .await
        .expect("empty operations should be a successful no-op");
    let cols = driver.list_columns(&db, &db, &tbl).await.unwrap();
    assert_eq!(cols.len(), 1);
    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_alter_table_add_not_null_default_backfills_existing_rows() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_backfill");

    driver
        .execute_query(&db, &format!("CREATE TABLE `{tbl}` (id INT PRIMARY KEY)"))
        .await
        .unwrap();
    driver
        .execute_query(&db, &format!("INSERT INTO `{tbl}` VALUES (1), (2)"))
        .await
        .unwrap();

    driver
        .alter_table(
            &db,
            &db,
            &tbl,
            &[AlterTableOperation::AddColumn {
                column: ColumnDefinition {
                    name: "status".into(),
                    data_type: "VARCHAR(20)".into(),
                    is_nullable: false,
                    is_primary_key: false,
                    default_value: Some("'pending'".into()),
                },
            }],
        )
        .await
        .unwrap();

    let result = driver
        .execute_query(&db, &format!("SELECT status FROM `{tbl}` ORDER BY id"))
        .await
        .unwrap();
    let values: Vec<&str> = result
        .rows
        .iter()
        .filter_map(|r| r.first().and_then(|v| v.as_str()))
        .collect();
    assert_eq!(values, vec!["pending", "pending"]);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_alter_table_rename_nonexistent_column_errors() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_rn_bad");

    driver
        .execute_query(&db, &format!("CREATE TABLE `{tbl}` (id INT PRIMARY KEY)"))
        .await
        .unwrap();

    let result = driver
        .alter_table(
            &db,
            &db,
            &tbl,
            &[AlterTableOperation::RenameColumn {
                old_name: "nope".into(),
                new_name: "noooo".into(),
            }],
        )
        .await;
    assert!(result.is_err());

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_alter_table_add_duplicate_column_errors() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ti_dup");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{tbl}` (id INT PRIMARY KEY, status TEXT)"),
        )
        .await
        .unwrap();

    let result = driver
        .alter_table(
            &db,
            &db,
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

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// Driver correctness regressions
// ---------------------------------------------------------------------------

fn tidb_changes(
    db: &str,
    tbl: &str,
    updates: Vec<CellChange>,
    deletes: Vec<DeleteRow>,
) -> DataChanges {
    DataChanges {
        connection_id: "test".into(),
        database: db.into(),
        schema: db.into(),
        table: tbl.into(),
        updates,
        inserts: vec![],
        deletes,
    }
}

#[tokio::test]
async fn tidb_console_open_transaction_does_not_leak_into_pool() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("tidb_txleak");
    driver
        .execute_query(&db, &format!("CREATE TABLE `{tbl}` (id INT)"))
        .await
        .unwrap();
    let res = driver
        .execute_query(
            &db,
            &format!("START TRANSACTION; INSERT INTO `{tbl}` VALUES (1); SELEC broken"),
        )
        .await;
    assert!(res.is_err());
    // A connection still inside that transaction would see its own
    // uncommitted row.
    for _ in 0..8 {
        let r = driver
            .execute_query(&db, &format!("SELECT COUNT(*) AS c FROM `{tbl}`"))
            .await
            .unwrap();
        assert_eq!(r.rows[0][0], serde_json::json!(0));
    }
    // Open at the end of a successful run: rolled back and reported.
    let res = driver
        .execute_query(&db, &format!("BEGIN; INSERT INTO `{tbl}` VALUES (2)"))
        .await;
    assert!(res.is_err());
    // A balanced script commits normally.
    driver
        .execute_query(
            &db,
            &format!("BEGIN; INSERT INTO `{tbl}` VALUES (3); COMMIT; SELECT 1"),
        )
        .await
        .unwrap();
    for _ in 0..8 {
        let r = driver
            .execute_query(&db, &format!("SELECT COUNT(*) AS c FROM `{tbl}`"))
            .await
            .unwrap();
        assert_eq!(r.rows[0][0], serde_json::json!(1));
    }
    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_console_runs_in_selected_database() {
    let (driver, db) = tidb_driver!();
    let other = unique_table("tidb_ctxdb");
    driver
        .execute_query(
            &db,
            &format!("CREATE DATABASE `{other}`; CREATE TABLE `{other}`.only_here (x INT); INSERT INTO `{other}`.only_here VALUES (7)"),
        )
        .await
        .unwrap();
    let r = driver
        .execute_query(&other, "SELECT x FROM only_here")
        .await
        .unwrap();
    assert_eq!(r.rows[0][0], serde_json::json!(7));
    let r = driver
        .execute_query(&other, "SELECT DATABASE() AS d")
        .await
        .unwrap();
    assert_eq!(r.rows[0][0], serde_json::json!(other.clone()));
    // The pool's default database is restored for everyone else.
    for _ in 0..8 {
        let r = driver
            .execute_query(&db, "SELECT DATABASE() AS d")
            .await
            .unwrap();
        assert_eq!(r.rows[0][0], serde_json::json!(db.clone()));
    }
    driver
        .execute_query(&db, &format!("DROP DATABASE `{other}`"))
        .await
        .unwrap();
}

#[tokio::test]
async fn tidb_big_integer_ids_round_trip_through_apply_changes() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("tidb_bigid");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{tbl}` (id BIGINT PRIMARY KEY, u BIGINT UNSIGNED, amount DECIMAL(40,20), note VARCHAR(20)); \
                 INSERT INTO `{tbl}` VALUES \
                 (1234567890123456789, 18446744073709551615, 12345678901234567890.12345678901234567890, 'a'), \
                 (1234567890123456790, 5, 1.5, 'b')"
            ),
        )
        .await
        .unwrap();
    let data = driver
        .fetch_rows(&db, &db, &tbl, 0, 10, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.rows[0][0], serde_json::json!("1234567890123456789"));
    assert_eq!(data.rows[0][1], serde_json::json!("18446744073709551615"));
    assert_eq!(
        data.rows[0][2],
        serde_json::json!("12345678901234567890.12345678901234567890")
    );
    assert_eq!(data.rows[1][1], serde_json::json!(5));

    // The two ids differ only beyond double precision: a quoted
    // comparison would match both rows.
    let changes = tidb_changes(
        &db,
        &tbl,
        vec![CellChange {
            row_index: 0,
            column_name: "note".into(),
            old_value: serde_json::json!("a"),
            new_value: serde_json::json!("edited"),
            primary_key_values: vec![("id".into(), data.rows[0][0].clone())],
        }],
        vec![DeleteRow {
            primary_key_values: vec![("id".into(), data.rows[1][0].clone())],
        }],
    );
    driver.apply_changes(&changes).await.unwrap();
    let after = driver
        .fetch_rows(&db, &db, &tbl, 0, 10, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(after.rows.len(), 1);
    assert_eq!(after.rows[0][0], serde_json::json!("1234567890123456789"));
    assert_eq!(after.rows[0][3], serde_json::json!("edited"));
    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_apply_changes_no_match_errors_but_identical_update_succeeds() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("tidb_nomatch");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{tbl}` (id INT PRIMARY KEY, v VARCHAR(10)); INSERT INTO `{tbl}` VALUES (1, 'a')"
            ),
        )
        .await
        .unwrap();
    // Writing the value the row already has matches (but changes) 0 rows
    // without CLIENT_FOUND_ROWS; it must not be reported as missing.
    let same = tidb_changes(
        &db,
        &tbl,
        vec![CellChange {
            row_index: 0,
            column_name: "v".into(),
            old_value: serde_json::json!("a"),
            new_value: serde_json::json!("a"),
            primary_key_values: vec![("id".into(), serde_json::json!(1))],
        }],
        vec![],
    );
    driver.apply_changes(&same).await.unwrap();

    let missing = tidb_changes(
        &db,
        &tbl,
        vec![
            CellChange {
                row_index: 0,
                column_name: "v".into(),
                old_value: serde_json::json!("a"),
                new_value: serde_json::json!("b"),
                primary_key_values: vec![("id".into(), serde_json::json!(1))],
            },
            CellChange {
                row_index: 1,
                column_name: "v".into(),
                old_value: serde_json::json!("x"),
                new_value: serde_json::json!("y"),
                primary_key_values: vec![("id".into(), serde_json::json!(999))],
            },
        ],
        vec![],
    );
    let err = driver.apply_changes(&missing).await.unwrap_err();
    assert!(err.to_string().contains("no matching row"), "{err}");
    let r = driver
        .execute_query(&db, &format!("SELECT v FROM `{tbl}` WHERE id = 1"))
        .await
        .unwrap();
    assert_eq!(r.rows[0][0], serde_json::json!("a"));
    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_modify_column_preserves_attributes() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("tidb_modify");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{tbl}` ( \
                   id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
                   status VARCHAR(20) NOT NULL DEFAULT 'pending' COMMENT 'state of it', \
                   updated TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, \
                   note VARCHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL, \
                   qty INT NOT NULL DEFAULT 3)"
            ),
        )
        .await
        .unwrap();
    driver
        .alter_table(
            &db,
            &db,
            &tbl,
            &[
                AlterTableOperation::ChangeColumnType {
                    column_name: "id".into(),
                    new_type: "BIGINT".into(),
                },
                AlterTableOperation::SetNullable {
                    column_name: "status".into(),
                    nullable: true,
                },
                AlterTableOperation::SetNullable {
                    column_name: "updated".into(),
                    nullable: false,
                },
                AlterTableOperation::ChangeColumnType {
                    column_name: "note".into(),
                    new_type: "VARCHAR(50)".into(),
                },
                AlterTableOperation::ChangeColumnType {
                    column_name: "qty".into(),
                    new_type: "BIGINT".into(),
                },
            ],
        )
        .await
        .unwrap();

    let r = driver
        .execute_query(
            &db,
            &format!(
                "SELECT CAST(COLUMN_NAME AS CHAR) AS n, CAST(COLUMN_TYPE AS CHAR) AS t, \
                 CAST(IS_NULLABLE AS CHAR) AS nl, CAST(COLUMN_DEFAULT AS CHAR) AS d, \
                 CAST(EXTRA AS CHAR) AS e, CAST(COLUMN_COMMENT AS CHAR) AS c, \
                 CAST(COLLATION_NAME AS CHAR) AS co \
                 FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '{db}' AND TABLE_NAME = '{tbl}'"
            ),
        )
        .await
        .unwrap();
    let col = |name: &str| -> Vec<String> {
        let row = r
            .rows
            .iter()
            .find(|row| row[0] == serde_json::json!(name))
            .unwrap_or_else(|| panic!("{name} missing"));
        row.iter()
            .map(|v| v.as_str().map(str::to_string).unwrap_or_default())
            .collect()
    };
    let id = col("id");
    assert!(id[1].to_lowercase().starts_with("bigint"), "{id:?}");
    assert!(id[4].to_lowercase().contains("auto_increment"), "{id:?}");

    let status = col("status");
    assert_eq!(status[2], "YES");
    assert!(status[3].trim_matches('\'') == "pending", "{status:?}");
    assert_eq!(status[5], "state of it");

    let updated = col("updated");
    assert_eq!(updated[2], "NO");
    assert!(
        updated[4].to_lowercase().contains("on update"),
        "{updated:?}"
    );
    assert!(
        updated[3].to_lowercase().starts_with("current_timestamp"),
        "{updated:?}"
    );

    let note = col("note");
    assert_eq!(note[1].to_lowercase(), "varchar(50)");
    assert_eq!(note[6], "utf8mb4_bin");

    let qty = col("qty");
    assert_eq!(qty[2], "NO");
    assert_eq!(qty[3].trim_matches('\''), "3");

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_special_values_render_instead_of_null() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("tidb_vals");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{tbl}` (id INT PRIMARY KEY, t TIME, y YEAR, b BIT(8), dt DATETIME(6), d DECIMAL(40,20)); \
                 INSERT INTO `{tbl}` VALUES (1, '-838:59:59', 2024, b'101', '2024-01-02 03:04:05.123456', 0.1), \
                                            (2, '100:00:00', 1999, b'11111111', '2024-01-02 03:04:05', 12.5)"
            ),
        )
        .await
        .unwrap();
    let data = driver
        .fetch_rows(&db, &db, &tbl, 0, 10, Vec::new(), None)
        .await
        .unwrap();
    use serde_json::json;
    assert_eq!(data.rows[0][1], json!("-838:59:59"));
    assert_eq!(data.rows[1][1], json!("100:00:00"));
    assert_eq!(data.rows[0][2], json!(2024));
    assert_eq!(data.rows[0][3], json!(5));
    assert_eq!(data.rows[1][3], json!(255));
    assert_eq!(data.rows[0][4], json!("2024-01-02 03:04:05.123456"));
    assert_eq!(data.rows[1][4], json!("2024-01-02 03:04:05"));
    assert_eq!(data.rows[0][5], json!(0.1));
    assert_eq!(data.rows[1][5], json!(12.5));

    // Console path renders the same way.
    let r = driver
        .execute_query(&db, &format!("SELECT t, y, b FROM `{tbl}` ORDER BY id"))
        .await
        .unwrap();
    assert_eq!(r.rows[0][0], json!("-838:59:59"));
    assert_eq!(r.rows[0][1], json!(2024));
    assert_eq!(r.rows[0][2], json!(5));
    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_fractional_datetime_pk_round_trips() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("tidb_dtpk");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{tbl}` (ts DATETIME(6) PRIMARY KEY, v VARCHAR(10)); \
                 INSERT INTO `{tbl}` VALUES ('2024-05-06 07:08:09.123456', 'a')"
            ),
        )
        .await
        .unwrap();
    let data = driver
        .fetch_rows(&db, &db, &tbl, 0, 10, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(
        data.rows[0][0],
        serde_json::json!("2024-05-06 07:08:09.123456")
    );
    let changes = tidb_changes(
        &db,
        &tbl,
        vec![CellChange {
            row_index: 0,
            column_name: "v".into(),
            old_value: serde_json::json!("a"),
            new_value: serde_json::json!("b"),
            primary_key_values: vec![("ts".into(), data.rows[0][0].clone())],
        }],
        vec![],
    );
    driver.apply_changes(&changes).await.unwrap();
    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_filter_and_console_handle_quoted_separators() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("tidb_fsemi");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{tbl}` (id INT PRIMARY KEY, v VARCHAR(20)); \
                 INSERT INTO `{tbl}` VALUES (1, 'a;b'), (2, 'it\\'s; here')"
            ),
        )
        .await
        .unwrap();
    let data = driver
        .fetch_rows(
            &db,
            &db,
            &tbl,
            0,
            10,
            Vec::new(),
            Some("`v` = 'a;b' OR `v` = 'it\\'s; here'".into()),
        )
        .await
        .unwrap();
    assert_eq!(data.rows.len(), 2);
    assert!(driver
        .fetch_rows(
            &db,
            &db,
            &tbl,
            0,
            10,
            Vec::new(),
            Some("1=1; DROP TABLE x".into())
        )
        .await
        .is_err());
    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// Console cancel
// ---------------------------------------------------------------------------

/// A token that fires after `ms` milliseconds, like a user pressing Stop.
fn cancel_after(ms: u64) -> tablio_lib::db::cancel::CancellationToken {
    let token = tablio_lib::db::cancel::CancellationToken::new();
    let t = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
        t.cancel();
    });
    token
}

fn marker() -> String {
    format!("cancel_{}", uuid::Uuid::new_v4().simple())
}

#[tokio::test]
async fn tidb_console_cancel_stops_running_query() {
    let (driver, db) = tidb_driver!();
    let m = marker();
    let start = std::time::Instant::now();
    let err = driver
        .execute_query_cancellable(
            &db,
            &format!("SELECT SLEEP(30), '{m}'", m = m),
            cancel_after(300),
        )
        .await
        .expect_err("cancelled run must fail");
    let elapsed = start.elapsed();
    assert!(
        tablio_lib::db::cancel::is_cancelled_error(&err),
        "expected cancelled error, got: {err:#}"
    );
    assert!(
        elapsed < std::time::Duration::from_millis(2500),
        "cancel took {elapsed:?}"
    );
    // The connection(s) are usable afterwards: run more queries than the
    // pool holds so a poisoned connection would be handed out.
    for _ in 0..8 {
        let r = driver.execute_query(&db, "SELECT 1").await.unwrap();
        assert_eq!(r.rows.len(), 1);
    }
}

#[tokio::test]
async fn tidb_console_cancel_rolls_back_script_transaction() {
    let (driver, db) = tidb_driver!();
    let t = unique_table("cancel_txn");
    driver
        .execute_query(&db, &format!("CREATE TABLE {t} (id INT)", t = t))
        .await
        .unwrap();
    let m = marker();
    let script = format!(
        "BEGIN; INSERT INTO {t} VALUES (1); SELECT SLEEP(30), '{m}'",
        t = t,
        m = m
    );
    let err = driver
        .execute_query_cancellable(&db, &script, cancel_after(300))
        .await
        .expect_err("cancelled run must fail");
    assert!(tablio_lib::db::cancel::is_cancelled_error(&err), "{err:#}");
    for _ in 0..8 {
        let r = driver
            .execute_query(&db, &format!("SELECT COUNT(*) FROM {t}", t = t))
            .await
            .unwrap();
        assert_eq!(
            r.rows[0][0]
                .as_i64()
                .or_else(|| r.rows[0][0].as_str().and_then(|s| s.parse().ok())),
            Some(0),
            "insert from the cancelled script must be rolled back"
        );
    }
    let _ = driver
        .execute_query(&db, &format!("DROP TABLE {t}", t = t))
        .await;
}

#[tokio::test]
async fn tidb_console_cancel_before_start_is_reported() {
    let (driver, db) = tidb_driver!();
    let token = tablio_lib::db::cancel::CancellationToken::new();
    token.cancel();
    let err = driver
        .execute_query_cancellable(&db, "SELECT 1", token)
        .await
        .expect_err("pre-cancelled run must fail");
    assert!(tablio_lib::db::cancel::is_cancelled_error(&err));
    driver.execute_query(&db, "SELECT 1").await.unwrap();
}

// ---------------------------------------------------------------------------
// Value decoding and write-back
// ---------------------------------------------------------------------------

async fn tidb_exec(driver: &impl DatabaseDriver, db: &str, sql: &str) -> QueryResult {
    driver
        .execute_query(db, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

fn tidb_update(
    db: &str,
    tbl: &str,
    pk: Vec<(String, serde_json::Value)>,
    col: &str,
    new: serde_json::Value,
) -> DataChanges {
    DataChanges {
        connection_id: "test".into(),
        database: db.into(),
        schema: db.into(),
        table: tbl.into(),
        updates: vec![CellChange {
            row_index: 0,
            column_name: col.into(),
            old_value: serde_json::Value::Null,
            new_value: new,
            primary_key_values: pk,
        }],
        inserts: vec![],
        deletes: vec![],
    }
}

#[tokio::test]
async fn tidb_bin_collated_text_is_text_and_editable() {
    use serde_json::json;
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("bincoll");
    tidb_exec(
        &driver,
        &db,
        &format!(
            "CREATE TABLE `{tbl}` (k VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PRIMARY KEY, \
             s TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)"
        ),
    )
    .await;
    tidb_exec(
        &driver,
        &db,
        &format!("INSERT INTO `{tbl}` VALUES ('Abc', 'x')"),
    )
    .await;
    let data = driver
        .fetch_rows(&db, &db, &tbl, 0, 10, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.rows[0], vec![json!("Abc"), json!("x")]);

    driver
        .apply_changes(&tidb_update(
            &db,
            &tbl,
            vec![("k".into(), json!("Abc"))],
            "s",
            json!("y"),
        ))
        .await
        .unwrap();
    let r = tidb_exec(
        &driver,
        &db,
        &format!("SELECT s, 'x' COLLATE utf8mb4_bin FROM `{tbl}`"),
    )
    .await;
    assert_eq!(r.rows[0], vec![json!("y"), json!("x")]);
    let r = tidb_exec(&driver, &db, &format!("SHOW COLUMNS FROM `{tbl}`")).await;
    assert_eq!(r.rows[0][0], json!("k"));
    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_binary_values_write_back_as_bytes() {
    use serde_json::json;
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("binwb");
    let id = "0x00112233445566778899aabbccddeeff";
    tidb_exec(
        &driver,
        &db,
        &format!("CREATE TABLE `{tbl}` (id BINARY(16) PRIMARY KEY, b VARBINARY(10), bl BLOB)"),
    )
    .await;
    tidb_exec(
        &driver,
        &db,
        &format!("INSERT INTO `{tbl}` VALUES ({id}, 0x00ff80, 0x01)"),
    )
    .await;
    let data = driver
        .fetch_rows(&db, &db, &tbl, 0, 10, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(
        data.rows[0],
        vec![json!(id), json!("0x00ff80"), json!("0x01")]
    );

    // Edit through the binary primary key.
    driver
        .apply_changes(&tidb_update(
            &db,
            &tbl,
            vec![("id".into(), json!(id))],
            "b",
            json!("0xdeadbeef"),
        ))
        .await
        .unwrap();
    // Text typed into a binary column is stored as its bytes.
    driver
        .apply_changes(&tidb_update(
            &db,
            &tbl,
            vec![("id".into(), json!(id))],
            "bl",
            json!("hi"),
        ))
        .await
        .unwrap();
    let r = tidb_exec(
        &driver,
        &db,
        &format!("SELECT HEX(b), HEX(bl) FROM `{tbl}`"),
    )
    .await;
    assert_eq!(r.rows[0], vec![json!("DEADBEEF"), json!("6869")]);

    let mut del = tidb_update(&db, &tbl, vec![], "b", json!(null));
    del.updates.clear();
    del.deletes.push(DeleteRow {
        primary_key_values: vec![("id".into(), json!(id))],
    });
    driver.apply_changes(&del).await.unwrap();
    let r = tidb_exec(&driver, &db, &format!("SELECT COUNT(*) FROM `{tbl}`")).await;
    assert_eq!(r.rows[0][0], json!(0));
    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_json_is_sent_as_server_text() {
    use serde_json::json;
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("jsontext");
    tidb_exec(
        &driver,
        &db,
        &format!("CREATE TABLE `{tbl}` (id INT PRIMARY KEY, j JSON)"),
    )
    .await;
    tidb_exec(
        &driver,
        &db,
        &format!(
            "INSERT INTO `{tbl}` VALUES (1, '{{\"b\":1,\"aa\":2,\"big\":12345678901234567890}}'), \
             (2, 'null'), (3, '\"str\"'), (4, NULL)"
        ),
    )
    .await;
    let data = driver
        .fetch_rows(&db, &db, &tbl, 0, 10, Vec::new(), None)
        .await
        .unwrap();
    let text = tidb_exec(
        &driver,
        &db,
        &format!("SELECT CAST(j AS CHAR) FROM `{tbl}` ORDER BY id"),
    )
    .await;
    for (i, row) in data.rows.iter().enumerate() {
        assert_eq!(row[1], text.rows[i][0], "row {}", i + 1);
    }
    assert!(data.rows[0][1]
        .as_str()
        .unwrap()
        .contains("12345678901234567890"));
    assert_eq!(data.rows[1][1], json!("null"));
    assert_eq!(data.rows[2][1], json!("\"str\""));
    assert_eq!(data.rows[3][1], json!(null));

    // The edited text is written as JSON: a JSON string scalar and a
    // JSON null (not SQL NULL).
    driver
        .apply_changes(&tidb_update(
            &db,
            &tbl,
            vec![("id".into(), json!(2))],
            "j",
            json!("\"hello\""),
        ))
        .await
        .unwrap();
    driver
        .apply_changes(&tidb_update(
            &db,
            &tbl,
            vec![("id".into(), json!(3))],
            "j",
            json!("null"),
        ))
        .await
        .unwrap();
    let r = tidb_exec(
        &driver,
        &db,
        &format!("SELECT JSON_TYPE(j), j IS NULL FROM `{tbl}` WHERE id IN (2, 3) ORDER BY id"),
    )
    .await;
    assert_eq!(r.rows[0][0], json!("STRING"));
    assert_eq!(r.rows[1][0], json!("NULL"));
    assert_eq!(r.rows[1][1], json!(0));
    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_tinyint1_decodes_as_integer() {
    use serde_json::json;
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("tiny1");
    tidb_exec(
        &driver,
        &db,
        &format!("CREATE TABLE `{tbl}` (id INT PRIMARY KEY, f TINYINT(1))"),
    )
    .await;
    tidb_exec(
        &driver,
        &db,
        &format!("INSERT INTO `{tbl}` VALUES (1, 5), (2, 1), (3, 0)"),
    )
    .await;
    let data = driver
        .fetch_rows(&db, &db, &tbl, 0, 10, Vec::new(), None)
        .await
        .unwrap();
    let got: Vec<_> = data.rows.iter().map(|r| r[1].clone()).collect();
    assert_eq!(got, vec![json!(5), json!(1), json!(0)]);
    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// Console: session state, result shapes, comments
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tidb_console_set_does_not_leak_into_the_pool() {
    let (driver, db) = tidb_driver!();
    let r = driver
        .execute_query(&db, "SET SESSION sql_mode = 'ANSI_QUOTES'")
        .await
        .unwrap();
    assert!(
        r.notices.iter().any(|n| n.contains("session state")),
        "{:?}",
        r.notices
    );
    // With ANSI_QUOTES "x" would be an identifier (unknown column).
    for _ in 0..8 {
        let r = driver
            .execute_query(&db, "SELECT \"x\" AS v")
            .await
            .expect("sql_mode must not be inherited");
        assert_eq!(r.rows[0][0], serde_json::json!("x"));
    }
}

#[tokio::test]
async fn tidb_console_autocommit_off_is_not_silently_rolled_back() {
    let (driver, db) = tidb_driver!();
    let tbl = unique_table("ac_off");
    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{tbl}` (id INT PRIMARY KEY, v INT) ENGINE=InnoDB"),
        )
        .await
        .unwrap();
    driver
        .execute_query(&db, &format!("INSERT INTO `{tbl}` VALUES (1, 1)"))
        .await
        .unwrap();
    let err = driver
        .execute_query(
            &db,
            &format!("SET autocommit = 0; UPDATE `{tbl}` SET v = 2 WHERE id = 1"),
        )
        .await
        .expect_err("uncommitted changes must be reported");
    assert!(err.to_string().contains("autocommit"), "{err}");
    let r = driver
        .execute_query(&db, &format!("SELECT v FROM `{tbl}`"))
        .await
        .unwrap();
    assert_eq!(r.rows[0][0], serde_json::json!(1));
    // With a COMMIT it is saved.
    driver
        .execute_query(
            &db,
            &format!("SET autocommit = 0; UPDATE `{tbl}` SET v = 3 WHERE id = 1; COMMIT"),
        )
        .await
        .unwrap();
    let r = driver
        .execute_query(&db, &format!("SELECT v FROM `{tbl}`"))
        .await
        .unwrap();
    assert_eq!(r.rows[0][0], serde_json::json!(3));
    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn tidb_console_empty_result_keeps_header() {
    let (driver, db) = tidb_driver!();
    let r = driver
        .execute_query(&db, "SELECT 1 AS a, 'x' AS b FROM DUAL WHERE 1 = 0")
        .await
        .unwrap();
    assert!(r.is_select);
    assert_eq!(r.columns, vec!["a", "b"]);
    assert!(r.rows.is_empty());
}
