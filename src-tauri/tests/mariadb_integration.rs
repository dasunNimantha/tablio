use tablio_lib::db::mariadb::MariadbDriver;
use tablio_lib::db::DatabaseDriver;
use tablio_lib::models::*;

macro_rules! mariadb_driver {
    () => {{
        let url = match std::env::var("TEST_MARIADB_URL") {
            Ok(v) if !v.is_empty() => v,
            _ => {
                eprintln!("Skipping: TEST_MARIADB_URL not set");
                return;
            }
        };
        let parts = url.strip_prefix("mysql://").expect("bad TEST_MARIADB_URL");
        let (user_pass, rest) = parts.split_once('@').expect("missing @");
        let (user, password) = user_pass.split_once(':').expect("missing :");
        let (host_port, database) = rest.split_once('/').expect("missing /");
        let database = database.split('?').next().unwrap();
        let (host, port) = host_port.split_once(':').expect("missing port");
        let config = ConnectionConfig {
            id: "test".into(),
            name: "test".into(),
            db_type: DbType::Mariadb,
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
        };
        (
            MariadbDriver::connect(&config).await.unwrap(),
            database.to_string(),
        )
    }};
}

macro_rules! mariadb_driver_no_db {
    () => {{
        let url = match std::env::var("TEST_MARIADB_URL") {
            Ok(v) if !v.is_empty() => v,
            _ => {
                eprintln!("Skipping: TEST_MARIADB_URL not set");
                return;
            }
        };
        let parts = url.strip_prefix("mysql://").expect("bad TEST_MARIADB_URL");
        let (user_pass, rest) = parts.split_once('@').expect("missing @");
        let (user, password) = user_pass.split_once(':').expect("missing :");
        let (host_port, _database) = rest.split_once('/').expect("missing /");
        let (host, port) = host_port.split_once(':').expect("missing port");
        let config = ConnectionConfig {
            id: "test-no-db".into(),
            name: "test-no-db".into(),
            db_type: DbType::Mariadb,
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
        };
        MariadbDriver::connect(&config).await.unwrap()
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
async fn mariadb_test_connection() {
    let (driver, _db) = mariadb_driver!();
    assert!(driver.test_connection().await.unwrap());
}

// ---------------------------------------------------------------------------
// Databases, schemas, tables
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mariadb_list_databases() {
    let (driver, db) = mariadb_driver!();
    let dbs = driver.list_databases().await.unwrap();
    assert!(dbs.iter().any(|d| d.name == db));
}

#[tokio::test]
async fn mariadb_list_schemas() {
    let (driver, db) = mariadb_driver!();
    let schemas = driver.list_schemas(&db).await.unwrap();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].name, db);
}

#[tokio::test]
async fn mariadb_list_tables() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_lst");
    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{}` (id INT PRIMARY KEY) ENGINE=InnoDB", tbl),
        )
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
async fn mariadb_list_columns_various_types() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_types");
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
                    jdoc JSON NULL, \
                    e ENUM('a','b','c') NOT NULL DEFAULT 'a', \
                    s SET('x','y','z') NULL\
                ) ENGINE=InnoDB",
                tbl
            ),
        )
        .await
        .unwrap();

    let cols = driver.list_columns(&db, &db, &tbl).await.unwrap();
    assert_eq!(cols.len(), 10);

    let by_name: std::collections::HashMap<_, _> =
        cols.iter().map(|c| (c.name.as_str(), c)).collect();

    assert!(by_name["id"].is_primary_key);
    assert!(by_name["id"].is_auto_generated);
    assert_eq!(by_name["id"].data_type, "int");
    assert_eq!(by_name["title"].data_type, "varchar");
    assert!(!by_name["title"].is_nullable);
    assert_eq!(by_name["amt"].data_type, "decimal");
    assert_eq!(by_name["body"].data_type, "text");
    assert_eq!(by_name["dtd"].data_type, "datetime");
    assert_eq!(by_name["ts"].data_type, "timestamp");

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// Indexes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mariadb_list_indexes() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_idx");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (\
                    id INT AUTO_INCREMENT PRIMARY KEY, \
                    name VARCHAR(80) NOT NULL, \
                    KEY idx_name (name), \
                    UNIQUE KEY uq_id_name (id, name)\
                ) ENGINE=InnoDB",
                tbl
            ),
        )
        .await
        .unwrap();

    let idx = driver.list_indexes(&db, &db, &tbl).await.unwrap();
    let names: Vec<&str> = idx.iter().map(|i| i.name.as_str()).collect();
    assert!(names.contains(&"PRIMARY"));
    assert!(names.iter().any(|n| *n == "idx_name"));
    assert!(names.iter().any(|n| *n == "uq_id_name"));

    let idx_name = idx.iter().find(|i| i.name == "idx_name").unwrap();
    assert!(!idx_name.is_unique);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// Foreign keys
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mariadb_list_foreign_keys() {
    let (driver, db) = mariadb_driver!();
    let parent = unique_table("mdb_fkp");
    let child = unique_table("mdb_fkc");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT PRIMARY KEY) ENGINE=InnoDB",
                parent
            ),
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
                ) ENGINE=InnoDB",
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
    assert_eq!(fks[0].referenced_column, "id");

    driver.drop_object(&db, &db, &child, "TABLE").await.unwrap();
    driver
        .drop_object(&db, &db, &parent, "TABLE")
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Functions
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mariadb_list_functions() {
    let (driver, db) = mariadb_driver!();
    let fname = unique_table("mdb_fn");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE FUNCTION `{}`() RETURNS INT DETERMINISTIC RETURN 42",
                fname
            ),
        )
        .await
        .unwrap();

    let funcs = driver.list_functions(&db, &db).await.unwrap();
    assert!(funcs.iter().any(|f| f.name == fname));

    driver
        .execute_query(&db, &format!("DROP FUNCTION `{}`", fname))
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Triggers
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mariadb_list_triggers() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_trgtbl");
    let trg = unique_table("mdb_trg");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT AUTO_INCREMENT PRIMARY KEY, val INT NOT NULL DEFAULT 0) ENGINE=InnoDB",
                tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TRIGGER `{}` BEFORE INSERT ON `{}` FOR EACH ROW SET NEW.val = NEW.val",
                trg, tbl
            ),
        )
        .await
        .unwrap();

    let triggers = driver.list_triggers(&db, &db, &tbl).await.unwrap();
    assert!(triggers.iter().any(|t| t.name == trg));
    assert_eq!(
        triggers.iter().find(|t| t.name == trg).unwrap().event,
        "INSERT"
    );

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// Table stats
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mariadb_get_table_stats() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_stats");
    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{}` (id INT PRIMARY KEY) ENGINE=InnoDB", tbl),
        )
        .await
        .unwrap();
    driver
        .execute_query(&db, &format!("INSERT INTO `{}` VALUES (1),(2),(3)", tbl))
        .await
        .unwrap();

    let stats = driver.get_table_stats(&db, &db, &tbl).await.unwrap();
    assert_eq!(stats.table_name, tbl);
    assert_eq!(stats.row_count, 3);
    assert!(!stats.total_size.is_empty());

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// fetch_rows
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mariadb_fetch_rows_empty_table() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_empty");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT PRIMARY KEY, val TEXT) ENGINE=InnoDB",
                tbl
            ),
        )
        .await
        .unwrap();

    let data = driver
        .fetch_rows(&db, &db, &tbl, 0, 50, None, None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 0);
    assert!(data.rows.is_empty());
    assert_eq!(data.columns.len(), 2);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mariadb_fetch_rows_with_data() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_data");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100) NOT NULL) ENGINE=InnoDB",
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
        .fetch_rows(&db, &db, &tbl, 0, 50, None, None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 1);
    assert_eq!(data.rows[0][1], serde_json::json!("Alice"));

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mariadb_fetch_rows_pagination() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_page");
    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{}` (id INT PRIMARY KEY) ENGINE=InnoDB", tbl),
        )
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
        .fetch_rows(&db, &db, &tbl, 0, 5, None, None)
        .await
        .unwrap();
    assert_eq!(page1.rows.len(), 5);
    assert_eq!(page1.total_rows, 10);

    let page2 = driver
        .fetch_rows(&db, &db, &tbl, 5, 5, None, None)
        .await
        .unwrap();
    assert_eq!(page2.rows.len(), 5);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mariadb_fetch_rows_sort_asc_desc() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_sort");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT PRIMARY KEY, name VARCHAR(50)) ENGINE=InnoDB",
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
            &db,
            &db,
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

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mariadb_fetch_rows_filter() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_flt");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT PRIMARY KEY, val INT NOT NULL) ENGINE=InnoDB",
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
        .fetch_rows(&db, &db, &tbl, 0, 50, None, Some("`val` > 15".into()))
        .await
        .unwrap();
    assert_eq!(data.total_rows, 2);
    assert_eq!(data.rows.len(), 2);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mariadb_fetch_rows_null_values() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_null");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT PRIMARY KEY, maybe_null INT NULL) ENGINE=InnoDB",
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
        .fetch_rows(&db, &db, &tbl, 0, 50, None, None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 1);
    assert_eq!(data.rows[0][1], serde_json::Value::Null);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mariadb_fetch_rows_various_data_types() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_ftypes");
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
                ) ENGINE=InnoDB",
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
        .fetch_rows(&db, &db, &tbl, 0, 10, None, None)
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
async fn mariadb_execute_query_select() {
    let (driver, db) = mariadb_driver!();
    let result = driver
        .execute_query(&db, "SELECT 1 AS num, 'hello' AS greeting")
        .await
        .unwrap();
    assert!(result.is_select);
    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.rows.len(), 1);
}

#[tokio::test]
async fn mariadb_execute_query_dml() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_dml");
    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{}` (id INT PRIMARY KEY) ENGINE=InnoDB", tbl),
        )
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
async fn mariadb_execute_query_invalid_sql_errors() {
    let (driver, db) = mariadb_driver!();
    let err = driver.execute_query(&db, "SELEC 1").await;
    assert!(err.is_err());
}

// ---------------------------------------------------------------------------
// explain_query
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mariadb_explain_query() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_expl");
    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{}` (id INT PRIMARY KEY) ENGINE=InnoDB", tbl),
        )
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
async fn mariadb_get_ddl() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_ddl");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT PRIMARY KEY, note VARCHAR(5)) ENGINE=InnoDB",
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
async fn mariadb_apply_changes_insert() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_ins");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100) NOT NULL) ENGINE=InnoDB",
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
        .fetch_rows(&db, &db, &tbl, 0, 50, None, None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 1);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mariadb_apply_changes_update() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_upd");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT PRIMARY KEY, val VARCHAR(50)) ENGINE=InnoDB",
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
        .fetch_rows(&db, &db, &tbl, 0, 50, None, None)
        .await
        .unwrap();
    assert_eq!(data.rows[0][1], serde_json::json!("new"));

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mariadb_apply_changes_delete() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_del");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT PRIMARY KEY, val TEXT) ENGINE=InnoDB",
                tbl
            ),
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
        .fetch_rows(&db, &db, &tbl, 0, 50, None, None)
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
async fn mariadb_create_table_basic() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_ct");
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
async fn mariadb_alter_table_add_rename_drop() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_alt");
    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{}` (id INT PRIMARY KEY) ENGINE=InnoDB", tbl),
        )
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
async fn mariadb_alter_table_change_column_type() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_chtype");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT PRIMARY KEY, n INT NOT NULL) ENGINE=InnoDB",
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
async fn mariadb_alter_table_set_nullable() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_setnull");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT PRIMARY KEY, note VARCHAR(100) NOT NULL) ENGINE=InnoDB",
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
async fn mariadb_alter_table_set_default() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_setdef");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT PRIMARY KEY, priority INT) ENGINE=InnoDB",
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
        .fetch_rows(&db, &db, &tbl, 0, 10, None, None)
        .await
        .unwrap();
    assert_eq!(data.rows[0][1], serde_json::json!(5));

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mariadb_alter_table_rename_table() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_rn");
    let new_name = unique_table("mdb_rn_new");
    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{}` (id INT PRIMARY KEY) ENGINE=InnoDB", tbl),
        )
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
async fn mariadb_truncate_table() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_trunc");
    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{}` (id INT PRIMARY KEY) ENGINE=InnoDB", tbl),
        )
        .await
        .unwrap();
    driver
        .execute_query(&db, &format!("INSERT INTO `{}` VALUES (1),(2),(3)", tbl))
        .await
        .unwrap();

    driver.truncate_table(&db, &db, &tbl).await.unwrap();
    let data = driver
        .fetch_rows(&db, &db, &tbl, 0, 50, None, None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 0);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mariadb_drop_object_view() {
    let (driver, db) = mariadb_driver!();
    let base = unique_table("mdb_vbase");
    let vname = unique_table("mdb_vw");
    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{}` (id INT PRIMARY KEY) ENGINE=InnoDB", base),
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
async fn mariadb_import_data() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_imp");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT PRIMARY KEY, label VARCHAR(40)) ENGINE=InnoDB",
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

    let data = driver
        .fetch_rows(&db, &db, &tbl, 0, 10, None, None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 2);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mariadb_import_data_large_batch() {
    let (driver, db) = mariadb_driver!();
    let tbl = unique_table("mdb_impbig");
    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{}` (id INT PRIMARY KEY) ENGINE=InnoDB", tbl),
        )
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
async fn mariadb_get_server_activity() {
    let (driver, _db) = mariadb_driver!();
    let activity = driver.get_server_activity().await.unwrap();
    assert!(!activity.is_empty());
}

#[tokio::test]
async fn mariadb_get_database_stats() {
    let (driver, _db) = mariadb_driver!();
    let stats = driver.get_database_stats().await.unwrap();
    assert!(stats.total_connections >= 1);
    assert!(stats.timestamp_ms > 0.0);
}

#[tokio::test]
async fn mariadb_get_locks() {
    let (driver, _db) = mariadb_driver!();
    let locks = driver.get_locks().await.unwrap();
    let _ = locks;
}

#[tokio::test]
async fn mariadb_get_server_config() {
    let (driver, _db) = mariadb_driver!();
    let cfg = driver.get_server_config().await.unwrap();
    assert!(!cfg.is_empty());
    assert!(cfg.iter().any(|e| e.name == "version"));
}

#[tokio::test]
async fn mariadb_get_query_stats() {
    let (driver, _db) = mariadb_driver!();
    let qs = driver.get_query_stats().await.unwrap();
    assert!(!qs.available);
}

#[tokio::test]
async fn mariadb_list_roles() {
    let (driver, _db) = mariadb_driver!();
    let roles = driver.list_roles().await.unwrap();
    let _ = roles;
}

#[tokio::test]
async fn mariadb_cancel_query_invalid_pid() {
    let (driver, _db) = mariadb_driver!();
    let r = driver.cancel_query("not_a_number").await;
    assert!(r.is_err());
}

// ---------------------------------------------------------------------------
// Optional database connection (connect without specifying a database)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mariadb_no_db_connect_and_list_databases() {
    let driver = mariadb_driver_no_db!();
    assert!(driver.test_connection().await.unwrap());
    let dbs = driver.list_databases().await.unwrap();
    assert!(!dbs.is_empty());
}

#[tokio::test]
async fn mariadb_no_db_list_tables_on_specific_database() {
    let (with_db, db) = mariadb_driver!();
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

    let driver = mariadb_driver_no_db!();
    let tables = driver.list_tables(&db, &db).await.unwrap();
    assert!(
        tables.iter().any(|t| t.name == tbl),
        "Table '{}' not found via no-db driver",
        tbl
    );

    with_db.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mariadb_no_db_fetch_rows_on_specific_database() {
    let (with_db, db) = mariadb_driver!();
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

    let driver = mariadb_driver_no_db!();
    let data = driver
        .fetch_rows(&db, &db, &tbl, 0, 50, None, None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 2);

    with_db.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}
