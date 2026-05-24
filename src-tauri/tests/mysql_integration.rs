use tablio_lib::db::mysql::MysqlDriver;
use tablio_lib::db::DatabaseDriver;
use tablio_lib::models::*;

macro_rules! mysql_driver {
    () => {{
        let url = match std::env::var("TEST_MYSQL_URL") {
            Ok(v) if !v.is_empty() => v,
            _ => {
                eprintln!("Skipping: TEST_MYSQL_URL not set");
                return;
            }
        };
        let parts = url.strip_prefix("mysql://").expect("bad TEST_MYSQL_URL");
        let (user_pass, rest) = parts.split_once('@').expect("missing @");
        let (user, password) = user_pass.split_once(':').expect("missing :");
        let (host_port, database) = rest.split_once('/').expect("missing /");
        let database = database.split('?').next().unwrap();
        let (host, port) = host_port.split_once(':').expect("missing port");
        let config = ConnectionConfig {
            id: "test".into(),
            name: "test".into(),
            db_type: DbType::Mysql,
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
            MysqlDriver::connect(&config).await.unwrap(),
            database.to_string(),
        )
    }};
}

macro_rules! mysql_driver_no_db {
    () => {{
        let url = match std::env::var("TEST_MYSQL_URL") {
            Ok(v) if !v.is_empty() => v,
            _ => {
                eprintln!("Skipping: TEST_MYSQL_URL not set");
                return;
            }
        };
        let parts = url.strip_prefix("mysql://").expect("bad TEST_MYSQL_URL");
        let (user_pass, rest) = parts.split_once('@').expect("missing @");
        let (user, password) = user_pass.split_once(':').expect("missing :");
        let (host_port, _database) = rest.split_once('/').expect("missing /");
        let (host, port) = host_port.split_once(':').expect("missing port");
        let config = ConnectionConfig {
            id: "test-no-db".into(),
            name: "test-no-db".into(),
            db_type: DbType::Mysql,
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
        MysqlDriver::connect(&config).await.unwrap()
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
async fn mysql_test_connection() {
    let (driver, _db) = mysql_driver!();
    assert!(driver.test_connection().await.unwrap());
}

// ---------------------------------------------------------------------------
// Databases, schemas, tables
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mysql_list_databases() {
    let (driver, db) = mysql_driver!();
    let dbs = driver.list_databases().await.unwrap();
    assert!(dbs.iter().any(|d| d.name == db));
}

#[tokio::test]
async fn mysql_list_schemas() {
    let (driver, db) = mysql_driver!();
    let schemas = driver.list_schemas(&db).await.unwrap();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].name, db);
}

#[tokio::test]
async fn mysql_list_tables() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("lst_tbl");
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
async fn mysql_list_columns_various_types() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("all_types");
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

    assert_eq!(by_name["flag"].data_type, "tinyint");

    assert_eq!(by_name["amt"].data_type, "decimal");
    assert_eq!(by_name["body"].data_type, "text");
    assert_eq!(by_name["dtd"].data_type, "datetime");
    assert_eq!(by_name["ts"].data_type, "timestamp");
    assert_eq!(by_name["jdoc"].data_type, "json");
    assert_eq!(by_name["e"].data_type, "enum");
    assert_eq!(by_name["s"].data_type, "set");

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// Indexes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mysql_list_indexes() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("idx_tbl");
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
    assert!(names.contains(&"idx_name"));
    assert!(names.contains(&"uq_id_name"));

    let idx_name = idx.iter().find(|i| i.name == "idx_name").unwrap();
    assert!(!idx_name.is_unique);
    assert!(idx_name.columns.contains(&"name".to_string()));

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// Foreign keys
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mysql_list_foreign_keys() {
    let (driver, db) = mysql_driver!();
    let parent = unique_table("fk_parent");
    let child = unique_table("fk_child");
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
async fn mysql_list_functions() {
    let (driver, db) = mysql_driver!();
    let fname = unique_table("intfn");
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
async fn mysql_list_triggers() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("trig_tbl");
    let trg = unique_table("trig");
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
async fn mysql_get_table_stats() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("stats_tbl");
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
    assert_eq!(stats.row_count, 3, "exact COUNT(*) should return 3 rows");
    assert!(!stats.total_size.is_empty());

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// Partitioned tables
//
// MySQL native partitioning (PARTITION BY RANGE/LIST/HASH/KEY) keeps every
// partition inside the same logical row of information_schema.TABLES, so
// a single get_table_stats call should already aggregate across all
// partitions without any driver changes. This test pins that contract so
// we notice if a future refactor accidentally filters on a single
// partition.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mysql_get_table_stats_partitioned_table_aggregates_partitions() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("stats_part");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (
                    id INT NOT NULL,
                    yr INT NOT NULL,
                    PRIMARY KEY (id, yr)
                ) ENGINE=InnoDB
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
    // 2 rows per partition, 6 total — chosen so any "single partition"
    // regression would surface as a wrong count.
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
        "stats must aggregate across all RANGE partitions"
    );
    assert!(!stats.total_size.is_empty());

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mysql_get_table_stats_unknown_table_returns_not_found() {
    let (driver, db) = mysql_driver!();
    let bogus = unique_table("mysql_no_such_table");
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
async fn mysql_fetch_rows_empty_table() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("my_empty");

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
        .fetch_rows(&db, &db, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 0);
    assert!(data.rows.is_empty());
    assert_eq!(data.columns.len(), 2);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mysql_fetch_rows_with_data() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("my_ins");

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
        .fetch_rows(&db, &db, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 1);
    assert_eq!(data.rows[0][1], serde_json::json!("Alice"));

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mysql_fetch_rows_pagination() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("my_page");

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
async fn mysql_fetch_rows_sort_asc_desc() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("my_sort");

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
async fn mysql_fetch_rows_multi_column_sort() {
    // Multi-column sort (issue #57) end-to-end on MySQL.
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("multi_sort");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT PRIMARY KEY, dept VARCHAR(32), salary INT) ENGINE=InnoDB",
                tbl,
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!(
                "INSERT INTO `{}` VALUES \
                 (1,'eng',100),(2,'sales',90),(3,'eng',200),(4,'sales',70),(5,'eng',150)",
                tbl,
            ),
        )
        .await
        .unwrap();

    let rows = driver
        .fetch_rows(
            &db,
            &db,
            &tbl,
            0,
            10,
            vec![
                SortSpec {
                    column: "dept".into(),
                    direction: SortDirection::Asc,
                },
                SortSpec {
                    column: "salary".into(),
                    direction: SortDirection::Desc,
                },
            ],
            None,
        )
        .await
        .unwrap();

    assert_eq!(rows.rows.len(), 5);
    let dept_idx = rows.columns.iter().position(|c| c.name == "dept").unwrap();
    let salary_idx = rows
        .columns
        .iter()
        .position(|c| c.name == "salary")
        .unwrap();
    let depts: Vec<_> = rows.rows.iter().map(|r| r[dept_idx].clone()).collect();
    let salaries: Vec<_> = rows.rows.iter().map(|r| r[salary_idx].clone()).collect();
    assert_eq!(
        depts,
        vec![
            serde_json::json!("eng"),
            serde_json::json!("eng"),
            serde_json::json!("eng"),
            serde_json::json!("sales"),
            serde_json::json!("sales"),
        ],
    );
    assert_eq!(
        salaries,
        vec![
            serde_json::json!(200),
            serde_json::json!(150),
            serde_json::json!(100),
            serde_json::json!(90),
            serde_json::json!(70),
        ],
    );

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mysql_fetch_rows_filter() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("my_filter");

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
        .fetch_rows(&db, &db, &tbl, 0, 50, Vec::new(), Some("`val` > 15".into()))
        .await
        .unwrap();
    assert_eq!(data.total_rows, 2);
    assert_eq!(data.rows.len(), 2);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mysql_fetch_rows_unsafe_filter_rejected() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("my_unsafe");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{}` (id INT PRIMARY KEY) ENGINE=InnoDB", tbl),
        )
        .await
        .unwrap();

    let result = driver
        .fetch_rows(
            &db,
            &db,
            &tbl,
            0,
            50,
            Vec::new(),
            Some("1=1; DROP TABLE x".into()),
        )
        .await;
    assert!(result.is_err());

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mysql_fetch_rows_null_values() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("my_null");

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
        .fetch_rows(&db, &db, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 1);
    assert_eq!(data.rows[0][1], serde_json::Value::Null);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mysql_fetch_rows_various_data_types() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("fetch_types");
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
                ('hi', 1, 12.34, 'long', CAST('{{\"k\":1}}' AS JSON), '2020-01-15 10:30:00')",
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
    assert!(row[2] == serde_json::json!(true) || row[2] == serde_json::json!(1));
    let amt_val = &row[3];
    let amt = amt_val
        .as_f64()
        .or_else(|| amt_val.as_str().and_then(|s| s.parse::<f64>().ok()))
        .or_else(|| amt_val.as_i64().map(|i| i as f64))
        .unwrap_or_else(|| panic!("amt unexpected format: {:?}", amt_val));
    assert!((amt - 12.34).abs() < 0.01);
    assert_eq!(row[4], serde_json::json!("long"));
    assert!(row[5].is_object() || row[5].is_string());
    assert!(
        row[6].is_string(),
        "DATETIME should be a string, got: {:?}",
        row[6]
    );
    assert!(
        row[6].as_str().unwrap().contains("2020-01-15"),
        "DATETIME should contain the date, got: {:?}",
        row[6]
    );

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// execute_query
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mysql_execute_query_select() {
    let (driver, db) = mysql_driver!();
    let result = driver
        .execute_query(&db, "SELECT 1 AS num, 'hello' AS greeting")
        .await
        .unwrap();
    assert!(result.is_select);
    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.rows.len(), 1);
}

#[tokio::test]
async fn mysql_execute_query_dml() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("my_dml");

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
async fn mysql_execute_query_invalid_sql_errors() {
    let (driver, db) = mysql_driver!();
    let err = driver.execute_query(&db, "SELEC 1").await;
    assert!(err.is_err());
}

// ---------------------------------------------------------------------------
// explain_query
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mysql_explain_query() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("expl_tbl");
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
    assert!(!ex.plan.node_type.is_empty());

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// get_ddl
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mysql_get_ddl() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("ddl_tbl");
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
async fn mysql_apply_changes_insert() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("ap_ins");

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
        .fetch_rows(&db, &db, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 1);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mysql_apply_changes_update() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("my_upd");

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
        .fetch_rows(&db, &db, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.rows[0][1], serde_json::json!("new"));

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mysql_apply_changes_delete() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("my_del");

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
        .fetch_rows(&db, &db, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 1);
    assert_eq!(data.rows[0][0], serde_json::json!(2));

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mysql_apply_changes_batch() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("ap_batch");

    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT PRIMARY KEY, name VARCHAR(50), val INT) ENGINE=InnoDB",
                tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!("INSERT INTO `{}` VALUES (1,'a',1),(2,'b',2)", tbl),
        )
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
            old_value: serde_json::json!(1),
            new_value: serde_json::json!(99),
            primary_key_values: vec![("id".into(), serde_json::json!(1))],
        }],
        inserts: vec![NewRow {
            values: vec![
                ("id".into(), serde_json::json!(3)),
                ("name".into(), serde_json::json!("c")),
                ("val".into(), serde_json::json!(3)),
            ],
        }],
        deletes: vec![DeleteRow {
            primary_key_values: vec![("id".into(), serde_json::json!(2))],
        }],
    };
    driver.apply_changes(&changes).await.unwrap();

    let data = driver
        .fetch_rows(&db, &db, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 2);
    let ids: Vec<i64> = data.rows.iter().map(|r| r[0].as_i64().unwrap()).collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&3));
    let row1 = data
        .rows
        .iter()
        .find(|r| r[0] == serde_json::json!(1))
        .unwrap();
    assert_eq!(row1[2], serde_json::json!(99));

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// create_table
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mysql_create_table_basic() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("my_tbl");

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

#[tokio::test]
async fn mysql_create_table_no_columns_error() {
    let (driver, db) = mysql_driver!();
    let result = driver.create_table(&db, &db, "should_fail", &[]).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn mysql_create_table_unsafe_type_error() {
    let (driver, db) = mysql_driver!();
    let cols = vec![ColumnDefinition {
        name: "x".into(),
        data_type: "INT); DROP TABLE evil; --".into(),
        is_nullable: true,
        is_primary_key: false,
        default_value: None,
    }];
    let result = driver.create_table(&db, &db, "should_fail", &cols).await;
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// alter_table
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mysql_alter_table_add_rename_drop() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("alt_tbl");

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

// ---------------------------------------------------------------------------
// truncate_table, drop_object
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mysql_truncate_table() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("my_trunc");

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
        .fetch_rows(&db, &db, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 0);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
    let tables = driver.list_tables(&db, &db).await.unwrap();
    assert!(!tables.iter().any(|t| t.name == tbl));
}

#[tokio::test]
async fn mysql_drop_object() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("drop_tbl");
    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{}` (id INT) ENGINE=InnoDB", tbl),
        )
        .await
        .unwrap();
    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
    let tables = driver.list_tables(&db, &db).await.unwrap();
    assert!(!tables.iter().any(|t| t.name == tbl));
}

// ---------------------------------------------------------------------------
// import_data
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mysql_import_data() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("imp_tbl");
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
        .fetch_rows(&db, &db, &tbl, 0, 10, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 2);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mysql_import_data_large_batch() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("imp_big");
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

    let data = driver
        .fetch_rows(&db, &db, &tbl, 0, 1, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 600);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// Server introspection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mysql_get_server_activity() {
    let (driver, _db) = mysql_driver!();
    let activity = driver.get_server_activity().await.unwrap();
    assert!(
        !activity.is_empty(),
        "MySQL should report at least our own connection"
    );
}

#[tokio::test]
async fn mysql_get_database_stats() {
    let (driver, _db) = mysql_driver!();
    let stats = driver.get_database_stats().await.unwrap();
    assert!(
        stats.total_connections >= 1,
        "expected at least 1 connection, got {}",
        stats.total_connections
    );
    assert!(stats.timestamp_ms > 0.0, "timestamp should be positive");
    assert!(
        stats.active_connections + stats.idle_connections > 0,
        "should have at least 1 active or idle connection"
    );
}

#[tokio::test]
async fn mysql_get_locks() {
    let (driver, _db) = mysql_driver!();
    let locks = driver.get_locks().await.unwrap();
    let _ = locks;
}

#[tokio::test]
async fn mysql_get_server_config() {
    let (driver, _db) = mysql_driver!();
    let cfg = driver.get_server_config().await.unwrap();
    assert!(
        !cfg.is_empty(),
        "MySQL SHOW VARIABLES should return config entries"
    );
    assert!(
        cfg.iter().any(|e| e.name == "version"),
        "should include 'version' variable"
    );
    assert!(
        cfg.iter().all(|e| !e.category.is_empty()),
        "all entries should have a category"
    );
}

#[tokio::test]
async fn mysql_list_roles() {
    let (driver, _db) = mysql_driver!();
    let roles = driver.list_roles().await.unwrap();
    let _ = roles;
}

// ---------------------------------------------------------------------------
// alter_table: additional operations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mysql_alter_table_change_column_type() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("alt_type");

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
        .execute_query(&db, &format!("INSERT INTO `{}` VALUES (1, 42)", tbl))
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
    assert!(
        n.data_type.contains("bigint"),
        "expected bigint, got: {}",
        n.data_type
    );

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mysql_alter_table_set_nullable() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("alt_null");

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
async fn mysql_alter_table_set_default() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("alt_def");

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
        .fetch_rows(&db, &db, &tbl, 0, 10, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.rows[0][1], serde_json::json!(5));

    driver
        .alter_table(
            &db,
            &db,
            &tbl,
            &[AlterTableOperation::SetDefault {
                column_name: "priority".into(),
                default_value: None,
            }],
        )
        .await
        .unwrap();

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mysql_alter_table_rename_table() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("alt_rn");
    let new_name = unique_table("alt_rn_new");

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

#[tokio::test]
async fn mysql_alter_table_change_type_unsafe_rejected() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("alt_bad");

    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE `{}` (id INT PRIMARY KEY, n INT) ENGINE=InnoDB",
                tbl
            ),
        )
        .await
        .unwrap();

    let result = driver
        .alter_table(
            &db,
            &db,
            &tbl,
            &[AlterTableOperation::ChangeColumnType {
                column_name: "n".into(),
                new_type: "INT; DROP TABLE x; --".into(),
            }],
        )
        .await;
    assert!(result.is_err());

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

// ---------------------------------------------------------------------------
// drop_object: VIEW
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mysql_drop_object_view() {
    let (driver, db) = mysql_driver!();
    let base = unique_table("vbase");
    let vname = unique_table("vw");

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
// cancel_query
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mysql_cancel_query_invalid_pid() {
    let (driver, _db) = mysql_driver!();
    let r = driver.cancel_query("not_a_number").await;
    assert!(r.is_err());
}

// ---------------------------------------------------------------------------
// Role management (unimplemented for MySQL)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mysql_create_role_unimplemented() {
    let (driver, _db) = mysql_driver!();
    let req = CreateRoleRequest {
        connection_id: "test".into(),
        name: "r".into(),
        password: None,
        is_superuser: false,
        can_login: true,
        can_create_db: false,
        can_create_role: false,
        connection_limit: -1,
        valid_until: None,
    };
    assert!(driver.create_role(&req).await.is_err());
}

#[tokio::test]
async fn mysql_drop_role_unimplemented() {
    let (driver, _db) = mysql_driver!();
    assert!(driver.drop_role("any").await.is_err());
}

#[tokio::test]
async fn mysql_alter_role_unimplemented() {
    let (driver, _db) = mysql_driver!();
    let req = AlterRoleRequest {
        connection_id: "test".into(),
        name: "r".into(),
        password: None,
        is_superuser: None,
        can_login: None,
        can_create_db: None,
        can_create_role: None,
        connection_limit: None,
        valid_until: None,
    };
    assert!(driver.alter_role(&req).await.is_err());
}

// ---------------------------------------------------------------------------
// Optional database connection (connect without specifying a database)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mysql_no_db_connect_and_list_databases() {
    let driver = mysql_driver_no_db!();
    assert!(driver.test_connection().await.unwrap());
    let dbs = driver.list_databases().await.unwrap();
    assert!(!dbs.is_empty());
}

#[tokio::test]
async fn mysql_no_db_list_tables_on_specific_database() {
    let (with_db, db) = mysql_driver!();
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

    let driver = mysql_driver_no_db!();
    let tables = driver.list_tables(&db, &db).await.unwrap();
    assert!(
        tables.iter().any(|t| t.name == tbl),
        "Table '{}' not found via no-db driver",
        tbl
    );

    with_db.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mysql_no_db_fetch_rows_on_specific_database() {
    let (with_db, db) = mysql_driver!();
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

    let driver = mysql_driver_no_db!();
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
    let (driver, db) = mysql_driver!();
    let result = driver.validate_query(&db, "SELECT 1").await.unwrap();
    assert!(result.is_none(), "valid SQL should return None");
}

#[tokio::test]
async fn validate_query_syntax_error() {
    let (driver, db) = mysql_driver!();
    let result = driver.validate_query(&db, "SELCT 1").await.unwrap();
    assert!(result.is_some(), "invalid SQL should return Some");
    let err = result.unwrap();
    assert!(!err.message.is_empty());
}

#[tokio::test]
async fn validate_query_nonexistent_table() {
    let (driver, db) = mysql_driver!();
    let result = driver
        .validate_query(&db, "SELECT * FROM nonexistent_table_xyz_12345")
        .await
        .unwrap();
    assert!(result.is_some());
}

#[tokio::test]
async fn validate_query_incomplete_where() {
    let (driver, db) = mysql_driver!();
    let result = driver.validate_query(&db, "SELECT 1 WHERE").await.unwrap();
    assert!(result.is_some(), "incomplete WHERE should be an error");
}

#[tokio::test]
async fn validate_query_empty_string() {
    let (driver, db) = mysql_driver!();
    let result = driver.validate_query(&db, "").await.unwrap();
    assert!(result.is_some(), "empty SQL should be an error");
}

// ---------------------------------------------------------------------------
// Big-data soak — nightly only (`#[ignore]`-gated)
// ---------------------------------------------------------------------------

const MYSQL_BIG_DATA_ROWS: i64 = 1_000_000;

/// MySQL has no `generate_series`, so we materialise a million rows by a
/// 6-way cross-join on a small persistent digits helper table. This works
/// identically on MySQL and MariaDB without depending on session-scoped
/// state: the previous CTE-based approach issued
/// `SET SESSION cte_max_recursion_depth` and the recursive `INSERT` on
/// separate sqlx pool checkouts, so the bump never reached the recursion.
async fn mysql_seed_million_rows(
    driver: &MysqlDriver,
    db: &str,
    table: &str,
) -> anyhow::Result<()> {
    let create_sql = format!(
        "CREATE TABLE `{db}`.`{table}` ( \
             id BIGINT PRIMARY KEY, \
             payload TEXT NOT NULL, \
             created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP \
         )"
    );
    driver.execute_query(db, &create_sql).await?;

    // Persistent (non-temporary) helper table so the digits survive
    // every pool-connection checkout. Six joins over 0..=9 generate the
    // 1_000_000-row sequence in a single INSERT.
    let digits_table = format!("`{db}`.`tablio_digits_seed_{table}`");
    let drop_digits = format!("DROP TABLE IF EXISTS {digits_table}");
    driver.execute_query(db, &drop_digits).await?;
    let create_digits = format!("CREATE TABLE {digits_table} (d TINYINT NOT NULL PRIMARY KEY)");
    driver.execute_query(db, &create_digits).await?;
    let fill_digits =
        format!("INSERT INTO {digits_table} (d) VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)");
    driver.execute_query(db, &fill_digits).await?;

    let insert_sql = format!(
        "INSERT INTO `{db}`.`{table}` (id, payload) \
         SELECT n, CONCAT('row-', n) FROM ( \
             SELECT (d6.d * 100000 + d5.d * 10000 + d4.d * 1000 \
                     + d3.d * 100 + d2.d * 10 + d1.d) + 1 AS n \
             FROM {dt} d1 \
             CROSS JOIN {dt} d2 CROSS JOIN {dt} d3 \
             CROSS JOIN {dt} d4 CROSS JOIN {dt} d5 CROSS JOIN {dt} d6 \
         ) AS series \
         WHERE n <= {n}",
        dt = digits_table,
        n = MYSQL_BIG_DATA_ROWS
    );
    driver.execute_query(db, &insert_sql).await?;

    driver.execute_query(db, &drop_digits).await?;
    Ok(())
}

async fn mysql_drop_table_silent(driver: &MysqlDriver, db: &str, table: &str) {
    let _ = driver
        .execute_query(db, &format!("DROP TABLE IF EXISTS `{db}`.`{table}`"))
        .await;
}

#[tokio::test]
#[ignore]
async fn million_row_paginate() {
    let (driver, db) = mysql_driver!();
    let table = unique_table("big_data_paginate");

    mysql_drop_table_silent(&driver, &db, &table).await;
    mysql_seed_million_rows(&driver, &db, &table)
        .await
        .expect("seed million rows");

    const PAGE: u64 = 5_000;
    const MAX_PAGE_MS: u128 = 5_000;
    let mut offset: u64 = 0;
    let mut total: u64 = 0;
    let mut pages: u64 = 0;
    let started = std::time::Instant::now();

    loop {
        let t = std::time::Instant::now();
        let data = driver
            .fetch_rows(&db, &db, &table, offset, PAGE, Vec::new(), None)
            .await
            .expect("fetch_rows page");
        let elapsed = t.elapsed().as_millis();
        assert!(
            elapsed < MAX_PAGE_MS,
            "page at offset {offset} took {elapsed}ms (>{MAX_PAGE_MS}ms budget)"
        );
        let n = data.rows.len() as u64;
        total += n;
        pages += 1;
        if n < PAGE {
            break;
        }
        offset += PAGE;
    }

    let total_elapsed = started.elapsed().as_secs_f64();
    eprintln!("[big-data][mysql] paginated {total} rows in {pages} pages in {total_elapsed:.2}s");

    assert_eq!(
        total, MYSQL_BIG_DATA_ROWS as u64,
        "expected {MYSQL_BIG_DATA_ROWS} rows total, got {total} across {pages} pages"
    );

    mysql_drop_table_silent(&driver, &db, &table).await;
}

#[tokio::test]
async fn my_get_query_stats_ok_or_perf_schema_message() {
    let (driver, _db) = mysql_driver!();
    let res = driver.get_query_stats().await;
    assert!(res.is_ok(), "get_query_stats returned: {:?}", res.err());
    let qs = res.unwrap();
    if qs.available {
        assert_eq!(qs.kind, QueryStatsKind::Available);
        assert!(qs.message.is_none());
    } else {
        assert_eq!(qs.kind, QueryStatsKind::MysqlPerfSchemaDisabled);
        let msg = qs.message.as_deref().unwrap_or("");
        assert!(
            msg.contains("performance_schema") || msg.contains("Performance Schema"),
            "unexpected unavailable message: {msg}"
        );
    }
}

// ---------------------------------------------------------------------------
// Multi-statement execute_query (PR #131 follow-up)
// ---------------------------------------------------------------------------

// Mirrors the same fix in sqlite + postgres. The MySQL driver used
// to route based on the FIRST keyword of the input, so a
// `CREATE VIEW ...; SELECT * FROM v;` would create the view but
// throw away the SELECT's rows.
#[tokio::test]
async fn mysql_multi_statement_create_view_then_select_returns_rows() {
    let (driver, db) = mysql_driver!();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let tbl = format!("mst_t_{}", &suffix[..8]);
    let view = format!("mst_v_{}", &suffix[..8]);
    let script = format!(
        "CREATE TABLE `{db}`.`{tbl}` (x INT); \
         INSERT INTO `{db}`.`{tbl}` VALUES (1), (2), (3); \
         CREATE VIEW `{db}`.`{view}` AS \
            SELECT x, CASE WHEN x % 2 = 0 THEN 'even' ELSE 'odd' END AS parity \
            FROM `{db}`.`{tbl}`; \
         SELECT * FROM `{db}`.`{view}` ORDER BY x;"
    );
    let result = driver
        .execute_query(&db, &script)
        .await
        .expect("multi-statement script failed");

    assert!(result.is_select);
    assert_eq!(result.rows.len(), 3);
    let parities: Vec<&str> = result
        .rows
        .iter()
        .filter_map(|r| r.get(1).and_then(|v| v.as_str()))
        .collect();
    assert_eq!(parities, vec!["odd", "even", "odd"]);

    driver
        .execute_query(&db, &format!("DROP VIEW IF EXISTS `{db}`.`{view}`"))
        .await
        .ok();
    driver
        .execute_query(&db, &format!("DROP TABLE IF EXISTS `{db}`.`{tbl}`"))
        .await
        .ok();
}

// MySQL-specific: backtick-quoted identifier with an embedded
// semicolon must not split the statement. The splitter is the
// only thing that prevents this from corrupting the batch.
#[tokio::test]
async fn mysql_multi_statement_semicolon_in_backtick_identifier_is_safe() {
    let (driver, db) = mysql_driver!();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let tbl = format!("mst_q_{}", &suffix[..8]);
    // Column named "weird;name" — perfectly legal MySQL identifier
    // when backticked.
    let script = format!(
        "CREATE TABLE `{db}`.`{tbl}` (`weird;name` INT); \
         INSERT INTO `{db}`.`{tbl}` (`weird;name`) VALUES (42); \
         SELECT `weird;name` FROM `{db}`.`{tbl}`;"
    );
    let result = driver
        .execute_query(&db, &script)
        .await
        .expect("backtick-with-semicolon script failed");
    assert!(result.is_select);
    let v = result
        .rows
        .first()
        .and_then(|r| r.first())
        .and_then(|v| v.as_i64())
        .unwrap_or(-1);
    assert_eq!(v, 42);

    driver
        .execute_query(&db, &format!("DROP TABLE IF EXISTS `{db}`.`{tbl}`"))
        .await
        .ok();
}

// All-DDL script with no trailing SELECT — should run every
// statement and return `is_select = false`.
#[tokio::test]
async fn mysql_multi_statement_all_ddl_no_rows() {
    let (driver, db) = mysql_driver!();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let t1 = format!("mst_a_{}", &suffix[..8]);
    let t2 = format!("mst_b_{}", &suffix[..8]);
    let script = format!("CREATE TABLE `{db}`.`{t1}` (x INT); CREATE TABLE `{db}`.`{t2}` (y INT);");
    let result = driver
        .execute_query(&db, &script)
        .await
        .expect("DDL script failed");
    assert!(!result.is_select);
    assert!(result.rows.is_empty());

    let check = driver
        .execute_query(
            &db,
            &format!(
                "SELECT COUNT(*) AS n FROM information_schema.tables \
                 WHERE table_schema = '{db}' \
                 AND table_name IN ('{t1}', '{t2}')"
            ),
        )
        .await
        .expect("post-check failed");
    let n = check
        .rows
        .first()
        .and_then(|r| r.first())
        .and_then(|v| v.as_i64().or_else(|| v.as_u64().map(|u| u as i64)))
        .unwrap_or(-1);
    assert_eq!(n, 2);

    driver
        .execute_query(&db, &format!("DROP TABLE IF EXISTS `{db}`.`{t1}`"))
        .await
        .ok();
    driver
        .execute_query(&db, &format!("DROP TABLE IF EXISTS `{db}`.`{t2}`"))
        .await
        .ok();
}

// ---------------------------------------------------------------------------
// alter_table: editor-driven multi-op sequences (issue #59 follow-up)
// ---------------------------------------------------------------------------
// MySQL passes the database name as both `database` and `schema` because
// MySQL collapses schema and database into one namespace. These tests
// mirror what the frontend AlterTableEditor submits in one save call.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mysql_alter_table_rename_then_change_type_same_column() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("my_seq_rn_ct");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{tbl}` (pk INT PRIMARY KEY, legacy_id INT) ENGINE=InnoDB"),
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
    let result = driver
        .execute_query(&db, &format!("SELECT id FROM `{tbl}`"))
        .await
        .unwrap();
    let v = result.rows[0][0]
        .as_i64()
        .or_else(|| result.rows[0][0].as_u64().map(|u| u as i64))
        .unwrap();
    assert_eq!(v, 42);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mysql_alter_table_rename_table_then_alter_columns() {
    let (driver, db) = mysql_driver!();
    let old = unique_table("my_rn_then");
    let new_name = unique_table("my_rn_then_new");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{old}` (id INT PRIMARY KEY, status TEXT) ENGINE=InnoDB"),
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
async fn mysql_alter_table_drop_then_add_same_name() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("my_drop_add");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{tbl}` (id INT PRIMARY KEY, payload TEXT) ENGINE=InnoDB"),
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
async fn mysql_alter_table_kitchen_sink_one_call() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("my_kitchen");

    driver
        .execute_query(
            &db,
            // `c` is VARCHAR(40) (not TEXT) because MySQL refuses
            // DEFAULT on TEXT/BLOB/JSON/GEOMETRY columns with error
            // 1101. The SetDefault op below would otherwise fail.
            &format!(
                "CREATE TABLE `{tbl}` (id INT PRIMARY KEY, a INT, b TEXT, c VARCHAR(40)) ENGINE=InnoDB"
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
                AlterTableOperation::RenameColumn {
                    old_name: "a".into(),
                    new_name: "alpha".into(),
                },
                AlterTableOperation::ChangeColumnType {
                    column_name: "alpha".into(),
                    new_type: "BIGINT".into(),
                },
                AlterTableOperation::DropColumn {
                    column_name: "b".into(),
                },
                AlterTableOperation::AddColumn {
                    column: ColumnDefinition {
                        name: "created_at".into(),
                        data_type: "DATETIME".into(),
                        is_nullable: false,
                        is_primary_key: false,
                        default_value: Some("CURRENT_TIMESTAMP".into()),
                    },
                },
                AlterTableOperation::AddColumn {
                    column: ColumnDefinition {
                        name: "tag".into(),
                        data_type: "VARCHAR(32)".into(),
                        is_nullable: true,
                        is_primary_key: false,
                        default_value: None,
                    },
                },
                AlterTableOperation::SetNullable {
                    column_name: "c".into(),
                    nullable: false,
                },
                AlterTableOperation::SetDefault {
                    column_name: "c".into(),
                    default_value: Some("'pending'".into()),
                },
            ],
        )
        .await
        .unwrap();

    let cols = driver.list_columns(&db, &db, &tbl).await.unwrap();
    let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
    assert!(names.contains(&"id"));
    assert!(names.contains(&"alpha"));
    assert!(!names.contains(&"a"));
    assert!(!names.contains(&"b"));
    assert!(names.contains(&"c"));
    assert!(names.contains(&"created_at"));
    assert!(names.contains(&"tag"));
    let alpha = cols.iter().find(|c| c.name == "alpha").unwrap();
    assert!(alpha.data_type.to_lowercase().contains("bigint"));
    let c = cols.iter().find(|c| c.name == "c").unwrap();
    assert!(!c.is_nullable);

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mysql_alter_table_empty_operations_is_noop() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("my_noop");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{tbl}` (id INT PRIMARY KEY) ENGINE=InnoDB"),
        )
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
async fn mysql_alter_table_add_not_null_default_backfills_existing_rows() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("my_backfill");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{tbl}` (id INT PRIMARY KEY) ENGINE=InnoDB"),
        )
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
async fn mysql_alter_table_set_then_clear_default_cycle() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("my_def_cycle");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{tbl}` (id INT PRIMARY KEY, status VARCHAR(20)) ENGINE=InnoDB"),
        )
        .await
        .unwrap();

    driver
        .alter_table(
            &db,
            &db,
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

    let cols = driver.list_columns(&db, &db, &tbl).await.unwrap();
    let status = cols.iter().find(|c| c.name == "status").unwrap();
    assert!(status.default_value.is_none(), "default should be cleared");

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mysql_alter_table_rename_nonexistent_column_errors() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("my_rn_bad");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{tbl}` (id INT PRIMARY KEY) ENGINE=InnoDB"),
        )
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
    assert!(result.is_err(), "rename of missing column must error");

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mysql_alter_table_add_duplicate_column_errors() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("my_dup");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{tbl}` (id INT PRIMARY KEY, status TEXT) ENGINE=InnoDB"),
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
    assert!(result.is_err(), "duplicate column add must error");

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}

#[tokio::test]
async fn mysql_alter_table_toggle_nullable_both_directions_one_call() {
    let (driver, db) = mysql_driver!();
    let tbl = unique_table("my_toggle_null");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE `{tbl}` (id INT PRIMARY KEY, note VARCHAR(50)) ENGINE=InnoDB"),
        )
        .await
        .unwrap();

    driver
        .alter_table(
            &db,
            &db,
            &tbl,
            &[
                AlterTableOperation::SetNullable {
                    column_name: "note".into(),
                    nullable: false,
                },
                AlterTableOperation::SetNullable {
                    column_name: "note".into(),
                    nullable: true,
                },
            ],
        )
        .await
        .unwrap();

    let cols = driver.list_columns(&db, &db, &tbl).await.unwrap();
    let note = cols.iter().find(|c| c.name == "note").unwrap();
    assert!(note.is_nullable, "last SetNullable wins → nullable");

    driver.drop_object(&db, &db, &tbl, "TABLE").await.unwrap();
}
