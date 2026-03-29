use tablio_lib::db::cassandra::CassandraDriver;
use tablio_lib::db::DatabaseDriver;
use tablio_lib::models::*;

fn cassandra_url() -> String {
    std::env::var("TEST_CASSANDRA_HOST").unwrap_or_else(|_| "127.0.0.1".to_string())
}

fn cassandra_port() -> u16 {
    std::env::var("TEST_CASSANDRA_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(9042)
}

async fn create_driver() -> CassandraDriver {
    let config = ConnectionConfig {
        id: "test".into(),
        name: "test".into(),
        db_type: DbType::Cassandra,
        host: cassandra_url(),
        port: cassandra_port(),
        user: String::new(),
        password: String::new(),
        database: String::new(),
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
    CassandraDriver::connect(&config).await.unwrap()
}

fn unique_keyspace() -> String {
    format!(
        "tablio_test_{}",
        uuid::Uuid::new_v4().simple().to_string().get(..8).unwrap()
    )
}

fn unique_table(prefix: &str) -> String {
    format!(
        "{}_{}",
        prefix,
        uuid::Uuid::new_v4().simple().to_string().get(..8).unwrap()
    )
}

async fn setup_keyspace(driver: &CassandraDriver) -> String {
    let ks = unique_keyspace();
    driver
        .execute_query(
            "",
            &format!(
                "CREATE KEYSPACE {} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}",
                ks
            ),
        )
        .await
        .unwrap();
    ks
}

async fn teardown_keyspace(driver: &CassandraDriver, ks: &str) {
    let _ = driver
        .execute_query("", &format!("DROP KEYSPACE IF EXISTS {}", ks))
        .await;
}

#[tokio::test]
async fn cassandra_test_connection() {
    let driver = create_driver().await;
    assert!(driver.test_connection().await.unwrap());
}

#[tokio::test]
async fn cassandra_list_databases() {
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;

    let dbs = driver.list_databases().await.unwrap();
    assert!(dbs.iter().any(|d| d.name == ks));

    teardown_keyspace(&driver, &ks).await;
}

#[tokio::test]
async fn cassandra_list_schemas() {
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;

    let schemas = driver.list_schemas(&ks).await.unwrap();
    assert_eq!(schemas.len(), 1);
    assert_eq!(schemas[0].name, ks);

    teardown_keyspace(&driver, &ks).await;
}

#[tokio::test]
async fn cassandra_create_and_list_tables() {
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;
    let tbl = unique_table("items");

    driver
        .create_table(
            &ks,
            &ks,
            &tbl,
            &[
                ColumnDefinition {
                    name: "id".into(),
                    data_type: "uuid".into(),
                    is_nullable: false,
                    is_primary_key: true,
                    default_value: None,
                },
                ColumnDefinition {
                    name: "name".into(),
                    data_type: "text".into(),
                    is_nullable: true,
                    is_primary_key: false,
                    default_value: None,
                },
            ],
        )
        .await
        .unwrap();

    let tables = driver.list_tables(&ks, &ks).await.unwrap();
    assert!(tables.iter().any(|t| t.name == tbl));

    teardown_keyspace(&driver, &ks).await;
}

#[tokio::test]
async fn cassandra_list_columns() {
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;
    let tbl = unique_table("cols");

    driver
        .execute_query(
            "",
            &format!(
                "CREATE TABLE {}.{} (id uuid PRIMARY KEY, name text, age int)",
                ks, tbl
            ),
        )
        .await
        .unwrap();

    let cols = driver.list_columns(&ks, &ks, &tbl).await.unwrap();
    assert_eq!(cols.len(), 3);

    let id_col = cols.iter().find(|c| c.name == "id").unwrap();
    assert!(id_col.is_primary_key);

    let name_col = cols.iter().find(|c| c.name == "name").unwrap();
    assert!(!name_col.is_primary_key);
    assert_eq!(name_col.data_type, "text");

    teardown_keyspace(&driver, &ks).await;
}

#[tokio::test]
async fn cassandra_execute_query() {
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;

    driver
        .execute_query(
            "",
            &format!(
                "CREATE TABLE {}.events (id uuid PRIMARY KEY, payload text)",
                ks
            ),
        )
        .await
        .unwrap();

    driver
        .execute_query(
            "",
            &format!(
                "INSERT INTO {}.events (id, payload) VALUES (uuid(), 'hello')",
                ks
            ),
        )
        .await
        .unwrap();

    let result = driver
        .execute_query("", &format!("SELECT * FROM {}.events", ks))
        .await
        .unwrap();

    assert!(result.is_select);
    assert_eq!(result.rows.len(), 1);

    teardown_keyspace(&driver, &ks).await;
}

#[tokio::test]
async fn cassandra_apply_changes_insert_and_delete() {
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;
    let tbl = unique_table("changes");

    driver
        .execute_query(
            "",
            &format!(
                "CREATE TABLE {}.{} (id int PRIMARY KEY, value text)",
                ks, tbl
            ),
        )
        .await
        .unwrap();

    let changes = DataChanges {
        connection_id: "test".into(),
        database: ks.clone(),
        schema: ks.clone(),
        table: tbl.clone(),
        updates: vec![],
        inserts: vec![NewRow {
            values: vec![
                ("id".into(), serde_json::json!(1)),
                ("value".into(), serde_json::json!("test_value")),
            ],
        }],
        deletes: vec![],
    };
    driver.apply_changes(&changes).await.unwrap();

    let result = driver
        .execute_query("", &format!("SELECT * FROM {}.{}", ks, tbl))
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);

    let delete_changes = DataChanges {
        connection_id: "test".into(),
        database: ks.clone(),
        schema: ks.clone(),
        table: tbl.clone(),
        updates: vec![],
        inserts: vec![],
        deletes: vec![DeleteRow {
            primary_key_values: vec![("id".into(), serde_json::json!(1))],
        }],
    };
    driver.apply_changes(&delete_changes).await.unwrap();

    let result = driver
        .execute_query("", &format!("SELECT * FROM {}.{}", ks, tbl))
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 0);

    teardown_keyspace(&driver, &ks).await;
}

#[tokio::test]
async fn cassandra_truncate_table() {
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;
    let tbl = unique_table("trunc");

    driver
        .execute_query(
            "",
            &format!(
                "CREATE TABLE {}.{} (id int PRIMARY KEY, data text)",
                ks, tbl
            ),
        )
        .await
        .unwrap();

    driver
        .execute_query(
            "",
            &format!("INSERT INTO {}.{} (id, data) VALUES (1, 'a')", ks, tbl),
        )
        .await
        .unwrap();

    driver.truncate_table(&ks, &ks, &tbl).await.unwrap();

    let result = driver
        .execute_query("", &format!("SELECT * FROM {}.{}", ks, tbl))
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 0);

    teardown_keyspace(&driver, &ks).await;
}

#[tokio::test]
async fn cassandra_drop_table() {
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;
    let tbl = unique_table("droptbl");

    driver
        .execute_query(
            "",
            &format!("CREATE TABLE {}.{} (id int PRIMARY KEY)", ks, tbl),
        )
        .await
        .unwrap();

    driver.drop_object(&ks, &ks, &tbl, "TABLE").await.unwrap();

    let tables = driver.list_tables(&ks, &ks).await.unwrap();
    assert!(!tables.iter().any(|t| t.name == tbl));

    teardown_keyspace(&driver, &ks).await;
}

#[tokio::test]
async fn cassandra_fetch_rows() {
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;
    let tbl = unique_table("fetch");

    driver
        .execute_query(
            "",
            &format!(
                "CREATE TABLE {}.{} (id int PRIMARY KEY, name text, active boolean)",
                ks, tbl
            ),
        )
        .await
        .unwrap();

    for i in 1..=5 {
        driver
            .execute_query(
                "",
                &format!(
                    "INSERT INTO {}.{} (id, name, active) VALUES ({}, 'item{}', {})",
                    ks,
                    tbl,
                    i,
                    i,
                    i % 2 == 0
                ),
            )
            .await
            .unwrap();
    }

    let data = driver
        .fetch_rows(&ks, &ks, &tbl, 0, 3, None, None)
        .await
        .unwrap();
    assert_eq!(data.rows.len(), 3);
    assert_eq!(data.columns.len(), 3);

    let data_all = driver
        .fetch_rows(&ks, &ks, &tbl, 0, 100, None, None)
        .await
        .unwrap();
    assert_eq!(data_all.rows.len(), 5);

    teardown_keyspace(&driver, &ks).await;
}

#[tokio::test]
async fn cassandra_apply_changes_update() {
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;
    let tbl = unique_table("upd");

    driver
        .execute_query(
            "",
            &format!(
                "CREATE TABLE {}.{} (id int PRIMARY KEY, value text)",
                ks, tbl
            ),
        )
        .await
        .unwrap();

    driver
        .execute_query(
            "",
            &format!(
                "INSERT INTO {}.{} (id, value) VALUES (1, 'original')",
                ks, tbl
            ),
        )
        .await
        .unwrap();

    let changes = DataChanges {
        connection_id: "test".into(),
        database: ks.clone(),
        schema: ks.clone(),
        table: tbl.clone(),
        updates: vec![CellChange {
            row_index: 0,
            primary_key_values: vec![("id".into(), serde_json::json!(1))],
            column_name: "value".into(),
            old_value: serde_json::json!("original"),
            new_value: serde_json::json!("updated"),
        }],
        inserts: vec![],
        deletes: vec![],
    };
    driver.apply_changes(&changes).await.unwrap();

    let result = driver
        .execute_query(
            "",
            &format!("SELECT value FROM {}.{} WHERE id = 1", ks, tbl),
        )
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], serde_json::json!("updated"));

    teardown_keyspace(&driver, &ks).await;
}

#[tokio::test]
async fn cassandra_list_indexes() {
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;
    let tbl = unique_table("idx");

    driver
        .execute_query(
            "",
            &format!(
                "CREATE TABLE {}.{} (id int PRIMARY KEY, email text)",
                ks, tbl
            ),
        )
        .await
        .unwrap();

    let idx_name = format!("{}_email_idx", tbl);
    driver
        .execute_query(
            "",
            &format!("CREATE INDEX {} ON {}.{} (email)", idx_name, ks, tbl),
        )
        .await
        .unwrap();

    // Wait briefly for index to register
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let indexes = driver.list_indexes(&ks, &ks, &tbl).await.unwrap();
    assert!(indexes.iter().any(|i| i.name == idx_name));
    let idx = indexes.iter().find(|i| i.name == idx_name).unwrap();
    assert_eq!(idx.columns, vec!["email"]);

    teardown_keyspace(&driver, &ks).await;
}

#[tokio::test]
async fn cassandra_get_ddl() {
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;
    let tbl = unique_table("ddl");

    driver
        .execute_query(
            "",
            &format!(
                "CREATE TABLE {}.{} (id uuid PRIMARY KEY, name text, age int)",
                ks, tbl
            ),
        )
        .await
        .unwrap();

    let ddl = driver.get_ddl(&ks, &ks, &tbl, "TABLE").await.unwrap();
    assert!(ddl.contains("CREATE TABLE"));
    assert!(ddl.contains(&tbl));
    assert!(ddl.contains("PRIMARY KEY"));
    assert!(ddl.contains("id"));
    assert!(ddl.contains("name"));
    assert!(ddl.contains("age"));

    teardown_keyspace(&driver, &ks).await;
}

#[tokio::test]
async fn cassandra_alter_table_add_drop_column() {
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;
    let tbl = unique_table("alter");

    driver
        .execute_query(
            "",
            &format!(
                "CREATE TABLE {}.{} (id int PRIMARY KEY, name text)",
                ks, tbl
            ),
        )
        .await
        .unwrap();

    driver
        .alter_table(
            &ks,
            &ks,
            &tbl,
            &[AlterTableOperation::AddColumn {
                column: ColumnDefinition {
                    name: "email".into(),
                    data_type: "text".into(),
                    is_nullable: true,
                    is_primary_key: false,
                    default_value: None,
                },
            }],
        )
        .await
        .unwrap();

    let cols = driver.list_columns(&ks, &ks, &tbl).await.unwrap();
    assert!(cols.iter().any(|c| c.name == "email"));

    driver
        .alter_table(
            &ks,
            &ks,
            &tbl,
            &[AlterTableOperation::DropColumn {
                column_name: "email".into(),
            }],
        )
        .await
        .unwrap();

    let cols = driver.list_columns(&ks, &ks, &tbl).await.unwrap();
    assert!(!cols.iter().any(|c| c.name == "email"));

    teardown_keyspace(&driver, &ks).await;
}

#[tokio::test]
async fn cassandra_import_data() {
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;
    let tbl = unique_table("import");

    driver
        .execute_query(
            "",
            &format!(
                "CREATE TABLE {}.{} (id int PRIMARY KEY, label text)",
                ks, tbl
            ),
        )
        .await
        .unwrap();

    let columns = vec!["id".to_string(), "label".to_string()];
    let rows = vec![
        vec![serde_json::json!(1), serde_json::json!("alpha")],
        vec![serde_json::json!(2), serde_json::json!("beta")],
        vec![serde_json::json!(3), serde_json::json!("gamma")],
    ];

    let count = driver
        .import_data(&ks, &ks, &tbl, &columns, &rows)
        .await
        .unwrap();
    assert_eq!(count, 3);

    let result = driver
        .execute_query("", &format!("SELECT * FROM {}.{}", ks, tbl))
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 3);

    teardown_keyspace(&driver, &ks).await;
}

#[tokio::test]
async fn cassandra_multiple_data_types() {
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;
    let tbl = unique_table("types");

    driver
        .execute_query(
            "",
            &format!(
                "CREATE TABLE {}.{} (id uuid PRIMARY KEY, name text, age int, score double, active boolean, data blob, tags list<text>, meta map<text, text>)",
                ks, tbl
            ),
        )
        .await
        .unwrap();

    driver
        .execute_query(
            "",
            &format!(
                "INSERT INTO {}.{} (id, name, age, score, active, data, tags, meta) VALUES (uuid(), 'alice', 30, 95.5, true, 0xdeadbeef, ['rust', 'cql'], {{'env': 'prod'}})",
                ks, tbl
            ),
        )
        .await
        .unwrap();

    let result = driver
        .execute_query("", &format!("SELECT * FROM {}.{}", ks, tbl))
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.columns.len(), 8);

    let row = &result.rows[0];
    let name_idx = result.columns.iter().position(|c| c == "name").unwrap();
    assert_eq!(row[name_idx], serde_json::json!("alice"));

    let age_idx = result.columns.iter().position(|c| c == "age").unwrap();
    assert_eq!(row[age_idx], serde_json::json!(30));

    let active_idx = result.columns.iter().position(|c| c == "active").unwrap();
    assert_eq!(row[active_idx], serde_json::json!(true));

    let tags_idx = result.columns.iter().position(|c| c == "tags").unwrap();
    assert!(row[tags_idx].is_array());

    let meta_idx = result.columns.iter().position(|c| c == "meta").unwrap();
    assert!(row[meta_idx].is_object());

    teardown_keyspace(&driver, &ks).await;
}

#[tokio::test]
async fn cassandra_get_table_stats() {
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;
    let tbl = unique_table("stats");

    driver
        .execute_query(
            "",
            &format!("CREATE TABLE {}.{} (id int PRIMARY KEY)", ks, tbl),
        )
        .await
        .unwrap();

    let stats = driver.get_table_stats(&ks, &ks, &tbl).await.unwrap();
    assert!(stats.table_name.contains(&tbl));

    teardown_keyspace(&driver, &ks).await;
}

#[tokio::test]
async fn cassandra_drop_index() {
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;
    let tbl = unique_table("dropidx");

    driver
        .execute_query(
            "",
            &format!("CREATE TABLE {}.{} (id int PRIMARY KEY, val text)", ks, tbl),
        )
        .await
        .unwrap();

    let idx_name = format!("{}_val_idx", tbl);
    driver
        .execute_query(
            "",
            &format!("CREATE INDEX {} ON {}.{} (val)", idx_name, ks, tbl),
        )
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    driver
        .drop_object(&ks, &ks, &idx_name, "INDEX")
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let indexes = driver.list_indexes(&ks, &ks, &tbl).await.unwrap();
    assert!(!indexes.iter().any(|i| i.name == idx_name));

    teardown_keyspace(&driver, &ks).await;
}

#[tokio::test]
async fn cassandra_execute_non_select() {
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;

    let result = driver
        .execute_query(
            "",
            &format!("CREATE TABLE {}.nonsel (id int PRIMARY KEY, data text)", ks),
        )
        .await
        .unwrap();

    assert!(!result.is_select);
    assert_eq!(result.rows.len(), 0);

    let tables = driver.list_tables(&ks, &ks).await.unwrap();
    assert!(tables.iter().any(|t| t.name == "nonsel"));

    teardown_keyspace(&driver, &ks).await;
}

#[tokio::test]
async fn cassandra_create_table_no_pk_fails() {
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;

    let result = driver
        .create_table(
            &ks,
            &ks,
            "nopk",
            &[ColumnDefinition {
                name: "name".into(),
                data_type: "text".into(),
                is_nullable: true,
                is_primary_key: false,
                default_value: None,
            }],
        )
        .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("primary key"));

    teardown_keyspace(&driver, &ks).await;
}
