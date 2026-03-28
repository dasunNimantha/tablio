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
            &format!(
                "INSERT INTO {}.{} (id, data) VALUES (1, 'a')",
                ks, tbl
            ),
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
