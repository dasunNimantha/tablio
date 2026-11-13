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
    let found = tables
        .iter()
        .find(|t| t.name == tbl)
        .expect("created table not surfaced by list_tables");

    // Regression for the empty-Tables-folder bug: the Cassandra
    // driver used to emit `table_type: "TABLE"` while the rest of
    // Tablio's drivers (and the sidebar's filter) expect
    // `"BASE TABLE"`. The mismatch caused every keyspace's
    // "Tables" folder to render as "(empty)" even though the table
    // was right there in `system_schema.tables`.
    assert_eq!(
        found.table_type, "BASE TABLE",
        "Cassandra driver must emit the same `table_type` the sidebar filters on \
         (other drivers all return \"BASE TABLE\" for base tables)"
    );
    assert_eq!(found.schema, ks);

    teardown_keyspace(&driver, &ks).await;
}

#[tokio::test]
async fn cassandra_list_tables_includes_materialized_views() {
    // Materialized views live in `system_schema.views`, not
    // `system_schema.tables`. Before this fix they were never
    // listed at all because `list_tables` only queried the latter.
    // Now they should surface alongside base tables with
    // `table_type = "VIEW"`, which routes them into the sidebar's
    // "Views" group.
    //
    // Cassandra 4.0+ flipped MVs to *experimental and disabled by
    // default*, so `CREATE MATERIALIZED VIEW` is rejected unless
    // the server was started with `enable_materialized_views: true`
    // (4.x) or `materialized_views_enabled: true` (5.x). Our CI
    // workflow patches the right key into `cassandra.yaml` before
    // launching each service container (see `.github/workflows/ci.yml`
    // → `test-cassandra`), so this test exercises the full MV path
    // on every supported version.
    //
    // If you see a "Materialized views are disabled" error when
    // running this test against your own cluster, set the matching
    // option in your `cassandra.yaml` and restart the server.
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;
    let base_tbl = unique_table("mv_src");
    let mv_name = unique_table("mv_view");

    driver
        .execute_query(
            "",
            &format!(
                "CREATE TABLE {ks}.{base_tbl} \
                 (id uuid PRIMARY KEY, owner text, name text)"
            ),
        )
        .await
        .unwrap();

    // The view must be filtered on every PK column being non-null.
    // Cassandra requires `owner` to be a clustering key so the view
    // has a defined sort order over the source table.
    driver
        .execute_query(
            "",
            &format!(
                "CREATE MATERIALIZED VIEW {ks}.{mv_name} AS \
                 SELECT id, owner, name FROM {ks}.{base_tbl} \
                 WHERE owner IS NOT NULL AND id IS NOT NULL \
                 PRIMARY KEY (owner, id)"
            ),
        )
        .await
        .expect(
            "CREATE MATERIALIZED VIEW failed — if the server says MVs are \
             disabled, see .github/workflows/ci.yml → test-cassandra for \
             how to enable them via cassandra.yaml",
        );

    let tables = driver.list_tables(&ks, &ks).await.unwrap();

    let base = tables
        .iter()
        .find(|t| t.name == base_tbl)
        .expect("base table missing from list_tables");
    assert_eq!(base.table_type, "BASE TABLE");

    let mv = tables
        .iter()
        .find(|t| t.name == mv_name)
        .expect("materialized view missing from list_tables");
    assert_eq!(
        mv.table_type, "VIEW",
        "materialized views must use table_type=\"VIEW\" so the sidebar's \
         Views group renders them"
    );
    assert_eq!(mv.schema, ks);

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
        .fetch_rows(&ks, &ks, &tbl, 0, 3, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.rows.len(), 3);
    assert_eq!(data.columns.len(), 3);

    let data_all = driver
        .fetch_rows(&ks, &ks, &tbl, 0, 100, Vec::new(), None)
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

fn col_index(data: &TableData, name: &str) -> usize {
    data.columns
        .iter()
        .position(|c| c.name == name)
        .unwrap_or_else(|| panic!("column {name} missing"))
}

#[tokio::test]
async fn cassandra_fetch_rows_headers_align_with_cells() {
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;
    let tbl = unique_table("align");

    // Regular columns all have position -1 in system_schema.columns;
    // names chosen so catalog order differs from alphabetical order.
    driver
        .execute_query(
            "",
            &format!(
                "CREATE TABLE {}.{} (id uuid PRIMARY KEY, name text, email text, age int)",
                ks, tbl
            ),
        )
        .await
        .unwrap();
    let id = uuid::Uuid::new_v4();
    driver
        .execute_query(
            "",
            &format!(
                "INSERT INTO {}.{} (id, name, email, age) VALUES ({}, 'Alice', 'alice@example.com', 42)",
                ks, tbl, id
            ),
        )
        .await
        .unwrap();

    let data = driver
        .fetch_rows(&ks, &ks, &tbl, 0, 10, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.rows.len(), 1);
    let row = &data.rows[0];
    assert_eq!(row.len(), data.columns.len());
    assert_eq!(
        row[col_index(&data, "id")],
        serde_json::json!(id.to_string())
    );
    assert_eq!(row[col_index(&data, "name")], serde_json::json!("Alice"));
    assert_eq!(
        row[col_index(&data, "email")],
        serde_json::json!("alice@example.com")
    );
    assert_eq!(row[col_index(&data, "age")], serde_json::json!(42));
    assert!(data.columns[col_index(&data, "id")].is_primary_key);

    // list_columns must agree with the fetched header order.
    let listed = driver.list_columns(&ks, &ks, &tbl).await.unwrap();
    let listed_names: Vec<&str> = listed.iter().map(|c| c.name.as_str()).collect();
    let fetched_names: Vec<&str> = data.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(listed_names, fetched_names);

    teardown_keyspace(&driver, &ks).await;
}

#[tokio::test]
async fn cassandra_fetch_rows_compound_key_alignment_and_paging() {
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;
    let tbl = unique_table("ckey");

    driver
        .execute_query(
            "",
            &format!(
                "CREATE TABLE {}.{} (zz int, aa int, val text, PRIMARY KEY (zz, aa))",
                ks, tbl
            ),
        )
        .await
        .unwrap();
    for i in 0..7 {
        driver
            .execute_query(
                "",
                &format!(
                    "INSERT INTO {}.{} (zz, aa, val) VALUES (1, {}, 'v{}')",
                    ks, tbl, i, i
                ),
            )
            .await
            .unwrap();
    }

    let data = driver
        .fetch_rows(&ks, &ks, &tbl, 2, 3, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.rows.len(), 3);
    assert_eq!(data.total_rows, 7);
    let names: Vec<&str> = data.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["zz", "aa", "val"]);
    for row in &data.rows {
        let aa = row[1].as_i64().unwrap();
        assert_eq!(row[2], serde_json::json!(format!("v{}", aa)));
    }
    assert_eq!(data.rows[0][1], serde_json::json!(2));

    teardown_keyspace(&driver, &ks).await;
}

#[tokio::test]
async fn cassandra_big_integers_round_trip_through_apply_changes() {
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;
    let tbl = unique_table("bigid");

    driver
        .execute_query(
            "",
            &format!(
                "CREATE TABLE {}.{} (id bigint PRIMARY KEY, v varint, d decimal, note text)",
                ks, tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            "",
            &format!(
                "INSERT INTO {}.{} (id, v, d, note) VALUES (1234567890123456789, 123456789012345678901234567890, 12345678901234567890.123456789, 'a')",
                ks, tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            "",
            &format!(
                "INSERT INTO {}.{} (id, v, d, note) VALUES (1234567890123456790, -5, -0.5, 'b')",
                ks, tbl
            ),
        )
        .await
        .unwrap();

    let data = driver
        .fetch_rows(&ks, &ks, &tbl, 0, 10, Vec::new(), None)
        .await
        .unwrap();
    let id_idx = col_index(&data, "id");
    let row = data
        .rows
        .iter()
        .find(|r| r[id_idx] == serde_json::json!("1234567890123456789"))
        .expect("big id must be an exact string");
    assert_eq!(
        row[col_index(&data, "v")],
        serde_json::json!("123456789012345678901234567890")
    );
    assert_eq!(
        row[col_index(&data, "d")],
        serde_json::json!("12345678901234567890.123456789")
    );
    let small = data
        .rows
        .iter()
        .find(|r| r[id_idx] == serde_json::json!("1234567890123456790"))
        .unwrap();
    assert_eq!(small[col_index(&data, "v")], serde_json::json!(-5));
    assert_eq!(small[col_index(&data, "d")], serde_json::json!(-0.5));

    let changes = DataChanges {
        connection_id: "test".into(),
        database: ks.clone(),
        schema: ks.clone(),
        table: tbl.clone(),
        updates: vec![CellChange {
            row_index: 0,
            primary_key_values: vec![("id".into(), serde_json::json!("1234567890123456789"))],
            column_name: "note".into(),
            old_value: serde_json::json!("a"),
            new_value: serde_json::json!("edited"),
        }],
        inserts: vec![],
        deletes: vec![DeleteRow {
            primary_key_values: vec![("id".into(), serde_json::json!("1234567890123456790"))],
        }],
    };
    driver.apply_changes(&changes).await.unwrap();

    let result = driver
        .execute_query("", &format!("SELECT id, note FROM {}.{}", ks, tbl))
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], serde_json::json!("1234567890123456789"));
    assert_eq!(result.rows[0][1], serde_json::json!("edited"));

    teardown_keyspace(&driver, &ks).await;
}

#[tokio::test]
async fn cassandra_filter_allows_semicolon_inside_quotes() {
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;
    let tbl = unique_table("fq");

    driver
        .execute_query(
            "",
            &format!(
                "CREATE TABLE {}.{} (id int PRIMARY KEY, note text)",
                ks, tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            "",
            &format!("INSERT INTO {}.{} (id, note) VALUES (1, 'a;b--c')", ks, tbl),
        )
        .await
        .unwrap();

    let data = driver
        .fetch_rows(
            &ks,
            &ks,
            &tbl,
            0,
            10,
            Vec::new(),
            Some("note = 'a;b--c'".into()),
        )
        .await
        .unwrap();
    assert_eq!(data.rows.len(), 1);

    let rejected = driver
        .fetch_rows(
            &ks,
            &ks,
            &tbl,
            0,
            10,
            Vec::new(),
            Some("id = 1; DROP TABLE x".into()),
        )
        .await;
    assert!(rejected.is_err());

    teardown_keyspace(&driver, &ks).await;
}

// ---------------------------------------------------------------------------
// Console cancel
// ---------------------------------------------------------------------------

/// Cassandra has no server-side cancel; the driver drops the in-flight
/// request. The session must keep working afterwards.
#[tokio::test]
async fn cassandra_console_cancel_aborts_and_session_recovers() {
    let driver = create_driver().await;
    let token = tablio_lib::db::cancel::CancellationToken::new();
    let t = token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        t.cancel();
    });
    let start = std::time::Instant::now();
    // Large enough that it is usually still in flight after 1 ms; if it
    // finishes first the result is simply returned.
    let res = driver
        .execute_query_cancellable("", "SELECT * FROM system_schema.columns", token)
        .await;
    assert!(start.elapsed() < std::time::Duration::from_secs(5));
    if let Err(e) = &res {
        assert!(tablio_lib::db::cancel::is_cancelled_error(e), "{e:#}");
    }

    let pre = tablio_lib::db::cancel::CancellationToken::new();
    pre.cancel();
    let err = driver
        .execute_query_cancellable("", "SELECT release_version FROM system.local", pre)
        .await
        .expect_err("pre-cancelled run must fail");
    assert!(tablio_lib::db::cancel::is_cancelled_error(&err));

    for _ in 0..4 {
        let r = driver
            .execute_query("", "SELECT release_version FROM system.local")
            .await
            .unwrap();
        assert_eq!(r.rows.len(), 1);
    }
}

// ---------------------------------------------------------------------------
// Single-row grid edits: IF EXISTS
// ---------------------------------------------------------------------------

fn row_changes(
    ks: &str,
    tbl: &str,
    updates: Vec<CellChange>,
    deletes: Vec<DeleteRow>,
) -> DataChanges {
    DataChanges {
        connection_id: "test".into(),
        database: ks.into(),
        schema: ks.into(),
        table: tbl.into(),
        updates,
        inserts: vec![],
        deletes,
    }
}

async fn row_count(driver: &CassandraDriver, ks: &str, tbl: &str) -> usize {
    driver
        .execute_query("", &format!("SELECT * FROM {ks}.{tbl}"))
        .await
        .unwrap()
        .rows
        .len()
}

fn cell(pk: Vec<(String, serde_json::Value)>, col: &str, new: serde_json::Value) -> CellChange {
    CellChange {
        row_index: 0,
        primary_key_values: pk,
        column_name: col.into(),
        old_value: serde_json::Value::Null,
        new_value: new,
    }
}

#[tokio::test]
async fn cassandra_grid_edit_of_missing_row_reports_row_gone() {
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;
    let tbl = unique_table("lwt");
    driver
        .execute_query(
            "",
            &format!(
                "CREATE TABLE {ks}.{tbl} (pk int, ck int, a text, b int, PRIMARY KEY (pk, ck))"
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            "",
            &format!("INSERT INTO {ks}.{tbl} (pk, ck, a, b) VALUES (1, 1, 'x', 1)"),
        )
        .await
        .unwrap();
    let key = |ck: i64| {
        vec![
            ("pk".to_string(), serde_json::json!(1)),
            ("ck".to_string(), serde_json::json!(ck)),
        ]
    };
    // Two cells of an existing row: applied, as one statement.
    driver
        .apply_changes(&row_changes(
            &ks,
            &tbl,
            vec![
                cell(key(1), "a", serde_json::json!("y")),
                cell(key(1), "b", serde_json::json!(2)),
            ],
            vec![],
        ))
        .await
        .unwrap();
    let r = driver
        .execute_query(
            "",
            &format!("SELECT a, b FROM {ks}.{tbl} WHERE pk = 1 AND ck = 1"),
        )
        .await
        .unwrap();
    assert_eq!(r.rows[0][0], serde_json::json!("y"));
    assert_eq!(r.rows[0][1], serde_json::json!(2));

    // Updating a row that does not exist (deleted concurrently) must fail
    // instead of upserting a partial row.
    let err = driver
        .apply_changes(&row_changes(
            &ks,
            &tbl,
            vec![cell(key(2), "a", serde_json::json!("ghost"))],
            vec![],
        ))
        .await
        .expect_err("update of a missing row must fail");
    assert!(err.to_string().contains("no longer exists"), "{err}");
    assert_eq!(row_count(&driver, &ks, &tbl).await, 1, "no row re-created");

    // Deleting a missing row reports the same.
    let err = driver
        .apply_changes(&row_changes(
            &ks,
            &tbl,
            vec![],
            vec![DeleteRow {
                primary_key_values: key(2),
            }],
        ))
        .await
        .expect_err("delete of a missing row must fail");
    assert!(err.to_string().contains("no longer exists"), "{err}");

    // Deleting the existing row works.
    driver
        .apply_changes(&row_changes(
            &ks,
            &tbl,
            vec![],
            vec![DeleteRow {
                primary_key_values: key(1),
            }],
        ))
        .await
        .unwrap();
    assert_eq!(row_count(&driver, &ks, &tbl).await, 0);

    teardown_keyspace(&driver, &ks).await;
}

#[tokio::test]
async fn cassandra_multi_row_edit_does_not_resurrect_deleted_rows() {
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;
    let tbl = unique_table("mrlwt");
    driver
        .execute_query(
            "",
            &format!("CREATE TABLE {ks}.{tbl} (id int PRIMARY KEY, v text, w int)"),
        )
        .await
        .unwrap();
    for (id, v) in [(1, "a"), (3, "c")] {
        driver
            .execute_query(
                "",
                &format!("INSERT INTO {ks}.{tbl} (id, v, w) VALUES ({id}, '{v}', 0)"),
            )
            .await
            .unwrap();
    }
    let key = |id: i64| vec![("id".to_string(), serde_json::json!(id))];

    // Row 2 was deleted elsewhere: editing rows 1 and 2 together must
    // fail without touching row 1 and without re-creating row 2.
    let err = driver
        .apply_changes(&row_changes(
            &ks,
            &tbl,
            vec![
                cell(key(1), "v", serde_json::json!("new1")),
                cell(key(2), "v", serde_json::json!("new2")),
            ],
            vec![],
        ))
        .await
        .expect_err("edit of a deleted row must fail");
    assert!(err.to_string().contains("no longer exists"), "{err}");
    assert!(err.to_string().contains("No changes were saved"), "{err}");
    assert_eq!(
        row_count(&driver, &ks, &tbl).await,
        2,
        "row 2 not re-created"
    );
    let r = driver
        .execute_query("", &format!("SELECT v FROM {ks}.{tbl} WHERE id = 1"))
        .await
        .unwrap();
    assert_eq!(r.rows[0][0], serde_json::json!("a"), "row 1 untouched");

    // A multi-row delete of rows that are gone reports every one.
    let err = driver
        .apply_changes(&row_changes(
            &ks,
            &tbl,
            vec![],
            vec![
                DeleteRow {
                    primary_key_values: key(7),
                },
                DeleteRow {
                    primary_key_values: key(8),
                },
            ],
        ))
        .await
        .expect_err("delete of missing rows must fail");
    assert!(
        err.to_string().contains("2 edited rows no longer exist"),
        "{err}"
    );

    // Edits of existing rows (several cells, an update and a delete)
    // are all applied.
    driver
        .apply_changes(&row_changes(
            &ks,
            &tbl,
            vec![
                cell(key(1), "v", serde_json::json!("b")),
                cell(key(1), "w", serde_json::json!(5)),
            ],
            vec![DeleteRow {
                primary_key_values: key(3),
            }],
        ))
        .await
        .unwrap();
    let r = driver
        .execute_query("", &format!("SELECT id, v, w FROM {ks}.{tbl}"))
        .await
        .unwrap();
    assert_eq!(
        r.rows,
        vec![vec![
            serde_json::json!(1),
            serde_json::json!("b"),
            serde_json::json!(5)
        ]]
    );

    // Console statements are not rewritten: an UPDATE of a missing row is
    // the usual CQL upsert.
    driver
        .execute_query("", &format!("UPDATE {ks}.{tbl} SET v = 'd' WHERE id = 3"))
        .await
        .unwrap();
    assert_eq!(row_count(&driver, &ks, &tbl).await, 2);

    teardown_keyspace(&driver, &ks).await;
}

#[tokio::test]
async fn cassandra_console_shows_rows_by_result_not_keyword() {
    let driver = create_driver().await;
    let ks = setup_keyspace(&driver).await;
    let tbl = unique_table("conrows");
    driver
        .execute_query(
            "",
            &format!("CREATE TABLE {ks}.{tbl} (id int PRIMARY KEY, v text)"),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            "",
            &format!("INSERT INTO {ks}.{tbl} (id, v) VALUES (1, 'a')"),
        )
        .await
        .unwrap();

    // A leading comment used to hide the SELECT.
    let r = driver
        .execute_query("", &format!("-- note\nSELECT id, v FROM {ks}.{tbl}"))
        .await
        .unwrap();
    assert!(r.is_select);
    assert_eq!(r.rows.len(), 1);

    // An empty result keeps its header.
    let r = driver
        .execute_query("", &format!("SELECT id, v FROM {ks}.{tbl} WHERE id = 99"))
        .await
        .unwrap();
    assert!(r.is_select);
    assert_eq!(r.columns, vec!["id", "v"]);
    assert!(r.rows.is_empty());

    // An LWT answers with `[applied]`.
    let r = driver
        .execute_query(
            "",
            &format!("UPDATE {ks}.{tbl} SET v = 'b' WHERE id = 1 IF EXISTS"),
        )
        .await
        .unwrap();
    assert!(r.is_select);
    assert_eq!(r.columns[0], "[applied]");

    // Plain writes still report no rows.
    let r = driver
        .execute_query(
            "",
            &format!("INSERT INTO {ks}.{tbl} (id, v) VALUES (2, 'x')"),
        )
        .await
        .unwrap();
    assert!(!r.is_select);

    teardown_keyspace(&driver, &ks).await;
}
