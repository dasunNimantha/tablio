use tablio_lib::db::mssql::MssqlDriver;
use tablio_lib::db::DatabaseDriver;
use tablio_lib::models::*;

macro_rules! mssql_driver {
    () => {{
        let url = match std::env::var("TEST_MSSQL_URL") {
            Ok(v) if !v.is_empty() => v,
            _ => {
                eprintln!("Skipping: TEST_MSSQL_URL not set");
                return;
            }
        };
        // Expected format: mssql://user:password@host:port/database
        let parts = url
            .strip_prefix("mssql://")
            .or_else(|| url.strip_prefix("sqlserver://"))
            .expect("bad TEST_MSSQL_URL — use mssql://user:pass@host:port/database");
        let (user_pass, rest) = parts.split_once('@').expect("missing @");
        let (user, password) = user_pass.split_once(':').expect("missing :");
        let (host_port, database) = rest.split_once('/').expect("missing /");
        let database = database.split('?').next().unwrap();
        let (host, port) = host_port.split_once(':').expect("missing port");
        let config = ConnectionConfig {
            id: "test".into(),
            name: "test".into(),
            db_type: DbType::Mssql,
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
            MssqlDriver::connect(&config).await.unwrap(),
            database.to_string(),
        )
    }};
}

macro_rules! mssql_driver_no_db {
    () => {{
        let url = match std::env::var("TEST_MSSQL_URL") {
            Ok(v) if !v.is_empty() => v,
            _ => {
                eprintln!("Skipping: TEST_MSSQL_URL not set");
                return;
            }
        };
        let parts = url
            .strip_prefix("mssql://")
            .or_else(|| url.strip_prefix("sqlserver://"))
            .expect("bad TEST_MSSQL_URL");
        let (user_pass, rest) = parts.split_once('@').expect("missing @");
        let (user, password) = user_pass.split_once(':').expect("missing :");
        let (host_port, _database) = rest.split_once('/').expect("missing /");
        let (host, port) = host_port.split_once(':').expect("missing port");
        let config = ConnectionConfig {
            id: "test-no-db".into(),
            name: "test-no-db".into(),
            db_type: DbType::Mssql,
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
        MssqlDriver::connect(&config).await.unwrap()
    }};
}

fn unique_table(prefix: &str) -> String {
    format!(
        "{}_{}",
        prefix,
        uuid::Uuid::new_v4().simple().to_string().get(..8).unwrap()
    )
}

const SCHEMA: &str = "dbo";

// ===========================================================================
// Connection
// ===========================================================================

#[tokio::test]
async fn mssql_test_connection() {
    let (driver, _db) = mssql_driver!();
    assert!(driver.test_connection().await.unwrap());
}

// ===========================================================================
// Databases
// ===========================================================================

#[tokio::test]
async fn mssql_list_databases() {
    let (driver, db) = mssql_driver!();
    let dbs = driver.list_databases().await.unwrap();
    assert!(!dbs.is_empty());
    assert!(
        dbs.iter().any(|d| d.name == db),
        "expected database '{}' in list, got: {:?}",
        db,
        dbs.iter().map(|d| &d.name).collect::<Vec<_>>()
    );
}

// ===========================================================================
// Schemas
// ===========================================================================

#[tokio::test]
async fn mssql_list_schemas() {
    let (driver, db) = mssql_driver!();
    let schemas = driver.list_schemas(&db).await.unwrap();
    let names: Vec<&str> = schemas.iter().map(|s| s.name.as_str()).collect();
    assert!(
        names.contains(&"dbo"),
        "expected 'dbo' schema, got: {:?}",
        names
    );
}

// ===========================================================================
// Tables & columns (create, list, column types)
// ===========================================================================

#[tokio::test]
async fn mssql_create_table_and_list_tables() {
    let (driver, db) = mssql_driver!();
    let tbl = unique_table("ms_tbl");

    let cols = vec![
        ColumnDefinition {
            name: "id".into(),
            data_type: "INT IDENTITY(1,1)".into(),
            is_nullable: false,
            is_primary_key: true,
            default_value: None,
        },
        ColumnDefinition {
            name: "label".into(),
            data_type: "NVARCHAR(100)".into(),
            is_nullable: true,
            is_primary_key: false,
            default_value: None,
        },
    ];
    driver.create_table(&db, SCHEMA, &tbl, &cols).await.unwrap();

    let tables = driver.list_tables(&db, SCHEMA).await.unwrap();
    assert!(
        tables.iter().any(|t| t.name == tbl),
        "table not found after create"
    );

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

#[tokio::test]
async fn mssql_create_table_no_columns_error() {
    let (driver, db) = mssql_driver!();
    let result = driver.create_table(&db, SCHEMA, "should_fail", &[]).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn mssql_list_columns_various_types() {
    let (driver, db) = mssql_driver!();
    let tbl = unique_table("ms_cols");

    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE [dbo].[{}] (\
                   id INT IDENTITY(1,1) PRIMARY KEY, \
                   title NVARCHAR(80) NOT NULL, \
                   body NVARCHAR(MAX) NULL, \
                   active BIT DEFAULT 0, \
                   price DECIMAL(14,3) NULL, \
                   created_at DATETIME2 NULL, \
                   ext_id UNIQUEIDENTIFIER NULL, \
                   amount MONEY NULL, \
                   small_num SMALLINT NULL, \
                   big_num BIGINT NULL\
                 )",
                tbl
            ),
        )
        .await
        .unwrap();

    let cols = driver.list_columns(&db, SCHEMA, &tbl).await.unwrap();
    assert_eq!(cols.len(), 10);

    let by_name: std::collections::HashMap<_, _> =
        cols.iter().map(|c| (c.name.as_str(), c)).collect();

    assert!(by_name["id"].is_primary_key);
    assert!(by_name["id"].is_auto_generated);
    assert!(!by_name["title"].is_nullable);
    assert!(by_name["body"].is_nullable);
    assert!(by_name["price"].data_type.contains("decimal"));

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ===========================================================================
// Indexes
// ===========================================================================

#[tokio::test]
async fn mssql_list_indexes() {
    let (driver, db) = mssql_driver!();
    let tbl = unique_table("ms_idx");

    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE [dbo].[{}] (\
                   id INT PRIMARY KEY, \
                   slug NVARCHAR(100) NOT NULL\
                 )",
                tbl
            ),
        )
        .await
        .unwrap();

    let idx_name = format!("ix_{}_slug", tbl);
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE NONCLUSTERED INDEX [{}] ON [dbo].[{}] ([slug])",
                idx_name, tbl
            ),
        )
        .await
        .unwrap();

    let indexes = driver.list_indexes(&db, SCHEMA, &tbl).await.unwrap();
    assert!(
        indexes.iter().any(|i| i.name == idx_name),
        "index '{}' not found; got: {:?}",
        idx_name,
        indexes.iter().map(|i| &i.name).collect::<Vec<_>>()
    );
    let slug_idx = indexes.iter().find(|i| i.name == idx_name).unwrap();
    assert_eq!(slug_idx.columns, vec!["slug"]);
    assert!(!slug_idx.is_unique);

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ===========================================================================
// Foreign keys
// ===========================================================================

#[tokio::test]
async fn mssql_list_foreign_keys() {
    let (driver, db) = mssql_driver!();
    let parent = unique_table("ms_fk_p");
    let child = unique_table("ms_fk_c");

    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE [dbo].[{}] (id INT PRIMARY KEY, label NVARCHAR(50))",
                parent
            ),
        )
        .await
        .unwrap();

    let fk_name = format!("fk_{}", child);
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE [dbo].[{}] (\
                   id INT PRIMARY KEY, \
                   parent_id INT NOT NULL, \
                   CONSTRAINT [{}] FOREIGN KEY (parent_id) \
                     REFERENCES [dbo].[{}](id) \
                     ON DELETE CASCADE ON UPDATE CASCADE\
                 )",
                child, fk_name, parent
            ),
        )
        .await
        .unwrap();

    let fks = driver.list_foreign_keys(&db, SCHEMA, &child).await.unwrap();
    assert_eq!(fks.len(), 1);
    assert_eq!(fks[0].column, "parent_id");
    assert!(fks[0].referenced_table.contains(&parent));
    assert_eq!(fks[0].referenced_column, "id");
    assert!(fks[0].on_delete.contains("CASCADE"));
    assert!(fks[0].on_update.contains("CASCADE"));

    driver
        .drop_object(&db, SCHEMA, &child, "TABLE")
        .await
        .unwrap();
    driver
        .drop_object(&db, SCHEMA, &parent, "TABLE")
        .await
        .unwrap();
}

// ===========================================================================
// Functions
// ===========================================================================

#[tokio::test]
async fn mssql_list_functions() {
    let (driver, db) = mssql_driver!();
    let fn_name = unique_table("msfn");

    driver
        .execute_query(
            &db,
            &format!(
                "CREATE FUNCTION [dbo].[{}]() RETURNS INT AS BEGIN RETURN 42 END",
                fn_name
            ),
        )
        .await
        .unwrap();

    let funcs = driver.list_functions(&db, SCHEMA).await.unwrap();
    assert!(
        funcs.iter().any(|f| f.name == fn_name),
        "function not found after create"
    );

    driver
        .execute_query(&db, &format!("DROP FUNCTION [dbo].[{}]", fn_name))
        .await
        .unwrap();
}

// ===========================================================================
// Triggers
// ===========================================================================

#[tokio::test]
async fn mssql_list_triggers() {
    let (driver, db) = mssql_driver!();
    let tbl = unique_table("ms_trg");
    let trg_name = format!("trg_{}", tbl);

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE [dbo].[{}] (id INT PRIMARY KEY, val INT)", tbl),
        )
        .await
        .unwrap();

    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TRIGGER [dbo].[{}] ON [dbo].[{}] AFTER INSERT AS BEGIN SELECT 1 END",
                trg_name, tbl
            ),
        )
        .await
        .unwrap();

    let triggers = driver.list_triggers(&db, SCHEMA, &tbl).await.unwrap();
    assert!(
        triggers.iter().any(|t| t.name == trg_name),
        "trigger not found"
    );

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ===========================================================================
// Table stats
// ===========================================================================

#[tokio::test]
async fn mssql_get_table_stats() {
    let (driver, db) = mssql_driver!();
    let tbl = unique_table("ms_stats");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE [dbo].[{}] (id INT PRIMARY KEY, n INT)", tbl),
        )
        .await
        .unwrap();

    driver
        .execute_query(
            &db,
            &format!(
                "INSERT INTO [dbo].[{}] VALUES (1,10),(2,20),(3,30),(4,40),(5,50)",
                tbl
            ),
        )
        .await
        .unwrap();

    let stats = driver.get_table_stats(&db, SCHEMA, &tbl).await.unwrap();
    assert!(stats.table_name.contains(&tbl));
    assert_eq!(stats.row_count, 5);
    assert!(!stats.total_size.is_empty());
    assert!(!stats.data_size.is_empty());

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// SQL Server table partitioning needs a partition function + scheme +
// table-on-scheme. The driver's COUNT_BIG(*) is unbounded by partition,
// and the size SQL aggregates `sys.partitions` rows, so a partitioned
// table should report an aggregate count and a non-empty size.
//
// We drop the table → scheme → function in that order because partition
// functions can't be dropped while a scheme references them, and schemes
// can't be dropped while a table references them.
#[tokio::test]
async fn mssql_get_table_stats_partitioned_table_aggregates_partitions() {
    let (driver, db) = mssql_driver!();
    let suffix = unique_table("");
    let tbl = format!("ms_stats_part_{}", suffix.trim_start_matches('_'));
    let pf = format!("pf_{}", suffix.trim_start_matches('_'));
    let ps = format!("ps_{}", suffix.trim_start_matches('_'));

    driver
        .execute_query(
            &db,
            &format!(
                "CREATE PARTITION FUNCTION [{}] (INT) AS RANGE LEFT FOR VALUES (2024, 2025)",
                pf
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE PARTITION SCHEME [{}] AS PARTITION [{}] ALL TO ([PRIMARY])",
                ps, pf
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE [dbo].[{}] (
                    id INT NOT NULL,
                    yr INT NOT NULL
                ) ON [{}](yr)",
                tbl, ps
            ),
        )
        .await
        .unwrap();
    // RANGE LEFT (2024, 2025) splits values into:
    //   p1: yr <= 2024 → (1,2023), (2,2024), (3,2024) = 3 rows
    //   p2: 2024 < yr <= 2025 → (4,2025), (5,2025) = 2 rows
    //   p3: yr > 2025 → (6,2026) = 1 row
    driver
        .execute_query(
            &db,
            &format!(
                "INSERT INTO [dbo].[{}] VALUES (1,2023),(2,2024),(3,2024),(4,2025),(5,2025),(6,2026)",
                tbl
            ),
        )
        .await
        .unwrap();

    let stats = driver.get_table_stats(&db, SCHEMA, &tbl).await.unwrap();
    assert!(stats.table_name.contains(&tbl));
    assert_eq!(
        stats.row_count, 6,
        "MSSQL stats must aggregate across all partitions"
    );
    assert!(!stats.total_size.is_empty());

    // Tear down in reverse dependency order.
    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
    driver
        .execute_query(&db, &format!("DROP PARTITION SCHEME [{}]", ps))
        .await
        .unwrap();
    driver
        .execute_query(&db, &format!("DROP PARTITION FUNCTION [{}]", pf))
        .await
        .unwrap();
}

#[tokio::test]
async fn mssql_get_table_stats_unknown_table_errors() {
    let (driver, db) = mssql_driver!();
    let bogus = unique_table("ms_no_such_table");
    let result = driver.get_table_stats(&db, SCHEMA, &bogus).await;
    // MSSQL surfaces unknown objects as a SQL "Invalid object name" error
    // from the COUNT_BIG(*) probe; we only assert is_err here because the
    // exact phrasing varies by server version and locale.
    assert!(
        result.is_err(),
        "unknown tables must error, not return zeros"
    );
}

// ===========================================================================
// fetch_rows: empty table
// ===========================================================================

#[tokio::test]
async fn mssql_fetch_rows_empty_table() {
    let (driver, db) = mssql_driver!();
    let tbl = unique_table("ms_empty");

    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE [dbo].[{}] (id INT PRIMARY KEY, val NVARCHAR(50))",
                tbl
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

// ===========================================================================
// fetch_rows: with data
// ===========================================================================

#[tokio::test]
async fn mssql_fetch_rows_with_data() {
    let (driver, db) = mssql_driver!();
    let tbl = unique_table("ms_ins");

    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE [dbo].[{}] (id INT IDENTITY(1,1) PRIMARY KEY, name NVARCHAR(100) NOT NULL)",
                tbl
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
            values: vec![("name".into(), serde_json::json!("Alice"))],
        }],
        deletes: vec![],
    };
    driver.apply_changes(&changes).await.unwrap();

    let data = driver
        .fetch_rows(&db, SCHEMA, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 1);

    let name_idx = data.columns.iter().position(|c| c.name == "name").unwrap();
    assert_eq!(data.rows[0][name_idx], serde_json::json!("Alice"));

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ===========================================================================
// fetch_rows: pagination (OFFSET / FETCH)
// ===========================================================================

#[tokio::test]
async fn mssql_fetch_rows_pagination() {
    let (driver, db) = mssql_driver!();
    let tbl = unique_table("ms_page");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE [dbo].[{}] (id INT PRIMARY KEY)", tbl),
        )
        .await
        .unwrap();

    let values: Vec<String> = (1..=10).map(|i| format!("({})", i)).collect();
    driver
        .execute_query(
            &db,
            &format!("INSERT INTO [dbo].[{}] VALUES {}", tbl, values.join(", ")),
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

// ===========================================================================
// fetch_rows: sort ascending / descending
// ===========================================================================

#[tokio::test]
async fn mssql_fetch_rows_sort_asc_desc() {
    let (driver, db) = mssql_driver!();
    let tbl = unique_table("ms_sort");

    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE [dbo].[{}] (id INT PRIMARY KEY, name NVARCHAR(50))",
                tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!(
                "INSERT INTO [dbo].[{}] VALUES (1,N'c'),(2,N'a'),(3,N'b')",
                tbl
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
    let name_idx = asc.columns.iter().position(|c| c.name == "name").unwrap();
    assert_eq!(asc.rows[0][name_idx], serde_json::json!("a"));
    assert_eq!(asc.rows[2][name_idx], serde_json::json!("c"));

    let desc = driver
        .fetch_rows(
            &db,
            SCHEMA,
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
    assert_eq!(desc.rows[0][name_idx], serde_json::json!("c"));
    assert_eq!(desc.rows[2][name_idx], serde_json::json!("a"));

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

#[tokio::test]
async fn mssql_fetch_rows_multi_column_sort() {
    // Multi-column sort (issue #57) end-to-end on SQL Server.
    // Uses bracket-quoted identifiers and the `OFFSET … FETCH
    // NEXT` paging that requires a non-empty ORDER BY.
    let (driver, db) = mssql_driver!();
    let tbl = unique_table("ms_multi_sort");
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE {}.{} (id INT PRIMARY KEY, dept NVARCHAR(32), salary INT)",
                SCHEMA, tbl,
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!(
                "INSERT INTO {}.{} VALUES \
                 (1,'eng',100),(2,'sales',90),(3,'eng',200),(4,'sales',70),(5,'eng',150)",
                SCHEMA, tbl,
            ),
        )
        .await
        .unwrap();

    let rows = driver
        .fetch_rows(
            &db,
            SCHEMA,
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

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ===========================================================================
// fetch_rows: filter (WHERE clause)
// ===========================================================================

#[tokio::test]
async fn mssql_fetch_rows_filter() {
    let (driver, db) = mssql_driver!();
    let tbl = unique_table("ms_filt");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE [dbo].[{}] (id INT PRIMARY KEY, val INT)", tbl),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!("INSERT INTO [dbo].[{}] VALUES (1,10),(2,20),(3,30)", tbl),
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
            Some("[val] > 15".into()),
        )
        .await
        .unwrap();
    assert_eq!(data.total_rows, 2);
    assert_eq!(data.rows.len(), 2);

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ===========================================================================
// fetch_rows: NULL values
// ===========================================================================

#[tokio::test]
async fn mssql_fetch_rows_null_values() {
    let (driver, db) = mssql_driver!();
    let tbl = unique_table("ms_null");

    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE [dbo].[{}] (id INT PRIMARY KEY, note NVARCHAR(100) NULL)",
                tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!("INSERT INTO [dbo].[{}] VALUES (1, NULL), (2, N'x')", tbl),
        )
        .await
        .unwrap();

    let data = driver
        .fetch_rows(&db, SCHEMA, &tbl, 0, 10, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 2);
    let note_idx = data.columns.iter().position(|c| c.name == "note").unwrap();
    assert_eq!(data.rows[0][note_idx], serde_json::Value::Null);
    assert_eq!(data.rows[1][note_idx], serde_json::json!("x"));

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ===========================================================================
// fetch_rows: various data types
// ===========================================================================

#[tokio::test]
async fn mssql_fetch_rows_various_data_types() {
    let (driver, db) = mssql_driver!();
    let tbl = unique_table("ms_types");

    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE [dbo].[{}] (\
                   id INT PRIMARY KEY, \
                   i INT, \
                   t NVARCHAR(50), \
                   b BIT, \
                   n DECIMAL(8,2), \
                   ts DATETIME2 NULL\
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
                "INSERT INTO [dbo].[{}] VALUES (\
                   1, 7, N'hi', 1, 12.34, '2020-01-15 10:30:00'\
                 )",
                tbl
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
    let by_name: std::collections::HashMap<&str, &serde_json::Value> = data
        .columns
        .iter()
        .zip(row.iter())
        .map(|(c, v)| (c.name.as_str(), v))
        .collect();
    assert_eq!(*by_name["i"], serde_json::json!(7));
    assert_eq!(*by_name["t"], serde_json::json!("hi"));
    assert_eq!(*by_name["b"], serde_json::json!(true));
    assert!(
        row.len() >= 6,
        "expected at least 6 columns, got {}",
        row.len()
    );

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ===========================================================================
// execute_query: SELECT
// ===========================================================================

#[tokio::test]
async fn mssql_execute_query_select() {
    let (driver, db) = mssql_driver!();
    let result = driver
        .execute_query(&db, "SELECT 1 AS num, N'hello' AS greeting")
        .await
        .unwrap();
    assert!(result.is_select);
    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.rows.len(), 1);
    assert!(result.execution_time_ms < 60_000);
}

// ===========================================================================
// execute_query: DML (INSERT)
// ===========================================================================

#[tokio::test]
async fn mssql_execute_query_dml_insert() {
    let (driver, db) = mssql_driver!();
    let tbl = unique_table("ms_dml");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE [dbo].[{}] (id INT PRIMARY KEY)", tbl),
        )
        .await
        .unwrap();

    let result = driver
        .execute_query(
            &db,
            &format!("INSERT INTO [dbo].[{}] VALUES (1),(2),(3)", tbl),
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

// ===========================================================================
// execute_query: invalid SQL → error
// ===========================================================================

#[tokio::test]
async fn mssql_execute_query_invalid_sql_errors() {
    let (driver, db) = mssql_driver!();
    let r = driver.execute_query(&db, "SELEC 1 FROM nowhere").await;
    assert!(r.is_err());
}

// ===========================================================================
// explain_query (SHOWPLAN_XML)
// ===========================================================================

#[tokio::test]
async fn mssql_explain_query() {
    let (driver, db) = mssql_driver!();
    let tbl = unique_table("ms_expl");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE [dbo].[{}] (id INT PRIMARY KEY)", tbl),
        )
        .await
        .unwrap();

    let ex = driver
        .explain_query(&db, &format!("SELECT * FROM [dbo].[{}] WHERE id = 1", tbl))
        .await
        .unwrap();
    assert!(!ex.raw_text.is_empty());
    assert!(!ex.plan.node_type.is_empty());

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ===========================================================================
// get_ddl: VIEW (OBJECT_DEFINITION works for views/stored procs)
// ===========================================================================

#[tokio::test]
async fn mssql_get_ddl_view() {
    let (driver, db) = mssql_driver!();
    let base = unique_table("ms_ddlb");
    let vname = unique_table("ms_ddlv");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE [dbo].[{}] (id INT PRIMARY KEY)", base),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE VIEW [dbo].[{}] AS SELECT id FROM [dbo].[{}]",
                vname, base
            ),
        )
        .await
        .unwrap();

    let ddl = driver.get_ddl(&db, SCHEMA, &vname, "VIEW").await.unwrap();
    assert!(ddl.contains(&vname), "DDL should contain view name");
    assert!(ddl.to_uppercase().contains("SELECT"));

    driver
        .drop_object(&db, SCHEMA, &vname, "VIEW")
        .await
        .unwrap();
    driver
        .drop_object(&db, SCHEMA, &base, "TABLE")
        .await
        .unwrap();
}

// ===========================================================================
// apply_changes: insert
// ===========================================================================

#[tokio::test]
async fn mssql_apply_changes_insert() {
    let (driver, db) = mssql_driver!();
    let tbl = unique_table("ms_ac_ins");

    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE [dbo].[{}] (id INT IDENTITY(1,1) PRIMARY KEY, code NVARCHAR(50))",
                tbl
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
            values: vec![("code".into(), serde_json::json!("Z1"))],
        }],
        deletes: vec![],
    };
    driver.apply_changes(&changes).await.unwrap();

    let data = driver
        .fetch_rows(&db, SCHEMA, &tbl, 0, 10, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 1);
    let code_idx = data.columns.iter().position(|c| c.name == "code").unwrap();
    assert_eq!(data.rows[0][code_idx], serde_json::json!("Z1"));

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ===========================================================================
// apply_changes: update
// ===========================================================================

#[tokio::test]
async fn mssql_apply_changes_update() {
    let (driver, db) = mssql_driver!();
    let tbl = unique_table("ms_upd");

    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE [dbo].[{}] (id INT PRIMARY KEY, val NVARCHAR(50))",
                tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!("INSERT INTO [dbo].[{}] VALUES (1, N'old')", tbl),
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
    let val_idx = data.columns.iter().position(|c| c.name == "val").unwrap();
    assert_eq!(data.rows[0][val_idx], serde_json::json!("new"));

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ===========================================================================
// apply_changes: delete
// ===========================================================================

#[tokio::test]
async fn mssql_apply_changes_delete() {
    let (driver, db) = mssql_driver!();
    let tbl = unique_table("ms_del");

    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE [dbo].[{}] (id INT PRIMARY KEY, val NVARCHAR(50))",
                tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!("INSERT INTO [dbo].[{}] VALUES (1, N'a'), (2, N'b')", tbl),
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
    let id_idx = data.columns.iter().position(|c| c.name == "id").unwrap();
    assert_eq!(data.rows[0][id_idx], serde_json::json!(2));

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ===========================================================================
// apply_changes: batch insert + update + delete
// ===========================================================================

#[tokio::test]
async fn mssql_apply_changes_batch_insert_update_delete() {
    let (driver, db) = mssql_driver!();
    let tbl = unique_table("ms_batch");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE [dbo].[{}] (id INT PRIMARY KEY, val INT)", tbl),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!(
                "INSERT INTO [dbo].[{}] VALUES (1, 10), (2, 20), (3, 30)",
                tbl
            ),
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
            old_value: serde_json::json!(10),
            new_value: serde_json::json!(99),
            primary_key_values: vec![("id".into(), serde_json::json!(1))],
        }],
        inserts: vec![NewRow {
            values: vec![
                ("id".into(), serde_json::json!(4)),
                ("val".into(), serde_json::json!(40)),
            ],
        }],
        deletes: vec![DeleteRow {
            primary_key_values: vec![("id".into(), serde_json::json!(2))],
        }],
    };
    driver.apply_changes(&changes).await.unwrap();

    let data = driver
        .fetch_rows(&db, SCHEMA, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 3);
    let id_idx = data.columns.iter().position(|c| c.name == "id").unwrap();
    let ids: Vec<i64> = data
        .rows
        .iter()
        .filter_map(|r| r[id_idx].as_i64())
        .collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&3));
    assert!(ids.contains(&4));
    assert!(!ids.contains(&2));

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ===========================================================================
// alter_table: add, rename, drop column
// ===========================================================================

#[tokio::test]
async fn mssql_alter_table_add_rename_drop_column() {
    let (driver, db) = mssql_driver!();
    let tbl = unique_table("ms_alter");

    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE [dbo].[{}] (id INT PRIMARY KEY, old_col NVARCHAR(50))",
                tbl
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
                    name: "new_col".into(),
                    data_type: "INT".into(),
                    is_nullable: true,
                    is_primary_key: false,
                    default_value: None,
                },
            }],
        )
        .await
        .unwrap();

    let cols = driver.list_columns(&db, SCHEMA, &tbl).await.unwrap();
    assert!(cols.iter().any(|c| c.name == "new_col"));

    driver
        .alter_table(
            &db,
            SCHEMA,
            &tbl,
            &[AlterTableOperation::RenameColumn {
                old_name: "old_col".into(),
                new_name: "renamed_col".into(),
            }],
        )
        .await
        .unwrap();

    let cols = driver.list_columns(&db, SCHEMA, &tbl).await.unwrap();
    let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
    assert!(names.contains(&"renamed_col"));
    assert!(!names.contains(&"old_col"));

    driver
        .alter_table(
            &db,
            SCHEMA,
            &tbl,
            &[AlterTableOperation::DropColumn {
                column_name: "new_col".into(),
            }],
        )
        .await
        .unwrap();

    let cols = driver.list_columns(&db, SCHEMA, &tbl).await.unwrap();
    assert!(!cols.iter().any(|c| c.name == "new_col"));

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ===========================================================================
// alter_table: change column type
// ===========================================================================

#[tokio::test]
async fn mssql_alter_table_change_column_type() {
    let (driver, db) = mssql_driver!();
    let tbl = unique_table("ms_alt_type");

    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE [dbo].[{}] (id INT PRIMARY KEY, n INT NOT NULL)",
                tbl
            ),
        )
        .await
        .unwrap();
    driver
        .execute_query(&db, &format!("INSERT INTO [dbo].[{}] VALUES (1, 42)", tbl))
        .await
        .unwrap();

    driver
        .alter_table(
            &db,
            SCHEMA,
            &tbl,
            &[AlterTableOperation::ChangeColumnType {
                column_name: "n".into(),
                new_type: "BIGINT".into(),
            }],
        )
        .await
        .unwrap();

    let cols = driver.list_columns(&db, SCHEMA, &tbl).await.unwrap();
    let n = cols.iter().find(|c| c.name == "n").unwrap();
    assert!(
        n.data_type.contains("bigint"),
        "expected bigint, got: {}",
        n.data_type
    );

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ===========================================================================
// alter_table: rename table (sp_rename)
// ===========================================================================

#[tokio::test]
async fn mssql_alter_table_rename_table() {
    let (driver, db) = mssql_driver!();
    let tbl = unique_table("ms_rn_tbl");
    let new_name = unique_table("ms_rn_new");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE [dbo].[{}] (id INT PRIMARY KEY)", tbl),
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

// ===========================================================================
// truncate_table
// ===========================================================================

#[tokio::test]
async fn mssql_truncate_table() {
    let (driver, db) = mssql_driver!();
    let tbl = unique_table("ms_trunc");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE [dbo].[{}] (id INT PRIMARY KEY)", tbl),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!("INSERT INTO [dbo].[{}] VALUES (1),(2),(3)", tbl),
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

// ===========================================================================
// drop_object: TABLE
// ===========================================================================

#[tokio::test]
async fn mssql_drop_object_table_removes_from_list() {
    let (driver, db) = mssql_driver!();
    let tbl = unique_table("ms_drop");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE [dbo].[{}] (id INT PRIMARY KEY)", tbl),
        )
        .await
        .unwrap();

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
    let tables = driver.list_tables(&db, SCHEMA).await.unwrap();
    assert!(!tables.iter().any(|t| t.name == tbl));
}

// ===========================================================================
// drop_object: VIEW
// ===========================================================================

#[tokio::test]
async fn mssql_drop_object_view() {
    let (driver, db) = mssql_driver!();
    let base = unique_table("ms_vbase");
    let vname = unique_table("ms_vw");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE [dbo].[{}] (id INT PRIMARY KEY)", base),
        )
        .await
        .unwrap();
    driver
        .execute_query(
            &db,
            &format!(
                "CREATE VIEW [dbo].[{}] AS SELECT id FROM [dbo].[{}]",
                vname, base
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

// ===========================================================================
// import_data
// ===========================================================================

#[tokio::test]
async fn mssql_import_data() {
    let (driver, db) = mssql_driver!();
    let tbl = unique_table("ms_import");

    driver
        .execute_query(
            &db,
            &format!(
                "CREATE TABLE [dbo].[{}] (id INT PRIMARY KEY, name NVARCHAR(50))",
                tbl
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
        .import_data(&db, SCHEMA, &tbl, &columns, &rows)
        .await
        .unwrap();
    assert_eq!(imported, 3);

    let data = driver
        .fetch_rows(&db, SCHEMA, &tbl, 0, 50, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 3);

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

#[tokio::test]
async fn mssql_import_data_large_batch() {
    let (driver, db) = mssql_driver!();
    let tbl = unique_table("ms_imp_big");

    driver
        .execute_query(
            &db,
            &format!("CREATE TABLE [dbo].[{}] (id INT PRIMARY KEY)", tbl),
        )
        .await
        .unwrap();

    let columns = vec!["id".to_string()];
    let rows: Vec<Vec<serde_json::Value>> = (0..100).map(|i| vec![serde_json::json!(i)]).collect();

    let imported = driver
        .import_data(&db, SCHEMA, &tbl, &columns, &rows)
        .await
        .unwrap();
    assert_eq!(imported, 100);

    let data = driver
        .fetch_rows(&db, SCHEMA, &tbl, 0, 1, Vec::new(), None)
        .await
        .unwrap();
    assert_eq!(data.total_rows, 100);

    driver
        .drop_object(&db, SCHEMA, &tbl, "TABLE")
        .await
        .unwrap();
}

// ===========================================================================
// Server monitoring: get_server_activity
// ===========================================================================

#[tokio::test]
async fn mssql_get_server_activity_ok() {
    let (driver, _db) = mssql_driver!();
    let _ = driver.get_server_activity().await.unwrap();
}

// ===========================================================================
// get_database_stats (returns defaults for SQL Server)
// ===========================================================================

#[tokio::test]
async fn mssql_get_database_stats_ok() {
    let (driver, _db) = mssql_driver!();
    let stats = driver.get_database_stats().await.unwrap();
    assert_eq!(stats.total_connections, 0);
}

// ===========================================================================
// get_locks (returns empty vec for SQL Server)
// ===========================================================================

#[tokio::test]
async fn mssql_get_locks_ok() {
    let (driver, _db) = mssql_driver!();
    let locks = driver.get_locks().await.unwrap();
    assert!(locks.is_empty());
}

// ===========================================================================
// get_server_config (returns empty vec for SQL Server)
// ===========================================================================

#[tokio::test]
async fn mssql_get_server_config_ok() {
    let (driver, _db) = mssql_driver!();
    let cfg = driver.get_server_config().await.unwrap();
    assert!(cfg.is_empty());
}

// ===========================================================================
// cancel_query: invalid session id
// ===========================================================================

#[tokio::test]
async fn mssql_cancel_query_invalid_pid_errors() {
    let (driver, _db) = mssql_driver!();
    let r = driver.cancel_query("not_a_number").await;
    assert!(r.is_err());
}

// ===========================================================================
// list_roles
// ===========================================================================

#[tokio::test]
async fn mssql_list_roles_ok() {
    let (driver, _db) = mssql_driver!();
    let roles = driver.list_roles().await.unwrap();
    // SA should be present in any SQL Server instance
    assert!(
        roles
            .iter()
            .any(|r| r.name.to_lowercase() == "sa" || !r.name.is_empty()),
        "expected at least one role/login"
    );
}

// ===========================================================================
// create_role / drop_role / alter_role: unsupported → errors
// ===========================================================================

#[tokio::test]
async fn mssql_create_role_unsupported() {
    let (driver, _db) = mssql_driver!();
    let req = CreateRoleRequest {
        connection_id: "test".into(),
        name: "test_role".into(),
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
async fn mssql_drop_role_unsupported() {
    let (driver, _db) = mssql_driver!();
    assert!(driver.drop_role("any").await.is_err());
}

#[tokio::test]
async fn mssql_alter_role_unsupported() {
    let (driver, _db) = mssql_driver!();
    let req = AlterRoleRequest {
        connection_id: "test".into(),
        name: "test_role".into(),
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
async fn mssql_no_db_connect_and_list_databases() {
    let driver = mssql_driver_no_db!();
    assert!(driver.test_connection().await.unwrap());
    let dbs = driver.list_databases().await.unwrap();
    assert!(!dbs.is_empty());
}

#[tokio::test]
async fn mssql_no_db_list_schemas_on_specific_database() {
    let (_, db) = mssql_driver!();
    let driver = mssql_driver_no_db!();
    let schemas = driver.list_schemas(&db).await.unwrap();
    assert!(!schemas.is_empty());
    assert!(schemas.iter().any(|s| s.name == "dbo"));
}

#[tokio::test]
async fn mssql_no_db_list_tables_on_specific_database() {
    let (with_db, db) = mssql_driver!();
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

    let driver = mssql_driver_no_db!();
    let mut tables = Vec::new();
    for _ in 0..3 {
        match driver.list_tables(&db, SCHEMA).await {
            Ok(t) => {
                tables = t;
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(500)).await,
        }
    }
    assert!(
        tables.iter().any(|t| t.name == tbl),
        "Table '{}' not found via no-db driver",
        tbl
    );

    let _ = with_db.drop_object(&db, SCHEMA, &tbl, "TABLE").await;
}

#[tokio::test]
async fn mssql_no_db_fetch_rows_on_specific_database() {
    let (with_db, db) = mssql_driver!();
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

    let driver = mssql_driver_no_db!();
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
    let (driver, db) = mssql_driver!();
    let result = driver.validate_query(&db, "SELECT 1").await.unwrap();
    assert!(result.is_none(), "valid SQL should return None");
}

#[tokio::test]
async fn validate_query_syntax_error() {
    let (driver, db) = mssql_driver!();
    let result = driver.validate_query(&db, "SELCT 1").await.unwrap();
    assert!(result.is_some(), "invalid SQL should return Some");
    let err = result.unwrap();
    assert!(!err.message.is_empty());
}

#[tokio::test]
async fn validate_query_nonexistent_table() {
    let (driver, db) = mssql_driver!();
    let result = driver
        .validate_query(&db, "SELECT * FROM nonexistent_table_xyz_12345")
        .await
        .unwrap();
    assert!(result.is_some());
}

#[tokio::test]
async fn validate_query_empty_string() {
    let (driver, db) = mssql_driver!();
    let result = driver.validate_query(&db, "").await.unwrap();
    assert!(result.is_some(), "empty SQL should be an error");
}

#[tokio::test]
async fn mssql_get_query_stats_ok_or_view_server_state_message() {
    let (driver, _db) = mssql_driver!();
    let res = driver.get_query_stats().await;
    assert!(res.is_ok(), "get_query_stats returned: {:?}", res.err());
    let qs = res.unwrap();
    if qs.available {
        assert_eq!(qs.kind, QueryStatsKind::Available);
        assert!(qs.message.is_none());
    } else {
        assert_eq!(qs.kind, QueryStatsKind::MssqlMissingViewServerState);
        let msg = qs.message.as_deref().unwrap_or("");
        assert!(
            msg.to_ascii_lowercase().contains("view server state"),
            "unexpected unavailable message: {msg}"
        );
    }
}

// ---------------------------------------------------------------------------
// Multi-statement execute_query (PR #131 follow-up)
// ---------------------------------------------------------------------------

// SQL Server's tiberius driver supports multi-statement batches
// natively, but Tablio's wrapper used to route the entire batch
// on the FIRST keyword. After the fix, the script is split at
// top-level semicolons and the LAST statement chooses between
// `run_select` / `run_exec` / `run_batch`.
//
// We exercise the CREATE VIEW → SELECT shape that triggered the
// original bug report.
#[tokio::test]
async fn mssql_multi_statement_create_view_then_select_returns_rows() {
    let (driver, db) = mssql_driver!();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let tbl = format!("mssql_mst_t_{}", &suffix[..8]);
    let view = format!("mssql_mst_v_{}", &suffix[..8]);

    // CREATE VIEW must be the first (and only) statement in its
    // batch on SQL Server, so we drive the multi-statement path
    // via separate batches that end up needing the LAST-statement
    // dispatch behaviour.
    let script = format!(
        "CREATE TABLE dbo.{tbl} (x INT); \
         INSERT INTO dbo.{tbl} VALUES (1), (2), (3); \
         SELECT x, CASE WHEN x % 2 = 0 THEN 'even' ELSE 'odd' END AS parity \
         FROM dbo.{tbl} ORDER BY x;"
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

    // CREATE VIEW + trailing SELECT, with the view created as a
    // standalone statement before the SELECT runs. This exercises
    // the per-statement dispatch path that calls `run_batch` for a
    // leading DDL and `run_select` for the trailing SELECT.
    let view_script = format!(
        "CREATE VIEW dbo.{view} AS SELECT x FROM dbo.{tbl}; \
         SELECT * FROM dbo.{view} ORDER BY x;"
    );
    let r2 = driver
        .execute_query(&db, &view_script)
        .await
        .expect("CREATE VIEW + SELECT failed");
    assert!(r2.is_select);
    assert_eq!(r2.rows.len(), 3);

    driver
        .execute_query(&db, &format!("DROP VIEW IF EXISTS dbo.{view}"))
        .await
        .ok();
    driver
        .execute_query(&db, &format!("DROP TABLE IF EXISTS dbo.{tbl}"))
        .await
        .ok();
}

// SQL Server-specific: bracket-quoted identifier with embedded
// semicolon must not split the statement. Plus a single-quoted
// string with embedded semicolons — both quote forms covered.
#[tokio::test]
async fn mssql_multi_statement_quotes_with_semicolons_are_safe() {
    let (driver, db) = mssql_driver!();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let tbl = format!("mssql_mst_q_{}", &suffix[..8]);
    let script = format!(
        "CREATE TABLE dbo.{tbl} ([weird;name] NVARCHAR(64)); \
         INSERT INTO dbo.{tbl} ([weird;name]) VALUES (N'first; second; third'); \
         SELECT [weird;name] FROM dbo.{tbl};"
    );
    let result = driver
        .execute_query(&db, &script)
        .await
        .expect("quoted-identifier-with-semicolons script failed");
    assert!(result.is_select);
    let v = result
        .rows
        .first()
        .and_then(|r| r.first())
        .and_then(|v| v.as_str())
        .unwrap_or("");
    assert_eq!(v, "first; second; third");

    driver
        .execute_query(&db, &format!("DROP TABLE IF EXISTS dbo.{tbl}"))
        .await
        .ok();
}

// All-DDL script: no trailing SELECT. Verify every leading
// statement runs and the wrapper reports `is_select = false`.
#[tokio::test]
async fn mssql_multi_statement_all_ddl_no_rows() {
    let (driver, db) = mssql_driver!();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let t1 = format!("mssql_mst_a_{}", &suffix[..8]);
    let t2 = format!("mssql_mst_b_{}", &suffix[..8]);
    let script = format!("CREATE TABLE dbo.{t1} (x INT); CREATE TABLE dbo.{t2} (y INT);");
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
                "SELECT COUNT(*) AS n FROM sys.tables \
                 WHERE name IN ('{t1}', '{t2}')"
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
        .execute_query(&db, &format!("DROP TABLE IF EXISTS dbo.{t1}"))
        .await
        .ok();
    driver
        .execute_query(&db, &format!("DROP TABLE IF EXISTS dbo.{t2}"))
        .await
        .ok();
}
