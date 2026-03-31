<p align="center">
  <img src="assets/logo.png" width="256" height="256" alt="Tablio — Open-Source Database Client" />
</p>

<h1 align="center">Tablio</h1>

<p align="center">
  <strong>Open-source, cross-platform desktop database client</strong><br/>
  Browse, query, and manage your databases from one native application.
</p>

<p align="center">
  <a href="https://github.com/dasunNimantha/tablio/releases"><img src="https://img.shields.io/github/v/release/dasunNimantha/tablio?style=flat-square" alt="Latest Release" /></a>
  <a href="https://github.com/dasunNimantha/tablio/actions"><img src="https://img.shields.io/github/actions/workflow/status/dasunNimantha/tablio/ci.yml?branch=master&style=flat-square&label=CI" alt="CI Status" /></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/dasunNimantha/tablio?style=flat-square" alt="License" /></a>
  <a href="https://github.com/dasunNimantha/tablio/stargazers"><img src="https://img.shields.io/github/stars/dasunNimantha/tablio?style=flat-square" alt="Stars" /></a>
</p>

<p align="center">
  <a href="#features">Features</a> •
  <a href="#installation">Installation</a> •
  <a href="#development">Development</a> •
  <a href="#architecture">Architecture</a>
</p>

---

## Why Tablio?

Most database GUIs are either bloated, expensive, or locked to a single engine. Tablio is a free, lightweight, native desktop application that connects to PostgreSQL, MySQL, MariaDB, SQL Server, CockroachDB, TiDB, SQLite, and Cassandra/ScyllaDB through a single unified interface. Built with Rust and React for speed and reliability.

---

## Features

### Multi-Database Support
Connect to PostgreSQL, MySQL, MariaDB, SQL Server, CockroachDB, TiDB, SQLite, and Cassandra/ScyllaDB from one application. Each database has a dedicated driver with engine-specific optimizations. Save, organize, and color-code your connections. Supports SSL/TLS with configurable certificate validation and SSH tunnels.

### Data Browsing and Inline Editing
- Paginated, sortable, and filterable data grid powered by AG Grid
- Edit cells inline with change tracking and single-transaction commits
- In-grid search with match navigation between results
- Show, hide, and reorder columns with persisted preferences
- Row detail view for tables with many columns
- Primary key and foreign key badges on column headers

### SQL Query Console
- Monaco-powered editor with syntax highlighting and table/column autocompletion
- Execute queries and view results in a resizable split pane
- Built-in SQL formatter, query history with pinning, and saved queries
- Visual query execution plan viewer
- Chart mode for visualizing query results as bar, line, pie, or scatter charts

### Schema Management
- Lazy-loaded object tree: databases, schemas, tables, views, and functions
- Create and alter tables through dialogs
- View DDL for any database object
- Drop and truncate with confirmation
- Table structure and storage statistics

### Server Administration
- Live activity dashboard with active sessions, locks, and server configuration
- Query performance statistics from pg_stat_statements
- Role management: create, alter, and drop database roles
- Application resource usage in the status bar

### Data Import and Export
- Export to CSV, JSON, or SQL INSERT statements
- Import data from files
- Backup and restore databases with cross-connection support
- Uses native tools (pg_dump, mysqldump) when available

### Visual Tools
- Entity-relationship diagram viewer with pan, zoom, and search
- Chart view for turning SELECT results into visualizations
- JSON column viewer and editor with structured tree navigation
- Light and dark themes with zoom control
- Tabbed interface for working with multiple tables and queries side by side

---

## Installation

### Quick Install (Linux)

```bash
curl -fsSL https://tablio.dasunnimantha.com/install.sh | bash
```

Automatically detects your distro and installs via APT (Debian/Ubuntu), RPM (Fedora/RHEL/SUSE), or AppImage (other).

### Download

Grab the latest build from [Releases](../../releases):

| Platform | Formats |
|----------|---------|
| Linux | `.deb`, `.rpm`, `.AppImage` |
| macOS | `.dmg` (Intel and Apple Silicon) |
| Windows | `.msi`, `.exe` |

### APT Repository (Debian/Ubuntu)

```bash
# Import the signing key
curl -fsSL https://dasunnimantha.github.io/tablio/apt/key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/tablio.gpg

# Add the repository
echo "deb [signed-by=/usr/share/keyrings/tablio.gpg] https://dasunnimantha.github.io/tablio/apt stable main" \
  | sudo tee /etc/apt/sources.list.d/tablio.list

# Install
sudo apt update && sudo apt install tablio
```

Updates are delivered through `apt upgrade` with each new release.

### RPM Repository (Fedora/RHEL/SUSE)

```bash
# Import the signing key
sudo rpm --import https://dasunnimantha.github.io/tablio/rpm/key.gpg

# Add the repository
sudo curl -fsSL -o /etc/yum.repos.d/tablio.repo https://dasunnimantha.github.io/tablio/rpm/tablio.repo

# Install
sudo dnf install tablio
```

Updates are delivered through `dnf upgrade tablio` with each new release.

### Build from Source

Prerequisites:
- [Rust](https://rustup.rs/) 1.70+
- [Node.js](https://nodejs.org/) 18+
- Linux system dependencies:

```bash
sudo apt install libwebkit2gtk-4.1-dev libsoup-3.0-dev \
  libjavascriptcoregtk-4.1-dev librsvg2-dev
```

```bash
git clone https://github.com/dasunNimantha/tablio.git
cd tablio
npm install
npm run tauri build
```

---

## Development

```bash
npm install
npm run tauri dev
```

Starts both the Vite dev server and the Tauri Rust backend with hot-reload.

### Running Tests

```bash
# Frontend
npm test

# Backend (SQLite tests run locally; others need running instances)
cd src-tauri && cargo test

# With real databases
TEST_POSTGRES_URL="postgres://user:pass@localhost/testdb" \
TEST_MYSQL_URL="mysql://user:pass@localhost/testdb" \
TEST_MARIADB_URL="mysql://user:pass@localhost:3307/testdb" \
TEST_COCKROACHDB_URL="postgres://root@localhost:26257/testdb?sslmode=disable" \
TEST_TIDB_URL="mysql://root@localhost:4000/testdb" \
TEST_MSSQL_URL="mssql://sa:Password@localhost:1433/testdb" \
TEST_CASSANDRA_HOST="127.0.0.1" TEST_CASSANDRA_PORT="9042" \
cargo test
```

---

## Architecture

```
tablio/
├── src/                    # React + TypeScript frontend
│   ├── components/         # DataGrid, QueryConsole, ERD, Sidebar
│   ├── stores/             # Zustand state (tabs, connections)
│   └── lib/                # Themes, Tauri IPC bridge, utilities
├── src-tauri/
│   ├── src/
│   │   ├── db/             # DatabaseDriver trait + dedicated drivers per engine
│   │   │   ├── postgres.rs, cockroachdb.rs, pg_common.rs
│   │   │   ├── mysql.rs, mariadb.rs, tidb.rs, mysql_common.rs
│   │   │   ├── mssql.rs
│   │   │   ├── cassandra.rs
│   │   │   └── sqlite.rs
│   │   ├── commands/       # Tauri IPC command handlers
│   │   └── lib.rs          # Command registration
│   └── tests/              # Integration tests per database engine
└── .github/workflows/      # CI and release pipeline
```

| Layer | Stack |
|-------|-------|
| Backend | Rust, sqlx, tiberius, scylla, Tokio, Tauri 2 |
| Frontend | React, TypeScript, AG Grid, Monaco Editor, Chart.js |
| State | Zustand with localStorage persistence |
| IPC | Tauri invoke commands |
| Build | Vite, cargo, GitHub Actions |

### Connection Management

Tablio uses sqlx connection pools for PostgreSQL, CockroachDB, MySQL, MariaDB, TiDB, and SQLite. MSSQL uses a single tiberius client connection.

#### Pool Settings

| Setting | Value | Rationale |
|---------|-------|-----------|
| Max connections | 4 | Sufficient for a single-user desktop app running queries, browsing data, and fetching metadata concurrently |
| Min connections | 0 | No connections are kept alive when idle; the pool scales down to zero |
| Idle timeout | 30 minutes | Idle connections are closed after 30 minutes of inactivity, matching DBeaver's production connection type default |

#### Per-Database Connection Pools

PostgreSQL and CockroachDB support connecting to multiple databases on the same server. Tablio creates a separate connection pool per database, stored in a `RwLock<HashMap<String, Pool>>`. When the user switches to a different database in the sidebar, a new pool is created on demand and cached for reuse.

#### Optional Database Name

The database field is optional when creating a connection. When no database is specified:
- **PostgreSQL** defaults to the `postgres` maintenance database
- **CockroachDB** defaults to `defaultdb` (implicit server behavior)
- **MySQL/MariaDB/TiDB** connect to the server without selecting a database; the user can browse and select one from the sidebar
- **SQL Server** connects to the default database configured for the login

This allows users to connect to a server first and then explore available databases, similar to pgAdmin's workflow.

#### Query Console Result Editing

When a query is executed in the SQL console, Tablio detects if the result is editable:
1. The SQL is parsed to check if it targets a single table (no JOINs, UNIONs, subqueries, or CTEs)
2. If a single table is detected, column metadata is fetched to find primary key columns
3. If all PK columns are present in the SELECT result, the grid becomes editable with Save writing changes back to the database via UPDATE/INSERT/DELETE statements
4. Otherwise the grid is read-only (cells are non-editable, Add Row/Delete/Save buttons are hidden)

A badge in the result toolbar shows "Editable" or "Read-only" with a tooltip explaining why.

---

## License

[MIT](LICENSE)

---

<p align="center">
  <sub>Built with <a href="https://tauri.app">Tauri</a>, <a href="https://react.dev">React</a>, and <a href="https://www.rust-lang.org">Rust</a></sub>
</p>
