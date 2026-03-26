<p align="center">
  <img src="assets/logo.png" width="128" height="128" alt="Tablio — Open-Source Database Client" />
</p>

<h1 align="center">Tablio</h1>

<p align="center">
  <strong>Open-source, cross-platform desktop database client</strong><br/>
  Browse, query, and manage PostgreSQL, MySQL, and SQLite databases — all from one app.
</p>

<p align="center">
  <a href="https://github.com/dasunNimantha/tablio/releases"><img src="https://img.shields.io/github/v/release/dasunNimantha/tablio?style=flat-square" alt="Latest Release" /></a>
  <a href="https://github.com/dasunNimantha/tablio/actions"><img src="https://img.shields.io/github/actions/workflow/status/dasunNimantha/tablio/ci.yml?branch=main&style=flat-square&label=CI" alt="CI Status" /></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/dasunNimantha/tablio?style=flat-square" alt="License" /></a>
  <a href="https://github.com/dasunNimantha/tablio/stargazers"><img src="https://img.shields.io/github/stars/dasunNimantha/tablio?style=flat-square" alt="Stars" /></a>
</p>

<p align="center">
  <a href="#-features">Features</a> •
  <a href="#-installation">Installation</a> •
  <a href="#-development">Development</a> •
  <a href="#-keyboard-shortcuts">Shortcuts</a> •
  <a href="#-architecture">Architecture</a>
</p>

---

## Why Tablio?

Most database GUIs are either bloated, expensive, or locked to a single engine. Tablio is a **free, lightweight, native desktop app** that connects to PostgreSQL, MySQL, and SQLite with a single unified interface. Built with Rust and React for speed and reliability.

---

## ✨ Features

### Multi-Database Support
Connect to **PostgreSQL**, **MySQL**, and **SQLite** databases from one app. Save, organize, and color-code your connections. Supports **SSL** and **SSH tunnels**.

### Data Browsing & Inline Editing
- Paginated, sortable, and filterable data grid powered by AG Grid
- Edit cells inline — changes are highlighted and saved as a single transaction
- Show, hide, and reorder columns with persisted preferences
- Row detail view for tables with many columns
- Primary key / foreign key badges on column headers

### SQL Query Console
- Monaco-powered editor with syntax highlighting and table/column autocompletion
- Execute with `Ctrl+Enter`, view results in a resizable split pane
- Built-in SQL formatter, query history with pinning, and saved queries
- EXPLAIN plan visualization
- **Chart mode** — visualize query results as bar, line, pie, or scatter charts

### Schema Management
- Lazy-loaded object tree: databases → schemas → tables, views, functions
- Create and alter tables through dialogs
- View DDL for any database object
- Drop and truncate with confirmation
- Table structure and storage statistics

### Server Administration
- Live activity dashboard: active sessions, locks, server configuration
- Query performance statistics (e.g. `pg_stat_statements`)
- Role management: create, alter, and drop database roles
- App resource usage in the status bar

### Data Import & Export
- Export to **CSV**, **JSON**, or **SQL INSERT** statements
- Import data from files
- Backup and restore databases
- Cross-connection dump and restore
- Uses native tools (`pg_dump`, `mysqldump`) when available

### Visual Tools
- **ERD viewer** — entity-relationship diagrams with pan, zoom, and search
- **Chart view** — turn any SELECT result into a chart
- **Multiple themes** — light and dark variants with zoom control (50–200%)
- **Tabbed interface** — work with multiple tables and queries side by side

---

## 📦 Installation

### Download

Grab the latest build from [**Releases**](../../releases):

| Platform | Formats |
|----------|---------|
| **Linux** | `.deb`, `.AppImage` |
| **macOS** | `.dmg` (Intel + Apple Silicon) |
| **Windows** | `.msi`, `.exe` (NSIS) |

### Build from Source

**Prerequisites:**
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

## 🛠 Development

```bash
npm install
npm run tauri dev
```

Starts both the Vite dev server and the Tauri Rust backend with hot-reload.

### Running Tests

```bash
# Frontend
npm test

# Backend (SQLite tests run locally; Postgres/MySQL need running instances)
cd src-tauri && cargo test

# With real databases
TEST_POSTGRES_URL="postgres://user:pass@localhost/testdb" \
TEST_MYSQL_URL="mysql://user:pass@localhost/testdb" \
cargo test
```

### CI Pipeline

GitHub Actions runs on every push and PR:
- TypeScript type checking + Vitest frontend tests
- `cargo fmt --check` + `cargo clippy` + integration tests with **Postgres 16** and **MySQL 8** service containers
- Cross-platform release builds (Linux, macOS, Windows)

---

## ⌨ Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `Ctrl+Enter` | Execute query |
| `Ctrl+?` | Show shortcuts overlay |
| `Ctrl+` `+` / `Ctrl+` `-` | Zoom in / out |
| `Ctrl+0` | Reset zoom |
| Double-click cell | Edit cell value |
| `Enter` | Commit cell edit |
| `Escape` | Cancel cell edit |
| `Tab` | Commit and move to next cell |

---

## 🏗 Architecture

```
tablio/
├── src/                    # React 18 + TypeScript frontend
│   ├── components/         # DataGrid, QueryConsole, ERD, Sidebar, etc.
│   ├── stores/             # Zustand state (tabs, connections)
│   └── lib/                # Themes, Tauri IPC bridge, utilities
├── src-tauri/
│   ├── src/
│   │   ├── db/             # DatabaseDriver trait + Postgres/MySQL/SQLite
│   │   ├── commands/       # Tauri IPC command handlers
│   │   └── lib.rs          # Command registration
│   └── tests/              # Integration tests per database engine
└── .github/workflows/      # CI + automated release pipeline
```

| Layer | Stack |
|-------|-------|
| **Backend** | Rust, sqlx, Tokio, Tauri 2 |
| **Frontend** | React 18, TypeScript, AG Grid, Monaco Editor, Chart.js |
| **State** | Zustand + localStorage persistence |
| **IPC** | Tauri invoke commands |
| **Build** | Vite 6, cargo, GitHub Actions |

---

## 🤝 Contributing

Contributions are welcome! Feel free to open issues and pull requests.

1. Fork the repository
2. Create your branch (`git checkout -b feature/my-feature`)
3. Commit your changes (`git commit -m 'Add my feature'`)
4. Push to the branch (`git push origin feature/my-feature`)
5. Open a Pull Request

---

## 📄 License

[MIT](LICENSE)

---

<p align="center">
  <sub>Built with <a href="https://tauri.app">Tauri</a>, <a href="https://react.dev">React</a>, and <a href="https://www.rust-lang.org">Rust</a></sub>
</p>
