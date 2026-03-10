# DB Studio

A cross-platform desktop database management tool built with Tauri (Rust) and React (TypeScript).

Supports **PostgreSQL**, **MySQL**, and **SQLite**.

## Features

- **Connection Manager** — Save, edit, test, and color-code database connections
- **Object Tree** — Browse databases, schemas, tables, and views in a lazy-loaded tree
- **Tabbed Browsing** — Open tables from different databases in separate tabs, drag to reorder
- **Inline Data Editing** — Click any cell to edit, modified cells are highlighted, save all changes as a transaction
- **SQL Query Console** — Monaco editor with syntax highlighting, Ctrl+Enter to execute, query history
- **Pagination** — Browse large tables with paginated results and column sorting

## Prerequisites

- [Rust](https://rustup.rs/) (1.70+)
- [Node.js](https://nodejs.org/) (18+)
- Linux system deps: `sudo apt install libwebkit2gtk-4.1-dev libsoup-3.0-dev libjavascriptcoregtk-4.1-dev librsvg2-dev`

## Development

```bash
# Install frontend dependencies
npm install

# Run in development mode (starts both Vite and Tauri)
npm run tauri dev

# Build for production
npm run tauri build
```

## Keyboard Shortcuts

| Shortcut | Action |
|---|---|
| Ctrl+Enter | Execute query (in query console) |
| Double-click cell | Edit cell value |
| Enter | Commit cell edit |
| Escape | Cancel cell edit |
| Tab | Commit and move to next cell |

## Architecture

- **Backend (Rust)**: Connection pooling, query execution, schema introspection via `sqlx`
- **Frontend (React)**: UI components, state management via `zustand`, SQL editor via `monaco-editor`
- **IPC**: Tauri invoke commands bridge frontend to backend
