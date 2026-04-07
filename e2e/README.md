# E2E Tests

Playwright end-to-end tests for the Tablio frontend. All tests run against the Vite dev server with mock backend data (no real database required).

## Running Tests

```bash
# Run all tests (headless)
npx playwright test

# Run a specific test file
npx playwright test e2e/sidebar.spec.ts

# Run with visible browser
npx playwright test --headed

# Run with UI mode
npx playwright test --ui
```

## Test Files & Covered Scenarios

### `app.spec.ts` — App Launch & Core Views

- Renders welcome screen with title and new connection button
- Sidebar shows Explorer header
- Statusbar is visible
- Connection dialog opens from welcome screen and sidebar
- Connection dialog has required form fields
- Test connection triggers success in mock mode
- Dialog closes on cancel
- Sidebar loads mock connections and grouped folders
- Double-click connects and expands databases
- Expands database → schema → tables
- Data grid opens with table name, column headers, row data, pagination
- Table structure shows Columns, Indexes, Foreign Keys tabs with badges
- Theme picker opens, lists themes, and selects a theme
- Keyboard shortcuts dialog opens via Ctrl+?

### `connection-dialog.spec.ts` — Connection Dialog

- Defaults to PostgreSQL with port 5432
- Switching DB type updates port (MySQL 3306, CockroachDB 26257, MSSQL 1433)
- SQLite shows file path and hides host/port/user/SSL
- Cassandra hides SSL toggles
- SSL toggle enables/disables trust server certificate
- Color picker selects and highlights active color
- Group input shows autocomplete, accepts custom text, fills on suggestion click
- Validation: empty name error, empty host error, duplicate name error
- Save creates connection and closes dialog
- Closes via X button

### `sidebar.spec.ts` — Sidebar

- Search filters tree nodes by name
- Clearing search shows all nodes again
- Type filter popover opens/closes with Tables, Views, Functions checkboxes
- Unchecking Tables hides table group
- Create folder via toolbar button
- Create folder cancel with Escape
- Collapse and expand group folder
- Rename folder via context menu
- Delete folder via context menu
- Connected connection shows green dot
- Disconnect via action button
- Connect via power button opens activity tab
- Edit connection opens dialog with pre-filled data
- Delete connection shows confirm dialog (cancel does not delete)
- Table context menu: Open Table, Query, View Structure, View DDL
- Database context menu: right-click shows menu, New Query opens console

### `tab-bar.spec.ts` — Tab Bar

- Opening a table creates a tab
- Clicking a tab activates it
- Close button removes the tab
- Closing all tabs shows welcome screen
- Tab shows connection color dot and icon
- Context menu shows Close, Close Others, Close All
- Close removes only the target tab
- Close Others keeps only the target tab
- Close All removes all tabs and shows welcome screen

### `data-grid.spec.ts` — Data Grid

- Toolbar shows schema.table name
- Filter button toggles filter bar
- Refresh button reloads data without error
- Auto-refresh dropdown shows interval options and active indicator
- Query button opens query console tab
- Columns button toggles column organizer
- Explain button shows explain panel
- Export button opens export dropdown
- Copy as SQL button exists and is clickable
- Add Row adds a pinned top row
- Test Data generates a row
- Ctrl+F opens search bar with match count
- Search with no match shows "No results"
- Search bar close via Escape and X button
- Search nav buttons navigate matches
- Pagination shows total row count and controls
- Right-click on row shows context menu
- View as JSON opens row detail panel
- Save and discard buttons appear after adding a row
- Discard removes pending changes
- Filter bar integration: open, add condition, AND/OR toggle, apply, clear, close, Enter applies

### `filter-bar.spec.ts` — Filter Bar

- Opens with one empty condition row
- Column dropdown shows all table columns
- Operator dropdown shows all operators
- Value input accepts text
- IS NULL / IS NOT NULL hides value input
- Add / remove condition buttons
- AND/OR toggle switches join type
- Apply button applies filter and shows active indicator
- Clear button resets filter and removes active indicator
- Close button hides filter bar
- Enter key in value input applies filter
- Column search in dropdown filters options

### `column-organizer.spec.ts` — Column Organizer

- Toggle button opens/closes the dropdown
- Lists all columns
- PK columns show locked badge and locked eye icon
- Unchecking a non-PK column hides it and updates button text
- Checking a hidden column shows it again
- Hide All hides all non-PK columns
- Show All shows all columns after Hide All
- Reset restores default order and visibility
- Non-PK items are draggable
- Hidden count updates toolbar button badge

### `row-detail.spec.ts` — Row Detail (JSON Viewer)

- Panel opens with header
- Panel shows all column values as JSON tree
- Shows key names in JSON format
- Filter input filters displayed fields
- Filter highlights matching text
- Copy JSON button copies to clipboard
- Close button closes panel
- PK values show lock icon and readonly title
- Non-PK values show editable title
- Double-click non-PK value opens inline editor
- Inline edit: Escape cancels
- Inline edit: Enter commits and shows Apply button
- Discard button removes pending edits
- Resize handle is present

### `query-console.spec.ts` — Query Console

- Query console visible with editor and toolbar
- Execute, Explain, Format, Save buttons present
- Hint shows Ctrl+Enter
- Saved, History, Suggest buttons present
- Empty results area shows placeholder
- Execute runs query and shows results with execution time
- Result table shows after SELECT execution
- Error shows in error strip for invalid SQL
- Explain view: shows node type, stats, Visual/Raw toggle, execution time
- Format button formats SQL
- Save dialog: open, Cancel/Save buttons, validation (empty/filled name), Enter/Escape keys
- Saved queries panel: opens, shows mock queries
- History panel: open/close, empty state, executing adds entry, click loads SQL, pin/copy buttons, execution meta
- Suggest toggle active state
- Result table: editable/readonly badge, chart toggle
- Chart toggle switches to chart view
- Split handle is present

### `chart-view.spec.ts` — Chart View

- Chart renders after executing a query
- Chart type buttons visible (Bar, Line, Pie, Scatter)
- Bar chart active by default
- Clicking Line/Pie/Scatter switches chart type
- Pie and Scatter hide Y-axis selector
- X-axis selector shows column options
- Y-axis multi-select shows for Bar/Line charts
- Selecting a Y column updates multi-select

### `ddl-viewer.spec.ts` — DDL Viewer

- Opens via context menu and shows DDL viewer
- Toolbar shows object name
- Shows DDL in Monaco editor
- Copy button is present and clickable
- DDL content is rendered in Monaco editor

### `table-info.spec.ts` — Table Info (Structure View)

- Opens via context menu and shows table info
- Toolbar shows table name
- Default tab is Columns
- Columns tab shows column rows with name, type, nullable, default
- PK badge shown on primary key column
- Indexes tab shows index list
- Foreign keys tab shows FK list or empty message
- Switching tabs updates active tab
- Tab labels show counts

### `table-stats.spec.ts` — Table Stats

- Opens via context menu and shows stats view
- Toolbar shows table name
- Shows stat cards: Row Count, Total Size, Last Vacuum, Last Analyze
- Shows storage breakdown chart
- Shows tuple health donut with Live/Dead legend

### `activity.spec.ts` — Activity Dashboard

- Opens and shows activity dashboard
- Shows sub-tabs: Overview, Sessions, Locks, Configuration
- Default sub-tab is Overview
- Live toggle visible and defaults to Live
- Clicking live toggle switches to Paused
- Pausing and resuming toggles back to Live
- Overview shows stat bar and charts
- Sessions tab shows session list with connection count
- Sessions search filters sessions
- Sessions search with no match shows empty state
- Locks tab shows empty state (mock returns no locks)
- Configuration tab shows empty state (mock returns no config)
- Live toggle is hidden on Configuration tab

### `erd.spec.ts` — Entity-Relationship Diagram

- Opens via context menu and shows ERD view
- Shows canvas with SVG
- Renders table boxes with names
- Table boxes show column names
- Filter input is present
- Filter input dims non-matching tables
- Zoom in / zoom out buttons work
- Fit button is present
- 1:1 reset button is present
- Edges toggle button is present
- Reload button is present
- Zoom percentage is displayed

### `role-manager.spec.ts` — Role Manager

- Opens via context menu and shows role manager
- Toolbar shows "Users & Roles" title
- Create Role button is present
- Shows role table with mock roles
- Role names are visible (postgres, app_user)
- Shows badges for role attributes
- Edit and Drop buttons present on role rows
- Create Role button opens form dialog with name input and checkboxes
- Create role form has Cancel and Create buttons
- Cancel closes create role dialog
- Edit button opens edit form dialog

### `query-stats.spec.ts` — Query Statistics

- Opens via context menu
- Shows unavailable state in mock mode
- Unavailable state shows setup card and setup steps
- Shows pg_stat_statements heading
- Enable Extension button is present
- Check button is present

### `create-table.spec.ts` — Create Table Dialog

- Opens via context menu and shows dialog
- Table name input is present
- Default column row is present
- Add column button adds a row
- Column row has name input, type select, nullable and PK checkboxes
- Preview SQL toggle shows/hides DDL
- Cancel button closes dialog
- Create Table button is present
- Remove column button removes a row

### `alter-table.spec.ts` — Alter Table Dialog

- Opens via context menu and shows dialog
- Loads existing columns
- Shows summary section with table path
- Shows existing columns badge
- Add column button adds a new row (marked as new)
- Preview SQL toggle shows/hides ALTER statements
- Cancel button closes dialog
- Apply button is present and disabled without changes
- PK badges shown on primary key columns
- Drop column button marks column as dropped

### `export.spec.ts` — Export Menu

- Export button is present in data grid toolbar
- Export button opens dropdown
- Dropdown shows CSV, JSON, SQL options
- Clicking outside closes dropdown

### `import.spec.ts` — Import Dialog

- Opens via context menu and shows dialog
- Shows target table name
- File input is present
- Cancel button closes dialog
- Import button is present

### `backup-restore.spec.ts` — Backup & Restore

- Opens via context menu and shows dialog
- Shows Backup and Restore tabs
- Backup tab is active by default
- Switching to Restore tab works
- Cancel button closes dialog
- Start Backup button is present
- Start Restore button visible on Restore tab

### `dump-restore.spec.ts` — Dump & Restore

- Opens via context menu and shows dialog
- Shows source database info
- Shows empty or target list
- Shows placeholder when no other connections available
- Cancel button closes dialog
- Next button is present

### `keyboard-zoom.spec.ts` — Keyboard Zoom

- Ctrl+= zooms in and shows indicator
- Ctrl+- zooms out and shows indicator
- Ctrl+0 resets zoom to 110%
- Zoom indicator disappears after timeout
- Clicking zoom indicator resets to 110%
- Zoom has a minimum of 50%

## Helpers

Shared navigation helpers are in `helpers.ts`:

| Helper | Description |
|---|---|
| `connectToLocalPostgres` | Connects to the mock Local Postgres connection |
| `navigateToTable` | Navigates the tree to a specific table node |
| `openTable` | Opens a table in the data grid |
| `openContextMenu` | Right-clicks a table to open context menu |
| `openStructureView` | Opens table structure via context menu |
| `openDDL` | Opens DDL viewer via context menu |
| `openQueryConsole` | Opens a new query console |
| `openConnectionDialog` | Opens the new connection dialog |
| `openTableStats` | Opens table stats via context menu |
| `openActivity` | Opens the activity dashboard |
| `openERD` | Opens the ERD view via schema context menu |
| `openRoleManager` | Opens role manager via connection context menu |
| `openQueryStats` | Opens query statistics via connection context menu |
| `openCreateTable` | Opens create table dialog via schema context menu |
| `openAlterTable` | Opens alter table dialog via table context menu |
| `openImportDialog` | Opens import dialog via table context menu |
| `openBackupRestore` | Opens backup/restore dialog via database context menu |
| `openDumpRestore` | Opens dump & restore dialog via database context menu |
| `fillConnectionForm` | Fills connection dialog form fields |
