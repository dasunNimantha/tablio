import { Page } from "@playwright/test";

export async function connectToLocalPostgres(page: Page) {
  const conn = page.locator(".tree-label", { hasText: "Local Postgres" });
  await conn.waitFor({ timeout: 5000 });
  await conn.dblclick();
  await page.locator(".tree-node .tree-label", { hasText: /^postgres$/ }).waitFor({ timeout: 8000 });
}

export async function navigateToTable(page: Page, tableName: string) {
  await connectToLocalPostgres(page);

  const db = page.locator(".tree-node .tree-label", { hasText: /^postgres$/ });
  await db.click();

  const schema = page.locator(".tree-node .tree-label", { hasText: /^public$/ });
  await schema.waitFor({ timeout: 8000 });
  await schema.click();

  const tables = page.locator(".tree-node .tree-label", { hasText: /^Tables$/ });
  await tables.waitFor({ timeout: 8000 });
  await tables.click();

  // Match both leaf tables AND partitioned parents (which render as
  // non-leaf branches because they have child partitions to expand
  // into). The `.icon-table` icon is the discriminator that lets us
  // exclude folder/group nodes that share the same tree-label text.
  const tableNode = page
    .locator(".tree-node:has(.icon-table) .tree-label", { hasText: tableName })
    .first();
  await tableNode.waitFor({ timeout: 8000 });
  return tableNode;
}

export async function openTable(page: Page, tableName: string) {
  const tableNode = await navigateToTable(page, tableName);
  // Open via context menu so this works uniformly for leaf tables
  // (single-click opens) and partitioned parents (single-click only
  // toggles expand; needs an explicit "Open Table" action).
  await tableNode.click({ button: "right" });
  await page.locator(".context-menu").waitFor({ timeout: 3000 });
  await page.locator(".context-menu button", { hasText: "Open Table" }).click();
  await page.locator(".tv-name", { hasText: tableName }).waitFor({ timeout: 10000 });
}

export async function openContextMenu(page: Page, tableName: string) {
  const tableNode = await navigateToTable(page, tableName);
  await tableNode.click({ button: "right" });
  await page.locator(".context-menu").waitFor({ timeout: 3000 });
}

/** Opens the unified Schema view via the "View Schema" context-menu
 *  item (formerly "View Structure" + "View Stats"). The new TableView
 *  exposes Data and Schema modes; this helper lands on Schema mode and
 *  waits for its sub-tab strip to render. */
export async function openStructureView(page: Page, tableName: string) {
  await openContextMenu(page, tableName);
  await page.locator(".context-menu button", { hasText: "View Schema" }).click();
  await page.locator(".tv-schema-strip").waitFor({ timeout: 8000 });
}

export async function openDDL(page: Page, tableName: string) {
  await openContextMenu(page, tableName);
  await page.locator(".context-menu button", { hasText: "View DDL" }).click();
}

export async function openQueryConsole(page: Page) {
  await connectToLocalPostgres(page);
  const db = page.locator(".tree-node .tree-label", { hasText: /^postgres$/ });
  await db.click({ button: "right" });
  await page.locator(".context-menu").waitFor({ timeout: 3000 });
  await page.locator(".context-menu button", { hasText: /^New Query$/ }).click();
}

export async function openConnectionDialog(page: Page) {
  await page.locator(".sidebar-header .btn-icon[title='New Connection']").click();
  await page.locator(".dialog").waitFor({ timeout: 3000 });
}

/** Opens the Schema view and switches to the Statistics sub-tab. The
 *  former "View Stats" menu item was folded into the unified Schema
 *  mode; the Statistics panel still renders the same `.table-stats`
 *  layout, just nested inside the schema tab strip. */
export async function openTableStats(page: Page, tableName: string) {
  await openStructureView(page, tableName);
  await page.locator(".tv-schema-tab", { hasText: "Statistics" }).click();
  await page.locator(".table-stats").waitFor({ timeout: 8000 });
}

/** Opens the Schema view of a partitioned table and switches to the
 *  Partitions sub-tab. Partitioned parents are rendered as non-leaf
 *  branches in the sidebar (so the user can drill into their leaf
 *  partitions), which means the standard `navigateToTable` selector
 *  (which requires `.leaf`) does not match. We walk the tree manually
 *  here and right-click the parent to reach "View Schema". */
export async function openPartitionsView(page: Page, tableName: string) {
  await connectToLocalPostgres(page);

  const db = page.locator(".tree-node .tree-label", { hasText: /^postgres$/ });
  await db.click();
  const schema = page.locator(".tree-node .tree-label", { hasText: /^public$/ });
  await schema.waitFor({ timeout: 8000 });
  await schema.click();
  const tables = page.locator(".tree-node .tree-label", { hasText: /^Tables$/ });
  await tables.waitFor({ timeout: 8000 });
  await tables.click();

  // Match the partitioned parent specifically — same `.tree-label`
  // text as a leaf, but the row is NOT marked `.leaf` because it has
  // child partitions to expand into.
  const parent = page
    .locator(".tree-node:not(.leaf) .tree-label", { hasText: tableName })
    .first();
  await parent.waitFor({ timeout: 8000 });
  await parent.click({ button: "right" });
  await page.locator(".context-menu").waitFor({ timeout: 3000 });
  await page.locator(".context-menu button", { hasText: "View Schema" }).click();
  await page.locator(".tv-schema-strip").waitFor({ timeout: 8000 });
  await page.locator(".tv-schema-tab", { hasText: "Partitions" }).click();
  await page.locator(".pv-table").waitFor({ timeout: 8000 });
}

export async function openActivity(page: Page) {
  await connectToLocalPostgres(page);
  await page.locator(".activity-dashboard").first().waitFor({ timeout: 8000 });
}

export async function openERD(page: Page) {
  await connectToLocalPostgres(page);
  const db = page.locator(".tree-node .tree-label", { hasText: /^postgres$/ });
  await db.click();
  const schema = page.locator(".tree-node .tree-label", { hasText: /^public$/ });
  await schema.waitFor({ timeout: 8000 });
  await schema.click({ button: "right" });
  await page.locator(".context-menu").waitFor({ timeout: 3000 });
  await page.locator(".context-menu button", { hasText: "View ERD" }).click();
  await page.locator(".erd-view").waitFor({ timeout: 8000 });
}

export async function openRoleManager(page: Page) {
  await connectToLocalPostgres(page);
  const conn = page.locator(".tree-label", { hasText: "Local Postgres" });
  await conn.click({ button: "right" });
  await page.locator(".context-menu").waitFor({ timeout: 3000 });
  await page.locator(".context-menu button", { hasText: "Manage Roles" }).click();
  await page.locator(".role-manager").waitFor({ timeout: 8000 });
}

export async function openQueryStats(page: Page) {
  await connectToLocalPostgres(page);
  const conn = page.locator(".tree-label", { hasText: "Local Postgres" });
  await conn.click({ button: "right" });
  await page.locator(".context-menu").waitFor({ timeout: 3000 });
  await page.locator(".context-menu button", { hasText: "Query Statistics" }).click();
}

export async function openCreateTable(page: Page) {
  await connectToLocalPostgres(page);
  const db = page.locator(".tree-node .tree-label", { hasText: /^postgres$/ });
  await db.click();
  const schema = page.locator(".tree-node .tree-label", { hasText: /^public$/ });
  await schema.waitFor({ timeout: 8000 });
  await schema.click({ button: "right" });
  await page.locator(".context-menu").waitFor({ timeout: 3000 });
  await page.locator(".context-menu button", { hasText: "Create Table" }).click();
  await page.locator(".create-table-dialog").waitFor({ timeout: 5000 });
}

export async function openAlterTable(page: Page, tableName: string) {
  await openContextMenu(page, tableName);
  await page.locator(".context-menu button", { hasText: "Alter Table" }).click();
  await page.locator(".alter-table-dialog").waitFor({ timeout: 8000 });
}

export async function openImportDialog(page: Page, tableName: string) {
  await openContextMenu(page, tableName);
  await page.locator(".context-menu button", { hasText: "Import Data" }).click();
  await page.locator(".import-dialog").waitFor({ timeout: 5000 });
}

export async function openBackupRestore(page: Page) {
  await connectToLocalPostgres(page);
  const db = page.locator(".tree-node .tree-label", { hasText: /^postgres$/ });
  await db.click({ button: "right" });
  await page.locator(".context-menu").waitFor({ timeout: 3000 });
  await page.locator(".context-menu button", { hasText: "Backup / Restore" }).click();
  await page.locator(".br-dialog").waitFor({ timeout: 5000 });
}

export async function openDumpRestore(page: Page) {
  await connectToLocalPostgres(page);
  const db = page.locator(".tree-node .tree-label", { hasText: /^postgres$/ });
  await db.click({ button: "right" });
  await page.locator(".context-menu").waitFor({ timeout: 3000 });
  await page.locator(".context-menu button", { hasText: /Dump.*Restore/ }).click();
  await page.locator(".dr-dialog").waitFor({ timeout: 5000 });
}

export async function fillConnectionForm(
  page: Page,
  fields: { name?: string; host?: string; port?: string; user?: string; password?: string; database?: string }
) {
  const dialog = page.locator(".dialog");
  if (fields.name !== undefined) {
    const nameInput = dialog.locator("label", { hasText: "Connection Name" }).locator("..").locator("input");
    await nameInput.fill(fields.name);
  }
  if (fields.host !== undefined) {
    const hostInput = dialog.locator("label", { hasText: "Host" }).locator("..").locator("input");
    await hostInput.fill(fields.host);
  }
  if (fields.port !== undefined) {
    const portInput = dialog.locator("label", { hasText: "Port" }).locator("..").locator("input");
    await portInput.fill(fields.port);
  }
  if (fields.user !== undefined) {
    const userInput = dialog.locator("label", { hasText: "Username" }).locator("..").locator("input");
    await userInput.fill(fields.user);
  }
  if (fields.password !== undefined) {
    const pwInput = dialog.locator("label", { hasText: "Password" }).locator("..").locator("input");
    await pwInput.fill(fields.password);
  }
  if (fields.database !== undefined) {
    const dbInput = dialog.locator("label", { hasText: /Database|Keyspace/ }).locator("..").locator("input");
    await dbInput.fill(fields.database);
  }
}
