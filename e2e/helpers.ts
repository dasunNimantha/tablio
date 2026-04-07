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

  const tableNode = page.locator(".tree-node.leaf .tree-label", { hasText: tableName });
  await tableNode.waitFor({ timeout: 8000 });
  return tableNode;
}

export async function openTable(page: Page, tableName: string) {
  const tableNode = await navigateToTable(page, tableName);
  await tableNode.click();
  await page.locator(".grid-table-name").waitFor({ timeout: 8000 });
}

export async function openContextMenu(page: Page, tableName: string) {
  const tableNode = await navigateToTable(page, tableName);
  await tableNode.click({ button: "right" });
  await page.locator(".context-menu").waitFor({ timeout: 3000 });
}

export async function openStructureView(page: Page, tableName: string) {
  await openContextMenu(page, tableName);
  await page.locator(".context-menu button", { hasText: "View Structure" }).click();
  await page.locator(".table-info").waitFor({ timeout: 5000 });
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

export async function openTableStats(page: Page, tableName: string) {
  await openContextMenu(page, tableName);
  await page.locator(".context-menu button", { hasText: "View Stats" }).click();
  await page.locator(".table-stats").waitFor({ timeout: 8000 });
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
