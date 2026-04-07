import { test, expect } from "@playwright/test";
import { connectToLocalPostgres, navigateToTable } from "./helpers";

test.describe("Sidebar — search and type filter", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await connectToLocalPostgres(page);
    const db = page.locator(".tree-node .tree-label", { hasText: /^postgres$/ });
    await db.click();
    const schema = page.locator(".tree-node .tree-label", { hasText: /^public$/ });
    await schema.waitFor({ timeout: 8000 });
    await schema.click();
    const tables = page.locator(".tree-node .tree-label", { hasText: /^Tables$/ });
    await tables.waitFor({ timeout: 8000 });
    await tables.click();
    await page.locator(".tree-node.leaf .tree-label", { hasText: "users" }).waitFor({ timeout: 5000 });
  });

  test("search filters tree nodes by name", async ({ page }) => {
    const searchInput = page.locator(".tree-search input");
    await searchInput.fill("users");
    await expect(page.locator(".tree-node.leaf .tree-label", { hasText: "users" })).toBeVisible();
    await expect(page.locator(".tree-node.leaf .tree-label", { hasText: "orders" })).not.toBeVisible();
  });

  test("clearing search shows all nodes again", async ({ page }) => {
    const searchInput = page.locator(".tree-search input");
    await searchInput.fill("users");
    await expect(page.locator(".tree-node.leaf .tree-label", { hasText: "orders" })).not.toBeVisible();
    await searchInput.fill("");
    await expect(page.locator(".tree-node.leaf .tree-label", { hasText: "orders" })).toBeVisible();
  });

  test("type filter popover opens and closes", async ({ page }) => {
    await page.locator(".tree-filter-btn").click();
    await expect(page.locator(".tree-type-filter-dropdown")).toBeVisible();
    await page.locator(".tree-filter-btn").click();
    await expect(page.locator(".tree-type-filter-dropdown")).not.toBeVisible();
  });

  test("type filter shows Tables, Views, Functions checkboxes", async ({ page }) => {
    await page.locator(".tree-filter-btn").click();
    await expect(page.locator(".tree-type-filter-item", { hasText: "Tables" })).toBeVisible();
    await expect(page.locator(".tree-type-filter-item", { hasText: "Views" })).toBeVisible();
    await expect(page.locator(".tree-type-filter-item", { hasText: "Functions" })).toBeVisible();
  });

  test("unchecking Tables hides table group", async ({ page }) => {
    await page.locator(".tree-filter-btn").click();
    await page.locator(".tree-type-filter-item", { hasText: "Tables" }).locator("input").uncheck();
    await expect(page.locator(".tree-node.leaf .tree-label", { hasText: "users" })).not.toBeVisible();
    await expect(page.locator(".tree-filter-btn")).toHaveClass(/tree-filter-active/);
  });
});

test.describe("Sidebar — folders", () => {
  test("create folder via toolbar button", async ({ page }) => {
    await page.goto("/");
    await page.locator(".sidebar-header .btn-icon[title='New Folder']").click();
    const input = page.locator(".folder-name-input");
    await expect(input).toBeVisible();
    await input.fill("Test Folder");
    await input.press("Enter");
    await expect(page.locator(".connection-group-name", { hasText: "Test Folder" })).toBeVisible();
  });

  test("create folder cancel with Escape", async ({ page }) => {
    await page.goto("/");
    await page.locator(".sidebar-header .btn-icon[title='New Folder']").click();
    const input = page.locator(".folder-name-input");
    await input.fill("Abandoned");
    await input.press("Escape");
    await expect(input).not.toBeVisible();
    await expect(page.locator(".connection-group-name", { hasText: "Abandoned" })).not.toBeVisible();
  });

  test("collapse and expand group folder", async ({ page }) => {
    await page.goto("/");
    const group = page.locator(".connection-group-header", { hasText: "Production" });
    await expect(page.locator(".tree-label", { hasText: "Staging DB" })).toBeVisible({ timeout: 3000 });
    await group.click();
    await expect(page.locator(".tree-label", { hasText: "Staging DB" })).not.toBeVisible();
    await group.click();
    await expect(page.locator(".tree-label", { hasText: "Staging DB" })).toBeVisible();
  });

  test("rename folder via context menu", async ({ page }) => {
    await page.goto("/");
    const group = page.locator(".connection-group-header", { hasText: "Production" });
    await group.click({ button: "right" });
    await page.locator(".context-menu button", { hasText: "Rename Folder" }).click();
    const renameInput = page.locator(".connection-group-header .folder-name-input");
    await renameInput.fill("Staging");
    await renameInput.press("Enter");
    await expect(page.locator(".connection-group-name", { hasText: "Staging" })).toBeVisible();
  });

  test("delete folder via context menu", async ({ page }) => {
    await page.goto("/");
    await page.locator(".sidebar-header .btn-icon[title='New Folder']").click();
    const input = page.locator(".folder-name-input");
    await input.fill("Temp Folder");
    await input.press("Enter");
    await expect(page.locator(".connection-group-name", { hasText: "Temp Folder" })).toBeVisible();

    const group = page.locator(".connection-group-header", { hasText: "Temp Folder" });
    await group.click({ button: "right" });
    await page.locator(".context-menu button", { hasText: "Delete Folder" }).click();
    await expect(page.locator(".connection-group-name", { hasText: "Temp Folder" })).not.toBeVisible();
  });
});

test.describe("Sidebar — connection actions", () => {
  test("connected connection shows green dot", async ({ page }) => {
    await page.goto("/");
    await connectToLocalPostgres(page);
    const dot = page.locator(".tree-root")
      .filter({ hasText: "Local Postgres" })
      .locator(".connection-dot");
    await expect(dot).toHaveCSS("background-color", "var(--success)").catch(async () => {
      const bg = await dot.evaluate((el) => getComputedStyle(el).backgroundColor);
      expect(bg).not.toBe("rgb(108, 115, 127)");
    });
  });

  test("disconnect via action button", async ({ page }) => {
    await page.goto("/");
    await connectToLocalPostgres(page);
    const disconnectBtn = page.locator(".tree-root")
      .filter({ hasText: "Local Postgres" })
      .locator("button[title='Disconnect']");
    await disconnectBtn.waitFor({ timeout: 5000 });
    await disconnectBtn.dispatchEvent("click");
    await expect(page.locator(".tree-node .tree-label", { hasText: /^postgres$/ })).not.toBeVisible({ timeout: 5000 });
  });

  test("connect via power button opens activity tab", async ({ page }) => {
    await page.goto("/");
    const connectBtn = page.locator(".tree-root")
      .filter({ hasText: "Local Postgres" })
      .locator("button[title='Connect']");
    await connectBtn.waitFor({ timeout: 5000 });
    await connectBtn.dispatchEvent("click");
    await expect(page.locator(".tab-bar .tab", { hasText: "Activity" })).toBeVisible({ timeout: 8000 });
  });

  test("edit connection opens dialog with pre-filled data", async ({ page }) => {
    await page.goto("/");
    const connRoot = page.locator(".tree-root").filter({ hasText: "Local Postgres" });
    const connRow = connRoot.locator(".tree-conn-row").first();
    await connRow.hover();
    await connRoot.locator(".btn-icon[title='Edit']").click({ force: true });
    const dialog = page.locator(".dialog");
    await expect(dialog.locator("h2")).toContainText("Edit Connection");
    const nameInput = dialog.locator("input").first();
    await expect(nameInput).toHaveValue("Local Postgres");
  });

  test("delete connection shows confirm dialog", async ({ page }) => {
    await page.goto("/");
    const connRoot = page.locator(".tree-root").filter({ hasText: "Local Postgres" });
    const connRow = connRoot.locator(".tree-conn-row").first();
    await connRow.hover();
    await connRoot.locator(".btn-icon[title='Delete']").click({ force: true });
    await expect(page.locator(".confirm-dialog")).toBeVisible();
    await expect(page.locator(".confirm-dialog")).toContainText("Local Postgres");
  });

  test("confirm dialog cancel does not delete", async ({ page }) => {
    await page.goto("/");
    const connRoot = page.locator(".tree-root").filter({ hasText: "Local Postgres" });
    const connRow = connRoot.locator(".tree-conn-row").first();
    await connRow.hover();
    await connRoot.locator(".btn-icon[title='Delete']").click({ force: true });
    await page.locator(".confirm-dialog button", { hasText: "Cancel" }).click();
    await expect(page.locator(".tree-label", { hasText: "Local Postgres" })).toBeVisible();
  });
});

test.describe("Sidebar — table context menu", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
  });

  test("right-click table shows context menu with expected items", async ({ page }) => {
    const tableNode = await navigateToTable(page, "users");
    await tableNode.click({ button: "right" });
    const menu = page.locator(".context-menu");
    await expect(menu).toBeVisible();
    await expect(menu.locator("button", { hasText: "Open Table" })).toBeVisible();
    await expect(menu.locator("button", { hasText: /^Query$/ })).toBeVisible();
    await expect(menu.locator("button", { hasText: "View Structure" })).toBeVisible();
    await expect(menu.locator("button", { hasText: "View DDL" })).toBeVisible();
  });

  test("Open Table from context menu opens data grid", async ({ page }) => {
    const tableNode = await navigateToTable(page, "users");
    await tableNode.click({ button: "right" });
    await page.locator(".context-menu button", { hasText: "Open Table" }).click();
    await expect(page.locator(".grid-table-name")).toContainText("users", { timeout: 5000 });
  });

  test("Query from context menu opens query console", async ({ page }) => {
    const tableNode = await navigateToTable(page, "users");
    await tableNode.click({ button: "right" });
    await page.locator(".context-menu button", { hasText: /^Query$/ }).click();
    await expect(page.locator(".query-console")).toBeVisible({ timeout: 5000 });
  });

  test("View Structure from context menu opens table info", async ({ page }) => {
    const tableNode = await navigateToTable(page, "orders");
    await tableNode.click({ button: "right" });
    await page.locator(".context-menu button", { hasText: "View Structure" }).click();
    await expect(page.locator(".table-info")).toBeVisible({ timeout: 5000 });
  });

  test("View DDL from context menu opens DDL viewer", async ({ page }) => {
    const tableNode = await navigateToTable(page, "users");
    await tableNode.click({ button: "right" });
    await page.locator(".context-menu button", { hasText: "View DDL" }).click();
    await expect(page.locator(".ddl-viewer")).toBeVisible({ timeout: 5000 });
  });
});

test.describe("Sidebar — database context menu", () => {
  test("right-click database shows context menu", async ({ page }) => {
    await page.goto("/");
    await connectToLocalPostgres(page);
    const db = page.locator(".tree-node .tree-label", { hasText: /^postgres$/ });
    await db.click({ button: "right" });
    const menu = page.locator(".context-menu");
    await expect(menu).toBeVisible();
    await expect(menu.locator("button", { hasText: /New Query/ })).toBeVisible();
  });

  test("New Query from database context menu opens query console", async ({ page }) => {
    await page.goto("/");
    await connectToLocalPostgres(page);
    const db = page.locator(".tree-node .tree-label", { hasText: /^postgres$/ });
    await db.click({ button: "right" });
    await page.locator(".context-menu button", { hasText: /New Query/ }).click();
    await expect(page.locator(".query-console")).toBeVisible({ timeout: 5000 });
  });
});
