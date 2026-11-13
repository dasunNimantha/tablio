import { test, expect } from "@playwright/test";
import { openCreateTable } from "./helpers";

test.describe("Create table dialog", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openCreateTable(page);
  });

  test("opens via context menu and shows dialog", async ({ page }) => {
    await expect(page.locator(".create-table-dialog")).toBeVisible();
    await expect(page.locator(".dialog-header h2")).toContainText("Create Table");
  });

  test("table name input is present", async ({ page }) => {
    const nameInput = page.locator(".create-table-dialog input").first();
    await expect(nameInput).toBeVisible();
  });

  test("default column row is present", async ({ page }) => {
    const rows = page.locator(".create-table-column-row");
    const count = await rows.count();
    expect(count).toBeGreaterThanOrEqual(1);
  });

  test("add column button adds a row", async ({ page }) => {
    const initialCount = await page.locator(".create-table-column-row").count();
    await page.locator(".add-column-btn").click();
    await expect(page.locator(".create-table-column-row")).toHaveCount(initialCount + 1);
  });

  test("column row has name input, type select, nullable and PK checkboxes", async ({ page }) => {
    const row = page.locator(".create-table-column-row").first();
    await expect(row.locator("input").first()).toBeVisible();
    await expect(row.locator("select")).toBeVisible();
  });

  test("preview SQL toggle shows DDL", async ({ page }) => {
    await page.locator(".btn-ghost", { hasText: "Preview SQL" }).click();
    await expect(page.locator(".ddl-preview")).toBeVisible();
  });

  test("preview SQL toggle hides DDL", async ({ page }) => {
    await page.locator(".btn-ghost", { hasText: "Preview SQL" }).click();
    await expect(page.locator(".ddl-preview")).toBeVisible();
    await page.locator(".btn-ghost", { hasText: "Hide SQL" }).click();
    await expect(page.locator(".ddl-preview")).not.toBeVisible();
  });

  test("cancel button closes dialog", async ({ page }) => {
    await page.locator(".dialog-footer .btn-secondary", { hasText: "Cancel" }).click();
    await expect(page.locator(".create-table-dialog")).not.toBeVisible();
  });

  test("create table button is present", async ({ page }) => {
    await expect(page.locator(".dialog-footer .btn-primary", { hasText: "Create Table" })).toBeVisible();
  });

  test("remove column button removes a row", async ({ page }) => {
    await page.locator(".add-column-btn").click();
    const countBefore = await page.locator(".create-table-column-row").count();
    const removeBtn = page.locator(".create-table-column-row").last().locator(".btn-icon").last();
    await removeBtn.dispatchEvent("click");
    await expect(page.locator(".create-table-column-row")).toHaveCount(countBefore - 1);
  });
});

test.describe("Create table dialog: keyboard and validation", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openCreateTable(page);
  });

  test("is a labelled modal dialog with focus in the table-name field", async ({ page }) => {
    const dialog = page.getByRole("dialog", { name: "Create Table" });
    await expect(dialog).toBeVisible();
    await expect(dialog).toHaveAttribute("aria-modal", "true");
    await expect(page.getByLabel("Table Name")).toBeFocused();
  });

  test("Escape closes the dialog", async ({ page }) => {
    await page.keyboard.press("Escape");
    await expect(page.locator(".create-table-dialog")).toHaveCount(0);
  });

  test("overlay click does not discard a dirty form", async ({ page }) => {
    await page.getByLabel("Table Name").fill("orders_draft");
    await page.locator(".dialog-overlay").click({ position: { x: 5, y: 5 } });
    await expect(page.locator(".create-table-dialog")).toBeVisible();
  });

  test("every column control has an accessible name", async ({ page }) => {
    await expect(page.getByLabel("Column 1 name")).toHaveValue("id");
    await expect(page.getByLabel("Type of id")).toBeVisible();
    await expect(page.getByLabel("id nullable")).toBeVisible();
    await expect(page.getByLabel("id primary key")).toBeChecked();
    await expect(page.getByRole("button", { name: "Remove column id" })).toBeVisible();
    await expect(page.locator(".create-table-dialog .dialog-header").getByRole("button", { name: "Close" })).toBeVisible();
  });

  test("duplicate column names show a warning and block Create", async ({ page }) => {
    await page.getByLabel("Table Name").fill("dupes");
    await page.getByLabel("Column 2 name").fill("ID");
    await expect(page.locator(".create-table-columns-error")).toContainText("Duplicate column name");
    await expect(page.getByRole("button", { name: "Create Table" })).toBeDisabled();
    await page.getByLabel("Column 2 name").fill("name");
    await expect(page.locator(".create-table-columns-error")).toHaveCount(0);
    await expect(page.getByRole("button", { name: "Create Table" })).toBeEnabled();
  });

  test("a row with settings but no name blocks Create", async ({ page }) => {
    await page.getByLabel("Table Name").fill("t");
    await page.getByLabel("column 2 default value").fill("0");
    await expect(page.locator(".create-table-columns-error")).toContainText("Column 2 has no name");
    await expect(page.getByRole("button", { name: "Create Table" })).toBeDisabled();
  });

  test("Postgres connection offers Postgres types and quoting", async ({ page }) => {
    await expect(page.getByLabel("Type of id")).toHaveValue("serial");
    await page.getByLabel("Table Name").fill("t");
    await page.locator(".btn-ghost", { hasText: "Preview SQL" }).click();
    await expect(page.locator(".ddl-preview")).toContainText('CREATE TABLE "public"."t"');
  });
});

/** Serve the mock with "Local Postgres" re-typed as another engine so the
 *  dialog's dialect handling can be exercised through the normal tree. */
async function asEngine(page: import("@playwright/test").Page, dbType: string) {
  await page.route("**/src/lib/mockData.ts*", async (route) => {
    const resp = await route.fetch();
    const body = (await resp.text()).replace(
      /(name: "Local Postgres",\s*db_type: )"postgres"/,
      `$1"${dbType}"`,
    );
    await route.fulfill({ response: resp, body });
  });
}

test.describe("Create table dialog: per-dialect types", () => {
  test("MSSQL defaults id to INT IDENTITY and previews bracket quoting", async ({ page }) => {
    await asEngine(page, "mssql");
    await page.goto("/");
    await openCreateTable(page);
    const idType = page.getByLabel("Type of id");
    await expect(idType).toHaveValue("INT IDENTITY(1,1)");
    const options = await idType.locator("option").allTextContents();
    expect(options).toContain("NVARCHAR(255)");
    expect(options).not.toContain("serial");
    await page.getByLabel("Table Name").fill("orders");
    await page.locator(".btn-ghost", { hasText: "Preview SQL" }).click();
    await expect(page.locator(".ddl-preview")).toContainText("CREATE TABLE [public].[orders]");
    await expect(page.locator(".ddl-preview")).toContainText("[id] INT IDENTITY(1,1) NOT NULL");
  });

  test("MySQL defaults id to INT AUTO_INCREMENT with backtick quoting", async ({ page }) => {
    await asEngine(page, "mysql");
    await page.goto("/");
    await openCreateTable(page);
    await expect(page.getByLabel("Type of id")).toHaveValue("INT AUTO_INCREMENT");
    await page.getByLabel("Table Name").fill("orders");
    await page.locator(".btn-ghost", { hasText: "Preview SQL" }).click();
    await expect(page.locator(".ddl-preview")).toContainText("`id` INT AUTO_INCREMENT NOT NULL");
  });
});
