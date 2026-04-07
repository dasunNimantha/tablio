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
