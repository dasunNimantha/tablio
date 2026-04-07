import { test, expect } from "@playwright/test";
import { openAlterTable } from "./helpers";

test.describe("Alter table dialog", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openAlterTable(page, "users");
  });

  test("opens via context menu and shows dialog", async ({ page }) => {
    await expect(page.locator(".alter-table-dialog")).toBeVisible();
    await expect(page.locator(".dialog-header h2")).toContainText("Alter Table");
  });

  test("loads existing columns", async ({ page }) => {
    const rows = page.locator(".alter-table-column-row");
    await rows.first().waitFor({ timeout: 5000 });
    const count = await rows.count();
    expect(count).toBeGreaterThanOrEqual(2);
  });

  test("shows summary section", async ({ page }) => {
    await expect(page.locator(".alter-table-summary")).toBeVisible();
    await expect(page.locator(".alter-table-summary")).toContainText("users");
  });

  test("shows existing columns badge", async ({ page }) => {
    await expect(page.locator(".alter-table-badge").first()).toBeVisible();
    await expect(page.locator(".alter-table-badge").first()).toContainText("existing column");
  });

  test("add column button adds a new row", async ({ page }) => {
    await page.locator(".alter-table-column-row").first().waitFor({ timeout: 5000 });
    const initialCount = await page.locator(".alter-table-column-row").count();
    await page.locator(".alter-table-add-btn").click();
    await expect(page.locator(".alter-table-column-row")).toHaveCount(initialCount + 1);
    await expect(page.locator(".alter-table-column-row.new-column")).toBeVisible();
  });

  test("preview SQL toggle shows ALTER statements", async ({ page }) => {
    await page.locator(".alter-table-add-btn").click();
    await page.locator(".btn-ghost", { hasText: "Preview SQL" }).click();
    await expect(page.locator(".ddl-preview")).toBeVisible();
  });

  test("preview SQL toggle hides ALTER statements", async ({ page }) => {
    await page.locator(".alter-table-add-btn").click();
    await page.locator(".btn-ghost", { hasText: "Preview SQL" }).click();
    await expect(page.locator(".ddl-preview")).toBeVisible();
    await page.locator(".btn-ghost", { hasText: "Hide SQL" }).click();
    await expect(page.locator(".ddl-preview")).not.toBeVisible();
  });

  test("cancel button closes dialog", async ({ page }) => {
    await page.locator(".dialog-footer .btn-secondary", { hasText: "Cancel" }).click();
    await expect(page.locator(".alter-table-dialog")).not.toBeVisible();
  });

  test("apply button is present and disabled without changes", async ({ page }) => {
    const applyBtn = page.locator(".dialog-footer .btn-primary", { hasText: "Apply Changes" });
    await expect(applyBtn).toBeVisible();
    await expect(applyBtn).toBeDisabled();
  });

  test("PK badges shown on primary key columns", async ({ page }) => {
    await page.locator(".alter-table-column-row").first().waitFor({ timeout: 5000 });
    await expect(page.locator(".alter-table-pk-badge").first()).toBeVisible();
  });

  test("drop column button marks column as dropped", async ({ page }) => {
    await page.locator(".alter-table-column-row").first().waitFor({ timeout: 5000 });
    const dropBtn = page.locator(".drop-column-btn").first();
    await dropBtn.click();
    await expect(page.locator(".alter-table-column-row.dropped").first()).toBeVisible();
  });
});
