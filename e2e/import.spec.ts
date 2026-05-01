import { test, expect } from "@playwright/test";
import { openImportDialog } from "./helpers";

test.describe("Import dialog", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openImportDialog(page, "users");
  });

  test("opens via context menu and shows dialog", async ({ page }) => {
    await expect(page.locator(".import-dialog")).toBeVisible();
    await expect(page.locator(".dialog-header h2")).toContainText("Import Data");
  });

  test("shows target table name", async ({ page }) => {
    await expect(page.locator(".import-target-input")).toBeVisible();
    await expect(page.locator(".import-target-input")).toHaveValue(/users/);
  });

  test("file input is present and accepts CSV + Excel", async ({ page }) => {
    await expect(page.locator(".import-file-label")).toBeVisible();
    await expect(page.locator(".import-file-label")).toContainText(
      /\.csv.*\.xlsx.*\.xls/,
    );
    const accept = await page
      .locator('#csv-file-input')
      .getAttribute("accept");
    expect(accept).toMatch(/\.csv/);
    expect(accept).toMatch(/\.xlsx/);
    expect(accept).toMatch(/\.xls(?!x)/);
  });

  test("cancel button closes dialog", async ({ page }) => {
    await page.locator(".dialog-footer .btn-secondary", { hasText: "Cancel" }).click();
    await expect(page.locator(".import-dialog")).not.toBeVisible();
  });

  test("import button is present", async ({ page }) => {
    await expect(page.locator(".dialog-footer .btn-primary", { hasText: "Import" })).toBeVisible();
  });
});
