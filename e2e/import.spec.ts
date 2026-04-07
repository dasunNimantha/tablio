import { test, expect } from "@playwright/test";
import { openImportDialog } from "./helpers";

test.describe("Import dialog", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openImportDialog(page, "users");
  });

  test("opens via context menu and shows dialog", async ({ page }) => {
    await expect(page.locator(".import-dialog")).toBeVisible();
    await expect(page.locator(".dialog-header h2")).toContainText("Import CSV");
  });

  test("shows target table name", async ({ page }) => {
    await expect(page.locator(".import-target-input")).toBeVisible();
    await expect(page.locator(".import-target-input")).toHaveValue(/users/);
  });

  test("file input is present", async ({ page }) => {
    await expect(page.locator(".import-file-label")).toBeVisible();
    await expect(page.locator(".import-file-label")).toContainText("Choose a CSV file");
  });

  test("cancel button closes dialog", async ({ page }) => {
    await page.locator(".dialog-footer .btn-secondary", { hasText: "Cancel" }).click();
    await expect(page.locator(".import-dialog")).not.toBeVisible();
  });

  test("import button is present", async ({ page }) => {
    await expect(page.locator(".dialog-footer .btn-primary", { hasText: "Import" })).toBeVisible();
  });
});
