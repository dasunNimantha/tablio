import { test, expect } from "@playwright/test";
import { openBackupRestore } from "./helpers";

test.describe("Backup & Restore dialog", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openBackupRestore(page);
  });

  test("opens via context menu and shows dialog", async ({ page }) => {
    await expect(page.locator(".br-dialog")).toBeVisible();
    await expect(page.locator(".dialog-header h2")).toContainText("Backup");
    await expect(page.locator(".dialog-header h2")).toContainText("Restore");
  });

  test("shows Backup and Restore tabs", async ({ page }) => {
    await expect(page.locator(".br-tab", { hasText: "Backup" })).toBeVisible();
    await expect(page.locator(".br-tab", { hasText: "Restore" })).toBeVisible();
  });

  test("Backup tab is active by default", async ({ page }) => {
    await expect(page.locator(".br-tab--active", { hasText: "Backup" })).toBeVisible();
  });

  test("switching to Restore tab", async ({ page }) => {
    await page.locator(".br-tab", { hasText: "Restore" }).click();
    await expect(page.locator(".br-tab--active", { hasText: "Restore" })).toBeVisible();
  });

  test("cancel button closes dialog", async ({ page }) => {
    await page.locator(".dialog-footer .btn-secondary", { hasText: "Cancel" }).click();
    await expect(page.locator(".br-dialog")).not.toBeVisible();
  });

  test("Start Backup button is present", async ({ page }) => {
    await expect(page.locator(".btn-primary", { hasText: "Start Backup" })).toBeVisible();
  });

  test("Start Restore button visible on Restore tab", async ({ page }) => {
    await page.locator(".br-tab", { hasText: "Restore" }).click();
    await expect(page.locator(".btn-primary", { hasText: "Start Restore" })).toBeVisible();
  });
});
