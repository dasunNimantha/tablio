import { test, expect } from "@playwright/test";
import { openDumpRestore } from "./helpers";

test.describe("Dump & Restore dialog", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openDumpRestore(page);
  });

  test("opens via context menu and shows dialog", async ({ page }) => {
    await expect(page.locator(".dr-dialog")).toBeVisible();
    await expect(page.locator(".dialog-header h2")).toContainText("Dump");
    await expect(page.locator(".dialog-header h2")).toContainText("Restore");
  });

  test("shows source database info", async ({ page }) => {
    await expect(page.locator(".dr-readonly-field")).toBeVisible();
  });

  test("shows empty or target list", async ({ page }) => {
    const placeholder = page.locator(".dr-placeholder");
    const list = page.locator(".dr-target-list");
    const hasPlaceholder = await placeholder.isVisible().catch(() => false);
    const hasList = await list.isVisible().catch(() => false);
    expect(hasPlaceholder || hasList).toBeTruthy();
  });

  test("shows placeholder when no other connections", async ({ page }) => {
    const placeholder = page.locator(".dr-placeholder");
    const count = await placeholder.count();
    if (count > 0) {
      await expect(placeholder).toContainText("No other connected databases available");
    }
  });

  test("cancel button closes dialog", async ({ page }) => {
    await page.locator(".dialog-footer .btn-secondary", { hasText: "Cancel" }).click();
    await expect(page.locator(".dr-dialog")).not.toBeVisible();
  });

  test("Next button is present", async ({ page }) => {
    const nextBtn = page.locator(".dialog-footer .btn-primary", { hasText: "Next" });
    await expect(nextBtn).toBeVisible();
  });
});
