import { test, expect } from "@playwright/test";
import { openTable } from "./helpers";

test.describe("Export menu", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openTable(page, "users");
  });

  test("export button is present in data grid toolbar", async ({ page }) => {
    const exportBtn = page.locator(".export-menu-wrapper .btn-ghost");
    await expect(exportBtn).toBeVisible();
  });

  test("export button opens dropdown", async ({ page }) => {
    await page.locator(".export-menu-wrapper .btn-ghost").click();
    await expect(page.locator(".export-menu-dropdown")).toBeVisible();
  });

  test("dropdown shows CSV, JSON, SQL options", async ({ page }) => {
    await page.locator(".export-menu-wrapper .btn-ghost").click();
    const dropdown = page.locator(".export-menu-dropdown");
    await expect(dropdown.locator("button", { hasText: "Export as CSV" })).toBeVisible();
    await expect(dropdown.locator("button", { hasText: "Export as JSON" })).toBeVisible();
    await expect(dropdown.locator("button", { hasText: "Export as SQL" })).toBeVisible();
  });

  test("clicking outside closes dropdown", async ({ page }) => {
    await page.locator(".export-menu-wrapper .btn-ghost").click();
    await expect(page.locator(".export-menu-dropdown")).toBeVisible();
    await page.locator(".grid-table-name").click();
    await expect(page.locator(".export-menu-dropdown")).not.toBeVisible();
  });
});
