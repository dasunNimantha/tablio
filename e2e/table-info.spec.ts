import { test, expect } from "@playwright/test";
import { openStructureView } from "./helpers";

test.describe("Table info — structure view", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openStructureView(page, "users");
  });

  test("opens via context menu and shows table info", async ({ page }) => {
    await expect(page.locator(".table-info")).toBeVisible();
    await expect(page.locator(".table-info-toolbar")).toBeVisible();
  });

  test("toolbar shows table name", async ({ page }) => {
    await expect(page.locator(".table-info-name")).toContainText("users");
  });

  test("default tab is Columns", async ({ page }) => {
    const columnsTab = page.locator(".table-info-tab", { hasText: /Columns/ });
    await expect(columnsTab).toHaveClass(/active/);
  });

  test("columns tab shows column rows with name, type, nullable, default", async ({ page }) => {
    const table = page.locator(".table-info-content .info-table");
    await expect(table).toBeVisible();
    await expect(table.locator("th", { hasText: "Name" })).toBeVisible();
    await expect(table.locator("th", { hasText: "Type" })).toBeVisible();
    await expect(table.locator("th", { hasText: "Nullable" })).toBeVisible();
    await expect(table.locator("th", { hasText: "Default" })).toBeVisible();
    const rows = table.locator("tbody tr");
    const count = await rows.count();
    expect(count).toBeGreaterThanOrEqual(2);
  });

  test("PK badge shown on primary key column", async ({ page }) => {
    await expect(page.locator(".pk-badge").first()).toBeVisible();
    await expect(page.locator(".pk-badge").first()).toHaveText("PK");
  });

  test("indexes tab shows index list", async ({ page }) => {
    await page.locator(".table-info-tab", { hasText: /Indexes/ }).click();
    const table = page.locator(".table-info-content .info-table");
    await expect(table).toBeVisible();
    await expect(table.locator("th", { hasText: "Name" })).toBeVisible();
    await expect(table.locator("th", { hasText: "Columns" })).toBeVisible();
    await expect(table.locator("th", { hasText: "Unique" })).toBeVisible();
  });

  test("foreign keys tab shows FK list or empty message", async ({ page }) => {
    await page.locator(".table-info-tab", { hasText: /Foreign Keys/ }).click();
    const content = page.locator(".table-info-content");
    const hasTable = await content.locator(".info-table").isVisible();
    if (hasTable) {
      await expect(content.locator("th", { hasText: "Name" })).toBeVisible();
      await expect(content.locator("th", { hasText: "References" })).toBeVisible();
    } else {
      await expect(content.locator(".info-empty")).toContainText("No foreign keys");
    }
  });

  test("switching tabs updates active tab", async ({ page }) => {
    await page.locator(".table-info-tab", { hasText: /Indexes/ }).click();
    await expect(page.locator(".table-info-tab", { hasText: /Indexes/ })).toHaveClass(/active/);
    await expect(page.locator(".table-info-tab", { hasText: /Columns/ })).not.toHaveClass(/active/);
  });

  test("tab labels show counts", async ({ page }) => {
    const columnsTab = page.locator(".table-info-tab", { hasText: /Columns/ });
    const text = await columnsTab.textContent();
    expect(text).toMatch(/Columns\s*\(\d+\)/);
  });
});
