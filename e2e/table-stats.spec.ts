import { test, expect } from "@playwright/test";
import { openTableStats } from "./helpers";

test.describe("Table stats", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openTableStats(page, "users");
  });

  test("opens via context menu and shows stats view", async ({ page }) => {
    await expect(page.locator(".table-stats")).toBeVisible();
    await expect(page.locator(".table-stats-toolbar")).toBeVisible();
  });

  test("toolbar shows table name", async ({ page }) => {
    await expect(page.locator(".table-stats-name")).toContainText("users");
  });

  test("shows stat cards with row count, total size, vacuum, analyze", async ({ page }) => {
    const cards = page.locator(".table-stats-card");
    const count = await cards.count();
    expect(count).toBeGreaterThanOrEqual(4);
    await expect(page.locator(".table-stats-label", { hasText: "Row Count" })).toBeVisible();
    await expect(page.locator(".table-stats-label", { hasText: "Total Size" })).toBeVisible();
    await expect(page.locator(".table-stats-label", { hasText: "Last Vacuum" })).toBeVisible();
    await expect(page.locator(".table-stats-label", { hasText: "Last Analyze" })).toBeVisible();
  });

  test("shows storage breakdown chart", async ({ page }) => {
    await expect(page.locator(".stats-chart-panel", { hasText: "Storage Breakdown" })).toBeVisible();
    const bars = page.locator(".stats-bar-row");
    const count = await bars.count();
    expect(count).toBeGreaterThanOrEqual(1);
  });

  test("shows tuple health donut", async ({ page }) => {
    await expect(page.locator(".stats-chart-panel", { hasText: "Tuple Health" })).toBeVisible();
    await expect(page.locator(".stats-donut")).toBeVisible();
    await expect(page.locator(".stats-legend-item", { hasText: "Live" })).toBeVisible();
    await expect(page.locator(".stats-legend-item", { hasText: "Dead" })).toBeVisible();
  });
});
