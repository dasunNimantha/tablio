import { test, expect } from "@playwright/test";
import { openERD } from "./helpers";

test.describe("ERD view", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openERD(page);
    await page.locator(".erd-table-name").first().waitFor({ timeout: 8000 });
  });

  test("opens via context menu and shows ERD view", async ({ page }) => {
    await expect(page.locator(".erd-view")).toBeVisible();
    await expect(page.locator(".erd-toolbar")).toBeVisible();
  });

  test("shows canvas with SVG", async ({ page }) => {
    await expect(page.locator(".erd-canvas")).toBeVisible();
    await expect(page.locator(".erd-svg")).toBeVisible();
  });

  test("renders table boxes with names", async ({ page }) => {
    const tableNames = page.locator(".erd-table-name");
    const count = await tableNames.count();
    expect(count).toBeGreaterThanOrEqual(1);
  });

  test("table boxes show column names", async ({ page }) => {
    const colNames = page.locator(".erd-col-name");
    const count = await colNames.count();
    expect(count).toBeGreaterThanOrEqual(1);
  });

  test("filter input is present", async ({ page }) => {
    const filterInput = page.locator(".erd-filter-input");
    await expect(filterInput).toBeVisible();
    await expect(filterInput).toHaveAttribute("placeholder", "Filter tables…");
  });

  test("filter input dims non-matching tables", async ({ page }) => {
    const filterInput = page.locator(".erd-filter-input");
    await filterInput.fill("users");
    await page.locator(".erd-table-dimmed").first().waitFor({ timeout: 3000 });
    const dimmedCount = await page.locator(".erd-table-dimmed").count();
    expect(dimmedCount).toBeGreaterThanOrEqual(1);
  });

  test("zoom in button works", async ({ page }) => {
    const zoomBtn = page.locator("button[title='Zoom in']");
    await expect(zoomBtn).toBeVisible();
    await zoomBtn.click();
  });

  test("zoom out button works", async ({ page }) => {
    const zoomBtn = page.locator("button[title='Zoom out']");
    await expect(zoomBtn).toBeVisible();
    await zoomBtn.click();
  });

  test("fit button is present", async ({ page }) => {
    const fitBtn = page.locator("button[title='Fit diagram to view']");
    await expect(fitBtn).toBeVisible();
    await fitBtn.click();
  });

  test("1:1 reset button is present", async ({ page }) => {
    const resetBtn = page.locator("button[title='Reset zoom and pan']");
    await expect(resetBtn).toBeVisible();
    await expect(resetBtn).toContainText("1:1");
  });

  test("edges toggle button is present", async ({ page }) => {
    const toggleBtn = page.locator("button[title*='relationship lines']");
    await expect(toggleBtn).toBeVisible();
  });

  test("reload button is present", async ({ page }) => {
    const reloadBtn = page.locator("button[title='Reload schema']");
    await expect(reloadBtn).toBeVisible();
    await reloadBtn.click();
  });

  test("zoom percentage is displayed", async ({ page }) => {
    const zoomText = page.locator(".erd-zoom span");
    const text = await zoomText.textContent();
    expect(text).toMatch(/\d+%/);
  });
});
