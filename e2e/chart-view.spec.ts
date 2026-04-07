import { test, expect } from "@playwright/test";
import { openQueryConsole } from "./helpers";

async function openChartAfterQuery(page: import("@playwright/test").Page) {
  await openQueryConsole(page);
  await page.locator(".query-console").waitFor({ timeout: 8000 });
  await page.locator(".query-toolbar .btn-primary", { hasText: "Execute" }).click();
  await page.locator(".result-table-ag-wrapper").waitFor({ timeout: 5000 });
  await page.locator(".result-table-toolbar .btn-ghost", { hasText: "Chart" }).click();
  await page.locator(".chart-view").waitFor({ timeout: 5000 });
}

test.describe("Chart view", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openChartAfterQuery(page);
  });

  test("chart renders after executing a query", async ({ page }) => {
    await expect(page.locator(".chart-view")).toBeVisible();
    await expect(page.locator(".chart-canvas")).toBeVisible();
  });

  test("chart type buttons are visible", async ({ page }) => {
    const btns = page.locator(".chart-type-btn");
    await expect(btns).toHaveCount(4);
    await expect(btns.nth(0)).toHaveText("Bar");
    await expect(btns.nth(1)).toHaveText("Line");
    await expect(btns.nth(2)).toHaveText("Pie");
    await expect(btns.nth(3)).toHaveText("Scatter");
  });

  test("bar chart is active by default", async ({ page }) => {
    await expect(page.locator(".chart-type-btn", { hasText: "Bar" })).toHaveClass(/active/);
  });

  test("clicking Line switches chart type", async ({ page }) => {
    await page.locator(".chart-type-btn", { hasText: "Line" }).click();
    await expect(page.locator(".chart-type-btn", { hasText: "Line" })).toHaveClass(/active/);
    await expect(page.locator(".chart-type-btn", { hasText: "Bar" })).not.toHaveClass(/active/);
  });

  test("clicking Pie switches chart type and hides Y selector", async ({ page }) => {
    await page.locator(".chart-type-btn", { hasText: "Pie" }).click();
    await expect(page.locator(".chart-type-btn", { hasText: "Pie" })).toHaveClass(/active/);
    const yGroup = page.locator(".chart-axis-group", { hasText: "Y / Values" });
    await expect(yGroup).not.toBeVisible();
  });

  test("clicking Scatter switches chart type and hides Y selector", async ({ page }) => {
    await page.locator(".chart-type-btn", { hasText: "Scatter" }).click();
    await expect(page.locator(".chart-type-btn", { hasText: "Scatter" })).toHaveClass(/active/);
    const yGroup = page.locator(".chart-axis-group", { hasText: "Y / Values" });
    await expect(yGroup).not.toBeVisible();
  });

  test("X axis selector shows column options", async ({ page }) => {
    const xGroup = page.locator(".chart-axis-group", { hasText: "X / Labels" });
    await expect(xGroup).toBeVisible();
    const trigger = xGroup.locator(".cs-trigger");
    await trigger.click();
    const dropdown = page.locator(".cs-dropdown");
    await expect(dropdown).toBeVisible();
    await expect(dropdown.locator(".cs-option", { hasText: "— Auto —" })).toBeVisible();
    await expect(dropdown.locator(".cs-option", { hasText: "id" })).toBeVisible();
  });

  test("Y axis multi-select shows for bar/line", async ({ page }) => {
    const yGroup = page.locator(".chart-axis-group", { hasText: "Y / Values" });
    await expect(yGroup).toBeVisible();
    const trigger = yGroup.locator(".cs-trigger");
    await trigger.click();
    const dropdown = page.locator(".cs-dropdown").last();
    await expect(dropdown).toBeVisible();
    await expect(dropdown.locator(".cs-option").first()).toBeVisible();
  });

  test("selecting a Y column updates multi-select", async ({ page }) => {
    const yGroup = page.locator(".chart-axis-group", { hasText: "Y / Values" });
    const trigger = yGroup.locator(".cs-trigger");
    await trigger.click();
    const firstOption = page.locator(".cs-dropdown").last().locator(".cs-option").first();
    await firstOption.click();
    await expect(firstOption).toHaveClass(/cs-option-selected/);
  });
});
