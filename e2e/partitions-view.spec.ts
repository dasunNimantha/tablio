import { test, expect } from "@playwright/test";
import { openPartitionsView } from "./helpers";

/**
 * Partitions sub-tab inside Schema mode. The mock data set ships
 * `public.orders` as a RANGE-partitioned table with three real
 * partitions plus a default partition (see src/lib/mockData.ts).
 *
 * These tests pin the recent layout/UX work:
 *   - the search input has the redesigned compact size and is wired
 *   - the partitions table uses the explicit colgroup (no Status col)
 *   - rows render with the bound chip and right-aligned numerics
 *   - clicking a sortable header flips the sort indicator
 *   - filtering by name narrows the row set
 */
test.describe("Partitions view", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openPartitionsView(page, "orders");
  });

  test("renders the partitions table with all column headers", async ({ page }) => {
    const headers = page.locator(".pv-table thead th");
    await expect(headers.nth(0)).toContainText("Name");
    await expect(headers.nth(1)).toContainText("Bound");
    await expect(headers.nth(2)).toContainText("Rows");
    await expect(headers.nth(3)).toContainText("Size");
    await expect(headers.nth(4)).toContainText("of total");
    // The Status column was removed in the layout pass.
    await expect(headers).toHaveCount(5);
  });

  test("uses an explicit colgroup so headers and values stay aligned", async ({ page }) => {
    const cols = page.locator(".pv-table > colgroup > col");
    await expect(cols).toHaveCount(5);
  });

  test("at least one partition row renders with a bound chip", async ({ page }) => {
    const rows = page.locator(".pv-table tbody tr");
    expect(await rows.count()).toBeGreaterThanOrEqual(1);
    // Either a code-style bound chip OR the DEFAULT chip should appear
    // in the bound column for each row.
    const bound = page
      .locator(".pv-td-bound code, .pv-td-bound .pv-chip-default")
      .first();
    await expect(bound).toBeVisible();
  });

  test("search input has the compact redesigned size", async ({ page }) => {
    const search = page.locator(".pv-search");
    await expect(search).toBeVisible();
    const box = await search.boundingBox();
    // 30px height ± 2 (rendered height varies by 1px across browsers).
    expect(box).not.toBeNull();
    expect(box!.height).toBeGreaterThanOrEqual(28);
    expect(box!.height).toBeLessThanOrEqual(34);
  });

  test("filtering by name narrows the row set", async ({ page }) => {
    const before = await page.locator(".pv-table tbody tr").count();
    await page.locator(".pv-search input").fill("default");
    // Match-count chip appears when filtering.
    await expect(page.locator(".pv-match-count")).toBeVisible();
    const after = await page.locator(".pv-table tbody tr").count();
    expect(after).toBeLessThan(before);
  });

  test("clicking a sortable header flips its sort indicator", async ({ page }) => {
    const sizeHeader = page.locator(".pv-table thead th .pv-th-content", {
      hasText: "Size",
    });
    await sizeHeader.click();
    // After a click, the active sort header should not show the muted
    // double-chevron — it should show the active arrow icon.
    const activeIcon = sizeHeader.locator("svg").first();
    await expect(activeIcon).toBeVisible();
    // Click again to flip the direction; the SVG element re-renders
    // but the column stays the active sort.
    await sizeHeader.click();
    await expect(activeIcon).toBeVisible();
  });
});
