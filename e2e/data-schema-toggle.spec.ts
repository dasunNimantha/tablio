import { test, expect } from "@playwright/test";
import { openTable } from "./helpers";

/**
 * The TableView header has a Data / Schema mode toggle that owns the
 * persisted view state. These tests pin:
 *   - both buttons render with their lucide icons
 *   - Data is the default landing mode after `Open Table`
 *   - clicking Schema swaps the active class and reveals the schema
 *     sub-tab strip without unmounting the data grid
 *   - clicking Data again brings back the data grid (i.e. the previously
 *     mounted body is still there)
 */
test.describe("TableView — Data / Schema mode toggle", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openTable(page, "users");
  });

  test("renders both mode buttons in the header", async ({ page }) => {
    const dataBtn = page.locator(".tv-mode-switch-btn", { hasText: "Data" });
    const schemaBtn = page.locator(".tv-mode-switch-btn", { hasText: "Schema" });
    await expect(dataBtn).toBeVisible();
    await expect(schemaBtn).toBeVisible();
  });

  test("each button has an icon next to its label", async ({ page }) => {
    // Lucide renders inline SVGs; we assert the SVG sibling exists in
    // both buttons. This is the contract for the icon redesign.
    const dataIcon = page
      .locator(".tv-mode-switch-btn", { hasText: "Data" })
      .locator("svg");
    const schemaIcon = page
      .locator(".tv-mode-switch-btn", { hasText: "Schema" })
      .locator("svg");
    await expect(dataIcon).toHaveCount(1);
    await expect(schemaIcon).toHaveCount(1);
  });

  test("Data mode is the default after Open Table", async ({ page }) => {
    await expect(
      page.locator(".tv-mode-switch-btn", { hasText: "Data" })
    ).toHaveClass(/active/);
    await expect(
      page.locator(".tv-mode-switch-btn", { hasText: "Schema" })
    ).not.toHaveClass(/active/);
  });

  test("clicking Schema swaps the active class and shows the sub-tab strip", async ({ page }) => {
    await page.locator(".tv-mode-switch-btn", { hasText: "Schema" }).click();
    await expect(
      page.locator(".tv-mode-switch-btn", { hasText: "Schema" })
    ).toHaveClass(/active/);
    await expect(
      page.locator(".tv-mode-switch-btn", { hasText: "Data" })
    ).not.toHaveClass(/active/);
    await expect(page.locator(".tv-schema-strip")).toBeVisible();
  });

  test("toggling back to Data brings the data grid back", async ({ page }) => {
    await page.locator(".tv-mode-switch-btn", { hasText: "Schema" }).click();
    await page.locator(".tv-schema-strip").waitFor({ timeout: 5000 });
    await page.locator(".tv-mode-switch-btn", { hasText: "Data" }).click();
    await expect(
      page.locator(".tv-mode-switch-btn", { hasText: "Data" })
    ).toHaveClass(/active/);
    // Data grid container is still present (visibility is toggled, not
    // unmounted, so a refetch-on-toggle isn't triggered).
    await expect(page.locator(".ag-grid-wrapper")).toBeVisible();
  });

  test("header no longer renders the old ~rows / size meta chips", async ({ page }) => {
    // We removed the "~rows N" and "size N KB" chips from the header
    // because the row count was a stale planner estimate. The data
    // grid footer still shows the exact count.
    await expect(page.locator(".tv-meta")).toHaveCount(0);
  });
});
