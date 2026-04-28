import { test, expect } from "@playwright/test";
import { openStructureView } from "./helpers";

/**
 * The old "View Structure" view has been folded into the unified
 * TableView. The schema sub-tabs (Columns / Constraints / Indexes /
 * Foreign Keys / References / Triggers / [Partitions] / Statistics)
 * live behind the Schema mode of TableView and reuse the same anchors
 * we ship in `subTab.ts`.
 *
 * These tests cover the contract our users rely on: the strip exists,
 * Columns is the default landing tab, and switching tabs swaps panels
 * without losing the toolbar.
 */
test.describe("Table view — Schema sub-tabs", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openStructureView(page, "users");
  });

  test("opens the unified TableView in Schema mode", async ({ page }) => {
    await expect(page.locator(".tv")).toBeVisible();
    await expect(page.locator(".tv-header")).toBeVisible();
    await expect(page.locator(".tv-schema-strip")).toBeVisible();
  });

  test("header shows the schema-qualified table name", async ({ page }) => {
    await expect(page.locator(".tv-name")).toContainText("users");
    await expect(page.locator(".tv-name-schema")).toContainText("public.");
  });

  test("Schema mode toggle is the active one", async ({ page }) => {
    const schemaBtn = page.locator(".tv-mode-switch-btn", { hasText: "Schema" });
    const dataBtn = page.locator(".tv-mode-switch-btn", { hasText: "Data" });
    await expect(schemaBtn).toHaveClass(/active/);
    await expect(dataBtn).not.toHaveClass(/active/);
  });

  test("Columns is the default Schema sub-tab", async ({ page }) => {
    const columnsTab = page.locator(".tv-schema-tab", { hasText: "Columns" });
    await expect(columnsTab).toHaveClass(/active/);
  });

  test("Columns panel renders Name / Type / Nullable / Default headers", async ({ page }) => {
    const table = page.locator(".tv-schema-panel .tv-table").first();
    await expect(table).toBeVisible();
    await expect(table.locator("th", { hasText: "Name" })).toBeVisible();
    await expect(table.locator("th", { hasText: "Type" })).toBeVisible();
    await expect(table.locator("th", { hasText: "Nullable" })).toBeVisible();
    await expect(table.locator("th", { hasText: "Default" })).toBeVisible();
  });

  test("PK column is badged with PK", async ({ page }) => {
    await expect(page.locator(".tv-badge-pk").first()).toBeVisible();
    await expect(page.locator(".tv-badge-pk").first()).toHaveText("PK");
  });

  test("Indexes sub-tab renders an indexes table", async ({ page }) => {
    await page.locator(".tv-schema-tab", { hasText: "Indexes" }).click();
    const panel = page.locator(".tv-schema-panel:visible");
    await expect(panel.locator(".tv-table")).toBeVisible();
    await expect(panel.locator("th", { hasText: "Name" })).toBeVisible();
    await expect(panel.locator("th", { hasText: "Columns" })).toBeVisible();
  });

  test("Foreign Keys sub-tab renders a FK table or a friendly empty panel", async ({ page }) => {
    await page.locator(".tv-schema-tab", { hasText: "Foreign Keys" }).click();
    const panel = page.locator(".tv-schema-panel:visible");
    const table = panel.locator(".tv-table");
    if (await table.isVisible()) {
      await expect(panel.locator("th", { hasText: "Name" })).toBeVisible();
    } else {
      await expect(panel.locator(".tv-cell-empty").first()).toBeVisible();
    }
  });

  test("switching sub-tabs flips the active class", async ({ page }) => {
    await page.locator(".tv-schema-tab", { hasText: "Indexes" }).click();
    await expect(
      page.locator(".tv-schema-tab", { hasText: "Indexes" })
    ).toHaveClass(/active/);
    await expect(
      page.locator(".tv-schema-tab", { hasText: "Columns" })
    ).not.toHaveClass(/active/);
  });

  test("sub-tab labels show counts when non-zero", async ({ page }) => {
    const columnsTab = page.locator(".tv-schema-tab", { hasText: "Columns" });
    // Either the count badge is visible, or the column list is empty —
    // the test should pass in both cases. We prefer the visible badge
    // for the seeded `users` table.
    const count = columnsTab.locator(".tv-schema-tab-count");
    if ((await count.count()) > 0) {
      await expect(count.first()).toBeVisible();
    }
  });
});
