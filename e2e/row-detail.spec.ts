import { test, expect } from "@playwright/test";
import { openTable } from "./helpers";

async function openRowDetail(page: import("@playwright/test").Page) {
  await openTable(page, "users");
  const firstRowNum = page.locator(".grid-row-number").first();
  await firstRowNum.waitFor({ timeout: 5000 });
  const cells = page.locator(".ag-cell.grid-row-number");
  await cells.first().click();
  await page.locator(".row-detail-panel").waitFor({ timeout: 5000 });
}

test.describe("Row detail — JSON viewer", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openRowDetail(page);
  });

  test("panel opens with header", async ({ page }) => {
    await expect(page.locator(".row-detail-panel")).toBeVisible();
    await expect(page.locator(".row-detail-header h2")).toHaveText("JSON VIEWER");
  });

  test("panel shows all column values as JSON tree", async ({ page }) => {
    const jsonTree = page.locator(".json-tree");
    await expect(jsonTree).toBeVisible();
    const rows = page.locator(".json-row");
    const count = await rows.count();
    expect(count).toBeGreaterThanOrEqual(2);
  });

  test("shows key names in JSON format", async ({ page }) => {
    const keys = page.locator(".json-key");
    const count = await keys.count();
    expect(count).toBeGreaterThanOrEqual(2);
    const firstKey = await keys.first().textContent();
    expect(firstKey).toContain('"');
  });

  test("filter input filters displayed fields", async ({ page }) => {
    const filterInput = page.locator(".row-detail-filter input");
    const initialCount = await page.locator(".json-row").count();
    await filterInput.fill("email");
    const filteredCount = await page.locator(".json-row").count();
    expect(filteredCount).toBeLessThan(initialCount);
  });

  test("filter highlights matching text", async ({ page }) => {
    const filterInput = page.locator(".row-detail-filter input");
    await filterInput.fill("email");
    await expect(page.locator(".json-highlight").first()).toBeVisible();
  });

  test("copy JSON button copies to clipboard", async ({ page }) => {
    const copyBtn = page.locator(".row-detail-header-actions .btn-icon[title='Copy as JSON']");
    await expect(copyBtn).toBeVisible();
    await copyBtn.click();
  });

  test("close button closes panel", async ({ page }) => {
    const closeBtn = page.locator(".row-detail-header-actions .btn-icon").last();
    await closeBtn.click();
    await expect(page.locator(".row-detail-panel")).not.toBeVisible();
  });

  test("PK values show lock icon and readonly title", async ({ page }) => {
    const pkRow = page.locator(".json-row-pk").first();
    await expect(pkRow).toBeVisible();
    await expect(pkRow.locator(".json-pk-icon")).toBeVisible();
    await expect(pkRow.locator(".json-value-inline")).toHaveAttribute("title", "Primary key — not editable");
  });

  test("non-PK values show editable title", async ({ page }) => {
    const nonPkRow = page.locator(".json-row:not(.json-row-pk)").first();
    await expect(nonPkRow.locator(".json-value-inline")).toHaveAttribute("title", "Double-click to edit");
  });

  test("double-click non-PK value opens inline editor", async ({ page }) => {
    const nonPkValue = page.locator(".json-row:not(.json-row-pk) .json-value-inline").first();
    await nonPkValue.dblclick();
    await expect(page.locator(".json-inline-input")).toBeVisible();
  });

  test("inline edit: Escape cancels", async ({ page }) => {
    const nonPkValue = page.locator(".json-row:not(.json-row-pk) .json-value-inline").first();
    await nonPkValue.dblclick();
    const input = page.locator(".json-inline-input");
    await expect(input).toBeVisible();
    await input.press("Escape");
    await expect(page.locator(".json-inline-input")).not.toBeVisible();
  });

  test("inline edit: Enter commits and shows Apply button", async ({ page }) => {
    const nonPkValue = page.locator(".json-row:not(.json-row-pk) .json-value-inline").first();
    await nonPkValue.dblclick();
    const input = page.locator(".json-inline-input");
    await input.fill("changed_value");
    await input.press("Enter");
    await expect(page.locator(".json-save-all-btn")).toBeVisible();
    await expect(page.locator(".json-save-all-btn")).toContainText("Apply");
  });

  test("discard button removes pending edits", async ({ page }) => {
    const nonPkValue = page.locator(".json-row:not(.json-row-pk) .json-value-inline").first();
    await nonPkValue.dblclick();
    const input = page.locator(".json-inline-input");
    await input.fill("changed_value");
    await input.press("Enter");
    await expect(page.locator(".json-discard-btn")).toBeVisible();
    await page.locator(".json-discard-btn").click();
    await expect(page.locator(".json-save-all-btn")).not.toBeVisible();
  });

  test("resize handle is present", async ({ page }) => {
    await expect(page.locator(".row-detail-resize-handle")).toBeVisible();
  });
});
