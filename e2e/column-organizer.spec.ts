import { test, expect } from "@playwright/test";
import { navigateToTable, openTable } from "./helpers";

test.describe("Column organizer", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openTable(page, "users");
    await page.locator(".col-organizer-wrapper > button").click();
    await page.locator(".col-organizer-dropdown").waitFor();
  });

  test("toggle button opens/closes the dropdown", async ({ page }) => {
    await expect(page.locator(".col-organizer-dropdown")).toBeVisible();
    await page.locator(".col-organizer-wrapper > button").click();
    await expect(page.locator(".col-organizer-dropdown")).not.toBeVisible();
  });

  test("lists all columns", async ({ page }) => {
    const items = page.locator(".col-organizer-item");
    const count = await items.count();
    expect(count).toBeGreaterThanOrEqual(2);
  });

  test("PK columns show locked badge", async ({ page }) => {
    const pkItems = page.locator(".col-organizer-item.col-organizer-pk");
    await expect(pkItems.first()).toBeVisible();
    await expect(pkItems.first().locator(".col-organizer-badge")).toHaveText("PK");
  });

  test("PK columns have locked eye icon", async ({ page }) => {
    const pkItem = page.locator(".col-organizer-item.col-organizer-pk").first();
    await expect(pkItem.locator(".col-organizer-eye-locked")).toBeVisible();
  });

  test("FK columns show FK badge", async ({ page }) => {
    await page.goto("/");
    const tableNode = await navigateToTable(page, "orders");
    await tableNode.click();
    await page.locator(".grid-table-name", { hasText: "public.orders" }).waitFor({ timeout: 8000 });
    await page.locator(".col-organizer-wrapper > button").click();
    await page.locator(".col-organizer-dropdown").waitFor();
    const fkItem = page.locator(".col-organizer-item", { hasText: "user_id" });
    await expect(fkItem.locator(".col-organizer-badge--fk")).toHaveText("FK");
  });

  test("unchecking a non-PK column hides it and updates button text", async ({ page }) => {
    const nonPkItem = page.locator(".col-organizer-item:not(.col-organizer-pk)").first();
    const colName = await nonPkItem.locator(".col-organizer-name").textContent();
    await nonPkItem.locator(".col-organizer-eye").click();
    await expect(nonPkItem).toHaveClass(/col-organizer-hidden/);
    const btn = page.locator(".col-organizer-wrapper > button");
    await expect(btn).toContainText("Columns");
    await expect(btn).toContainText("/");
  });

  test("checking a hidden column shows it again", async ({ page }) => {
    const nonPkItem = page.locator(".col-organizer-item:not(.col-organizer-pk)").first();
    await nonPkItem.locator(".col-organizer-eye").click();
    await expect(nonPkItem).toHaveClass(/col-organizer-hidden/);
    await nonPkItem.locator(".col-organizer-eye").click();
    await expect(nonPkItem).not.toHaveClass(/col-organizer-hidden/);
  });

  test("Hide All hides all non-PK columns", async ({ page }) => {
    await page.locator(".col-organizer-hide-all").click();
    const nonPkItems = page.locator(".col-organizer-item:not(.col-organizer-pk)");
    const count = await nonPkItems.count();
    for (let i = 0; i < count; i++) {
      await expect(nonPkItems.nth(i)).toHaveClass(/col-organizer-hidden/);
    }
    await expect(page.locator(".col-organizer-hide-all")).toHaveText("Show All");
  });

  test("Show All shows all columns after Hide All", async ({ page }) => {
    await page.locator(".col-organizer-hide-all").click();
    await page.locator(".col-organizer-hide-all").click();
    const nonPkItems = page.locator(".col-organizer-item:not(.col-organizer-pk)");
    const count = await nonPkItems.count();
    for (let i = 0; i < count; i++) {
      await expect(nonPkItems.nth(i)).not.toHaveClass(/col-organizer-hidden/);
    }
  });

  test("Reset restores default order and visibility", async ({ page }) => {
    const nonPkItem = page.locator(".col-organizer-item:not(.col-organizer-pk)").first();
    await nonPkItem.locator(".col-organizer-eye").click();
    await expect(nonPkItem).toHaveClass(/col-organizer-hidden/);
    await page.locator(".col-organizer-reset").click();
    const allNonPk = page.locator(".col-organizer-item:not(.col-organizer-pk)");
    const count = await allNonPk.count();
    for (let i = 0; i < count; i++) {
      await expect(allNonPk.nth(i)).not.toHaveClass(/col-organizer-hidden/);
    }
  });

  test("non-PK items are draggable", async ({ page }) => {
    const nonPkItem = page.locator(".col-organizer-item:not(.col-organizer-pk)").first();
    await expect(nonPkItem).toHaveAttribute("draggable", "true");
  });

  test("hidden count updates toolbar button badge", async ({ page }) => {
    const nonPkItems = page.locator(".col-organizer-item:not(.col-organizer-pk)");
    await nonPkItems.nth(0).locator(".col-organizer-eye").click();
    await nonPkItems.nth(1).locator(".col-organizer-eye").click();
    const btn = page.locator(".col-organizer-wrapper > button");
    await expect(btn).toHaveClass(/active-filter/);
  });

  test("search input is present and auto-focused", async ({ page }) => {
    const input = page.locator(".col-organizer-search-input");
    await expect(input).toBeVisible();
    await expect(input).toBeFocused();
  });

  test("search filters columns by name", async ({ page }) => {
    const allBefore = await page.locator(".col-organizer-item").count();
    const input = page.locator(".col-organizer-search-input");
    const firstName = await page.locator(".col-organizer-item .col-organizer-name").first().textContent();
    await input.fill(firstName!);
    const allAfter = await page.locator(".col-organizer-item").count();
    expect(allAfter).toBeLessThan(allBefore);
    expect(allAfter).toBeGreaterThanOrEqual(1);
  });

  test("search disables drag handles", async ({ page }) => {
    const input = page.locator(".col-organizer-search-input");
    await input.fill("a");
    const nonPkItem = page.locator(".col-organizer-item:not(.col-organizer-pk)").first();
    if (await nonPkItem.isVisible()) {
      await expect(nonPkItem).toHaveAttribute("draggable", "false");
    }
  });

  test("clearing search restores all columns", async ({ page }) => {
    const allBefore = await page.locator(".col-organizer-item").count();
    const input = page.locator(".col-organizer-search-input");
    await input.fill("xyz_no_match");
    const filtered = await page.locator(".col-organizer-item").count();
    expect(filtered).toBeLessThan(allBefore);
    await input.fill("");
    const allAfter = await page.locator(".col-organizer-item").count();
    expect(allAfter).toBe(allBefore);
  });
});
