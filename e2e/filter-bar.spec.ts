import { test, expect } from "@playwright/test";
import { openTable } from "./helpers";

test.describe("Filter bar", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openTable(page, "users");
    await page.locator(".btn-ghost", { hasText: "Filter" }).click();
    await page.locator(".filter-bar").waitFor();
  });

  test("opens with one empty condition row", async ({ page }) => {
    await expect(page.locator(".filter-condition")).toHaveCount(1);
  });

  test("column dropdown shows all table columns", async ({ page }) => {
    const triggers = page.locator(".filter-condition .cs-trigger");
    await triggers.first().click();
    const dropdown = page.locator(".cs-dropdown").first();
    await expect(dropdown).toBeVisible();
    const count = await dropdown.locator(".cs-option").count();
    expect(count).toBeGreaterThanOrEqual(9);
    await expect(dropdown.locator(".cs-option", { hasText: /^id$/ })).toBeVisible();
    await expect(dropdown.locator(".cs-option", { hasText: /^email$/ })).toBeVisible();
  });

  test("operator dropdown shows all operators", async ({ page }) => {
    const triggers = page.locator(".filter-condition .cs-trigger");
    await triggers.nth(1).click();
    const dropdown = page.locator(".cs-dropdown").first();
    await expect(dropdown).toBeVisible();
    await expect(dropdown.locator(".cs-option", { hasText: /^=$/ })).toBeVisible();
    await expect(dropdown.locator(".cs-option", { hasText: /^LIKE$/ })).toBeVisible();
    await expect(dropdown.locator(".cs-option", { hasText: /^IS NULL$/ })).toBeVisible();
    await expect(dropdown.locator(".cs-option", { hasText: /^IS NOT NULL$/ })).toBeVisible();
  });

  test("value input accepts text", async ({ page }) => {
    const input = page.locator(".filter-value-input");
    await input.fill("test_value");
    await expect(input).toHaveValue("test_value");
  });

  test("IS NULL hides value input", async ({ page }) => {
    const opTrigger = page.locator(".filter-condition .cs-trigger").nth(1);
    await opTrigger.click();
    await page.locator(".cs-option", { hasText: "IS NULL" }).click();
    await expect(page.locator(".filter-value-input")).not.toBeVisible();
  });

  test("IS NOT NULL hides value input", async ({ page }) => {
    const opTrigger = page.locator(".filter-condition .cs-trigger").nth(1);
    await opTrigger.click();
    await page.locator(".cs-option", { hasText: "IS NOT NULL" }).click();
    await expect(page.locator(".filter-value-input")).not.toBeVisible();
  });

  test("add condition button adds another row", async ({ page }) => {
    await page.locator(".filter-actions .btn-ghost", { hasText: "Add" }).click();
    await expect(page.locator(".filter-condition")).toHaveCount(2);
  });

  test("remove condition button removes a row", async ({ page }) => {
    await page.locator(".filter-actions .btn-ghost", { hasText: "Add" }).click();
    await expect(page.locator(".filter-condition")).toHaveCount(2);
    await page.locator(".filter-condition").nth(1).locator(".btn-icon[title='Remove condition']").click();
    await expect(page.locator(".filter-condition")).toHaveCount(1);
  });

  test("AND/OR toggle switches join type", async ({ page }) => {
    await page.locator(".filter-actions .btn-ghost", { hasText: "Add" }).click();
    const joinBtn = page.locator(".filter-join-btn");
    await expect(joinBtn).toHaveText("AND");
    await joinBtn.click();
    await expect(joinBtn).toHaveText("OR");
    await joinBtn.click();
    await expect(joinBtn).toHaveText("AND");
  });

  test("apply button applies filter and shows active indicator", async ({ page }) => {
    await page.locator(".filter-value-input").fill("alice");
    await page.locator(".filter-actions .btn-primary", { hasText: "Apply" }).click();
    const filterBtn = page.locator(".btn-ghost", { hasText: "Filter" });
    await expect(filterBtn).toContainText("(active)");
  });

  test("clear button resets filter and removes active indicator", async ({ page }) => {
    await page.locator(".filter-value-input").fill("alice");
    await page.locator(".filter-actions .btn-primary", { hasText: "Apply" }).click();
    await page.locator(".filter-actions .btn-ghost", { hasText: "Clear" }).click();
    const filterBtn = page.locator(".btn-ghost", { hasText: "Filter" });
    await expect(filterBtn).not.toContainText("(active)");
  });

  test("close button hides filter bar", async ({ page }) => {
    await page.locator(".filter-actions .btn-icon[title='Close filter bar']").click();
    await expect(page.locator(".filter-bar")).not.toBeVisible();
  });

  test("Enter key in value input applies filter", async ({ page }) => {
    await page.locator(".filter-value-input").fill("bob");
    await page.locator(".filter-value-input").press("Enter");
    const filterBtn = page.locator(".btn-ghost", { hasText: "Filter" });
    await expect(filterBtn).toContainText("(active)");
  });

  test("column search in dropdown filters options", async ({ page }) => {
    const colTrigger = page.locator(".filter-condition .cs-trigger").first();
    await colTrigger.click();
    const searchInput = page.locator(".cs-search");
    await searchInput.fill("email");
    const options = page.locator(".cs-dropdown .cs-option");
    await expect(options).toHaveCount(1);
    await expect(options.first()).toContainText("email");
  });
});
