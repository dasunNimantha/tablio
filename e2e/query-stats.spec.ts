import { test, expect } from "@playwright/test";
import { openQueryStats } from "./helpers";

test.describe("Query stats", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openQueryStats(page);
  });

  test("opens via context menu", async ({ page }) => {
    await expect(page.locator(".qs-unavailable, .qs-dashboard, .qs-loading").first()).toBeVisible({ timeout: 8000 });
  });

  test("shows unavailable state in mock mode", async ({ page }) => {
    await expect(page.locator(".qs-unavailable")).toBeVisible({ timeout: 8000 });
  });

  test("unavailable state shows setup card", async ({ page }) => {
    await page.locator(".qs-unavailable").waitFor({ timeout: 8000 });
    await expect(page.locator(".qs-setup-card")).toBeVisible();
  });

  test("unavailable state shows setup steps", async ({ page }) => {
    await page.locator(".qs-unavailable").waitFor({ timeout: 8000 });
    const steps = page.locator(".qs-step");
    const count = await steps.count();
    expect(count).toBeGreaterThanOrEqual(1);
  });

  test("shows pg_stat_statements heading", async ({ page }) => {
    await page.locator(".qs-unavailable").waitFor({ timeout: 8000 });
    await expect(page.locator(".qs-unavailable")).toContainText("pg_stat_statements");
  });

  test("enable extension button is present", async ({ page }) => {
    await page.locator(".qs-unavailable").waitFor({ timeout: 8000 });
    await expect(page.locator(".qs-enable-btn", { hasText: "Enable Extension" })).toBeVisible();
  });

  test("check button is present", async ({ page }) => {
    await page.locator(".qs-unavailable").waitFor({ timeout: 8000 });
    await expect(page.locator(".btn-ghost", { hasText: "Check" })).toBeVisible();
  });
});
