import { test, expect } from "@playwright/test";
import { openActivity } from "./helpers";

test.describe("Activity dashboard — shell", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openActivity(page);
  });

  test("opens and shows activity dashboard", async ({ page }) => {
    const dash = page.locator(".activity-dashboard").first();
    await expect(dash).toBeVisible();
    await expect(dash.locator(".activity-toolbar")).toBeVisible();
  });

  test("shows sub-tabs: Overview, Sessions, Locks, Configuration", async ({ page }) => {
    const dash = page.locator(".activity-dashboard").first();
    await expect(dash.locator(".dashboard-sub-tab", { hasText: "Overview" })).toBeVisible();
    await expect(dash.locator(".dashboard-sub-tab", { hasText: "Sessions" })).toBeVisible();
    await expect(dash.locator(".dashboard-sub-tab", { hasText: "Locks" })).toBeVisible();
    await expect(dash.locator(".dashboard-sub-tab", { hasText: "Configuration" })).toBeVisible();
  });

  test("default sub-tab is Overview", async ({ page }) => {
    const dash = page.locator(".activity-dashboard").first();
    await expect(dash.locator(".dashboard-sub-tab", { hasText: "Overview" })).toHaveClass(/active/);
  });

  test("live toggle is visible and defaults to Live", async ({ page }) => {
    const toggle = page.locator(".activity-dashboard").first().locator(".activity-live-toggle");
    await expect(toggle).toBeVisible();
    await expect(toggle).toHaveClass(/live/);
    await expect(toggle).toContainText("Live");
  });

  test("clicking live toggle switches to Paused", async ({ page }) => {
    const toggle = page.locator(".activity-dashboard").first().locator(".activity-live-toggle");
    await toggle.click();
    await expect(toggle).toHaveClass(/paused/);
    await expect(toggle).toContainText("Paused");
  });

  test("pausing and resuming toggles back to Live", async ({ page }) => {
    const toggle = page.locator(".activity-dashboard").first().locator(".activity-live-toggle");
    await toggle.click();
    await expect(toggle).toHaveClass(/paused/);
    await toggle.click();
    await expect(toggle).toHaveClass(/live/);
  });
});

test.describe("Activity dashboard — Overview", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openActivity(page);
  });

  test("overview shows stat bar", async ({ page }) => {
    const dash = page.locator(".activity-dashboard").first();
    await expect(dash.locator(".overview-stats-bar")).toBeVisible({ timeout: 5000 });
    await expect(dash.locator(".overview-stat", { hasText: "Total" })).toBeVisible();
    await expect(dash.locator(".overview-stat", { hasText: "Active" })).toBeVisible();
  });

  test("overview shows charts", async ({ page }) => {
    const dash = page.locator(".activity-dashboard").first();
    await dash.locator(".overview-stats-bar").waitFor({ timeout: 5000 });
    const charts = dash.locator(".chart-card");
    const count = await charts.count();
    expect(count).toBeGreaterThanOrEqual(2);
  });
});

test.describe("Activity dashboard — Sessions", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openActivity(page);
    await page.locator(".activity-dashboard").first().locator(".dashboard-sub-tab", { hasText: "Sessions" }).click();
  });

  test("sessions tab shows session list", async ({ page }) => {
    const dash = page.locator(".activity-dashboard").first();
    await expect(dash.locator(".info-table")).toBeVisible({ timeout: 5000 });
    const rows = dash.locator(".info-table tbody tr");
    const count = await rows.count();
    expect(count).toBeGreaterThanOrEqual(1);
  });

  test("sessions toolbar shows connection count", async ({ page }) => {
    const dash = page.locator(".activity-dashboard").first();
    await expect(dash.locator(".activity-count")).toBeVisible({ timeout: 5000 });
  });

  test("sessions search filters sessions", async ({ page }) => {
    const dash = page.locator(".activity-dashboard").first();
    await dash.locator(".info-table").waitFor({ timeout: 5000 });
    const search = dash.locator("input[placeholder*='Filter sessions']");
    await search.fill("app_user");
    const rows = dash.locator(".info-table tbody tr");
    const count = await rows.count();
    expect(count).toBeGreaterThanOrEqual(1);
  });

  test("sessions search with no match shows empty state", async ({ page }) => {
    const dash = page.locator(".activity-dashboard").first();
    await dash.locator(".info-table").waitFor({ timeout: 5000 });
    const search = dash.locator("input[placeholder*='Filter sessions']");
    await search.fill("zzz_nonexistent_session_zzz");
    await expect(dash.locator(".dashboard-empty-state")).toBeVisible();
  });
});

test.describe("Activity dashboard — Locks", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openActivity(page);
    await page.locator(".activity-dashboard").first().locator(".dashboard-sub-tab", { hasText: "Locks" }).click();
  });

  test("locks tab shows empty state (mock returns no locks)", async ({ page }) => {
    const dash = page.locator(".activity-dashboard").first();
    await expect(dash.locator(".dashboard-empty-state")).toBeVisible({ timeout: 5000 });
    await expect(dash.locator(".dashboard-empty-state")).toContainText("No active locks");
  });
});

test.describe("Activity dashboard — Configuration", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openActivity(page);
    await page.locator(".activity-dashboard").first().locator(".dashboard-sub-tab", { hasText: "Configuration" }).click();
  });

  test("configuration tab shows empty state (mock returns no config)", async ({ page }) => {
    const dash = page.locator(".activity-dashboard").first();
    await expect(dash.locator(".dashboard-empty-state")).toBeVisible({ timeout: 5000 });
  });

  test("live toggle is hidden on Configuration tab", async ({ page }) => {
    const dash = page.locator(".activity-dashboard").first();
    await expect(dash.locator(".activity-live-toggle")).not.toBeVisible();
  });
});
