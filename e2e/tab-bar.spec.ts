import { test, expect } from "@playwright/test";
import { navigateToTable, connectToLocalPostgres } from "./helpers";

test.describe("Tab bar", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
  });

  test("opening a table creates a tab", async ({ page }) => {
    const tableNode = await navigateToTable(page, "users");
    await tableNode.click();
    await expect(page.locator(".tab-bar .tab")).toHaveCount(2);
    await expect(page.locator(".tab-bar .tab.active .tab-title")).toContainText("users");
  });

  test("clicking a tab activates it", async ({ page }) => {
    const usersNode = await navigateToTable(page, "users");
    await usersNode.click();
    await page.locator(".grid-table-name").waitFor({ timeout: 5000 });

    const ordersNode = page.locator(".tree-node.leaf .tree-label", { hasText: "orders" });
    await ordersNode.click();
    await expect(page.locator(".tab-bar .tab.active .tab-title")).toContainText("orders");

    const usersTab = page.locator(".tab-bar .tab .tab-title", { hasText: "users" }).locator("..");
    await usersTab.click();
    await expect(page.locator(".tab-bar .tab.active .tab-title")).toContainText("users");
  });

  test("close button removes the tab", async ({ page }) => {
    const tableNode = await navigateToTable(page, "users");
    await tableNode.click();
    await page.locator(".grid-table-name").waitFor({ timeout: 5000 });

    const tabCount = await page.locator(".tab-bar .tab").count();
    const usersTab = page.locator(".tab-bar .tab").filter({ hasText: "users" });
    await usersTab.locator(".tab-close").click();
    await expect(page.locator(".tab-bar .tab")).toHaveCount(tabCount - 1);
  });

  test("closing all tabs shows welcome screen", async ({ page }) => {
    await connectToLocalPostgres(page);
    const activityTab = page.locator(".tab-bar .tab").first();
    await activityTab.locator(".tab-close").click();
    await expect(page.locator("h2", { hasText: "Welcome to Tablio" })).toBeVisible();
  });

  test("tab shows connection color dot", async ({ page }) => {
    const tableNode = await navigateToTable(page, "users");
    await tableNode.click();
    await page.locator(".grid-table-name").waitFor({ timeout: 5000 });
    const activeTab = page.locator(".tab-bar .tab.active");
    await expect(activeTab.locator(".tab-color-dot")).toBeVisible();
  });

  test("tab shows icon", async ({ page }) => {
    const tableNode = await navigateToTable(page, "users");
    await tableNode.click();
    await page.locator(".grid-table-name").waitFor({ timeout: 5000 });
    await expect(page.locator(".tab-bar .tab.active .tab-icon")).toBeVisible();
  });
});

test.describe("Tab bar — context menu", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    const usersNode = await navigateToTable(page, "users");
    await usersNode.click();
    await page.locator(".grid-table-name").waitFor({ timeout: 5000 });
    const ordersNode = page.locator(".tree-node.leaf .tree-label", { hasText: "orders" });
    await ordersNode.click();
    await expect(page.locator(".tab-bar .tab")).toHaveCount(3);
  });

  test("context menu shows Close, Close Others, Close All", async ({ page }) => {
    const usersTab = page.locator(".tab-bar .tab").filter({ hasText: "users" });
    await usersTab.click({ button: "right" });
    const menu = page.locator(".context-menu");
    await expect(menu).toBeVisible();
    await expect(menu.locator("button", { hasText: "Close" }).first()).toBeVisible();
    await expect(menu.locator("button", { hasText: "Close Others" })).toBeVisible();
    await expect(menu.locator("button", { hasText: "Close All" })).toBeVisible();
  });

  test("Close removes only the target tab", async ({ page }) => {
    const ordersTab = page.locator(".tab-bar .tab").filter({ hasText: "orders" });
    await ordersTab.click({ button: "right" });
    await page.locator(".context-menu button", { hasText: /^Close$/ }).click();
    await expect(page.locator(".tab-bar .tab")).toHaveCount(2);
    await expect(page.locator(".tab-bar .tab", { hasText: "orders" })).not.toBeVisible();
  });

  test("Close Others keeps only the target tab", async ({ page }) => {
    const usersTab = page.locator(".tab-bar .tab").filter({ hasText: "users" });
    await usersTab.click({ button: "right" });
    await page.locator(".context-menu button", { hasText: "Close Others" }).click();
    await expect(page.locator(".tab-bar .tab")).toHaveCount(1);
    await expect(page.locator(".tab-bar .tab .tab-title")).toContainText("users");
  });

  test("Close All removes all tabs and shows welcome screen", async ({ page }) => {
    const usersTab = page.locator(".tab-bar .tab").filter({ hasText: "users" });
    await usersTab.click({ button: "right" });
    await page.locator(".context-menu button", { hasText: "Close All" }).click();
    await expect(page.locator(".tab-bar .tab")).toHaveCount(0);
    await expect(page.locator("h2", { hasText: "Welcome to Tablio" })).toBeVisible();
  });
});
