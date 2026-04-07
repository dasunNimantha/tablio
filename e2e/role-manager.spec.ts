import { test, expect } from "@playwright/test";
import { openRoleManager } from "./helpers";

test.describe("Role manager", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openRoleManager(page);
  });

  test("opens via context menu and shows role manager", async ({ page }) => {
    await expect(page.locator(".role-manager")).toBeVisible();
    await expect(page.locator(".role-toolbar")).toBeVisible();
  });

  test("toolbar shows title", async ({ page }) => {
    await expect(page.locator(".role-toolbar-title")).toContainText("Users & Roles");
  });

  test("create role button is present", async ({ page }) => {
    const btn = page.locator(".role-toolbar .btn-primary", { hasText: "Create Role" });
    await expect(btn).toBeVisible();
  });

  test("shows role table with mock roles", async ({ page }) => {
    await page.locator(".role-table").waitFor({ timeout: 5000 });
    const rows = page.locator(".role-table tbody tr");
    const count = await rows.count();
    expect(count).toBeGreaterThanOrEqual(3);
  });

  test("role names are visible", async ({ page }) => {
    await page.locator(".role-table").waitFor({ timeout: 5000 });
    await expect(page.locator(".role-name", { hasText: "postgres" })).toBeVisible();
    await expect(page.locator(".role-name", { hasText: "app_user" })).toBeVisible();
  });

  test("shows badges for role attributes", async ({ page }) => {
    await page.locator(".role-table").waitFor({ timeout: 5000 });
    const badges = page.locator(".role-badge");
    const count = await badges.count();
    expect(count).toBeGreaterThanOrEqual(1);
  });

  test("edit button is present on role rows", async ({ page }) => {
    await page.locator(".role-table").waitFor({ timeout: 5000 });
    const editBtn = page.locator(".role-actions-cell .btn-icon[title='Edit']").first();
    await expect(editBtn).toBeVisible();
  });

  test("drop button is present on role rows", async ({ page }) => {
    await page.locator(".role-table").waitFor({ timeout: 5000 });
    const dropBtn = page.locator(".role-actions-cell .btn-icon[title='Drop']").first();
    await expect(dropBtn).toBeVisible();
  });

  test("create role button opens form dialog", async ({ page }) => {
    await page.locator(".btn-primary", { hasText: "Create Role" }).click();
    await expect(page.locator(".dialog.role-form")).toBeVisible();
    await expect(page.locator(".dialog-header h2")).toContainText("Create Role");
  });

  test("create role form has name input and checkboxes", async ({ page }) => {
    await page.locator(".btn-primary", { hasText: "Create Role" }).click();
    const dialog = page.locator(".dialog.role-form");
    await expect(dialog.locator("input").first()).toBeVisible();
    await expect(dialog.locator(".checkbox-label", { hasText: "Can Login" })).toBeVisible();
  });

  test("create role form has cancel and create buttons", async ({ page }) => {
    await page.locator(".btn-primary", { hasText: "Create Role" }).click();
    const footer = page.locator(".dialog-footer");
    await expect(footer.locator(".btn-secondary", { hasText: "Cancel" })).toBeVisible();
    await expect(footer.locator(".btn-primary", { hasText: "Create" })).toBeVisible();
  });

  test("cancel closes create role dialog", async ({ page }) => {
    await page.locator(".btn-primary", { hasText: "Create Role" }).click();
    await page.locator(".dialog-footer .btn-secondary", { hasText: "Cancel" }).click();
    await expect(page.locator(".dialog.role-form")).not.toBeVisible();
  });

  test("edit button opens edit form dialog", async ({ page }) => {
    await page.locator(".role-table").waitFor({ timeout: 5000 });
    await page.locator(".role-actions-cell .btn-icon[title='Edit']").first().click();
    await expect(page.locator(".dialog.role-form")).toBeVisible();
    await expect(page.locator(".dialog-header h2")).toContainText("Edit Role");
  });
});
