import { test, expect } from "@playwright/test";
import { openDDL } from "./helpers";

test.describe("DDL viewer", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openDDL(page, "users");
    await page.locator(".ddl-viewer").waitFor({ timeout: 8000 });
  });

  test("opens via context menu and shows DDL viewer", async ({ page }) => {
    await expect(page.locator(".ddl-viewer")).toBeVisible();
    await expect(page.locator(".ddl-toolbar")).toBeVisible();
  });

  test("toolbar shows object name", async ({ page }) => {
    await expect(page.locator(".ddl-object-name")).toContainText("users");
  });

  test("shows DDL in Monaco editor", async ({ page }) => {
    await expect(page.locator(".ddl-editor-wrapper")).toBeVisible();
    await expect(page.locator(".monaco-editor")).toBeVisible({ timeout: 5000 });
  });

  test("copy button is present and clickable", async ({ page }) => {
    await page.context().grantPermissions(["clipboard-read", "clipboard-write"]);
    const copyBtn = page.locator(".ddl-toolbar .btn-ghost", { hasText: "Copy" });
    await expect(copyBtn).toBeVisible();
    await copyBtn.click();
    await expect(page.locator(".ddl-toolbar .btn-ghost", { hasText: "Copied" })).toBeVisible({ timeout: 3000 });
  });

  test("DDL content is rendered in Monaco editor", async ({ page }) => {
    const editor = page.locator(".ddl-editor-wrapper .monaco-editor");
    await editor.waitFor({ timeout: 8000 });
    await expect(editor).toBeVisible();
  });
});
