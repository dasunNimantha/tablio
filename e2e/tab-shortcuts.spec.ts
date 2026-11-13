import { test, expect, type Page } from "@playwright/test";
import { openQueryConsole } from "./helpers";

// These run several edits and saves each; allow for a loaded machine.
test.describe.configure({ timeout: 60_000 });

async function setSql(page: Page, sql: string) {
  await page.locator(".query-console:visible .query-editor-wrapper .monaco-editor").click();
  await page.keyboard.press("ControlOrMeta+A");
  await page.keyboard.insertText(sql);
}

test.describe("Tab bar — query tabs and shortcuts", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openQueryConsole(page);
    await page.locator(".query-console").first().waitFor({ timeout: 8000 });
    // Connecting also opens the Activity tab; keep only the console.
    const activity = page.locator(".tab", { hasText: "Activity" });
    if (await activity.count()) await activity.locator(".tab-close").click({ force: true });
    await expect(page.locator(".tab")).toHaveCount(1);
  });

  test("untitled consoles are numbered", async ({ page }) => {
    await openQueryConsole(page);
    await expect(page.locator(".tab .tab-title")).toHaveText(["Query - postgres", "Query 2 - postgres"]);
  });

  test("Ctrl+N opens a new query tab for the active connection", async ({ page }) => {
    await page.locator(".tab.active").click();
    await page.keyboard.press("Control+N");
    await expect(page.locator(".tab")).toHaveCount(2);
    await expect(page.locator(".tab.active .tab-title")).toHaveText("Query 2 - postgres");
  });

  test("Ctrl+W closes the active tab", async ({ page }) => {
    await openQueryConsole(page);
    await expect(page.locator(".tab")).toHaveCount(2);
    await page.locator(".tab.active").click();
    await page.keyboard.press("Control+W");
    await expect(page.locator(".tab")).toHaveCount(1);
  });

  test("middle-click closes a tab", async ({ page }) => {
    await openQueryConsole(page);
    await page.locator(".tab").first().click({ button: "middle" });
    await expect(page.locator(".tab")).toHaveCount(1);
    await expect(page.locator(".tab .tab-title")).toHaveText("Query 2 - postgres");
  });

  test("tabs are exposed as tabs with labelled close buttons", async ({ page }) => {
    const tab = page.locator(".tab").first();
    await expect(page.locator(".tab-bar")).toHaveAttribute("role", "tablist");
    await expect(tab).toHaveAttribute("role", "tab");
    await expect(tab).toHaveAttribute("aria-selected", "true");
    await expect(tab).toHaveAttribute("title", /Query - postgres/);
    await expect(tab.locator(".tab-close")).toHaveAttribute("aria-label", "Close Query - postgres");
  });

  test("closing a console with typed SQL asks first and shows a dirty marker", async ({ page }) => {
    await setSql(page, "SELECT 'draft'");
    const tab = page.locator(".tab").first();
    await expect(tab).toHaveClass(/dirty/);
    await expect(tab.locator(".tab-dirty-marker")).toHaveCount(1);
    await page.keyboard.press("Control+W");
    const dialog = page.locator(".confirm-dialog");
    await expect(dialog).toContainText("unsaved changes");
    await dialog.locator("button", { hasText: "Cancel" }).click();
    await expect(page.locator(".tab")).toHaveCount(1);
  });

  test("a console with the default SQL closes without asking", async ({ page }) => {
    await page.locator(".tab").first().locator(".tab-close").click({ force: true });
    await expect(page.locator(".tab")).toHaveCount(0);
    await expect(page.locator(".confirm-dialog")).toHaveCount(0);
  });
});
