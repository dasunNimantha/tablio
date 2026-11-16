import { test, expect, type Page } from "@playwright/test";
import { openTable, openQueryConsole } from "./helpers";

test.describe("Export menu", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openTable(page, "users");
  });

  test("export button is present in data grid toolbar", async ({ page }) => {
    const exportBtn = page.locator(".export-menu-wrapper .btn-ghost");
    await expect(exportBtn).toBeVisible();
  });

  test("export button opens dropdown", async ({ page }) => {
    await page.locator(".export-menu-wrapper .btn-ghost").click();
    await expect(page.locator(".export-menu-dropdown")).toBeVisible();
  });

  test("dropdown shows CSV, JSON, SQL, Excel options", async ({ page }) => {
    await page.locator(".export-menu-wrapper .btn-ghost").click();
    const dropdown = page.locator(".export-menu-dropdown");
    await expect(dropdown.locator("button", { hasText: "Export as CSV" })).toBeVisible();
    await expect(dropdown.locator("button", { hasText: "Export as JSON" })).toBeVisible();
    await expect(dropdown.locator("button", { hasText: "Export as SQL" })).toBeVisible();
    await expect(dropdown.locator("button", { hasText: "Export as Excel" })).toBeVisible();
  });

  test("clicking outside closes dropdown", async ({ page }) => {
    await page.locator(".export-menu-wrapper .btn-ghost").click();
    await expect(page.locator(".export-menu-dropdown")).toBeVisible();
    await page.locator(".tv-name").click();
    await expect(page.locator(".export-menu-dropdown")).not.toBeVisible();
  });
});

/*
 * Query console results the backend cut short (`total_rows` set): Export
 * runs the query again through `export_query_to_file` instead of sending
 * the rows on screen. Mock hooks from src/lib/tauri.ts; the save dialog
 * is stubbed through `__TAURI_INTERNALS__`, installed after the app has
 * loaded so the rest of the app stays in mock mode.
 */
test.describe("Query console export", () => {
  test.beforeEach(async ({ page }) => {
    await page.addInitScript(() => {
      const w = window as any;
      w.__mockInvokeLog = [];
      w.__mockOverride = async (cmd: string) => {
        if (cmd === "execute_query" && w.__truncated) {
          return {
            columns: ["n"],
            rows: [[1], [2], [3]],
            rows_affected: 0,
            execution_time_ms: 4,
            is_select: true,
            notices: ["Showing the first 3 of 60,000 rows."],
            total_rows: 60000,
          };
        }
        if (cmd === "export_query_to_file") return { rows: 60000, warning: null };
        return undefined;
      };
    });
    await page.goto("/");
    await page.evaluate(() => {
      (window as any).__TAURI_INTERNALS__ = {
        invoke: async (cmd: string) => (cmd === "plugin:dialog|save" ? "/tmp/result.csv" : null),
      };
    });
    await openQueryConsole(page);
    await page.locator(".query-console").waitFor({ timeout: 8000 });
  });

  const calls = (page: Page, cmd: string) =>
    page.evaluate(
      (c) => ((window as any).__mockInvokeLog as { cmd: string; args?: any }[]).filter((x) => x.cmd === c).map((x) => x.args),
      cmd,
    );

  async function runAndOpenExport(page: Page) {
    await page.locator(".query-toolbar .btn-primary", { hasText: "Execute" }).click();
    await page.locator(".query-result-info").waitFor({ timeout: 5000 });
    await page.locator(".query-console:visible .export-menu-wrapper .btn-ghost").click();
    return page.locator(".export-menu-dropdown");
  }

  test("a truncated result exports all rows by running the query again", async ({ page }) => {
    await page.evaluate(() => { (window as any).__truncated = true; });
    const dropdown = await runAndOpenExport(page);
    await expect(dropdown.locator(".export-menu-note")).toHaveText("Export all 60,000 rows");
    await dropdown.locator("button", { hasText: "Export as CSV" }).click();

    await expect(page.getByTestId("toast-success").filter({ hasText: "Exported all 60,000 rows as CSV" }))
      .toBeVisible({ timeout: 5000 });
    const ran = (await calls(page, "execute_query"))[0].request;
    const exports = await calls(page, "export_query_to_file");
    expect(exports).toHaveLength(1);
    expect(exports[0].filePath).toBe("/tmp/result.csv");
    expect(exports[0].request).toMatchObject({
      connection_id: ran.connection_id,
      database: ran.database,
      sql: ran.sql,
      format: "csv",
    });
    expect(await calls(page, "export_query_result_to_file")).toHaveLength(0);
  });

  test("a complete result exports the rows on screen", async ({ page }) => {
    const dropdown = await runAndOpenExport(page);
    await expect(dropdown.locator(".export-menu-note")).toHaveCount(0);
    await dropdown.locator("button", { hasText: "Export as JSON" }).click();

    await expect(page.getByTestId("toast-success").filter({ hasText: "Exported query result as JSON" }))
      .toBeVisible({ timeout: 5000 });
    const exports = await calls(page, "export_query_result_to_file");
    expect(exports).toHaveLength(1);
    expect(exports[0].request.format).toBe("json");
    expect(await calls(page, "export_query_to_file")).toHaveLength(0);
  });
});
