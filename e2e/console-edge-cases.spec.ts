import { test, expect, type Page } from "@playwright/test";
import { openQueryConsole, openLargeTextEditor } from "./helpers";

// These run several edits and saves each; allow for a loaded machine.
test.describe.configure({ timeout: 60_000 });

/*
 * Query console + result grid edge cases. Mock SQL keywords (see
 * generateQueryResult in src/lib/mockData.ts):
 *   dup_cols / dotted_cols, LIMIT 0, FROM app_settings (editable).
 */

declare global {
  interface Window {
    __mockInvokeLog?: { cmd: string; args?: Record<string, any> }[];
    __mockOverride?: (cmd: string, args?: Record<string, any>) => unknown;
    __copied?: string[];
    __slowSave?: boolean;
    __failQuery?: string;
  }
}

async function installHooks(page: Page) {
  await page.addInitScript(() => {
    window.__mockInvokeLog = [];
    window.__copied = [];
    window.__mockOverride = async (cmd) => {
      if ((cmd === "apply_changes" || cmd === "save_query") && window.__slowSave) {
        await new Promise((r) => setTimeout(r, 800));
      }
      if (cmd === "execute_query" && window.__failQuery) throw window.__failQuery;
      return undefined;
    };
    Object.defineProperty(navigator, "clipboard", {
      configurable: true,
      value: { writeText: async (t: string) => { window.__copied!.push(t); }, readText: async () => "" },
    });
  });
}

const calls = (page: Page, cmd: string) =>
  page.evaluate((c) => (window.__mockInvokeLog ?? []).filter((x) => x.cmd === c).map((x) => x.args), cmd);

const execBtn = (page: Page) => page.locator(".query-toolbar .btn-primary", { hasText: "Execute" });

async function setSql(page: Page, sql: string) {
  await page.locator(".query-editor-wrapper .monaco-editor").click();
  await page.keyboard.press("ControlOrMeta+A");
  await page.keyboard.insertText(sql);
}

async function run(page: Page, sql: string) {
  await setSql(page, sql);
  await execBtn(page).click();
  await expect(page.locator(".query-result-info")).toBeVisible({ timeout: 5000 });
}

const editorText = (page: Page) =>
  page.evaluate(() => (window as any).monaco?.editor?.getModels?.()[0]?.getValue?.() ??
    document.querySelector(".query-editor-wrapper .view-lines")?.textContent ?? "");

/** Result cell by visual row and column position. */
const rcell = (page: Page, row: number, col: number) =>
  page.locator(`.result-table-ag-wrapper .ag-grid-scrolling-container .ag-row[row-index="${row}"] .ag-cell[col-id="c${col}"]`);

test.beforeEach(async ({ page }) => {
  await installHooks(page);
  await page.goto("/");
  await openQueryConsole(page);
  await page.locator(".query-console").waitFor({ timeout: 8000 });
});

test.describe("Result grid — column names", () => {
  test("duplicate column names each show their own value", async ({ page }) => {
    await run(page, "SELECT dup_cols");
    await expect(rcell(page, 0, 0)).toHaveText("1");
    await expect(rcell(page, 0, 1)).toHaveText("left");
    await expect(rcell(page, 0, 2)).toHaveText("101");
  });

  test("Copy row as JSON keeps every duplicate column", async ({ page }) => {
    await run(page, "SELECT dup_cols");
    await rcell(page, 0, 2).click({ button: "right" });
    await page.locator(".context-menu button", { hasText: "Copy row as JSON" }).click();
    const [text] = await page.evaluate(() => window.__copied ?? []);
    expect(JSON.parse(text)).toEqual({ id: 1, name: "left", id_2: 101 });
  });

  test("Copy cell value copies the right-clicked cell", async ({ page }) => {
    await run(page, "SELECT dup_cols");
    await rcell(page, 1, 2).click({ button: "right" });
    await page.locator(".context-menu button", { hasText: "Copy cell value" }).click();
    expect(await page.evaluate(() => window.__copied ?? [])).toEqual(["102"]);
  });

  test("dotted and expression column names show their values", async ({ page }) => {
    await run(page, "SELECT dotted_cols");
    await expect(page.locator(".result-table-ag-wrapper .ag-header-cell", { hasText: "price*1.1" })).toBeVisible();
    await expect(rcell(page, 0, 1)).toHaveText("11");
    await expect(rcell(page, 0, 2)).toHaveText("alpha");
  });
});

test.describe("Result grid — empty and read-only results", () => {
  test("a SELECT with no rows still shows its columns and Export", async ({ page }) => {
    await run(page, "SELECT id, name, value FROM t LIMIT 0");
    await expect(page.locator(".query-result-info")).toContainText("0 rows returned");
    for (const name of ["id", "name", "value"]) {
      await expect(page.locator(".result-table-ag-wrapper .ag-header-cell", { hasText: new RegExp(`^(PK)?${name}`) }).first()).toBeVisible();
    }
    await expect(page.locator(".result-table-toolbar .btn-ghost", { hasText: "Export" })).toBeVisible();
    await expect(page.locator(".result-table-toolbar .btn-ghost", { hasText: "Chart" })).toBeDisabled();
  });

  test("row detail of a read-only result can't edit", async ({ page }) => {
    await run(page, "SELECT 1");
    await expect(page.locator(".result-updatable-badge")).toHaveText(/Read-only/);
    await page.locator(".result-table-ag-wrapper .ag-cell.grid-row-number").first().click();
    await expect(page.locator(".row-detail-readonly")).toBeVisible();
    await page.locator(".json-row .json-value-inline").nth(1).dblclick();
    await expect(page.locator(".json-inline-input")).toHaveCount(0);
  });

  test("the row detail panel doesn't cover Chart / Export", async ({ page }) => {
    await run(page, "SELECT 1");
    await page.locator(".result-table-ag-wrapper .ag-cell.grid-row-number").first().click();
    await expect(page.locator(".row-detail-panel")).toBeVisible();
    const chart = page.locator(".result-table-toolbar .btn-ghost", { hasText: "Chart" });
    await chart.click({ trial: true, timeout: 2000 });
    const panel = await page.locator(".row-detail-panel").boundingBox();
    const bar = await page.locator(".result-table-toolbar").boundingBox();
    expect(panel!.y).toBeGreaterThanOrEqual(bar!.y + bar!.height - 1);
  });

  test("row numbers follow the visual order after sorting", async ({ page }) => {
    await run(page, "SELECT 1");
    const header = page.locator(".result-table-ag-wrapper .ag-header-cell[col-id='c0']");
    await header.click();
    await header.click(); // descending
    await expect(rcell(page, 0, 0)).toHaveText("25");
    await expect(page.locator(".result-table-ag-wrapper .ag-grid-scrolling-container .ag-row[row-index='0'] .grid-row-number")).toHaveText("1");
  });
});

test.describe("Result grid — editing", () => {
  test("saving twice quickly saves once and clears the pending edits", async ({ page }) => {
    await run(page, "SELECT * FROM app_settings");
    await expect(page.locator(".result-updatable-badge")).toHaveText(/Editable/);
    await page.evaluate(() => { window.__slowSave = true; });
    await rcell(page, 0, 1).dblclick();
    const input = page.locator(".ag-cell-inline-editing input");
    await input.fill("ui.colour");
    await input.press("Enter");
    await page.locator(".result-table-toolbar .btn-ghost", { hasText: "Add Row" }).click();
    const save = page.locator(".result-save-btn");
    await expect(save).toHaveText("Save 2 changes");
    await save.click();
    await save.click({ force: true }).catch(() => {});
    await expect(save).toHaveCount(0, { timeout: 5000 });
    const applied = await calls(page, "apply_changes");
    expect(applied).toHaveLength(1);
    expect(applied[0]!.changes.inserts).toHaveLength(1);
    expect(applied[0]!.changes.updates[0]).toMatchObject({ column_name: "config.key", new_value: "ui.colour" });
  });

  test("multi-line text keeps its line breaks", async ({ page }) => {
    await run(page, "SELECT * FROM app_settings");
    const textarea = await openLargeTextEditor(page, rcell(page, 2, 2));
    await expect(textarea).toHaveValue("First\nSecond\nThird");
    await textarea.press("ControlOrMeta+End");
    await textarea.press("Shift+Enter");
    await textarea.pressSequentially("Fourth");
    await textarea.press("Enter");
    await page.locator(".result-save-btn").click();
    await expect.poll(() => calls(page, "apply_changes").then((c) => c.length)).toBe(1);
    const [req] = await calls(page, "apply_changes");
    expect(req!.changes.updates[0]).toMatchObject({ column_name: "description", new_value: "First\nSecond\nThird\nFourth" });
  });
});

test.describe("Query console — saving and loading SQL", () => {
  test("pressing Enter twice in the Save dialog saves once and returns focus to the editor", async ({ page }) => {
    await page.evaluate(() => { window.__slowSave = true; });
    await page.locator("[data-testid='save-query-btn']").click();
    const input = page.locator(".save-query-input");
    await input.fill("Twice");
    await input.press("Enter");
    await input.press("Enter").catch(() => {});
    await expect(page.locator(".save-query-dialog")).toHaveCount(0, { timeout: 5000 });
    expect(await calls(page, "save_query")).toHaveLength(1);
    await expect.poll(() => page.evaluate(() => !!document.activeElement?.closest(".monaco-editor"))).toBe(true);
  });

  test("loading a saved query over unsaved SQL asks first", async ({ page }) => {
    await setSql(page, "SELECT 'my unsaved work'");
    await page.locator(".query-toolbar .btn-ghost", { hasText: "Saved" }).click();
    await page.locator(".saved-queries-item", { hasText: "Active users this month" }).click();
    const dialog = page.locator(".confirm-dialog");
    await expect(dialog).toContainText("Replace unsaved SQL?");
    await dialog.locator("button", { hasText: "Cancel" }).click();
    await expect(page.locator(".query-editor-wrapper .view-lines")).toContainText("my unsaved work");
  });

  test("loading a history entry over unsaved SQL asks first", async ({ page }) => {
    await run(page, "SELECT 1");
    await setSql(page, "SELECT 'typed, never run'");
    await page.locator(".query-toolbar .btn-ghost", { hasText: "History" }).click();
    await page.locator(".query-history-item").first().click();
    await expect(page.locator(".confirm-dialog")).toContainText("Replace unsaved SQL?");
    await page.locator(".confirm-dialog button", { hasText: "Replace" }).click();
    await expect(page.locator(".query-editor-wrapper .view-lines")).toContainText("SELECT 1");
  });

  test("history pins survive later runs", async ({ page }) => {
    await run(page, "SELECT 1");
    await page.locator(".query-toolbar .btn-ghost", { hasText: "History" }).click();
    await page.locator(".query-history-action[title='Pin']").first().click();
    await page.locator(".query-history-header .btn-icon").click();
    await run(page, "SELECT 2");
    await run(page, "SELECT 3");
    await page.locator(".query-toolbar .btn-ghost", { hasText: "History" }).click();
    const pinned = page.locator(".query-history-item.pinned");
    await expect(pinned).toHaveCount(1);
    await expect(pinned.locator(".query-history-sql")).toHaveText("SELECT 1");
    await expect(page.locator(".query-history-item").first()).toHaveClass(/pinned/);
  });
});

test.describe("Query console — keyboard and errors", () => {
  test("Ctrl+Shift+F formats the SQL", async ({ page }) => {
    await setSql(page, "select a, b from t where x = 1");
    await page.keyboard.press("ControlOrMeta+Shift+F");
    await expect(page.locator(".query-editor-wrapper .view-line")).toHaveCount(7);
    await expect(page.locator(".query-editor-wrapper .view-lines")).toContainText("from");
  });

  test("Esc stops a running query with focus outside the editor", async ({ page }) => {
    await setSql(page, "SELECT pg_sleep(30)");
    await execBtn(page).click();
    await expect(page.locator("[data-testid='stop-query-btn']")).toBeVisible({ timeout: 5000 });
    await page.locator(".query-results-section").click();
    await page.keyboard.press("Escape");
    await expect(page.locator("[data-testid='query-cancelled']")).toBeVisible({ timeout: 5000 });
  });

  test("a query error can be copied", async ({ page }) => {
    await page.evaluate(() => { window.__failQuery = 'syntax error at or near "SELEC"'; });
    await setSql(page, "SELEC 1");
    await execBtn(page).click();
    const err = page.locator(".query-error");
    await expect(err).toContainText("syntax error");
    await err.locator("button[aria-label='Copy error']").click();
    expect(await page.evaluate(() => window.__copied ?? [])).toEqual(['syntax error at or near "SELEC"']);
  });
});

test.describe("Query console — zoom", () => {
  for (const zoom of [100, 110, 125]) {
    test(`the editor fits its pane at ${zoom}% zoom`, async ({ page }) => {
      await page.evaluate((z) => { localStorage.setItem("tablio-zoom", String(z)); sessionStorage.clear(); }, zoom);
      await page.setViewportSize({ width: 1280, height: 720 });
      await page.goto("/");
      await openQueryConsole(page);
      const console_ = page.locator(".query-console:visible");
      await console_.locator(".query-editor-wrapper .monaco-editor").waitFor({ timeout: 8000 });
      // A long error with an unbreakable run used to make the console
      // scroll sideways and clip the toolbar ("ecute").
      await page.evaluate(() => {
        window.__failQuery = `ERROR: syntax error at or near "fail"\nLINE 1: SELECT fail FROM ${"x".repeat(400)}\nHINT: ${"very long hint text ".repeat(30)}`;
      });
      await console_.locator(".query-toolbar .btn-primary", { hasText: "Execute" }).click();
      await expect(console_.locator(".query-error")).toBeVisible();
      // Let ResizeObserver / Monaco settle.
      await page.waitForTimeout(500);
      const m = await console_.evaluate((root) => {
        const w = root.querySelector(".query-editor-wrapper") as HTMLElement;
        const ed = w.querySelector(".monaco-editor") as HTMLElement;
        const section = root.querySelector(".query-editor-section") as HTMLElement;
        const toolbar = root.querySelector(".query-toolbar") as HTMLElement;
        const results = root.querySelector(".query-results-section") as HTMLElement;
        return {
          consoleScroll: root.scrollWidth - root.clientWidth,
          resultsScroll: results.scrollWidth - results.clientWidth,
          wrapperScroll: w.scrollWidth - w.clientWidth,
          // On-screen sizes: the editor box is un-zoomed, the wrapper isn't.
          editorWidth: ed.getBoundingClientRect().width,
          wrapperWidth: w.getBoundingClientRect().width,
          editorRight: ed.getBoundingClientRect().right,
          consoleRight: root.getBoundingClientRect().right,
          sectionScroll: section.scrollLeft + toolbar.scrollLeft + root.scrollLeft,
        };
      });
      expect(m.consoleScroll).toBeLessThanOrEqual(1);
      expect(m.resultsScroll).toBeLessThanOrEqual(1);
      expect(m.wrapperScroll).toBeLessThanOrEqual(1);
      expect(m.editorWidth).toBeLessThanOrEqual(m.wrapperWidth + 1);
      expect(m.editorWidth).toBeGreaterThan(m.wrapperWidth - 2);
      expect(m.editorRight).toBeLessThanOrEqual(m.consoleRight + 1);
      expect(m.sectionScroll).toBe(0);
      await expect(console_.locator(".query-toolbar .btn-primary", { hasText: "Execute" })).toBeInViewport({ ratio: 1 });
    });
  }
});
