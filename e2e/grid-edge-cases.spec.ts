import { test, expect, type Page } from "@playwright/test";
import { openTable } from "./helpers";

// These run several edits and saves each; allow for a loaded machine.
test.describe.configure({ timeout: 60_000 });

/*
 * Data grid edge cases, against the mock tables:
 *   app_settings: id (PK), "config.key", description (multi-line text),
 *                 payload (jsonb as raw text), tags (map as an object), enabled
 *   audit_log:    no primary key
 *
 * Hooks from src/lib/tauri.ts (mock mode): `__mockInvokeLog` records
 * backend calls, `__mockOverride(cmd, args)` can answer / delay them.
 */

declare global {
  interface Window {
    __mockInvokeLog?: { cmd: string; args?: Record<string, any> }[];
    __mockOverride?: (cmd: string, args?: Record<string, any>) => unknown;
    __copied?: string[];
    __slowFetch?: boolean;
    __failSave?: boolean;
    __slowSave?: boolean;
  }
}

async function installHooks(page: Page) {
  await page.addInitScript(() => {
    window.__mockInvokeLog = [];
    window.__copied = [];
    window.__mockOverride = async (cmd) => {
      if (cmd === "fetch_rows" && window.__slowFetch) {
        await new Promise((r) => setTimeout(r, 2500));
      }
      if (cmd === "apply_changes") {
        if (window.__slowSave) await new Promise((r) => setTimeout(r, 800));
        if (window.__failSave) throw "duplicate key value violates unique constraint";
      }
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

/** A body cell by data row and column position (`c<index>`). */
const cell = (page: Page, row: number, col: number) =>
  page.locator(`.ag-grid-scrolling-container .ag-row[row-index="${row}"] .ag-cell[col-id="c${col}"]`);

const toolbar = (page: Page) => page.locator(".grid-toolbar");

async function openSettings(page: Page) {
  await openTable(page, "app_settings");
  await cell(page, 0, 1).waitFor({ timeout: 8000 });
}

test.describe("Data grid — column names and values", () => {
  test.beforeEach(async ({ page }) => {
    await installHooks(page);
    await page.goto("/");
    await openSettings(page);
  });

  test("a dotted column name shows its value, not NULL", async ({ page }) => {
    await expect(page.locator(".ag-header-cell", { hasText: "config.key" })).toBeVisible();
    await expect(cell(page, 0, 1)).toHaveText("ui.theme");
    await expect(cell(page, 0, 1)).not.toHaveClass(/cell-null/);
  });

  test("JSON text shows as-is and structured values as JSON", async ({ page }) => {
    await expect(cell(page, 0, 3)).toHaveText('{"theme": "dark", "size": 12}');
    await expect(cell(page, 0, 4)).toHaveText('{"owner":"ui"}');
    await expect(page.locator(".ag-cell", { hasText: "[object Object]" })).toHaveCount(0);
  });

  test("real NULL is a pill, the text NULL is plain text", async ({ page }) => {
    await expect(cell(page, 3, 2).locator(".cell-null-pill")).toHaveText("NULL");
    await expect(cell(page, 3, 1)).toHaveText("NULL");
    await expect(cell(page, 3, 1).locator(".cell-null-pill")).toHaveCount(0);
  });

  test("editing a dotted column saves that column", async ({ page }) => {
    await cell(page, 1, 1).dblclick();
    const input = page.locator(".ag-cell-inline-editing input");
    await input.fill("ui.font.size");
    await input.press("Enter");
    await expect(cell(page, 1, 1)).toHaveText("ui.font.size");
    await toolbar(page).locator("button", { hasText: "Save 1 change" }).click();
    await expect.poll(() => calls(page, "apply_changes").then((c) => c.length)).toBe(1);
    const [req] = await calls(page, "apply_changes");
    expect(req!.changes.updates).toEqual([
      expect.objectContaining({ column_name: "config.key", new_value: "ui.font.size", primary_key_values: [["id", 2]] }),
    ]);
  });
});

test.describe("Data grid — multi-line and JSON editing", () => {
  test.beforeEach(async ({ page }) => {
    await installHooks(page);
    await page.goto("/");
    await openSettings(page);
  });

  test("multi-line text is edited in a textarea and keeps its CRLF line breaks", async ({ page }) => {
    await cell(page, 0, 2).dblclick();
    const textarea = page.locator(".ag-large-text textarea");
    await expect(textarea).toBeVisible();
    await expect(textarea).toHaveValue("Line one\nLine two");
    await textarea.press("End");
    await textarea.press("Shift+Enter");
    await textarea.pressSequentially("Line three");
    await textarea.press("Enter");
    await expect(textarea).toHaveCount(0);
    await toolbar(page).locator("button", { hasText: "Save 1 change" }).click();
    await expect.poll(() => calls(page, "apply_changes").then((c) => c.length)).toBe(1);
    const [req] = await calls(page, "apply_changes");
    expect(req!.changes.updates[0]).toMatchObject({
      column_name: "description",
      new_value: "Line one\r\nLine two\r\nLine three",
    });
  });

  test("JSON is edited as its text and saved as text", async ({ page }) => {
    await cell(page, 1, 3).dblclick();
    const textarea = page.locator(".ag-large-text textarea");
    await expect(textarea).toHaveValue("[1, 2, 3]");
    await textarea.fill('{"a": [1, 2]}');
    await textarea.press("Enter");
    await expect(cell(page, 1, 3)).toHaveText('{"a": [1, 2]}');
    await toolbar(page).locator("button", { hasText: "Save 1 change" }).click();
    await expect.poll(() => calls(page, "apply_changes").then((c) => c.length)).toBe(1);
    const [req] = await calls(page, "apply_changes");
    expect(req!.changes.updates[0]).toMatchObject({ column_name: "payload", new_value: '{"a": [1, 2]}' });
  });

  test("typed null in a JSON cell is the JSON null literal, Set NULL is SQL NULL", async ({ page }) => {
    await cell(page, 1, 3).dblclick();
    const textarea = page.locator(".ag-large-text textarea");
    await textarea.fill("null");
    await textarea.press("Enter");
    await cell(page, 2, 3).click({ button: "right" });
    await page.locator(".context-menu button", { hasText: "Set payload to NULL" }).click();
    await toolbar(page).locator("button", { hasText: "Save 2 changes" }).click();
    await expect.poll(() => calls(page, "apply_changes").then((c) => c.length)).toBe(1);
    const [req] = await calls(page, "apply_changes");
    const byId = Object.fromEntries(req!.changes.updates.map((u: any) => [u.primary_key_values[0][1], u.new_value]));
    expect(byId).toEqual({ 2: "null", 3: null });
  });

  test("invalid JSON is rejected", async ({ page }) => {
    await cell(page, 1, 3).dblclick();
    const textarea = page.locator(".ag-large-text textarea");
    await textarea.fill("{not json");
    await textarea.press("Enter");
    await expect(page.locator(".toast", { hasText: "Invalid JSON" })).toBeVisible();
    await expect(cell(page, 1, 3)).toHaveText("[1, 2, 3]");
    await expect(toolbar(page).locator("button", { hasText: /^Save/ })).toHaveCount(0);
  });

  test("a structured value opens as JSON text, not [object Object]", async ({ page }) => {
    await cell(page, 0, 4).dblclick();
    await expect(page.locator(".ag-large-text textarea")).toHaveValue('{"owner":"ui"}');
  });
});

test.describe("Data grid — keyboard editing", () => {
  test.beforeEach(async ({ page }) => {
    await installHooks(page);
    await page.goto("/");
    await openTable(page, "users");
    await cell(page, 0, 2).waitFor({ timeout: 8000 });
  });

  test("Enter on a focused cell starts editing", async ({ page }) => {
    await cell(page, 0, 2).click();
    await page.keyboard.press("Enter");
    await expect(page.locator(".ag-cell-inline-editing input")).toBeVisible();
  });

  test("F2 starts editing", async ({ page }) => {
    await cell(page, 0, 2).click();
    await page.keyboard.press("F2");
    await expect(page.locator(".ag-cell-inline-editing input")).toBeVisible();
  });

  test("typing on a focused cell replaces its value", async ({ page }) => {
    await cell(page, 0, 2).click();
    await page.keyboard.type("zed");
    await page.keyboard.press("Enter");
    await expect(cell(page, 0, 2)).toHaveText("zed");
    await expect(cell(page, 0, 2)).toHaveClass(/cell-modified/);
  });

  test("Delete on a focused text cell clears it to an empty string", async ({ page }) => {
    await cell(page, 0, 3).click();
    await page.keyboard.press("Delete");
    await expect(cell(page, 0, 3)).toHaveClass(/cell-modified/);
    await expect(cell(page, 0, 3)).toHaveText("");
  });
});

test.describe("Data grid — tables without a primary key", () => {
  test.beforeEach(async ({ page }) => {
    await installHooks(page);
    await page.goto("/");
    await openTable(page, "audit_log");
    await cell(page, 0, 1).waitFor({ timeout: 8000 });
  });

  test("the grid is read-only", async ({ page }) => {
    await expect(toolbar(page).locator(".grid-readonly-badge")).toBeVisible();
    await expect(toolbar(page).locator(".grid-readonly-badge")).toHaveAttribute("title", /no primary key/);
    await expect(toolbar(page).locator("button[title='Add Row']")).toHaveCount(0);
    await expect(toolbar(page).locator("button", { hasText: "Test Data" })).toHaveCount(0);
    await cell(page, 0, 1).dblclick();
    await expect(page.locator(".ag-cell-inline-editing")).toHaveCount(0);
    await cell(page, 0, 1).click();
    await page.keyboard.type("x");
    await expect(page.locator(".ag-cell-inline-editing")).toHaveCount(0);
  });

  test("Delete / Ctrl+A don't mark rows for deletion", async ({ page }) => {
    await cell(page, 0, 1).click();
    await page.keyboard.press("ControlOrMeta+A");
    await page.keyboard.press("Delete");
    await expect(page.locator(".ag-row.row-deleted")).toHaveCount(0);
    await expect(toolbar(page).locator("button", { hasText: /^Save/ })).toHaveCount(0);
  });

  test("the context menu has no edit actions and row detail is read-only", async ({ page }) => {
    await cell(page, 0, 2).click({ button: "right" });
    const menu = page.locator(".context-menu");
    await expect(menu).toBeVisible();
    await expect(menu.locator("button", { hasText: "Delete row" })).toHaveCount(0);
    await expect(menu.locator("button", { hasText: /to NULL/ })).toHaveCount(0);
    await menu.locator("button", { hasText: "View as JSON" }).click();
    await expect(page.locator(".row-detail-readonly")).toBeVisible();
    await page.locator(".json-value-inline").first().dblclick();
    await expect(page.locator(".json-inline-input")).toHaveCount(0);
  });
});

test.describe("Data grid — saving", () => {
  test.beforeEach(async ({ page }) => {
    await installHooks(page);
    await page.goto("/");
    await openTable(page, "users");
    await cell(page, 0, 2).waitFor({ timeout: 8000 });
  });

  async function editUsername(page: Page, value: string) {
    await cell(page, 0, 2).dblclick();
    const input = page.locator(".ag-cell-inline-editing input");
    await input.fill(value);
    await input.press("Enter");
  }

  test("Save shows the change count and saves once when clicked twice", async ({ page }) => {
    await page.evaluate(() => { window.__slowSave = true; });
    await editUsername(page, "renamed");
    await toolbar(page).locator("button[title='Add Row']").click();
    const save = toolbar(page).locator("button", { hasText: "Save 2 changes" });
    await expect(save).toBeVisible();
    await save.dblclick();
    await expect(toolbar(page).locator(".grid-toolbar-changes")).toHaveCount(0, { timeout: 5000 });
    await expect.poll(() => calls(page, "apply_changes").then((c) => c.length)).toBe(1);
    await page.waitForTimeout(500);
    expect(await calls(page, "apply_changes")).toHaveLength(1);
  });

  test("a save error stays until dismissed", async ({ page }) => {
    await page.evaluate(() => { window.__failSave = true; });
    await editUsername(page, "renamed");
    await toolbar(page).locator("button", { hasText: "Save 1 change" }).click();
    const banner = page.locator(".grid-error-banner");
    await expect(banner).toContainText("duplicate key value");
    await page.waitForTimeout(6000);
    await expect(banner).toBeVisible();
    await expect(cell(page, 0, 2)).toHaveText("renamed");
    await banner.locator("button[aria-label='Dismiss error']").click();
    await expect(banner).toHaveCount(0);
  });

  test("a background refresh landing after an edit doesn't wipe it", async ({ page }) => {
    // Auto-refresh every 5 s, each fetch taking 2.5 s: edit while one is in flight.
    await toolbar(page).locator("button[title='Auto-refresh interval']").click();
    await page.locator(".refresh-menu-item", { hasText: "5s" }).click();
    await page.evaluate(() => { window.__slowFetch = true; });
    const before = (await calls(page, "fetch_rows")).length;
    await expect.poll(() => calls(page, "fetch_rows").then((c) => c.length), { timeout: 9000 }).toBeGreaterThan(before);
    await editUsername(page, "edited-during-refresh");
    await page.waitForTimeout(3000);
    await expect(cell(page, 0, 2)).toHaveText("edited-during-refresh");
    await expect(toolbar(page).locator("button", { hasText: "Save 1 change" })).toBeVisible();
  });
});

test.describe("Data grid — rows and copying", () => {
  test.beforeEach(async ({ page }) => {
    await installHooks(page);
    await page.goto("/");
    await openTable(page, "users");
    await cell(page, 0, 2).waitFor({ timeout: 8000 });
  });

  test("a deleted row is struck through and marked", async ({ page }) => {
    await cell(page, 1, 2).click({ button: "right" });
    await page.locator(".context-menu button", { hasText: "Delete row" }).click();
    const row = page.locator(".ag-grid-scrolling-container .ag-row.row-deleted");
    await expect(row).toHaveCount(1);
    await expect(row.locator(".ag-cell[col-id='c1']")).toHaveCSS("text-decoration-line", "line-through");
    await expect(page.locator(".ag-grid-scrolling-container .ag-row[row-index='1'] .grid-row-number")).toHaveText("−");
  });

  test("a new row shows default / auto for columns the database fills in", async ({ page }) => {
    await toolbar(page).locator("button[title='Add Row']").click();
    const newRow = page.locator(".ag-row.ag-row-pinned").first();
    await expect(newRow.locator(".ag-cell[col-id='c0']")).toHaveText("auto");
    await expect(newRow.locator(".ag-cell[col-id='c5']")).toHaveText("default"); // is_active DEFAULT true
    await expect(newRow.locator(".ag-cell[col-id='c1']")).toHaveText("NULL"); // email, no default
  });

  test("Copy as SQL skips deleted rows, omits untouched insert columns and confirms", async ({ page }) => {
    await cell(page, 0, 2).click({ button: "right" });
    await page.locator(".context-menu button", { hasText: "Delete row" }).click();
    await toolbar(page).locator("button[title='Add Row']").click();
    await toolbar(page).locator("button[title='Copy all rows as INSERT statements']").click();
    await expect(page.locator(".toast", { hasText: /Copied \d+ INSERT statements/ })).toBeVisible();
    const [text] = await page.evaluate(() => window.__copied ?? []);
    const lines = text.split("\n");
    expect(lines.some((l) => l.includes("'alice_johnson@example.com'"))).toBe(false);
    expect(lines.some((l) => l.includes("'bob_smith@example.com'"))).toBe(true);
    expect(lines[lines.length - 1]).toBe('INSERT INTO "public"."users" DEFAULT VALUES;');
  });

  test("search placeholder describes what it searches", async ({ page }) => {
    await cell(page, 0, 2).click();
    await page.keyboard.press("ControlOrMeta+F");
    await expect(page.locator(".grid-search-input")).toHaveAttribute("placeholder", "Search values in visible columns...");
  });
});

test.describe("Data grid — filters and foreign keys", () => {
  test.beforeEach(async ({ page }) => {
    await installHooks(page);
    await page.goto("/");
  });

  test("filter conditions survive closing and reopening the filter bar", async ({ page }) => {
    await openTable(page, "users");
    await cell(page, 0, 2).waitFor({ timeout: 8000 });
    await toolbar(page).locator("button[title='Toggle Filter']").click();
    const value = page.locator(".filter-value-input").first();
    await value.fill("42");
    await value.press("Enter");
    await expect(toolbar(page).locator("button", { hasText: "Filter (active)" })).toBeVisible();
    await page.locator(".filter-bar .btn-icon[title='Close filter bar']").click();
    await expect(page.locator(".filter-bar")).toHaveCount(0);
    await toolbar(page).locator("button", { hasText: "Filter (active)" }).click();
    await expect(page.locator(".filter-value-input").first()).toHaveValue("42");
  });

  test("Jump to FK opens the referenced table filtered to the value", async ({ page }) => {
    await openTable(page, "orders");
    await cell(page, 0, 1).waitFor({ timeout: 8000 });
    const userId = (await cell(page, 0, 1).textContent())!.trim();
    await cell(page, 0, 1).click({ button: "right" });
    await page.locator(".context-menu button", { hasText: `Jump to users.id = ${userId}` }).click();
    await expect(page.locator(".tab.active .tab-title")).toHaveText("public.users");
    await expect(page.locator(".filter-bar")).toBeVisible();
    await expect(page.locator(".filter-value-input").first()).toHaveValue(userId);
    await expect.poll(async () => (await calls(page, "fetch_rows")).some(
      (a) => a?.request?.table === "users" && a?.request?.filter === `"id" = ${userId}`,
    )).toBe(true);
  });
});

test.describe("Data grid — toolbar layout", () => {
  for (const width of [1024, 1280]) {
    test(`buttons don't overlap or shrink at ${width}px`, async ({ page }) => {
      await page.setViewportSize({ width, height: 720 });
      await installHooks(page);
      await page.goto("/");
      await openTable(page, "users");
      await cell(page, 0, 2).waitFor({ timeout: 8000 });
      await cell(page, 0, 2).dblclick();
      await page.locator(".ag-cell-inline-editing input").fill("x");
      await page.keyboard.press("Enter");
      const boxes = await page.evaluate(() => {
        const bar = document.querySelector(".grid-toolbar")!.getBoundingClientRect();
        const buttons = [...document.querySelectorAll(".grid-toolbar button")].map((b) => {
          const r = b.getBoundingClientRect();
          const svg = b.querySelector("svg")?.getBoundingClientRect();
          return { l: r.left, r: r.right, svgW: svg?.width ?? 14, label: b.textContent };
        });
        return { bar: { l: bar.left, r: bar.right }, buttons };
      });
      const sorted = [...boxes.buttons].sort((a, b) => a.l - b.l);
      for (let i = 0; i < sorted.length; i++) {
        expect(sorted[i].svgW, `icon of ${sorted[i].label}`).toBeGreaterThanOrEqual(13);
        expect(sorted[i].r).toBeLessThanOrEqual(boxes.bar.r + 0.5);
        if (i > 0) expect(sorted[i].l, `${sorted[i].label} overlaps ${sorted[i - 1].label}`).toBeGreaterThanOrEqual(sorted[i - 1].r - 0.5);
      }
    });
  }
});
