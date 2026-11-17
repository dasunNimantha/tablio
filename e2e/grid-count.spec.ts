import { test, expect, type Page } from "@playwright/test";
import { openTable } from "./helpers";

// Moving between pages must not re-count the table (a full COUNT(*) scan,
// expensive on big tables and on Cassandra); refreshing still counts.
declare global {
  interface Window {
    __countFlags?: boolean[];
    __mockOverride?: (cmd: string, args?: Record<string, any>) => unknown;
  }
}

const TOTAL = 120; // three pages of 50

async function install(page: Page) {
  await page.addInitScript((total) => {
    window.__countFlags = [];
    window.__mockOverride = (cmd, args) => {
      if (cmd !== "fetch_rows" || args?.request?.table !== "users") return undefined;
      const req = args.request;
      window.__countFlags!.push(req.count !== false);
      const n = Math.max(0, Math.min(req.limit, total - req.offset));
      return {
        columns: [
          { name: "id", data_type: "integer", is_nullable: false, default_value: null, is_primary_key: true, ordinal_position: 1, is_auto_generated: false },
          { name: "label", data_type: "text", is_nullable: true, default_value: null, is_primary_key: false, ordinal_position: 2, is_auto_generated: false },
        ],
        rows: Array.from({ length: n }, (_, i) => [req.offset + i + 1, `row ${req.offset + i + 1}`]),
        total_rows: req.count === false ? -1 : total,
        offset: req.offset,
        limit: req.limit,
      };
    };
  }, TOTAL);
}

const flags = (page: Page) => page.evaluate(() => window.__countFlags ?? []);
const info = (page: Page) => page.locator(".grid-pagination-info");
const nextPage = (page: Page) => page.locator(".grid-pagination-controls button").nth(2);

test("paging skips the row count but keeps the total; refresh counts again", async ({ page }) => {
  test.setTimeout(90_000);
  await install(page);
  await page.goto("/");
  await openTable(page, "users");
  await expect(info(page)).toContainText("120 rows");
  // Let the initial (StrictMode-doubled) fetches finish.
  await page.waitForTimeout(1000);
  const afterOpen = (await flags(page)).length;
  expect((await flags(page)).every(Boolean)).toBe(true);

  await nextPage(page).click();
  await expect.poll(async () => (await flags(page)).length).toBe(afterOpen + 1);
  await expect(info(page)).toContainText("120 rows");
  await nextPage(page).click();
  await expect.poll(async () => (await flags(page)).length).toBe(afterOpen + 2);
  await expect(info(page)).toContainText("120 rows");
  await expect(page.locator(".grid-page-info")).toHaveText("Page 3 of 3");
  const paging = (await flags(page)).slice(afterOpen);
  expect(paging.length).toBeGreaterThanOrEqual(2);
  expect(paging.every((c) => c === false)).toBe(true);

  // Re-fetching the same page (what refresh does) counts.
  const before = (await flags(page)).length;
  await page.getByRole("button", { name: "Refresh now" }).click();
  await expect.poll(async () => (await flags(page)).length).toBeGreaterThan(before);
  expect((await flags(page)).slice(before).some(Boolean)).toBe(true);
});
