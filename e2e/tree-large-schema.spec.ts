import { test, expect, type Page } from "@playwright/test";
import AxeBuilder from "@axe-core/playwright";
import { connectToLocalPostgres } from "./helpers";

/**
 * The explorer tree with a schema of 2,000 tables: long child lists are
 * virtualised, yet every row stays reachable from the keyboard and the
 * filter, and moving focus re-renders only the rows involved.
 */

const TABLE_COUNT = 2000;
const tableName = (i: number) => `t_${String(i).padStart(4, "0")}`;

async function installLargeSchema(page: Page) {
  await page.addInitScript((n) => {
    const w = window as unknown as {
      __mockOverrides?: Record<string, unknown>;
      __REACT_DEVTOOLS_GLOBAL_HOOK__?: unknown;
      __renders?: number;
    };
    const tables = Array.from({ length: n }, (_, i) => ({
      name: `t_${String(i).padStart(4, "0")}`,
      schema: "public",
      table_type: "BASE TABLE",
      row_count_estimate: i,
      total_bytes: 8192,
    }));
    w.__mockOverrides = { ...(w.__mockOverrides ?? {}), list_tables: () => tables };

    // A minimal React DevTools hook that counts the components rendered in
    // each commit (fibers flagged PerformedWork, skipping untouched
    // subtrees the way DevTools does).
    w.__renders = 0;
    type Fiber = { child: Fiber | null; sibling: Fiber | null; alternate: Fiber | null; flags: number };
    const countRendered = (root: Fiber) => {
      let count = 0;
      const visit = (fiber: Fiber, mounting: boolean) => {
        for (let child = fiber.child; child; child = child.sibling) {
          const prev = child.alternate;
          if (child.flags & 1) count++;
          if (mounting || !prev) visit(child, true);
          else if (child.child !== prev.child) visit(child, false);
        }
      };
      visit(root, false);
      return count;
    };
    w.__REACT_DEVTOOLS_GLOBAL_HOOK__ = {
      supportsFiber: true,
      renderers: new Map(),
      inject: () => 1,
      checkDCE: () => {},
      onScheduleFiberRoot: () => {},
      onCommitFiberUnmount: () => {},
      onPostCommitFiberRoot: () => {},
      onCommitFiberRoot: (_id: number, root: { current: Fiber }) => {
        w.__renders = (w.__renders ?? 0) + countRendered(root.current);
      },
    };
  }, TABLE_COUNT);
}

const row = (page: Page, i: number) => page.locator(`[role='treeitem'][data-tree-id$=':${tableName(i)}']`);

async function openTables(page: Page) {
  await connectToLocalPostgres(page);
  await page.locator(".tree-node .tree-label", { hasText: /^postgres$/ }).click();
  await page.locator(".tree-node .tree-label", { hasText: /^public$/ }).click();
  await page.locator(".tree-node .tree-label", { hasText: /^Tables$/ }).click();
  await expect(row(page, 0)).toBeVisible({ timeout: 8000 });
}

test.describe("Explorer tree — large schema", () => {
  test.beforeEach(async ({ page }) => {
    await installLargeSchema(page);
    await page.goto("/");
    await page.locator(".tree-label", { hasText: "Local Postgres" }).waitFor({ timeout: 15000 });
    await openTables(page);
  });

  test("renders only a window of a long table list", async ({ page }) => {
    const rendered = await page.locator("[role='treeitem'][data-tree-id*=':t_']").count();
    expect(rendered).toBeGreaterThan(10);
    expect(rendered).toBeLessThan(300);
    await expect(row(page, 0)).toHaveAttribute("aria-setsize", String(TABLE_COUNT));
    await expect(row(page, 0)).toHaveAttribute("aria-posinset", "1");

    // The spacers standing in for unrendered rows keep the tree valid ARIA.
    await page.locator(".sidebar-content").evaluate((el) => (el.scrollTop = 20000));
    await expect(page.locator(".tree-virtual-gap[data-tree-gap]")).toHaveCount(2);
    const results = await new AxeBuilder({ page })
      .include("[role='tree']")
      .withRules(["aria-required-children", "aria-required-parent", "aria-valid-attr-value", "aria-allowed-attr", "nested-interactive"])
      .analyze();
    expect(results.violations.map((v) => `${v.id}: ${v.nodes.length}`)).toEqual([]);
  });

  test("keyboard navigation reaches every row of a virtualised list", async ({ page }) => {
    const first = row(page, 0);
    await first.focus();

    for (let i = 0; i < 60; i++) await page.keyboard.press("ArrowDown");
    await expect(row(page, 60)).toBeFocused();
    await expect(row(page, 60)).toBeInViewport();

    // Filter to the tables, so the long list ends the tree and End has to
    // reach a row that isn't rendered yet.
    await page.getByRole("textbox", { name: "Filter objects" }).fill("t_");
    await expect(page.locator(".tree-label", { hasText: /^analytics$/ })).toHaveCount(0);
    await expect(row(page, TABLE_COUNT - 1)).toHaveCount(0);
    await row(page, 60).focus();

    await page.keyboard.press("End");
    const last = row(page, TABLE_COUNT - 1);
    await expect(last).toBeFocused();
    await expect(last).toBeInViewport();
    await expect(last).toHaveAttribute("aria-posinset", String(TABLE_COUNT));
    await expect(last).toHaveAttribute("tabindex", "0");

    await page.keyboard.press("ArrowUp");
    await expect(row(page, TABLE_COUNT - 2)).toBeFocused();
    await expect(row(page, TABLE_COUNT - 2)).toBeInViewport();

    await page.keyboard.press("Home");
    await expect(page.getByRole("treeitem", { name: "Production folder" })).toBeFocused();

    // Exactly one tab stop survives the jumps.
    const tabStops = await page
      .getByRole("tree", { name: "Connections" })
      .evaluate((el) => el.querySelectorAll("[role='treeitem'][tabindex='0']").length);
    expect(tabStops).toBe(1);
  });

  test("scrolling far away keeps the focused row as the tab stop", async ({ page }) => {
    await row(page, 3).focus();
    await page.locator(".sidebar-content").evaluate((el) => (el.scrollTop = el.scrollHeight));
    await expect(row(page, TABLE_COUNT - 1)).toBeVisible();
    await expect(row(page, 3)).toHaveAttribute("tabindex", "0");
    await page.keyboard.press("ArrowDown");
    await expect(row(page, 4)).toBeFocused();
    await expect(row(page, 4)).toBeInViewport();
  });

  test("the filter finds a table deep in the list", async ({ page }) => {
    const deep = tableName(1987);
    await expect(row(page, 1987)).toHaveCount(0);
    await page.getByRole("textbox", { name: "Filter objects" }).fill(deep);
    await expect(row(page, 1987)).toBeVisible();
    await expect(page.locator("[role='treeitem'][data-tree-id*=':t_']")).toHaveCount(1);
    await expect(row(page, 1987)).toHaveAttribute("aria-setsize", "1");

    await page.getByRole("textbox", { name: "Filter objects" }).fill("");
    await expect(row(page, 0)).toBeVisible();
    await expect(row(page, 0)).toHaveAttribute("aria-setsize", String(TABLE_COUNT));
  });

  test("moving focus re-renders only the rows involved", async ({ page }) => {
    await row(page, 5).focus();
    await page.waitForTimeout(200);
    const renders = () => page.evaluate(() => (window as unknown as { __renders: number }).__renders);
    const before = await renders();
    const presses = 10;
    for (let i = 0; i < presses; i++) await page.keyboard.press("ArrowDown");
    await expect(row(page, 5 + presses)).toBeFocused();
    await page.waitForTimeout(200);
    const perPress = ((await renders()) - before) / presses;
    // Two rows (old and new focus) and their icons; re-rendering the list
    // would be thousands of components per key press.
    expect(perPress).toBeGreaterThan(0);
    expect(perPress).toBeLessThan(40);
  });
});
