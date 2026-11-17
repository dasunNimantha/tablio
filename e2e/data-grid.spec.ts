import { test, expect } from "@playwright/test";
import { openTable, navigateToTable } from "./helpers";

test.describe("Data grid — toolbar", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openTable(page, "users");
  });

  test("toolbar shows schema.table name", async ({ page }) => {
    await expect(page.locator(".tv-name")).toContainText("public.users");
  });

  test("filter button toggles filter bar", async ({ page }) => {
    await page.locator(".btn-ghost", { hasText: "Filter" }).click();
    await expect(page.locator(".filter-bar")).toBeVisible();
    await page.locator(".btn-ghost", { hasText: "Filter" }).click();
    await expect(page.locator(".filter-bar")).not.toBeVisible();
  });

  test("refresh button reloads data without error", async ({ page }) => {
    await page.locator(".btn-ghost[title='Refresh now']").click();
    await expect(page.locator(".tv-name")).toBeVisible();
    await expect(page.locator(".grid-error")).not.toBeVisible();
  });

  test("auto-refresh dropdown shows interval options", async ({ page }) => {
    await page.locator("button[title='Auto-refresh interval']").click();
    const menu = page.locator(".refresh-menu");
    await expect(menu).toBeVisible();
    await expect(menu.locator(".refresh-menu-item", { hasText: "Off" })).toBeVisible();
    await expect(menu.locator(".refresh-menu-item", { hasText: "5s" })).toBeVisible();
    await expect(menu.locator(".refresh-menu-item", { hasText: "10s" })).toBeVisible();
    await expect(menu.locator(".refresh-menu-item", { hasText: "30s" })).toBeVisible();
    await expect(menu.locator(".refresh-menu-item", { hasText: "1m" })).toBeVisible();
    await expect(menu.locator(".refresh-menu-item", { hasText: "5m" })).toBeVisible();
  });

  test("selecting auto-refresh interval shows active indicator", async ({ page }) => {
    await page.locator("button[title='Auto-refresh interval']").click();
    await page.locator(".refresh-menu-item", { hasText: "5s" }).click();
    const btn = page.locator("button[title='Auto-refresh interval']");
    await expect(btn).toHaveClass(/active-filter/);
    await expect(btn).toContainText("5s");
  });

  test("Query button opens query console tab", async ({ page }) => {
    // The Query button moved from the DataGrid toolbar to the
    // TableView header and dropped its `.btn-ghost` class
    // (the global rule was clobbering its height). Now it's the
    // dedicated `.tv-query-btn` in `.tv-header-right`.
    await page.locator(".tv-query-btn").click();
    await expect(page.locator(".query-console")).toBeVisible({ timeout: 5000 });
  });

  test("Columns button toggles column organizer", async ({ page }) => {
    await page.locator(".col-organizer-wrapper > button").click();
    await expect(page.locator(".col-organizer-dropdown")).toBeVisible();
    await page.locator(".col-organizer-wrapper > button").click();
    await expect(page.locator(".col-organizer-dropdown")).not.toBeVisible();
  });

  test("Explain button shows explain panel", async ({ page }) => {
    await page.locator(".btn-ghost", { hasText: "Explain" }).click();
    await expect(page.locator(".grid-explain-panel")).toBeVisible({ timeout: 5000 });
  });

  test("Export button opens export dropdown", async ({ page }) => {
    await page.locator(".export-menu-wrapper .btn-ghost").click();
    await expect(page.locator(".export-menu-dropdown")).toBeVisible();
  });

  test("Copy as SQL button exists and is clickable", async ({ page }) => {
    const btn = page.locator(".btn-ghost", { hasText: "Copy as SQL" });
    await expect(btn).toBeVisible();
    await btn.click();
  });

  test("Add Row adds a pinned top row", async ({ page }) => {
    await page.locator(".btn-ghost", { hasText: "Add Row" }).click();
    await expect(page.locator(".btn-discard")).toBeVisible();
    await expect(page.locator(".btn-primary", { hasText: "Save" })).toBeVisible();
  });

  test("Test Data generates a row", async ({ page }) => {
    await page.locator(".btn-ghost", { hasText: "Test Data" }).click();
    await expect(page.locator(".btn-discard")).toBeVisible();
  });
});

test.describe("Data grid — search", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openTable(page, "users");
  });

  test("Ctrl+F opens search bar", async ({ page }) => {
    await page.keyboard.press("Control+f");
    await expect(page.locator(".grid-search-bar")).toBeVisible();
  });

  test("search bar shows match count", async ({ page }) => {
    await page.keyboard.press("Control+f");
    await page.locator(".grid-search-input").fill("alice");
    await expect(page.locator(".grid-search-count")).toBeVisible();
    await expect(page.locator(".grid-search-count")).not.toContainText("No results");
  });

  test("search with no match shows 'No results'", async ({ page }) => {
    await page.keyboard.press("Control+f");
    await page.locator(".grid-search-input").fill("zzz_nonexistent_zzz");
    await expect(page.locator(".grid-search-count")).toContainText("No results");
  });

  test("search bar close via Escape", async ({ page }) => {
    await page.keyboard.press("Control+f");
    await expect(page.locator(".grid-search-bar")).toBeVisible();
    await page.locator(".grid-search-input").press("Escape");
    await expect(page.locator(".grid-search-bar")).not.toBeVisible();
  });

  test("search bar close via X button", async ({ page }) => {
    await page.keyboard.press("Control+f");
    await page.locator(".grid-search-close").click();
    await expect(page.locator(".grid-search-bar")).not.toBeVisible();
  });

  test("search nav buttons navigate matches", async ({ page }) => {
    await page.keyboard.press("Control+f");
    await page.locator(".grid-search-input").fill("example");
    await expect(page.locator(".grid-search-count")).not.toContainText("No results");
    const nextBtn = page.locator(".grid-search-nav-btn[title='Next match (Enter)']");
    await expect(nextBtn).toBeEnabled();
    await nextBtn.click();
  });

  test("search highlights matches and moves the current match", async ({ page }) => {
    await page.keyboard.press("Control+f");
    const grid = page.locator(".data-grid-container");
    await page.locator(".grid-search-input").fill("example");
    await expect(grid.locator(".cell-search-match").first()).toBeVisible();
    const current = grid.locator(".cell-search-current");
    const currentCell = () =>
      current.evaluate((el) => `${el.closest(".ag-row")?.getAttribute("row-index")}:${el.getAttribute("col-id")}`);
    await expect(current).toHaveCount(1);
    const first = await currentCell();
    await page.locator(".grid-search-nav-btn[title='Next match (Enter)']").click();
    await expect.poll(currentCell).not.toBe(first);
    await expect(current).toHaveCount(1);
    await page.locator(".grid-search-input").fill("");
    await expect(grid.locator(".cell-search-match")).toHaveCount(0);
  });

  test("repeated searching doesn't pile up queued grid cell updates", async ({ page }) => {
    // ag-grid-react queues a React state update per visible cell on every
    // forced refresh, and an unchanged cell never renders to clear it.
    await page.keyboard.press("Control+f");
    const input = page.locator(".grid-search-input");
    const next = page.locator(".grid-search-nav-btn[title='Next match (Enter)']");
    const pendingCellUpdates = () =>
      page.evaluate(() => {
        const root = document.getElementById("root")!;
        const key = Object.keys(root).find((k) => k.startsWith("__reactContainer$"))!;
        let total = 0;
        const stack = [(root as any)[key].stateNode.current];
        while (stack.length) {
          const fiber = stack.pop();
          if (fiber.type?.name === "CellComp") {
            for (let hook = fiber.memoizedState; hook; hook = hook.next) {
              const last = hook.queue?.pending;
              if (!last) continue;
              let u = last;
              do { total++; u = u.next; } while (u !== last);
            }
          }
          if (fiber.child) stack.push(fiber.child);
          if (fiber.sibling) stack.push(fiber.sibling);
        }
        return total;
      });

    await input.fill("example");
    await expect(next).toBeEnabled();
    const before = await pendingCellUpdates();
    for (let i = 0; i < 20; i++) {
      await next.click();
      await input.fill(i % 2 ? "example" : "exampl");
    }
    await expect(next).toBeEnabled();
    const after = await pendingCellUpdates();
    // Before the fix this grew by one per visible cell per refresh
    // (thousands here); allow a little slack for cells that re-render.
    expect(after - before).toBeLessThan(200);
  });
});

test.describe("Data grid — pagination", () => {
  test("shows total row count", async ({ page }) => {
    await page.goto("/");
    await openTable(page, "users");
    await expect(page.locator(".grid-pagination-info")).toContainText("rows");
  });

  test("pagination controls are present", async ({ page }) => {
    await page.goto("/");
    await openTable(page, "users");
    await expect(page.locator(".grid-pagination-controls")).toBeVisible();
  });
});

test.describe("Data grid — row context menu", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openTable(page, "users");
  });

  test("right-click on row shows context menu", async ({ page }) => {
    const firstDataCell = page.locator(".ag-cell").first();
    await firstDataCell.waitFor({ timeout: 5000 });
    await firstDataCell.click({ button: "right" });
    const menu = page.locator(".context-menu");
    await expect(menu).toBeVisible();
    await expect(menu.locator("button", { hasText: "View as JSON" })).toBeVisible();
    await expect(menu.locator("button", { hasText: "Copy row as JSON" })).toBeVisible();
    await expect(menu.locator("button", { hasText: "Copy as INSERT" })).toBeVisible();
  });

  test("View as JSON opens row detail panel", async ({ page }) => {
    const firstDataCell = page.locator(".ag-cell").first();
    await firstDataCell.waitFor({ timeout: 5000 });
    await firstDataCell.click({ button: "right" });
    await page.locator(".context-menu button", { hasText: "View as JSON" }).click();
    await expect(page.locator(".row-detail-panel")).toBeVisible();
  });
});

test.describe("Data grid — save and discard", () => {
  test("save and discard buttons appear after adding a row", async ({ page }) => {
    await page.goto("/");
    await openTable(page, "users");
    await page.locator(".btn-ghost", { hasText: "Add Row" }).click();
    await expect(page.locator(".grid-toolbar-changes")).toBeVisible();
    await expect(page.locator(".btn-discard")).toBeVisible();
    await expect(page.locator(".btn-primary", { hasText: "Save" })).toBeVisible();
  });

  test("discard removes pending changes", async ({ page }) => {
    await page.goto("/");
    await openTable(page, "users");
    await page.locator(".btn-ghost", { hasText: "Add Row" }).click();
    await page.locator(".btn-discard").click();
    await expect(page.locator(".grid-toolbar-changes")).not.toBeVisible();
  });
});

test.describe("Data grid — filter bar integration", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openTable(page, "users");
    await page.locator(".btn-ghost", { hasText: "Filter" }).click();
    await page.locator(".filter-bar").waitFor();
  });

  test("filter bar opens with one condition", async ({ page }) => {
    await expect(page.locator(".filter-condition")).toHaveCount(1);
  });

  test("add condition button adds another row", async ({ page }) => {
    await page.locator(".filter-actions .btn-ghost", { hasText: "Add" }).click();
    await expect(page.locator(".filter-condition")).toHaveCount(2);
  });

  test("AND/OR toggle switches join type", async ({ page }) => {
    await page.locator(".filter-actions .btn-ghost", { hasText: "Add" }).click();
    const joinBtn = page.locator(".filter-join-btn");
    await expect(joinBtn).toHaveText("AND");
    await joinBtn.click();
    await expect(joinBtn).toHaveText("OR");
  });

  test("apply button applies filter", async ({ page }) => {
    await page.locator(".filter-value-input").fill("alice");
    await page.locator(".filter-actions .btn-primary", { hasText: "Apply" }).click();
    const filterBtn = page.locator(".btn-ghost", { hasText: "Filter" });
    await expect(filterBtn).toContainText("(active)");
  });

  test("clear button resets filter", async ({ page }) => {
    await page.locator(".filter-value-input").fill("alice");
    await page.locator(".filter-actions .btn-primary", { hasText: "Apply" }).click();
    await page.locator(".filter-actions .btn-ghost", { hasText: "Clear" }).click();
    const filterBtn = page.locator(".btn-ghost", { hasText: "Filter" });
    await expect(filterBtn).not.toContainText("(active)");
  });

  test("close button hides filter bar", async ({ page }) => {
    await page.locator(".filter-actions .btn-icon[title='Close filter bar']").click();
    await expect(page.locator(".filter-bar")).not.toBeVisible();
  });

  test("Enter key applies filter from value input", async ({ page }) => {
    await page.locator(".filter-value-input").fill("bob");
    await page.locator(".filter-value-input").press("Enter");
    const filterBtn = page.locator(".btn-ghost", { hasText: "Filter" });
    await expect(filterBtn).toContainText("(active)");
  });
});
