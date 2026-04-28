import { test, expect } from "@playwright/test";
import { openTableStats } from "./helpers";

/**
 * Statistics is now a sub-tab inside the Schema view rather than a
 * top-level "View Stats" tab. The internal layout (row count card,
 * total size card, vacuum/analyze cards, storage breakdown chart,
 * tuple-health donut) hasn't changed — we just removed its standalone
 * toolbar (`embedded` mode) when it sits inside SchemaPage.
 */
test.describe("Table view — Statistics sub-tab", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openTableStats(page, "users");
  });

  test("renders the stats panel inside the Schema sub-tab strip", async ({ page }) => {
    await expect(page.locator(".tv-schema-strip")).toBeVisible();
    await expect(
      page.locator(".tv-schema-tab", { hasText: "Statistics" })
    ).toHaveClass(/active/);
    await expect(page.locator(".table-stats")).toBeVisible();
  });

  test("embedded stats hides its standalone toolbar", async ({ page }) => {
    // Embedded mode in SchemaPage suppresses the inline schema.table
    // toolbar so we don't render the same identity twice.
    await expect(page.locator(".table-stats-toolbar")).toHaveCount(0);
  });

  test("shows the four headline cards: Row Count, Total Size, Vacuum, Analyze", async ({ page }) => {
    const cards = page.locator(".table-stats-card");
    expect(await cards.count()).toBeGreaterThanOrEqual(4);
    await expect(
      page.locator(".table-stats-label", { hasText: "Row Count" })
    ).toBeVisible();
    await expect(
      page.locator(".table-stats-label", { hasText: "Total Size" })
    ).toBeVisible();
    await expect(
      page.locator(".table-stats-label", { hasText: "Last Vacuum" })
    ).toBeVisible();
    await expect(
      page.locator(".table-stats-label", { hasText: "Last Analyze" })
    ).toBeVisible();
  });

  test("vacuum/analyze values are either '—' or a relative time label", async ({ page }) => {
    // The mock returns ISO timestamps, so we expect the relative-time
    // formatter to render them as one of: "just now", "Ns ago",
    // "Nm ago", "Nh ago", "Nd ago", or a calendar fallback. An empty
    // mock value renders as "—". The em-dash itself is a single char.
    const value = page.locator(".table-stats-value-time").first();
    if ((await value.count()) > 0) {
      const txt = (await value.textContent())?.trim() ?? "";
      expect(
        txt === "—" || /^just now$|ago$|^[A-Za-z]+\s\d+$/.test(txt),
        `unexpected vacuum/analyze label: '${txt}'`
      ).toBe(true);
    }
  });

  test("storage breakdown panel renders at least one size bar", async ({ page }) => {
    await expect(
      page.locator(".stats-chart-panel", { hasText: "Storage Breakdown" })
    ).toBeVisible();
    expect(await page.locator(".stats-bar-row").count()).toBeGreaterThanOrEqual(1);
  });

  test("tuple health donut renders with Live and Dead legend entries", async ({ page }) => {
    await expect(
      page.locator(".stats-chart-panel", { hasText: "Tuple Health" })
    ).toBeVisible();
    await expect(page.locator(".stats-donut")).toBeVisible();
    await expect(
      page.locator(".stats-legend-item", { hasText: "Live" })
    ).toBeVisible();
    await expect(
      page.locator(".stats-legend-item", { hasText: "Dead" })
    ).toBeVisible();
  });
});
