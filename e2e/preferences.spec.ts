import { test, expect, type Page } from "@playwright/test";
import { openTable } from "./helpers";

/**
 * Preferences dialog (issue #62).
 *
 * Verifies the full pipeline: open dialog → change a setting →
 * the corresponding CSS variable on `documentElement` updates →
 * the value survives a page reload (localStorage persistence).
 *
 * The dialog mirrors the Connection Dialog layout (left nav + one
 * `.connection-section-card` at a time on the right) so selectors
 * dig through that scaffold rather than referencing a flat list of
 * sections.
 */

const SETTINGS_KEY = "tablio-user-settings";

async function openPreferencesDialog(page: Page) {
  await page.locator("button[title='Settings']").click();
  await page.locator('[data-testid="preferences-menu-item"]').click();
  await page.locator(".preferences-dialog").waitFor({ timeout: 4000 });
}

async function readCssVar(page: Page, name: string): Promise<string> {
  return page.evaluate(
    ([n]) => document.documentElement.style.getPropertyValue(n).trim(),
    [name],
  );
}

async function gotoInterface(page: Page) {
  await page.locator('[data-testid="preferences-nav-interface"]').click();
  await page
    .locator(".connection-section-heading h3", { hasText: /interface font/i })
    .waitFor({ timeout: 2000 });
}

async function gotoEditor(page: Page) {
  await page.locator('[data-testid="preferences-nav-editor"]').click();
  await page
    .locator(".connection-section-heading h3", { hasText: /sql editor font/i })
    .waitFor({ timeout: 2000 });
}

test.describe("Preferences dialog (#62)", () => {
  test.beforeEach(async ({ page }) => {
    // Reset user-settings localStorage before navigating so the
    // first paint starts from defaults.
    await page.goto("/");
    await page.evaluate(
      (key) => window.localStorage.removeItem(key),
      SETTINGS_KEY,
    );
    await page.reload();
  });

  test("opens via the Settings gear → Preferences menu entry", async ({ page }) => {
    await openPreferencesDialog(page);
    await expect(page.locator(".preferences-dialog h2")).toHaveText(
      "Preferences",
    );
    // Left-rail nav has both sections.
    await expect(
      page.locator('[data-testid="preferences-nav-interface"]'),
    ).toBeVisible();
    await expect(
      page.locator('[data-testid="preferences-nav-editor"]'),
    ).toBeVisible();
    // Interface section is the default active view.
    await expect(
      page.locator(".connection-section-heading h3"),
    ).toContainText(/interface font/i);
  });

  test("clicking the SQL editor nav swaps the visible section", async ({ page }) => {
    await openPreferencesDialog(page);
    await gotoEditor(page);
    await expect(
      page.locator(".connection-section-heading h3"),
    ).toContainText(/sql editor font/i);
  });

  test("changing the UI font size writes --ui-font-size on documentElement", async ({ page }) => {
    await openPreferencesDialog(page);
    await gotoInterface(page);
    const uiSizeInput = page.locator(".preferences-size-input");
    await uiSizeInput.fill("16");
    await uiSizeInput.blur();
    await expect
      .poll(() => readCssVar(page, "--ui-font-size"))
      .toBe("16px");
  });

  test("changing the editor font size writes --editor-font-size on documentElement", async ({ page }) => {
    await openPreferencesDialog(page);
    await gotoEditor(page);
    const editorSizeInput = page.locator(".preferences-size-input");
    await editorSizeInput.fill("18");
    await editorSizeInput.blur();
    await expect
      .poll(() => readCssVar(page, "--editor-font-size"))
      .toBe("18px");
  });

  test("changing the UI font family writes --font-sans on documentElement", async ({ page }) => {
    await openPreferencesDialog(page);
    await gotoInterface(page);
    await page.locator(".preferences-font-select .cs-trigger").click();
    // Pick the bundled "Fira Sans" option — always present even if
    // document.fonts.check is flaky.
    await page
      .locator(".cs-dropdown .cs-option", { hasText: "Fira Sans" })
      .first()
      .click();
    await expect
      .poll(() => readCssVar(page, "--font-sans"))
      .toContain("Fira Sans");
  });

  test("Reset to defaults clears family overrides and restores sizes", async ({ page }) => {
    await openPreferencesDialog(page);
    await gotoInterface(page);
    const uiSizeInput = page.locator(".preferences-size-input");
    await uiSizeInput.fill("17");
    await uiSizeInput.blur();
    await expect
      .poll(() => readCssVar(page, "--ui-font-size"))
      .toBe("17px");

    await page.locator(".preferences-reset-btn").click();

    await expect
      .poll(() => readCssVar(page, "--ui-font-size"))
      .toBe("13px");
    await expect
      .poll(() => readCssVar(page, "--font-sans"))
      .toBe("");
  });

  test("settings persist across a page reload (localStorage round-trip)", async ({ page }) => {
    await openPreferencesDialog(page);
    await gotoEditor(page);
    const editorSizeInput = page.locator(".preferences-size-input");
    await editorSizeInput.fill("20");
    await editorSizeInput.blur();
    await expect
      .poll(() => readCssVar(page, "--editor-font-size"))
      .toBe("20px");

    // The store debounces persistence by 300ms — wait for it to
    // flush before reloading.
    await page.waitForTimeout(400);
    await page.reload();

    await expect
      .poll(() => readCssVar(page, "--editor-font-size"))
      .toBe("20px");
  });

  test("Done button closes the dialog", async ({ page }) => {
    await openPreferencesDialog(page);
    await page
      .locator(".preferences-dialog .btn-primary", { hasText: "Done" })
      .click();
    await expect(page.locator(".preferences-dialog")).not.toBeVisible();
  });

  test("live preview strip reflects the chosen UI font size", async ({ page }) => {
    // Confirms the dialog gives the user immediate visual feedback
    // before they close — the preview reads from the same CSS
    // variable the rest of the UI does, so seeing it change is a
    // proxy for the whole pipeline being wired up.
    await openPreferencesDialog(page);
    await gotoInterface(page);
    const uiSizeInput = page.locator(".preferences-size-input");
    await uiSizeInput.fill("17");
    await uiSizeInput.blur();
    const preview = page.locator('[data-testid="preferences-ui-preview"]');
    await expect
      .poll(async () =>
        preview.evaluate((el) =>
          parseInt(window.getComputedStyle(el).fontSize, 10),
        ),
      )
      .toBe(17);
  });

  test("live preview strip reflects the chosen editor font size", async ({ page }) => {
    await openPreferencesDialog(page);
    await gotoEditor(page);
    const editorSizeInput = page.locator(".preferences-size-input");
    await editorSizeInput.fill("19");
    await editorSizeInput.blur();
    const preview = page.locator(
      '[data-testid="preferences-editor-preview"]',
    );
    await expect
      .poll(async () =>
        preview.evaluate((el) =>
          parseInt(window.getComputedStyle(el).fontSize, 10),
        ),
      )
      .toBe(19);
  });

  test("editor font size flows end-to-end into AG Grid result cells", async ({ page }) => {
    // The "editor font" preference is supposed to govern every
    // monospace surface — Monaco editors AND the result grid.
    // This test exercises the longest wiring path: store → CSS
    // var → ag-grid-theme.css rule → computed font-size on a
    // real .ag-cell DOM element.
    await openPreferencesDialog(page);
    await gotoEditor(page);
    const editorSizeInput = page.locator(".preferences-size-input");
    await editorSizeInput.fill("18");
    await editorSizeInput.blur();
    await expect
      .poll(() => readCssVar(page, "--editor-font-size"))
      .toBe("18px");
    // Close the dialog and open a real data grid view.
    await page
      .locator(".preferences-dialog .btn-primary", { hasText: "Done" })
      .click();
    await openTable(page, "users");
    // Target a real data cell by `col-id` so we skip AG Grid's
    // row-number gutter cell (which renders at a fixed smaller
    // font and isn't covered by the editor-font setting).
    const idCell = page
      .locator('.ag-cell[col-id="id"], .ag-cell[col-id="ID"]')
      .first();
    await idCell.waitFor({ timeout: 8000 });
    await expect
      .poll(async () =>
        idCell.evaluate((el) =>
          parseInt(window.getComputedStyle(el).fontSize, 10),
        ),
      )
      .toBe(18);
  });
});
