import { test, expect } from "@playwright/test";
import { openAlterTable, openStructureView } from "./helpers";

test.describe("Alter table dialog", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openAlterTable(page, "users");
  });

  test("opens via context menu and shows dialog", async ({ page }) => {
    await expect(page.locator(".alter-table-dialog")).toBeVisible();
    await expect(page.locator(".dialog-header h2")).toContainText("Alter Table");
  });

  test("loads existing columns", async ({ page }) => {
    const rows = page.locator(".alter-table-column-row");
    await rows.first().waitFor({ timeout: 5000 });
    const count = await rows.count();
    expect(count).toBeGreaterThanOrEqual(2);
  });

  test("shows summary section", async ({ page }) => {
    await expect(page.locator(".alter-table-summary")).toBeVisible();
    await expect(page.locator(".alter-table-summary")).toContainText("users");
  });

  test("shows existing columns badge", async ({ page }) => {
    await expect(page.locator(".alter-table-badge").first()).toBeVisible();
    await expect(page.locator(".alter-table-badge").first()).toContainText("existing column");
  });

  test("add column button adds a new row", async ({ page }) => {
    await page.locator(".alter-table-column-row").first().waitFor({ timeout: 5000 });
    const initialCount = await page.locator(".alter-table-column-row").count();
    await page.locator(".alter-table-add-btn").click();
    await expect(page.locator(".alter-table-column-row")).toHaveCount(initialCount + 1);
    await expect(page.locator(".alter-table-column-row.new-column")).toBeVisible();
  });

  test("preview SQL toggle shows ALTER statements", async ({ page }) => {
    await page.locator(".alter-table-add-btn").click();
    await page.locator(".btn-ghost", { hasText: "Preview SQL" }).click();
    await expect(page.locator(".ddl-preview")).toBeVisible();
  });

  test("preview SQL toggle hides ALTER statements", async ({ page }) => {
    await page.locator(".alter-table-add-btn").click();
    await page.locator(".btn-ghost", { hasText: "Preview SQL" }).click();
    await expect(page.locator(".ddl-preview")).toBeVisible();
    await page.locator(".btn-ghost", { hasText: "Hide SQL" }).click();
    await expect(page.locator(".ddl-preview")).not.toBeVisible();
  });

  test("cancel button closes dialog", async ({ page }) => {
    await page.locator(".dialog-footer .btn-secondary", { hasText: "Cancel" }).click();
    await expect(page.locator(".alter-table-dialog")).not.toBeVisible();
  });

  test("apply button is present and disabled without changes", async ({ page }) => {
    const applyBtn = page.locator(".dialog-footer .btn-primary", { hasText: "Apply Changes" });
    await expect(applyBtn).toBeVisible();
    await expect(applyBtn).toBeDisabled();
  });

  test("PK badges shown on primary key columns", async ({ page }) => {
    await page.locator(".alter-table-column-row").first().waitFor({ timeout: 5000 });
    await expect(page.locator(".alter-table-pk-badge").first()).toBeVisible();
  });

  test("drop column button marks column as dropped", async ({ page }) => {
    await page.locator(".alter-table-column-row").first().waitFor({ timeout: 5000 });
    const dropBtn = page.locator(".drop-column-btn").first();
    await dropBtn.click();
    await expect(page.locator(".alter-table-column-row.dropped").first()).toBeVisible();
  });

  test("type cell uses a themed CustomSelect — single click opens the dropdown", async ({ page }) => {
    // Regression: the type column used to hide behind a
    // double-click → reveal-a-<select> dance. `<select autoFocus>`
    // doesn't open the native dropdown either, so the user got
    // stuck. We now render a CustomSelect which (a) is visible
    // up-front and (b) uses theme CSS variables instead of the
    // native dropdown's hardcoded colors.
    await page.locator(".alter-table-column-row").first().waitFor({ timeout: 5000 });
    const trigger = page
      .locator(
        ".alter-table-column-row:not(.new-column) .alter-table-type-select .cs-trigger",
      )
      .first();
    await expect(trigger).toBeVisible();
    await expect(trigger).toBeEnabled();
    // A single click opens the themed popover.
    await trigger.click();
    await expect(page.locator(".cs-dropdown").first()).toBeVisible();
  });
});

/**
 * Inline editor in the Schema view's Columns sub-tab (issue #59).
 *
 * The editor body is mounted via `AlterTableEditor variant="inline"`
 * — same selectors as the modal (`.alter-table-column-row`,
 * `.alter-table-add-btn`, `.new-column`, `.ddl-preview`) — wrapped
 * in a sticky-toolbar shell. Tests below confirm:
 *
 *  1. View mode renders the read-only `ColumnsPanel`.
 *  2. The Edit button is discoverable in the Columns header.
 *  3. Clicking Edit swaps to the in-tab editor.
 *  4. Add Column / Preview / Discard all work the same as in the modal.
 *  5. Discard wipes the persisted draft (no leak between sessions).
 */
test.describe("Alter table — inline editor in Schema view", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openStructureView(page, "users");
    // Columns sub-tab is the default landing tab; just wait for the
    // read-only panel to render.
    await page.locator(".tv-table").waitFor({ timeout: 8000 });
  });

  test("renders the read-only ColumnsPanel by default (View mode)", async ({ page }) => {
    await expect(page.locator(".tv-table")).toBeVisible();
    // The in-tab editor must NOT be visible yet.
    await expect(page.locator(".alter-table-editor-inline")).not.toBeVisible();
  });

  test("Edit button is visible in the Columns header", async ({ page }) => {
    const editBtn = page.locator('[data-testid="schema-edit-btn"]');
    await expect(editBtn).toBeVisible();
    await expect(editBtn).toContainText("Edit");
  });

  test("clicking Edit swaps in the in-tab editor with column rows", async ({ page }) => {
    await page.locator('[data-testid="schema-edit-btn"]').click();
    await expect(page.locator(".alter-table-editor-inline")).toBeVisible();
    // Same class as the modal so existing selectors continue to work.
    const rows = page.locator(".alter-table-editor-inline .alter-table-column-row");
    await rows.first().waitFor({ timeout: 5000 });
    expect(await rows.count()).toBeGreaterThanOrEqual(1);
    // Inline toolbar visible (variant="inline" path).
    await expect(page.locator(".alter-table-inline-toolbar")).toBeVisible();
    // Modal chrome must NOT have leaked in.
    await expect(page.locator(".alter-table-dialog")).not.toBeVisible();
  });

  test("Add Column appends a new-column row inside the inline editor", async ({ page }) => {
    await page.locator('[data-testid="schema-edit-btn"]').click();
    await page.locator(".alter-table-editor-inline .alter-table-column-row").first().waitFor();
    const before = await page
      .locator(".alter-table-editor-inline .alter-table-column-row")
      .count();
    await page.locator(".alter-table-editor-inline .alter-table-add-btn").click();
    await expect(
      page.locator(".alter-table-editor-inline .alter-table-column-row"),
    ).toHaveCount(before + 1);
    await expect(
      page.locator(".alter-table-editor-inline .alter-table-column-row.new-column"),
    ).toBeVisible();
  });

  test("Preview SQL toggle inside the inline editor reveals the SQL pane", async ({ page }) => {
    await page.locator('[data-testid="schema-edit-btn"]').click();
    await page.locator(".alter-table-editor-inline .alter-table-add-btn").click();
    await page
      .locator(".alter-table-inline-toolbar .btn-ghost", { hasText: "Preview SQL" })
      .click();
    await expect(page.locator(".alter-table-editor-inline .ddl-preview")).toBeVisible();
  });

  test("Discard flips the panel back to read-only View mode", async ({ page }) => {
    await page.locator('[data-testid="schema-edit-btn"]').click();
    await expect(page.locator(".alter-table-editor-inline")).toBeVisible();
    // Add a row so we exercise the "discard clears the draft" path.
    await page.locator(".alter-table-editor-inline .alter-table-add-btn").click();
    await page
      .locator(".alter-table-inline-toolbar .btn-secondary", { hasText: "Discard" })
      .click();
    // Back to View mode — read-only panel is mounted again.
    await expect(page.locator(".tv-table")).toBeVisible();
    await expect(page.locator(".alter-table-editor-inline")).not.toBeVisible();
    // Re-entering Edit mode must NOT show the pending-add row (the
    // store was cleared on Discard).
    await page.locator('[data-testid="schema-edit-btn"]').click();
    await expect(
      page.locator(".alter-table-editor-inline .alter-table-column-row.new-column"),
    ).toHaveCount(0);
  });
});
