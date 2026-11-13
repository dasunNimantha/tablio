import { test, expect, type Page } from "@playwright/test";
import AxeBuilder from "@axe-core/playwright";
import {
  openBackupRestore,
  openConnectionDialog,
  openCreateTable,
  openDumpRestore,
  openImportDialog,
} from "./helpers";

/**
 * Automated accessibility checks (axe-core) for the app shell and its
 * dialogs in both default themes. The data grid and Monaco editor are
 * excluded: they're third-party widgets with their own a11y models.
 *
 * Rules that must stay at zero everywhere: names for buttons, form
 * fields and selects, valid ARIA, and text contrast.
 */
const RULES = [
  "button-name",
  "label",
  "select-name",
  "aria-required-children",
  "aria-required-parent",
  "aria-valid-attr-value",
  "aria-allowed-attr",
  "aria-dialog-name",
  "nested-interactive",
  "color-contrast",
];

async function expectNoViolations(page: Page, screen: string, include?: string) {
  // Popovers and dialogs fade in; measuring contrast mid-animation reports
  // the half-transparent colours.
  await page.waitForFunction(() =>
    document.getAnimations().every((a) => a.playState !== "running" || a.effect?.getTiming().iterations === Infinity),
  );
  let builder = new AxeBuilder({ page })
    .withRules(RULES)
    .exclude(".ag-root-wrapper")
    .exclude(".monaco-editor");
  if (include) builder = builder.include(include);
  const { violations } = await builder.analyze();
  const summary = violations.map(
    (v) => `${v.id} (${v.nodes.length}): ${v.nodes.slice(0, 3).map((n) => n.target.join(" ")).join(" | ")}`,
  );
  expect(summary, `axe violations on ${screen}`).toEqual([]);
}

for (const theme of ["dark", "light"]) {
  test.describe(`axe — ${theme} theme`, () => {
    test.beforeEach(async ({ page }) => {
      await page.addInitScript((t) => localStorage.setItem("tablio-theme", t), theme);
      await page.goto("/");
      await page.locator(".tree-label", { hasText: "Local Postgres" }).waitFor({ timeout: 15000 });
    });

    test("main window, explorer and menus", async ({ page }) => {
      await expectNoViolations(page, "main window");
      await page.locator(".tree-label", { hasText: "Local Postgres" }).dblclick();
      await page.locator(".tree-node .tree-label", { hasText: /^postgres$/ }).waitFor({ timeout: 8000 });
      await page.locator(".tree-node .tree-label", { hasText: /^postgres$/ }).click();
      await page.locator(".tree-node .tree-label", { hasText: /^public$/ }).click();
      await page.locator(".tree-node .tree-label", { hasText: /^Tables$/ }).click();
      await page.locator(".tree-node:has(.icon-table)").first().waitFor({ timeout: 8000 });
      await expectNoViolations(page, "expanded explorer", ".sidebar");
      await page.locator(".tree-label", { hasText: "Local Postgres" }).click({ button: "right" });
      await expectNoViolations(page, "connection context menu", ".context-menu");
      await page.keyboard.press("Escape");
      await page.getByRole("button", { name: "Change theme" }).click();
      await expectNoViolations(page, "theme menu", ".theme-picker-popover");
      await page.keyboard.press("Escape");
    });

    test("connection dialog sections", async ({ page }) => {
      await openConnectionDialog(page);
      for (const section of ["general", "authentication", "security", "ssh"]) {
        await page.locator(`.connection-nav-item[data-section=${section}]`).click();
        if (section === "ssh") {
          await page.getByRole("checkbox", { name: "Use an SSH tunnel" }).check({ force: true });
        }
        await expectNoViolations(page, `connection dialog: ${section}`, ".dialog");
      }
      await page.locator(".connection-nav-item[data-section=general]").click();
      await page.getByRole("button", { name: "Save" }).click();
      await expectNoViolations(page, "connection dialog: validation errors", ".dialog");
    });

    test("preferences, shortcuts and known hosts", async ({ page }) => {
      await page.getByRole("button", { name: "Settings" }).click();
      await page.getByRole("menuitem", { name: "Preferences" }).click();
      await expectNoViolations(page, "preferences", ".dialog");
      await page.keyboard.press("Escape");
      await page.getByRole("button", { name: "Keyboard shortcuts" }).click();
      await expectNoViolations(page, "keyboard shortcuts", ".dialog");
      await page.keyboard.press("Escape");
      await page.getByRole("button", { name: "Settings" }).click();
      await page.getByRole("menuitem", { name: "SSH known hosts" }).click();
      await expectNoViolations(page, "known hosts", ".known-hosts-dialog");
    });

    test("table dialogs", async ({ page }) => {
      await openCreateTable(page);
      await expectNoViolations(page, "create table", ".dialog");
      await page.keyboard.press("Escape");
      await page.reload();
      await openImportDialog(page, "users");
      await expectNoViolations(page, "import", ".dialog");
      await page.keyboard.press("Escape");
    });

    test("backup and dump dialogs", async ({ page }) => {
      await openBackupRestore(page);
      await expectNoViolations(page, "backup & restore", ".dialog");
      await page.keyboard.press("Escape");
      await page.reload();
      await openDumpRestore(page);
      await expectNoViolations(page, "dump & restore", ".dialog");
    });
  });
}
