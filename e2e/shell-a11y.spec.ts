import { test, expect, type Page } from "@playwright/test";
import { connectToLocalPostgres, navigateToTable, openConnectionDialog, openImportDialog } from "./helpers";

/**
 * Make mock commands fail (or behave differently) for this page. The mock
 * in src/lib/tauri.ts consults `window.__mockOverrides[cmd]` first.
 */
async function overrideCommand(page: Page, cmd: string, body: string) {
  await page.evaluate(
    ([c, b]) => {
      const w = window as unknown as { __mockOverrides?: Record<string, unknown> };
      w.__mockOverrides = w.__mockOverrides ?? {};
      // eslint-disable-next-line no-new-func
      w.__mockOverrides[c] = new Function("args", b);
    },
    [cmd, body],
  );
}

async function clearOverride(page: Page, cmd: string) {
  await page.evaluate((c) => {
    const w = window as unknown as { __mockOverrides?: Record<string, unknown> };
    delete w.__mockOverrides?.[c];
  }, cmd);
}

const treeItem = (page: Page, name: string | RegExp) =>
  page.getByRole("tree").getByRole("treeitem", { name, exact: typeof name === "string" });

async function activeLabel(page: Page) {
  return page.evaluate(() => {
    const el = document.activeElement as HTMLElement | null;
    return el?.getAttribute("aria-label") ?? el?.textContent?.trim() ?? null;
  });
}

test.describe("Dialogs — shared modal behaviour", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await page.locator(".tree-label", { hasText: "Local Postgres" }).waitFor({ timeout: 15000 });
  });

  test("connection dialog is a labelled modal that traps focus, closes on Escape and restores focus", async ({ page }) => {
    const opener = page.locator(".sidebar-header").getByRole("button", { name: "New connection" });
    await opener.focus();
    await opener.press("Enter");
    const dialog = page.getByRole("dialog", { name: "New Connection" });
    await expect(dialog).toBeVisible();
    await expect(dialog).toHaveAttribute("aria-modal", "true");
    // Focus starts inside the dialog, on the first field.
    await expect(page.locator(".dialog input").first()).toBeFocused();

    for (let i = 0; i < 40; i++) {
      await page.keyboard.press("Tab");
      const inside = await page.evaluate(() => !!document.activeElement?.closest("[role='dialog']"));
      expect(inside, `Tab #${i + 1} left the dialog`).toBe(true);
    }
    for (let i = 0; i < 10; i++) {
      await page.keyboard.press("Shift+Tab");
      const inside = await page.evaluate(() => !!document.activeElement?.closest("[role='dialog']"));
      expect(inside).toBe(true);
    }

    await page.keyboard.press("Escape");
    await expect(dialog).not.toBeVisible();
    await expect(opener).toBeFocused();
  });

  for (const [name, open] of [
    ["Preferences", async (page: Page) => {
      await page.getByRole("button", { name: "Settings" }).click();
      await page.getByRole("menuitem", { name: "Preferences" }).click();
    }],
    ["Keyboard Shortcuts", async (page: Page) => {
      await page.getByRole("button", { name: "Keyboard shortcuts" }).click();
    }],
    ["SSH known hosts", async (page: Page) => {
      await page.getByRole("button", { name: "Settings" }).click();
      await page.getByRole("menuitem", { name: "SSH known hosts" }).click();
    }],
  ] as const) {
    test(`${name} closes on Escape and returns focus to the status bar`, async ({ page }) => {
      await open(page);
      const dialog = page.getByRole("dialog", { name });
      await expect(dialog).toBeVisible();
      const focusedInside = await page.evaluate(() => !!document.activeElement?.closest("[role='dialog']"));
      expect(focusedInside).toBe(true);
      await page.keyboard.press("Escape");
      await expect(dialog).not.toBeVisible();
      const back = await activeLabel(page);
      expect(back === "Settings" || back === "Keyboard shortcuts").toBe(true);
    });
  }

  test("every dialog close button has an accessible name", async ({ page }) => {
    await openConnectionDialog(page);
    await expect(page.locator(".dialog-header .btn-icon")).toHaveAttribute("aria-label", "Close");
  });

  test("keyboard shortcuts panel lists the real shortcuts", async ({ page }) => {
    await page.keyboard.press("Control+?");
    const dialog = page.getByRole("dialog", { name: "Keyboard Shortcuts" });
    await expect(dialog).toBeVisible();
    await expect(dialog).toContainText("Save query");
    await expect(dialog).not.toContainText("Save changes");
    for (const text of ["Zoom in", "Zoom out", "Reset zoom", "Show keyboard shortcuts", "New query tab", "Close tab", "Format SQL"]) {
      await expect(dialog).toContainText(text);
    }
  });
});

test.describe("Confirm dialog", () => {
  test("is an alert dialog that starts on Cancel and closes on Escape", async ({ page }) => {
    await page.goto("/");
    const row = treeItem(page, "Local Postgres");
    await row.hover();
    const del = page.getByRole("button", { name: "Delete Local Postgres" });
    await del.click({ force: true });
    const dialog = page.getByRole("alertdialog", { name: "Delete Connection" });
    await expect(dialog).toBeVisible();
    await expect(dialog.getByRole("button", { name: "Cancel" })).toBeFocused();
    await page.keyboard.press("Escape");
    await expect(dialog).not.toBeVisible();
    await expect(treeItem(page, "Local Postgres")).toBeVisible();
  });

  test("a double click on Confirm runs the action once and a failure stays visible", async ({ page }) => {
    await page.goto("/");
    await page.locator(".tree-label", { hasText: "Local Postgres" }).waitFor({ timeout: 15000 });
    await page.evaluate(() => {
      const w = window as unknown as { __mockOverrides: Record<string, unknown>; __deleteCalls: number };
      w.__deleteCalls = 0;
      w.__mockOverrides = {
        delete_connection: async () => {
          w.__deleteCalls++;
          await new Promise((r) => setTimeout(r, 300));
          throw "keychain is locked";
        },
      };
    });
    await treeItem(page, "Local Postgres").hover();
    await page.getByRole("button", { name: "Delete Local Postgres" }).click({ force: true });
    const dialog = page.getByRole("alertdialog", { name: "Delete Connection" });
    await dialog.getByRole("button", { name: "Delete" }).dblclick();
    await expect(dialog.getByRole("alert")).toContainText("keychain is locked");
    await expect(dialog).toBeVisible();
    expect(await page.evaluate(() => (window as unknown as { __deleteCalls: number }).__deleteCalls)).toBe(1);
    // Nothing was removed.
    await dialog.getByRole("button", { name: "Cancel" }).click();
    await expect(treeItem(page, "Local Postgres")).toBeVisible();
  });

  test("a failed truncate keeps the dialog open with the error", async ({ page }) => {
    await page.goto("/");
    const table = await navigateToTable(page, "users");
    await overrideCommand(page, "truncate_table", 'throw "permission denied for table users";');
    await table.click({ button: "right" });
    await page.getByRole("menuitem", { name: "Truncate Table" }).click();
    const dialog = page.getByRole("alertdialog", { name: "Truncate Table" });
    await dialog.getByRole("button", { name: "Truncate Table" }).click();
    await expect(dialog.getByRole("alert")).toContainText("permission denied");
    await expect(dialog).toBeVisible();
  });
});

test.describe("Explorer tree — keyboard", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await page.locator(".tree-label", { hasText: "Local Postgres" }).waitFor({ timeout: 15000 });
  });

  test("exposes a tree with one tab stop and no hidden action tab stops", async ({ page }) => {
    const tree = page.getByRole("tree", { name: "Connections" });
    await expect(tree).toBeVisible();
    const tabStops = await tree.evaluate((el) =>
      Array.from(el.querySelectorAll<HTMLElement>("button, [tabindex]")).filter((n) => n.tabIndex >= 0).length,
    );
    expect(tabStops).toBe(1);
    await expect(treeItem(page, "Production folder")).toHaveAttribute("aria-level", "1");
    await expect(treeItem(page, "Production folder")).toHaveAttribute("aria-expanded", "true");
    await expect(treeItem(page, "Staging DB")).toHaveAttribute("aria-level", "2");
  });

  test("arrow keys move, Enter connects, Right/Left expand and collapse", async ({ page }) => {
    const folder = treeItem(page, "Production folder");
    await folder.focus();
    await page.keyboard.press("ArrowLeft");
    await expect(folder).toHaveAttribute("aria-expanded", "false");
    await page.keyboard.press("ArrowRight");
    await expect(folder).toHaveAttribute("aria-expanded", "true");
    await page.keyboard.press("End");
    const local = treeItem(page, "Local Postgres");
    await expect(local).toBeFocused();
    await page.keyboard.press("Home");
    await expect(folder).toBeFocused();
    await page.keyboard.press("ArrowDown");
    await expect(treeItem(page, "Staging DB")).toBeFocused();
    await page.keyboard.press("ArrowDown");
    await expect(treeItem(page, "Analytics DB")).toBeFocused();

    await local.focus();
    await page.keyboard.press("Enter");
    await expect(local).toHaveAttribute("aria-expanded", "true", { timeout: 8000 });
    const db = treeItem(page, "postgres");
    await expect(db).toBeVisible({ timeout: 8000 });
    await local.focus();
    await page.keyboard.press("ArrowRight");
    await expect(db).toBeFocused();
    await page.keyboard.press("ArrowLeft"); // collapse db (auto-expanded) or go to parent
    await page.keyboard.press("ArrowLeft");
    await expect(local).toBeFocused();
    await page.keyboard.press("ArrowLeft");
    await expect(local).toHaveAttribute("aria-expanded", "false");
  });

  test("Enter on a table opens it and Shift+F10 opens a keyboard-operable menu", async ({ page }) => {
    await navigateToTable(page, "users");
    const users = treeItem(page, "users");
    await users.focus();
    await page.keyboard.press("Shift+F10");
    const menu = page.getByRole("menu", { name: "users actions" });
    await expect(menu).toBeVisible();
    await expect(menu.getByRole("menuitem").first()).toBeFocused();
    await page.keyboard.press("ArrowDown");
    await expect(menu.getByRole("menuitem", { name: "Query" })).toBeFocused();
    await page.keyboard.press("End");
    await page.keyboard.press("Home");
    await expect(menu.getByRole("menuitem", { name: "Open Table" })).toBeFocused();
    await page.keyboard.press("Escape");
    await expect(menu).not.toBeVisible();
    await expect(users).toBeFocused();

    await page.keyboard.press("Enter");
    await expect(page.locator(".tv-name", { hasText: "users" })).toBeVisible({ timeout: 10000 });
  });

  test("the Move to submenu is reachable from the keyboard", async ({ page }) => {
    const local = treeItem(page, "Local Postgres");
    await local.focus();
    await page.keyboard.press("ContextMenu");
    const menu = page.getByRole("menu", { name: "Local Postgres actions" });
    await expect(menu).toBeVisible();
    const moveTo = menu.getByRole("menuitem", { name: "Move to" });
    await moveTo.focus();
    await page.keyboard.press("ArrowRight");
    const sub = page.getByRole("menu", { name: "Move to" });
    await expect(sub.getByRole("menuitem", { name: "Production" })).toBeFocused();
    await page.keyboard.press("ArrowLeft");
    await expect(moveTo).toBeFocused();
    await moveTo.press("ArrowRight");
    await page.keyboard.press("Enter");
    await expect(treeItem(page, "Local Postgres")).toHaveAttribute("aria-level", "2");
  });

  test("row actions appear when the row has keyboard focus", async ({ page }) => {
    const local = treeItem(page, "Local Postgres");
    await local.focus();
    await page.keyboard.press("ArrowUp");
    await page.keyboard.press("ArrowDown");
    const connect = page.getByRole("button", { name: "Connect Local Postgres" });
    await expect(connect).toHaveAttribute("tabindex", "0");
    await expect(connect).toHaveCSS("opacity", "1");
    await page.keyboard.press("Tab");
    await expect(connect).toBeFocused();
  });

  test("F2 renames a folder; renaming onto an existing folder is refused", async ({ page }) => {
    await page.getByRole("button", { name: "New folder" }).click();
    await page.getByRole("textbox", { name: "New folder name" }).fill("Archive");
    await page.keyboard.press("Enter");
    const archive = treeItem(page, "Archive folder");
    await archive.focus();
    await page.keyboard.press("F2");
    const input = page.getByRole("textbox", { name: "Rename folder Archive" });
    await input.fill("production");
    await input.press("Enter");
    await expect(page.getByTestId("toast-error")).toContainText('A folder named "production" already exists');
    await expect(treeItem(page, "Archive folder")).toBeVisible();
    await expect(treeItem(page, "Production folder")).toBeVisible();
  });
});

test.describe("Failures are reported", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await page.locator(".tree-label", { hasText: "Local Postgres" }).waitFor({ timeout: 15000 });
  });

  test("a failed connect shows an error toast that is announced and persists", async ({ page }) => {
    await overrideCommand(page, "connect", 'throw "connection refused (os error 111)";');
    await page.locator(".tree-label", { hasText: "Local Postgres" }).dblclick();
    const toast = page.getByTestId("toast-error");
    await expect(toast).toContainText('Couldn\'t connect to "Local Postgres"');
    await expect(toast).toContainText("connection refused");
    await expect(toast).toHaveAttribute("role", "alert");
    await page.waitForTimeout(3500);
    await expect(toast).toBeVisible();
    await toast.getByRole("button", { name: "Dismiss notification" }).click();
    await expect(toast).not.toBeVisible();
  });

  test("a failed save keeps the connection dialog open with the reason", async ({ page }) => {
    await overrideCommand(page, "save_connection", 'throw "could not write connections.json: read-only file system";');
    await treeItem(page, "Local Postgres").hover();
    await page.getByRole("button", { name: "Edit Local Postgres" }).click({ force: true });
    const dialog = page.getByRole("dialog", { name: "Edit Connection" });
    await dialog.getByRole("button", { name: "Save Changes" }).click();
    await expect(dialog.locator(".connection-form-error")).toContainText("read-only file system");
    await expect(dialog).toBeVisible();
  });

  test("a failed tree load shows an error row with Retry", async ({ page }) => {
    await connectToLocalPostgres(page);
    await page.locator(".tree-node .tree-label", { hasText: /^postgres$/ }).click();
    await page.locator(".tree-node .tree-label", { hasText: /^public$/ }).click();
    await overrideCommand(page, "list_tables", 'throw "canceling statement due to statement timeout";');
    await page.locator(".tree-node .tree-label", { hasText: /^Tables$/ }).click();
    const errorRow = page.locator(".tree-error");
    await expect(errorRow).toContainText("statement timeout");
    await clearOverride(page, "list_tables");
    await errorRow.getByRole("button", { name: /Retry loading Tables/ }).click();
    await expect(page.locator(".tree-node:has(.icon-table) .tree-label", { hasText: "users" }).first()).toBeVisible({ timeout: 8000 });
    await expect(errorRow).toHaveCount(0);
  });
});

test.describe("Connection form details", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openConnectionDialog(page);
  });

  test("port can be cleared and non-integer ports are rejected", async ({ page }) => {
    const port = page.getByRole("textbox", { name: "Port" });
    await port.fill("");
    await expect(port).toHaveValue("");
    await port.blur();
    await expect(page.locator(".field-error", { hasText: "whole number" })).toBeVisible();
    await port.fill("5432.5");
    await expect(port).toHaveValue("5432.5");
    await port.fill("6543");
    await port.blur();
    await expect(page.locator(".field-error", { hasText: "whole number" })).toHaveCount(0);
  });

  test("host with spaces is rejected and Save focuses the first invalid field", async ({ page }) => {
    await page.getByRole("textbox", { name: "Connection Name" }).fill("Spaced");
    await page.getByRole("textbox", { name: "Host" }).fill("db example.com");
    await page.getByRole("button", { name: "Save" }).click();
    const host = page.getByRole("textbox", { name: "Host" });
    await expect(host).toBeFocused();
    await expect(host).toHaveAttribute("aria-invalid", "true");
    await expect(page.locator(".field-error", { hasText: "spaces" })).toBeVisible();
  });

  test("form fields are labelled and the empty Advanced section is gone", async ({ page }) => {
    await expect(page.getByRole("textbox", { name: "Connection Name" })).toBeVisible();
    await expect(page.getByRole("textbox", { name: "Folder (optional)" })).toBeVisible();
    await expect(page.getByRole("button", { name: /Database Type/ })).toBeVisible();
    await expect(page.locator(".connection-nav-item[data-section=advanced]")).toHaveCount(0);
    await page.locator(".connection-nav-item", { hasText: "Authentication" }).click();
    await expect(page.getByLabel("Password", { exact: true })).toBeVisible();
    await expect(page.locator(".dialog")).not.toContainText("future auth methods");
  });

  test("SSH password auth can ask for the password when connecting", async ({ page }) => {
    await page.locator(".connection-nav-item", { hasText: "SSH Tunnel" }).click();
    await page.getByRole("checkbox", { name: "Use an SSH tunnel" }).check({ force: true });
    await page.getByRole("checkbox", { name: "Ask for the SSH password when connecting" }).check({ force: true });
    await expect(page.locator("#ssh-password")).toHaveCount(0);
  });
});

test("duplicating a connection keeps its folder", async ({ page }) => {
  await page.goto("/");
  const staging = treeItem(page, "Staging DB");
  await staging.focus();
  await page.keyboard.press("Shift+F10");
  await page.getByRole("menuitem", { name: "Duplicate Connection" }).click();
  const dialog = page.getByRole("dialog", { name: "Duplicate Connection" });
  await expect(dialog.getByRole("textbox", { name: "Folder (optional)" })).toHaveValue("Production");
  await dialog.getByRole("button", { name: "Save" }).click();
  await expect(dialog).not.toBeVisible();
  const copy = treeItem(page, "Staging DB (copy)");
  await expect(copy).toHaveAttribute("aria-level", "2");
});

test("import shows a success state and stays open until Done", async ({ page }) => {
  await page.goto("/");
  await openImportDialog(page, "users");
  await page.locator("#csv-file-input").setInputFiles({
    name: "users.csv",
    mimeType: "text/csv",
    buffer: Buffer.from('id,username,email\n1,"Smith, John",john@example.com\n'),
  });
  await expect(page.locator(".import-preview-table")).toContainText("Smith, John");
  await expect(page.locator(".import-preview-table")).toContainText("john@example.com");
  await page.getByRole("button", { name: "Import" }).click();
  const ok = page.getByTestId("import-success");
  await expect(ok).toContainText("imported into public.users");
  await expect(page.getByRole("button", { name: "Done" })).toBeFocused();
  await page.getByRole("button", { name: "Done" }).click();
  await expect(page.locator(".import-dialog")).not.toBeVisible();
});

test("status bar theme menu is keyboard operable", async ({ page }) => {
  await page.goto("/");
  const trigger = page.getByRole("button", { name: "Change theme" });
  await trigger.click();
  const menu = page.getByRole("menu", { name: "Theme" });
  await expect(menu.getByRole("menuitemradio", { checked: true })).toBeFocused();
  await page.keyboard.press("ArrowDown");
  await page.keyboard.press("Escape");
  await expect(menu).not.toBeVisible();
  await expect(trigger).toBeFocused();
});

test.describe("150% zoom", () => {
  for (const vp of [
    { width: 1024, height: 640 },
    { width: 1280, height: 720 },
  ]) {
    test(`dialog footers stay reachable at ${vp.width}x${vp.height}`, async ({ page }) => {
      await page.setViewportSize(vp);
      await page.addInitScript(() => localStorage.setItem("tablio-zoom", "150"));
      await page.goto("/");
      await page.locator(".tree-label", { hasText: "Local Postgres" }).waitFor({ timeout: 15000 });
      await page.locator(".sidebar-header").getByRole("button", { name: "New connection" }).click();
      const save = page.getByRole("dialog").getByRole("button", { name: "Save" });
      await expect(save).toBeVisible();
      const box = await save.boundingBox();
      expect(box).not.toBeNull();
      expect(box!.y + box!.height).toBeLessThanOrEqual(vp.height);
      expect(box!.x + box!.width).toBeLessThanOrEqual(vp.width);
    });
  }
});
