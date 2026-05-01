import { test, expect } from "@playwright/test";
import { navigateToTable } from "./helpers";

test.describe("App launch", () => {
  test("renders welcome screen with title and new connection button", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator("h2", { hasText: "Welcome to Tablio" })).toBeVisible();
    await expect(page.locator(".empty-state .btn-primary")).toContainText("New Connection");
  });

  test("sidebar shows Explorer header", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".sidebar-header", { hasText: "Explorer" })).toBeVisible();
  });

  test("statusbar is visible", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".statusbar")).toBeVisible();
  });
});

test.describe("Connection dialog", () => {
  test("opens from welcome screen button", async ({ page }) => {
    await page.goto("/");
    await page.locator(".empty-state .btn-primary").click();
    await expect(page.locator(".dialog h2", { hasText: "New Connection" })).toBeVisible();
  });

  test("opens from sidebar plus button", async ({ page }) => {
    await page.goto("/");
    await page.locator(".sidebar-header .btn-icon[title='New Connection']").click();
    await expect(page.locator(".dialog h2", { hasText: "New Connection" })).toBeVisible();
  });

  test("has required form fields", async ({ page }) => {
    await page.goto("/");
    await page.locator(".empty-state .btn-primary").click();
    const dialog = page.locator(".dialog");
    // General section is the default landing page after the sectioned-dialog
    // refactor; it now hosts host/port/database alongside connection name.
    await expect(dialog.locator("label", { hasText: "Connection Name" })).toBeVisible();
    await expect(dialog.locator("label", { hasText: "Host" })).toBeVisible();
    await expect(dialog.locator("label", { hasText: "Port" })).toBeVisible();
    // Username + Password live in the Authentication section.
    await dialog.locator(".connection-nav-item", { hasText: "Authentication" }).click();
    await expect(dialog.locator("label", { hasText: "Username" })).toBeVisible();
    await expect(dialog.locator("label", { hasText: "Password" })).toBeVisible();
  });

  test("test connection button triggers success in mock mode", async ({ page }) => {
    await page.goto("/");
    await page.locator(".empty-state .btn-primary").click();
    const dialog = page.locator(".dialog");

    // Connection Name is always the first input in the General section;
    // Host follows. Username defaults to "postgres" so we don't need to
    // touch the Authentication section here.
    await dialog.locator("label", { hasText: "Connection Name" }).locator("..").locator("input").fill("Test DB");
    await dialog.locator("label", { hasText: "Host" }).locator("..").locator("input").fill("localhost");

    await page.locator(".btn-test-conn").click();
    // Post-redesign: the action button keeps its label ("Test again")
    // and a separate `.connection-test-status` chip surfaces success/failure.
    await expect(page.locator(".connection-test-status--success")).toContainText(
      "Connected",
      { timeout: 5000 },
    );
  });

  test("closes on cancel", async ({ page }) => {
    await page.goto("/");
    await page.locator(".empty-state .btn-primary").click();
    await expect(page.locator(".dialog")).toBeVisible();
    await page.locator(".btn-secondary", { hasText: "Cancel" }).click();
    await expect(page.locator(".dialog")).not.toBeVisible();
  });
});

test.describe("Sidebar connection tree", () => {
  test("loads mock connections in sidebar", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".tree-label", { hasText: "Local Postgres" })).toBeVisible({ timeout: 5000 });
  });

  test("shows grouped connections under Production folder", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".connection-group-name", { hasText: "Production" })).toBeVisible({ timeout: 5000 });
  });

  test("double-click connects and expands to show databases", async ({ page }) => {
    await page.goto("/");
    const conn = page.locator(".tree-label", { hasText: "Local Postgres" });
    await conn.waitFor({ timeout: 5000 });
    await conn.dblclick();
    await expect(page.locator(".tree-label", { hasText: "postgres" }).first()).toBeVisible({ timeout: 5000 });
  });

  test("expands database to schema to tables", async ({ page }) => {
    await page.goto("/");
    await navigateToTable(page, "users");
    await expect(page.locator(".tree-label", { hasText: "users" })).toBeVisible();
    await expect(page.locator(".tree-label", { hasText: "orders" })).toBeVisible();
  });
});

test.describe("Data grid", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    const tableNode = await navigateToTable(page, "users");
    await tableNode.click();
  });

  test("opens table tab with table name in toolbar", async ({ page }) => {
    await expect(page.locator(".tv-name")).toContainText("users", { timeout: 5000 });
  });

  test("shows column headers", async ({ page }) => {
    await expect(page.locator(".tv-name")).toBeVisible({ timeout: 5000 });
    await expect(page.locator("text=email")).toBeVisible({ timeout: 5000 });
    await expect(page.locator("text=username")).toBeVisible({ timeout: 5000 });
  });

  test("shows row data", async ({ page }) => {
    await expect(page.locator(".tv-name")).toBeVisible({ timeout: 5000 });
    await expect(page.locator("text=alice_johnson@example.com").first()).toBeVisible({ timeout: 5000 });
  });

  test("shows row count in pagination", async ({ page }) => {
    await expect(page.locator(".grid-pagination-info")).toBeVisible({ timeout: 5000 });
    await expect(page.locator(".grid-pagination-info")).toContainText("rows");
  });
});

test.describe("Table structure (now Schema mode of TableView)", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    const tableNode = await navigateToTable(page, "orders");
    await tableNode.click({ button: "right" });

    // The "View Structure" + "View Stats" menu items were folded into
    // a single "View Schema" entry that opens TableView in Schema mode.
    const viewSchema = page.locator(".context-menu button", { hasText: "View Schema" });
    await viewSchema.waitFor({ timeout: 3000 });
    await viewSchema.click();
    await page.locator(".tv-schema-strip").waitFor({ timeout: 8000 });
  });

  test("shows Columns, Indexes, Foreign Keys schema sub-tabs", async ({ page }) => {
    await expect(
      page.locator(".tv-schema-tab", { hasText: "Columns" })
    ).toBeVisible({ timeout: 5000 });
    await expect(
      page.locator(".tv-schema-tab", { hasText: "Indexes" })
    ).toBeVisible();
    await expect(
      page.locator(".tv-schema-tab", { hasText: "Foreign Keys" })
    ).toBeVisible();
  });

  test("shows FK badge on foreign key columns", async ({ page }) => {
    await expect(page.locator(".tv-badge-fk").first()).toBeVisible({ timeout: 5000 });
  });

  test("shows PK badge on primary key columns", async ({ page }) => {
    await expect(page.locator(".tv-badge-pk").first()).toBeVisible({ timeout: 5000 });
  });

  test("indexes tab shows table-specific indexes", async ({ page }) => {
    await page.locator(".tv-schema-tab", { hasText: "Indexes" }).click();
    await expect(page.locator("text=orders_pkey")).toBeVisible({ timeout: 5000 });
    await expect(page.locator("text=orders_user_id_idx")).toBeVisible();
  });

  test("foreign keys tab shows FK details", async ({ page }) => {
    await page.locator(".tv-schema-tab", { hasText: "Foreign Keys" }).click();
    await expect(page.locator("text=orders_user_id_fkey")).toBeVisible({ timeout: 5000 });
    await expect(page.locator("text=users.id")).toBeVisible();
  });
});

test.describe("Theme picker", () => {
  test("opens and lists themes", async ({ page }) => {
    await page.goto("/");
    await page.locator(".btn-icon[title='Change Theme']").click();
    await expect(page.locator(".theme-picker-popover")).toBeVisible();
    await expect(page.locator(".theme-picker-group-label", { hasText: "Dark" })).toBeVisible();
    await expect(page.locator(".theme-picker-group-label", { hasText: "Light" })).toBeVisible();
  });

  test("selects a theme and closes picker", async ({ page }) => {
    await page.goto("/");
    await page.locator(".btn-icon[title='Change Theme']").click();
    const themeItem = page.locator(".theme-picker-item").nth(2);
    await themeItem.click();
    await expect(page.locator(".theme-picker-popover")).not.toBeVisible();
  });
});

test.describe("Keyboard shortcuts", () => {
  test("opens shortcuts dialog", async ({ page }) => {
    await page.goto("/");
    await page.locator(".btn-icon[title='Keyboard Shortcuts (Ctrl+?)']").click();
    await expect(page.locator(".dialog", { hasText: "Keyboard Shortcuts" })).toBeVisible();
  });
});
