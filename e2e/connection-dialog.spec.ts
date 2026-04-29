import { test, expect } from "@playwright/test";
import { openConnectionDialog } from "./helpers";

async function openDialogSection(page: import("@playwright/test").Page, name: string) {
  await page.locator(".connection-nav-item", { hasText: name }).click();
}

function dialogField(page: import("@playwright/test").Page, label: string | RegExp) {
  return page.locator(".dialog").locator("label", { hasText: label }).locator("..").locator("input").first();
}

test.describe("Connection dialog — DB type switching", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openConnectionDialog(page);
  });

  test("left navigation switches between configuration sections", async ({ page }) => {
    await expect(page.locator(".connection-nav-item", { hasText: "General" })).toHaveClass(/connection-nav-item--active/);
    // General now also owns the connection target (host/port/database).
    await expect(dialogField(page, /^Host$/)).toBeVisible();

    await openDialogSection(page, "Authentication");
    await expect(page.locator(".connection-section-heading h3")).toHaveText("Authentication");
    await expect(dialogField(page, /^Username$/)).toBeVisible();
  });

  test("defaults to PostgreSQL with port 5432", async ({ page }) => {
    const dialog = page.locator(".dialog");
    await expect(dialog.locator(".db-dropdown-value")).toHaveText("PostgreSQL");
    const portInput = dialogField(page, /^Port$/);
    await expect(portInput).toHaveValue("5432");
  });

  test("switching to MySQL changes port to 3306", async ({ page }) => {
    await page.locator(".db-dropdown-trigger").click();
    await page.locator(".db-dropdown-item", { hasText: "MySQL" }).click();
    await expect(page.locator(".db-dropdown-value")).toHaveText("MySQL");
    const portInput = dialogField(page, /^Port$/);
    await expect(portInput).toHaveValue("3306");
  });

  test("switching to SQLite shows file path instead of host/port/user", async ({ page }) => {
    await page.locator(".db-dropdown-trigger").click();
    await page.locator(".db-dropdown-item", { hasText: "SQLite" }).click();
    await expect(page.locator(".dialog").locator("label", { hasText: "Database File Path" })).toBeVisible();
    await expect(page.locator(".dialog").locator("label", { hasText: "Host" })).not.toBeVisible();
    await expect(page.locator(".dialog").locator("label", { hasText: "Port" })).not.toBeVisible();
    await expect(page.locator(".dialog").locator("label", { hasText: "Username" })).not.toBeVisible();
    await expect(page.locator(".connection-nav-item", { hasText: "Authentication" })).not.toBeVisible();
  });

  test("SQLite hides SSL toggles", async ({ page }) => {
    await page.locator(".db-dropdown-trigger").click();
    await page.locator(".db-dropdown-item", { hasText: "SQLite" }).click();
    // SSL block (and the whole SSH section) are gated off for SQLite.
    await expect(page.locator(".connection-nav-item", { hasText: "Security" })).not.toBeVisible();
    await expect(
      page.locator(".security-toggle__label", { hasText: "SSL / TLS" }),
    ).not.toBeVisible();
  });

  test("Cassandra hides SSL toggles", async ({ page }) => {
    await page.locator(".db-dropdown-trigger").click();
    await page.locator(".db-dropdown-item", { hasText: "Cassandra" }).click();
    await openDialogSection(page, "Security");
    // SSL block hidden for cassandra; SSH section still applies and is
    // independently tested below.
    await expect(
      page.locator(".security-toggle__label", { hasText: "SSL / TLS" }),
    ).not.toBeVisible();
  });

  test("switching to CockroachDB changes port to 26257", async ({ page }) => {
    await page.locator(".db-dropdown-trigger").click();
    await page.locator(".db-dropdown-item", { hasText: "CockroachDB" }).click();
    const portInput = dialogField(page, /^Port$/);
    await expect(portInput).toHaveValue("26257");
  });

  test("switching to MSSQL changes port to 1433", async ({ page }) => {
    await page.locator(".db-dropdown-trigger").click();
    await page.locator(".db-dropdown-item", { hasText: "Microsoft SQL Server" }).click();
    const portInput = dialogField(page, /^Port$/);
    await expect(portInput).toHaveValue("1433");
  });
});

test.describe("Connection dialog — SSL toggles", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openConnectionDialog(page);
  });

  test("SSL toggle enables trust server certificate sub-toggle", async ({ page }) => {
    await openDialogSection(page, "Security");
    const trustToggle = page.locator(".security-toggle--nested .security-toggle__input");
    await expect(trustToggle).toBeDisabled();
    await page.locator(".security-toggle__input").first().check({ force: true });
    await expect(trustToggle).not.toBeDisabled();
  });

  test("disabling SSL auto-unchecks trust cert", async ({ page }) => {
    await openDialogSection(page, "Security");
    const sslToggle = page.locator(".security-toggle__input").first();
    const trustToggle = page.locator(".security-toggle--nested .security-toggle__input");
    await sslToggle.check({ force: true });
    await trustToggle.check({ force: true });
    await expect(trustToggle).toBeChecked();
    await sslToggle.uncheck({ force: true });
    await expect(trustToggle).not.toBeChecked();
  });
});

test.describe("Connection dialog — color picker", () => {
  test("selects a color and highlights the active dot", async ({ page }) => {
    await page.goto("/");
    await openConnectionDialog(page);
    const dots = page.locator(".color-dot");
    await dots.nth(3).click();
    await expect(dots.nth(3)).toHaveClass(/active/);
    await dots.nth(0).click();
    await expect(dots.nth(0)).toHaveClass(/active/);
    await expect(dots.nth(3)).not.toHaveClass(/active/);
  });
});

test.describe("Connection dialog — group input", () => {
  test("group input shows autocomplete suggestions from existing groups", async ({ page }) => {
    await page.goto("/");
    await openConnectionDialog(page);
    const groupInput = page.locator(".group-input-wrapper input");
    await groupInput.focus();
    await expect(page.locator(".group-suggestions")).toBeVisible({ timeout: 3000 });
    await expect(page.locator(".group-suggestion-item").first()).toBeVisible();
  });

  test("group input accepts custom text", async ({ page }) => {
    await page.goto("/");
    await openConnectionDialog(page);
    const groupInput = page.locator(".group-input-wrapper input");
    await groupInput.fill("My Custom Group");
    await expect(groupInput).toHaveValue("My Custom Group");
  });

  test("clicking a suggestion fills the group input", async ({ page }) => {
    await page.goto("/");
    await openConnectionDialog(page);
    const groupInput = page.locator(".group-input-wrapper input");
    await groupInput.focus();
    const suggestion = page.locator(".group-suggestion-item").first();
    await suggestion.waitFor({ timeout: 3000 });
    const text = await suggestion.locator(".group-suggestion-text").textContent();
    await suggestion.click();
    await expect(groupInput).toHaveValue(text!);
  });
});

test.describe("Connection dialog — validation", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openConnectionDialog(page);
  });

  test("empty name shows error on test connection", async ({ page }) => {
    const nameInput = page.locator(".dialog input").first();
    await nameInput.fill("");
    await page.locator(".btn-test-conn").click();
    await expect(page.locator(".connection-form-error")).toContainText("fix the highlighted");
  });

  test("empty host shows error after blur", async ({ page }) => {
    await page.locator(".dialog input").first().fill("Missing Host");
    const hostInput = dialogField(page, /^Host$/);
    await hostInput.fill("");
    await hostInput.blur();
    await page.locator(".btn-test-conn").click();
    await expect(page.locator(".field-error", { hasText: "Host is required" })).toBeVisible();
  });

  test("duplicate name shows error", async ({ page }) => {
    const nameInput = page.locator(".dialog input").first();
    await nameInput.fill("Local Postgres");
    await page.locator(".btn-test-conn").click();
    await expect(page.locator(".field-error", { hasText: "already exists" })).toBeVisible();
  });

  test("test connection jumps to the first section with validation errors", async ({ page }) => {
    // Username lives in Authentication; clearing host on General + missing
    // username triggers errors in two distinct sections, so the dialog
    // should land on General (the first invalid one) and badge both nav
    // items.
    await page.locator(".dialog input").first().fill("Needs Host");
    await dialogField(page, /^Host$/).fill("");
    await page.locator(".btn-test-conn").click();

    await expect(page.locator(".connection-nav-item--active")).toContainText("General");
    await expect(page.locator(".connection-nav-item--error", { hasText: "General" })).toBeVisible();
    await expect(page.locator(".connection-nav-item--error", { hasText: "Authentication" })).toBeVisible();
    await expect(page.locator(".field-error", { hasText: "Host is required" })).toBeVisible();
  });
});

test.describe("Connection dialog — SSH tunnel section", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openConnectionDialog(page);
  });

  test("SSH section is visible for postgres and hidden for sqlite", async ({ page }) => {
    await openDialogSection(page, "SSH Tunnel");
    await expect(
      page.locator(".ssh-tunnel-section .security-toggle__label", {
        hasText: "Use SSH tunneling",
      }),
    ).toBeVisible();

    await openDialogSection(page, "General");
    await page.locator(".db-dropdown-trigger").click();
    await page.locator(".db-dropdown-item", { hasText: "SQLite" }).click();
    await expect(page.locator(".connection-nav-item", { hasText: "SSH Tunnel" })).not.toBeVisible();
    await expect(page.locator(".ssh-tunnel-section")).not.toBeVisible();
  });

  test("toggling SSH on reveals tunnel host / port / username inputs", async ({ page }) => {
    await openDialogSection(page, "SSH Tunnel");
    const tunnelToggle = page
      .locator(".ssh-tunnel-section .security-toggle")
      .first()
      .locator(".security-toggle__input");
    await expect(
      page.locator(".ssh-tunnel-section label", { hasText: "Tunnel host" }),
    ).not.toBeVisible();
    await tunnelToggle.check({ force: true });
    await expect(
      page.locator(".ssh-tunnel-section label", { hasText: "Tunnel host" }),
    ).toBeVisible();
    await expect(
      page.locator(".ssh-tunnel-section label", { hasText: "Tunnel port" }),
    ).toBeVisible();
    await expect(
      page.locator(".ssh-tunnel-section label", { hasText: "SSH username" }),
    ).toBeVisible();
  });

  test("auth toggle swaps Password input <-> Identity file picker", async ({ page }) => {
    await openDialogSection(page, "SSH Tunnel");
    const tunnelToggle = page
      .locator(".ssh-tunnel-section .security-toggle")
      .first()
      .locator(".security-toggle__input");
    await tunnelToggle.check({ force: true });

    // Default = Password: password field visible, identity file hidden.
    await expect(
      page.locator(".ssh-tunnel-section label", { hasText: "Password" }),
    ).toBeVisible();
    await expect(
      page.locator(".ssh-tunnel-section label", { hasText: "Identity file" }),
    ).not.toBeVisible();

    // Switch to Identity file.
    await page
      .locator(".ssh-auth-toggle__btn", { hasText: "Identity file" })
      .click();
    await expect(
      page.locator(".ssh-tunnel-section label", { hasText: "Identity file" }),
    ).toBeVisible();
    // The password field's label flips to "Key passphrase".
    await expect(
      page.locator(".ssh-tunnel-section label", { hasText: "Key passphrase" }),
    ).toBeVisible();
    await expect(
      page.locator(".ssh-tunnel-section label", { hasText: /^Password$/ }),
    ).not.toBeVisible();
    // Prompt-for-passphrase nested toggle becomes available.
    await expect(
      page.locator(".ssh-tunnel-section .security-toggle__label", {
        hasText: "Prompt for passphrase?",
      }),
    ).toBeVisible();
  });

  test("'Prompt for passphrase' hides the passphrase input", async ({ page }) => {
    await openDialogSection(page, "SSH Tunnel");
    const tunnelToggle = page
      .locator(".ssh-tunnel-section .security-toggle")
      .first()
      .locator(".security-toggle__input");
    await tunnelToggle.check({ force: true });
    await page
      .locator(".ssh-auth-toggle__btn", { hasText: "Identity file" })
      .click();
    const promptToggle = page
      .locator(".ssh-tunnel-section .security-toggle--nested .security-toggle__input");
    await promptToggle.check({ force: true });
    await expect(
      page.locator(".ssh-tunnel-section label", { hasText: "Key passphrase" }),
    ).not.toBeVisible();
  });

  test("validation flags missing tunnel host / username on test", async ({ page }) => {
    // Fill in a name + DB connection details so the only outstanding errors
    // come from the SSH section.
    await page.locator(".dialog input").nth(0).fill("SSH Test");
    await dialogField(page, /^Host$/).fill("127.0.0.1");
    await openDialogSection(page, "Authentication");
    await dialogField(page, /^Username$/).fill("admin");

    await openDialogSection(page, "SSH Tunnel");
    const tunnelToggle = page
      .locator(".ssh-tunnel-section .security-toggle")
      .first()
      .locator(".security-toggle__input");
    await tunnelToggle.check({ force: true });
    await page.locator(".btn-test-conn").click();

    await expect(page.locator(".connection-nav-item--active")).toContainText("SSH Tunnel");
    await expect(
      page.locator(".ssh-tunnel-section .field-error", {
        hasText: "SSH host is required",
      }),
    ).toBeVisible();
    await expect(
      page.locator(".ssh-tunnel-section .field-error", {
        hasText: "SSH username is required",
      }),
    ).toBeVisible();
    await expect(
      page.locator(".ssh-tunnel-section .field-error", {
        hasText: "SSH password is required",
      }),
    ).toBeVisible();
  });
});

test.describe("Connection dialog — save and close", () => {
  test("save creates connection and closes dialog", async ({ page }) => {
    await page.goto("/");
    await openConnectionDialog(page);
    const dialog = page.locator(".dialog");
    await dialog.locator("input").nth(0).fill("E2E Test DB");
    await dialogField(page, /^Host$/).fill("127.0.0.1");
    await openDialogSection(page, "Authentication");
    await dialogField(page, /^Username$/).fill("admin");
    await page.locator(".btn-primary", { hasText: "Save" }).click();
    await expect(page.locator(".dialog")).not.toBeVisible({ timeout: 3000 });
    await expect(page.locator(".tree-label", { hasText: "E2E Test DB" })).toBeVisible({ timeout: 3000 });
  });

  test("closes via X button", async ({ page }) => {
    await page.goto("/");
    await openConnectionDialog(page);
    await page.locator(".dialog-header .btn-icon").click();
    await expect(page.locator(".dialog")).not.toBeVisible();
  });
});
