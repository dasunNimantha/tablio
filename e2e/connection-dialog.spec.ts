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

  test("SQLite path field has a Browse button and accepts tilde paths", async ({ page }) => {
    await page.locator(".db-dropdown-trigger").click();
    await page.locator(".db-dropdown-item", { hasText: "SQLite" }).click();

    // The Browse… button must be wired up next to the path input so
    // users can avoid typing the full path. Issue #106 was filed
    // because typed `~/database.db` paths weren't being expanded.
    const pathField = dialogField(page, "Database File Path");
    await expect(pathField).toBeVisible();
    await expect(pathField).toHaveAttribute("placeholder", /^~/);
    const browseButton = page
      .locator(".dialog")
      .locator("label", { hasText: "Database File Path" })
      .locator("..")
      .locator("button", { hasText: "Browse" });
    await expect(browseButton).toBeVisible();

    // The input accepts tilde-prefixed paths verbatim — backend
    // expansion happens on connect. Just confirm the typed value
    // round-trips through the input without any UI-side rewriting.
    await pathField.fill("~/database.db");
    await expect(pathField).toHaveValue("~/database.db");
  });

  test("Browse button is hidden for non-SQLite database types", async ({ page }) => {
    // The new file picker is SQLite-specific; PostgreSQL and friends
    // get the network host/port form instead. Make sure we didn't
    // accidentally render it everywhere.
    await expect(
      page
        .locator(".dialog")
        .locator("label", { hasText: "Database File Path" })
        .locator("..")
        .locator("button", { hasText: "Browse" }),
    ).not.toBeVisible();
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

  // Regression for issue #126. Error text everywhere in the app was
  // non-selectable because the global root sets `user-select: none`
  // and only a tiny whitelist of element classes opted back into
  // `text`. Users couldn't highlight + Ctrl-C the error to email it
  // to their DBA. The fix extends the whitelist to cover every error
  // / banner / warning class in the app — this test pins the
  // connection-form-error case as a representative check; the rest
  // are covered by the same CSS rule and verified manually in dev.
  test("error text is selectable (issue #126)", async ({ page }) => {
    const nameInput = page.locator(".dialog input").first();
    await nameInput.fill("");
    await page.locator(".btn-test-conn").click();
    const errorEl = page.locator(".connection-form-error");
    await expect(errorEl).toBeVisible();
    // `user-select` resolves to `text` (not `none`) when the
    // whitelist rule applies. We can't reliably test user-initiated
    // mouse-drag selection through Playwright, but the computed
    // `userSelect` is the property that gates browser behaviour
    // here, so it's the right check.
    const userSelect = await errorEl.evaluate(
      (el) => window.getComputedStyle(el).userSelect,
    );
    expect(userSelect).toBe("text");
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
    // Username lives in Authentication; clearing host on General + clearing
    // username (which is pre-filled with the driver default) triggers errors
    // in two distinct sections, so the dialog should land on General (the
    // first invalid one) and badge both nav items.
    await page.locator(".dialog input").first().fill("Needs Host");
    await dialogField(page, /^Host$/).fill("");
    await openDialogSection(page, "Authentication");
    await dialogField(page, "Username").fill("");
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
    // Post-redesign: the SSH enable toggle lives in the section heading
    // (`SectionCard`'s `action` slot), labelled "Enabled"/"Disabled" rather
    // than "Use SSH tunneling".
    await expect(
      page.locator(".connection-section-heading__action .security-toggle"),
    ).toBeVisible();

    await openDialogSection(page, "General");
    await page.locator(".db-dropdown-trigger").click();
    await page.locator(".db-dropdown-item", { hasText: "SQLite" }).click();
    await expect(page.locator(".connection-nav-item", { hasText: "SSH Tunnel" })).not.toBeVisible();
    await expect(page.locator(".ssh-tunnel-section")).not.toBeVisible();
  });

  test("toggling SSH on reveals tunnel host / port / username inputs", async ({ page }) => {
    await openDialogSection(page, "SSH Tunnel");
    // SSH enable toggle now lives in the section heading's action slot.
    const tunnelToggle = page
      .locator(".connection-section-heading__action .security-toggle__input");
    await expect(
      page.locator(".ssh-tunnel-section label", { hasText: "Tunnel host" }),
    ).not.toBeVisible();
    await tunnelToggle.check({ force: true });
    await expect(
      page.locator(".ssh-tunnel-section label", { hasText: "Tunnel host" }),
    ).toBeVisible();
    // The Tunnel-port input was relabelled to just "Port" during the
    // 86b384c connection-dialog tightening pass.
    await expect(
      page.locator(".ssh-tunnel-section label", { hasText: /^Port$/ }),
    ).toBeVisible();
    await expect(
      page.locator(".ssh-tunnel-section label", { hasText: "SSH username" }),
    ).toBeVisible();
  });

  test("auth toggle swaps Password input <-> Identity file picker", async ({ page }) => {
    await openDialogSection(page, "SSH Tunnel");
    const tunnelToggle = page
      .locator(".connection-section-heading__action .security-toggle__input");
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
    // The prompt-for-passphrase nested toggle was relabelled
    // "Ask when connecting" during the SSH section tightening pass.
    await expect(
      page.locator(".ssh-tunnel-section .security-toggle__label", {
        hasText: "Ask when connecting",
      }),
    ).toBeVisible();
  });

  test("'Ask when connecting' hides the passphrase input", async ({ page }) => {
    await openDialogSection(page, "SSH Tunnel");
    const tunnelToggle = page
      .locator(".connection-section-heading__action .security-toggle__input");
    await tunnelToggle.check({ force: true });
    await page
      .locator(".ssh-auth-toggle__btn", { hasText: "Identity file" })
      .click();
    const promptToggle = page
      .locator(".ssh-tunnel-section .security-toggle--inline .security-toggle__input");
    await promptToggle.check({ force: true });
    // The "Key passphrase" label intentionally stays put (it shares the
    // row with the toggle); the input itself is what disappears, since
    // the passphrase will be prompted at connect time instead of stored.
    await expect(page.locator("#ssh-password")).not.toBeVisible();
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
      .locator(".connection-section-heading__action .security-toggle__input");
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

  test("warns when SSL hostname verification will fail through the tunnel", async ({ page }) => {
    // Turn on SSL (default Trust server cert is off) so we have the
    // verify-full + tunnel combination that the advisory flags.
    await openDialogSection(page, "Security");
    await page
      .locator(".security-toggle", { hasText: "SSL / TLS" })
      .locator(".security-toggle__input")
      .check({ force: true });

    await openDialogSection(page, "SSH Tunnel");
    const tunnelToggle = page
      .locator(".connection-section-heading__action .security-toggle__input");
    await tunnelToggle.check({ force: true });
    const warning = page.locator(".ssh-tunnel-warning", {
      hasText: "SSL hostname verification will fail",
    });
    await expect(warning).toBeVisible();

    // Toggling Trust server certificate should clear the warning.
    await openDialogSection(page, "Security");
    await page
      .locator(".security-toggle", { hasText: "Trust server certificate" })
      .locator(".security-toggle__input")
      .check({ force: true });
    await openDialogSection(page, "SSH Tunnel");
    await expect(warning).not.toBeVisible();
  });

  test("warns about Cassandra peer discovery when SSH is enabled", async ({ page }) => {
    await page.locator(".db-dropdown-trigger").click();
    await page.locator(".db-dropdown-item", { hasText: "Cassandra" }).click();
    await openDialogSection(page, "SSH Tunnel");
    const tunnelToggle = page
      .locator(".connection-section-heading__action .security-toggle__input");
    await tunnelToggle.check({ force: true });
    await expect(
      page.locator(".ssh-tunnel-warning", {
        hasText: "Cassandra cluster discovery bypasses the tunnel",
      }),
    ).toBeVisible();
  });

  test("warns when an identity-file passphrase will be persisted to disk", async ({ page }) => {
    await openDialogSection(page, "SSH Tunnel");
    const tunnelToggle = page
      .locator(".connection-section-heading__action .security-toggle__input");
    await tunnelToggle.check({ force: true });
    await page
      .locator(".ssh-auth-toggle__btn", { hasText: "Identity file" })
      .click();
    // The persisted-passphrase notice should not appear until the user
    // actually types something, because an empty passphrase = unencrypted key.
    const warning = page.locator(".ssh-tunnel-warning", {
      hasText: "Passphrase will be saved to disk",
    });
    await expect(warning).not.toBeVisible();

    // The passphrase input shares its row with the "Ask when connecting"
    // toggle, so target it by its stable `id="ssh-password"` rather than
    // walking up from the label (which would hit the toggle's checkbox).
    await page.locator("#ssh-password").fill("hunter2");
    await expect(warning).toBeVisible();

    // Enabling the "Ask when connecting" toggle replaces the persisted
    // passphrase with a runtime prompt, so the notice must disappear.
    const promptToggle = page
      .locator(".ssh-tunnel-section .security-toggle--inline .security-toggle__input");
    await promptToggle.check({ force: true });
    await expect(warning).not.toBeVisible();
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
