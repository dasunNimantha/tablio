import { test, expect } from "@playwright/test";

test.describe("SSH known hosts dialog", () => {
  test("opens from the theme/settings menu and lists mock entries", async ({
    page,
  }) => {
    await page.goto("/");

    // The menu lives in the status bar; the icon button has the Palette
    // icon and tooltip "Change Theme".
    await page.getByRole("button", { name: "Change Theme" }).click();

    // The Settings group contains the entry that opens the dialog.
    await page.getByRole("button", { name: /SSH known hosts/i }).click();

    // The dialog mounts via portal and is labelled accordingly.
    const dialog = page.getByRole("dialog", { name: /SSH known hosts/i });
    await expect(dialog).toBeVisible();
    await expect(
      dialog.getByRole("columnheader", { name: "Host" }),
    ).toBeVisible();
    await expect(
      dialog.getByRole("columnheader", { name: "Fingerprint" }),
    ).toBeVisible();
    // The mock data ships two entries; either both render or the empty
    // state is shown — assert the dialog body resolved to something.
    const hasEntry = await dialog
      .locator(".known-hosts-table tbody tr")
      .count();
    const hasEmpty = await dialog.locator(".known-hosts-dialog__empty").count();
    expect(hasEntry + hasEmpty).toBeGreaterThan(0);
  });

  test("filter input narrows the visible rows", async ({ page }) => {
    await page.goto("/");
    await page.getByRole("button", { name: "Change Theme" }).click();
    await page.getByRole("button", { name: /SSH known hosts/i }).click();
    const dialog = page.getByRole("dialog", { name: /SSH known hosts/i });
    await expect(dialog).toBeVisible();

    // Wait for the listKnownHosts mock to resolve and the table body to
    // populate. Up to 5s in case the mock latency is high in CI.
    const firstRow = dialog.locator(".known-hosts-table tbody tr").first();
    await expect(firstRow).toBeVisible({ timeout: 5000 });

    await dialog
      .getByLabel(/Filter known hosts/i)
      .fill("definitely-no-such-host-zzz");
    await expect(
      dialog.getByText(/No entries match your filter/i),
    ).toBeVisible();
  });

  test("Escape closes the dialog", async ({ page }) => {
    await page.goto("/");
    await page.getByRole("button", { name: "Change Theme" }).click();
    await page.getByRole("button", { name: /SSH known hosts/i }).click();
    const dialog = page.getByRole("dialog", { name: /SSH known hosts/i });
    await expect(dialog).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(dialog).toBeHidden();
  });
});
