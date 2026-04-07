import { test, expect } from "@playwright/test";

async function triggerZoomKey(page: import("@playwright/test").Page, key: string) {
  await page.evaluate((k) => {
    window.dispatchEvent(new KeyboardEvent("keydown", { key: k, ctrlKey: true, bubbles: true }));
  }, key);
}

test.describe("Keyboard zoom", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
  });

  test("Ctrl+= zooms in and shows indicator", async ({ page }) => {
    await triggerZoomKey(page, "=");
    const indicator = page.locator(".zoom-indicator");
    await expect(indicator).toBeVisible({ timeout: 3000 });
    const text = await indicator.textContent();
    expect(text).toContain("%");
  });

  test("Ctrl+- zooms out and shows indicator", async ({ page }) => {
    await triggerZoomKey(page, "-");
    const indicator = page.locator(".zoom-indicator");
    await expect(indicator).toBeVisible({ timeout: 3000 });
    const text = await indicator.textContent();
    expect(text).toContain("%");
  });

  test("Ctrl+0 resets zoom to 110%", async ({ page }) => {
    await triggerZoomKey(page, "=");
    await triggerZoomKey(page, "=");
    await triggerZoomKey(page, "0");
    const indicator = page.locator(".zoom-indicator");
    await expect(indicator).toBeVisible({ timeout: 3000 });
    await expect(indicator).toContainText("110%");
  });

  test("zoom indicator disappears after timeout", async ({ page }) => {
    await triggerZoomKey(page, "=");
    await expect(page.locator(".zoom-indicator")).toBeVisible({ timeout: 3000 });
    await expect(page.locator(".zoom-indicator")).not.toBeVisible({ timeout: 5000 });
  });

  test("clicking zoom indicator resets to 110%", async ({ page }) => {
    await triggerZoomKey(page, "=");
    await triggerZoomKey(page, "=");
    const indicator = page.locator(".zoom-indicator");
    await expect(indicator).toBeVisible({ timeout: 3000 });
    await indicator.click();
    await expect(indicator).toContainText("110%");
  });

  test("zoom has a minimum of 50%", async ({ page }) => {
    for (let i = 0; i < 15; i++) {
      await triggerZoomKey(page, "-");
    }
    const indicator = page.locator(".zoom-indicator");
    await expect(indicator).toBeVisible({ timeout: 3000 });
    await expect(indicator).toContainText("50%");
  });
});
