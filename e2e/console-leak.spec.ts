import { test, expect, type Page } from "@playwright/test";
import { openQueryConsole } from "./helpers";

// Closing a query console must release it. Monaco keeps `addAction`
// commands in a global registry that `editor.dispose()` doesn't clear, and
// their closures used to keep every closed console (editor, DOM, result
// rows) alive. Counts DOM nodes and listeners after a forced GC; a leaked
// console holds hundreds of detached nodes, so the per-cycle budget is
// generous enough to ignore JIT/style-cache noise.
async function counters(page: Page) {
  const cdp = await page.context().newCDPSession(page);
  await cdp.send("HeapProfiler.enable");
  await cdp.send("HeapProfiler.collectGarbage");
  await cdp.send("HeapProfiler.collectGarbage");
  const { nodes, jsEventListeners } = await cdp.send("Memory.getDOMCounters");
  await cdp.detach();
  return { nodes, listeners: jsEventListeners };
}

async function openRunClose(page: Page) {
  const consoles = page.locator(".query-console");
  const count = await consoles.count();
  await page.keyboard.press("ControlOrMeta+n");
  await expect(consoles).toHaveCount(count + 1);
  const console_ = page.locator(".query-console:visible");
  await console_.waitFor({ timeout: 8000 });
  await console_.locator(".btn-primary", { hasText: "Execute" }).click();
  await expect(console_.locator(".query-result-info")).toBeVisible({ timeout: 5000 });
  await page.keyboard.press("Escape");
  await page.locator(".tab.active .tab-close").click();
  await expect(consoles).toHaveCount(count);
}

test("closed query consoles are released", async ({ page }) => {
  test.setTimeout(120_000);
  await page.goto("/");
  await openQueryConsole(page);
  await page.locator(".query-console").first().waitFor({ timeout: 8000 });
  // Warm up: first console pays one-time Monaco/ag-grid setup.
  await openRunClose(page);
  await openRunClose(page);
  const before = await counters(page);
  const cycles = 12;
  for (let i = 0; i < cycles; i++) await openRunClose(page);
  const after = await counters(page);
  const perCycleNodes = (after.nodes - before.nodes) / cycles;
  const perCycleListeners = (after.listeners - before.listeners) / cycles;
  console.log(`console leak: ${perCycleNodes.toFixed(1)} nodes/cycle, ${perCycleListeners.toFixed(1)} listeners/cycle`);
  expect(perCycleNodes).toBeLessThan(60);
  expect(perCycleListeners).toBeLessThan(3);
});
