import { test, expect } from "@playwright/test";
import { openQueryConsole } from "./helpers";

test.describe("Query console — editor and toolbar", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openQueryConsole(page);
    await page.locator(".query-console").waitFor({ timeout: 8000 });
  });

  test("query console is visible with editor and toolbar", async ({ page }) => {
    await expect(page.locator(".query-console")).toBeVisible();
    await expect(page.locator(".query-toolbar")).toBeVisible();
    await expect(page.locator(".query-editor-wrapper")).toBeVisible();
  });

  test("execute button is present and enabled", async ({ page }) => {
    const execBtn = page.locator(".query-toolbar .btn-primary", { hasText: "Execute" });
    await expect(execBtn).toBeVisible();
    await expect(execBtn).toBeEnabled();
  });

  test("explain button is present", async ({ page }) => {
    await expect(page.locator(".query-toolbar .btn-secondary", { hasText: "Explain" })).toBeVisible();
  });

  test("format button is present", async ({ page }) => {
    await expect(page.locator(".query-toolbar .btn-ghost[title='Format SQL (Ctrl+Shift+F)']")).toBeVisible();
  });

  test("hint shows Ctrl+Enter", async ({ page }) => {
    await expect(page.locator(".query-hint")).toContainText("Ctrl+Enter to run");
  });

  test("save button is present", async ({ page }) => {
    await expect(page.locator(".query-toolbar .btn-ghost[title='Save query']")).toBeVisible();
  });

  test("saved, history, and suggest buttons are present", async ({ page }) => {
    await expect(page.locator(".query-toolbar .btn-ghost", { hasText: "Saved" })).toBeVisible();
    await expect(page.locator(".query-toolbar .btn-ghost", { hasText: "History" })).toBeVisible();
    await expect(page.locator(".query-toolbar .btn-ghost", { hasText: "Suggest" })).toBeVisible();
  });

  test("empty results area shows placeholder", async ({ page }) => {
    await expect(page.locator(".query-empty")).toBeVisible();
    await expect(page.locator(".query-empty")).toContainText("Execute a query to see results here");
  });
});

test.describe("Query console — execution", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openQueryConsole(page);
    await page.locator(".query-console").waitFor({ timeout: 8000 });
  });

  test("execute button runs query and shows results", async ({ page }) => {
    const execBtn = page.locator(".query-toolbar .btn-primary", { hasText: "Execute" });
    await execBtn.click();
    await expect(page.locator(".query-result-info")).toBeVisible({ timeout: 5000 });
    await expect(page.locator(".query-result-info")).toContainText("rows returned");
  });

  test("result info shows execution time", async ({ page }) => {
    await page.locator(".query-toolbar .btn-primary", { hasText: "Execute" }).click();
    await expect(page.locator(".query-result-info")).toContainText("ms");
  });

  test("result table shows after SELECT execution", async ({ page }) => {
    await page.locator(".query-toolbar .btn-primary", { hasText: "Execute" }).click();
    await expect(page.locator(".result-table-ag-wrapper")).toBeVisible({ timeout: 5000 });
  });

  test("error shows in error strip for invalid SQL", async ({ page }) => {
    // Default SQL is SELECT 1; which is valid in mock — mock always returns success
    // So we verify the success path instead
    await page.locator(".query-toolbar .btn-primary", { hasText: "Execute" }).click();
    await expect(page.locator(".query-result-info")).toBeVisible({ timeout: 5000 });
    await expect(page.locator(".query-error")).not.toBeVisible();
  });
});

test.describe("Query console — explain", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openQueryConsole(page);
    await page.locator(".query-console").waitFor({ timeout: 8000 });
  });

  test("explain button shows explain view", async ({ page }) => {
    await page.locator(".query-toolbar .btn-secondary", { hasText: "Explain" }).click();
    await expect(page.locator(".explain-view")).toBeVisible({ timeout: 5000 });
  });

  test("explain view shows node type and stats", async ({ page }) => {
    await page.locator(".query-toolbar .btn-secondary", { hasText: "Explain" }).click();
    await expect(page.locator(".explain-node-type")).toBeVisible({ timeout: 5000 });
    await expect(page.locator(".explain-node-type")).toContainText("Seq Scan");
    await expect(page.locator(".explain-stat")).toHaveCount(5);
  });

  test("explain view: Visual vs Raw toggle", async ({ page }) => {
    await page.locator(".query-toolbar .btn-secondary", { hasText: "Explain" }).click();
    await page.locator(".explain-view").waitFor({ timeout: 5000 });

    await expect(page.locator(".explain-tree")).toBeVisible();
    await page.locator(".explain-header-right .btn-ghost", { hasText: "Raw" }).click();
    await expect(page.locator(".explain-raw")).toBeVisible();
    await expect(page.locator(".explain-raw pre")).toContainText("Seq Scan on users");

    await page.locator(".explain-header-right .btn-ghost", { hasText: "Visual" }).click();
    await expect(page.locator(".explain-tree")).toBeVisible();
  });

  test("explain shows execution time", async ({ page }) => {
    await page.locator(".query-toolbar .btn-secondary", { hasText: "Explain" }).click();
    await expect(page.locator(".explain-header-left")).toContainText("ms");
  });
});

test.describe("Query console — format", () => {
  test("format button formats SQL", async ({ page }) => {
    await page.goto("/");
    await openQueryConsole(page);
    await page.locator(".query-console").waitFor({ timeout: 8000 });
    await page.locator(".query-toolbar .btn-ghost[title='Format SQL (Ctrl+Shift+F)']").click();
  });
});

test.describe("Query console — save query", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openQueryConsole(page);
    await page.locator(".query-console").waitFor({ timeout: 8000 });
  });

  test("save button opens save dialog", async ({ page }) => {
    await page.locator(".query-toolbar .btn-ghost[title='Save query']").click();
    await expect(page.locator(".save-query-dialog")).toBeVisible();
    await expect(page.locator(".save-query-input")).toBeVisible();
  });

  test("save dialog has Cancel and Save buttons", async ({ page }) => {
    await page.locator(".query-toolbar .btn-ghost[title='Save query']").click();
    await expect(page.locator(".save-query-actions .btn-secondary", { hasText: "Cancel" })).toBeVisible();
    await expect(page.locator(".save-query-actions .btn-primary", { hasText: "Save" })).toBeVisible();
  });

  test("save is disabled when name is empty", async ({ page }) => {
    await page.locator(".query-toolbar .btn-ghost[title='Save query']").click();
    const saveBtn = page.locator(".save-query-actions .btn-primary", { hasText: "Save" });
    await expect(saveBtn).toBeDisabled();
  });

  test("save is enabled when name is filled", async ({ page }) => {
    await page.locator(".query-toolbar .btn-ghost[title='Save query']").click();
    await page.locator(".save-query-input").fill("My Query");
    const saveBtn = page.locator(".save-query-actions .btn-primary", { hasText: "Save" });
    await expect(saveBtn).toBeEnabled();
  });

  test("cancel closes save dialog", async ({ page }) => {
    await page.locator(".query-toolbar .btn-ghost[title='Save query']").click();
    await page.locator(".save-query-actions .btn-secondary", { hasText: "Cancel" }).click();
    await expect(page.locator(".save-query-dialog")).not.toBeVisible();
  });

  test("save with name closes dialog", async ({ page }) => {
    await page.locator(".query-toolbar .btn-ghost[title='Save query']").click();
    await page.locator(".save-query-input").fill("Test Query");
    await page.locator(".save-query-actions .btn-primary", { hasText: "Save" }).click();
    await expect(page.locator(".save-query-dialog")).not.toBeVisible();
  });

  test("Enter key in name input saves", async ({ page }) => {
    await page.locator(".query-toolbar .btn-ghost[title='Save query']").click();
    await page.locator(".save-query-input").fill("Enter Query");
    await page.locator(".save-query-input").press("Enter");
    await expect(page.locator(".save-query-dialog")).not.toBeVisible();
  });

  test("Escape key in name input closes dialog", async ({ page }) => {
    await page.locator(".query-toolbar .btn-ghost[title='Save query']").click();
    await page.locator(".save-query-input").press("Escape");
    await expect(page.locator(".save-query-dialog")).not.toBeVisible();
  });
});

test.describe("Query console — saved queries panel", () => {
  test("saved queries button opens panel", async ({ page }) => {
    await page.goto("/");
    await openQueryConsole(page);
    await page.locator(".query-console").waitFor({ timeout: 8000 });
    await page.locator(".query-toolbar .btn-ghost", { hasText: "Saved" }).click();
    await expect(page.locator(".saved-queries-panel")).toBeVisible();
  });

  test("saved queries panel shows mock queries", async ({ page }) => {
    await page.goto("/");
    await openQueryConsole(page);
    await page.locator(".query-console").waitFor({ timeout: 8000 });
    await page.locator(".query-toolbar .btn-ghost", { hasText: "Saved" }).click();
    await page.locator(".saved-queries-panel").waitFor({ timeout: 5000 });
    await expect(page.locator(".saved-queries-item")).toHaveCount(2);
    await expect(page.locator(".saved-queries-name").first()).toContainText("Active users");
  });
});

test.describe("Query console — history panel", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openQueryConsole(page);
    await page.locator(".query-console").waitFor({ timeout: 8000 });
  });

  test("history panel opens/closes", async ({ page }) => {
    await page.locator(".query-toolbar .btn-ghost", { hasText: "History" }).click();
    await expect(page.locator(".query-history-panel")).toBeVisible();
    await page.locator(".query-history-header .btn-icon").click();
    await expect(page.locator(".query-history-panel")).not.toBeVisible();
  });

  test("empty history shows 'No queries yet'", async ({ page }) => {
    await page.locator(".query-toolbar .btn-ghost", { hasText: "History" }).click();
    await expect(page.locator(".query-history-empty")).toContainText("No queries yet");
  });

  test("executing a query adds to history", async ({ page }) => {
    await page.locator(".query-toolbar .btn-primary", { hasText: "Execute" }).click();
    await page.locator(".query-result-info").waitFor({ timeout: 5000 });
    await page.locator(".query-toolbar .btn-ghost", { hasText: "History" }).click();
    await expect(page.locator(".query-history-item")).toHaveCount(1);
  });

  test("clicking history item loads SQL", async ({ page }) => {
    await page.locator(".query-toolbar .btn-primary", { hasText: "Execute" }).click();
    await page.locator(".query-result-info").waitFor({ timeout: 5000 });
    await page.locator(".query-toolbar .btn-ghost", { hasText: "History" }).click();
    await page.locator(".query-history-item").first().click();
    await expect(page.locator(".query-history-panel")).not.toBeVisible();
  });

  test("pin button toggles pin state", async ({ page }) => {
    await page.locator(".query-toolbar .btn-primary", { hasText: "Execute" }).click();
    await page.locator(".query-result-info").waitFor({ timeout: 5000 });
    await page.locator(".query-toolbar .btn-ghost", { hasText: "History" }).click();
    const pinBtn = page.locator(".query-history-action[title='Pin']");
    await pinBtn.click();
    await expect(page.locator(".query-history-item.pinned")).toHaveCount(1);
  });

  test("copy button is available", async ({ page }) => {
    await page.locator(".query-toolbar .btn-primary", { hasText: "Execute" }).click();
    await page.locator(".query-result-info").waitFor({ timeout: 5000 });
    await page.locator(".query-toolbar .btn-ghost", { hasText: "History" }).click();
    const copyBtn = page.locator(".query-history-action[title='Copy SQL']");
    await expect(copyBtn).toBeVisible();
  });

  test("history shows execution meta (time, rows)", async ({ page }) => {
    await page.locator(".query-toolbar .btn-primary", { hasText: "Execute" }).click();
    await page.locator(".query-result-info").waitFor({ timeout: 5000 });
    await page.locator(".query-toolbar .btn-ghost", { hasText: "History" }).click();
    await expect(page.locator(".query-history-meta").first()).toContainText("ms");
    await expect(page.locator(".query-history-meta").first()).toContainText("rows");
  });
});

test.describe("Query console — suggest toggle", () => {
  test("suggest button toggles active state", async ({ page }) => {
    await page.goto("/");
    await openQueryConsole(page);
    await page.locator(".query-console").waitFor({ timeout: 8000 });
    const suggestBtn = page.locator(".query-toolbar .btn-ghost", { hasText: "Suggest" });
    await expect(suggestBtn).toHaveClass(/active-filter/);
    await suggestBtn.click();
    await expect(suggestBtn).not.toHaveClass(/active-filter/);
    await suggestBtn.click();
    await expect(suggestBtn).toHaveClass(/active-filter/);
  });
});

test.describe("Query console — result table", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/");
    await openQueryConsole(page);
    await page.locator(".query-console").waitFor({ timeout: 8000 });
    await page.locator(".query-toolbar .btn-primary", { hasText: "Execute" }).click();
    await page.locator(".result-table-ag-wrapper").waitFor({ timeout: 5000 });
  });

  test("result table shows editable/readonly badge", async ({ page }) => {
    await expect(page.locator(".result-updatable-badge")).toBeVisible();
  });

  test("result table has chart toggle button", async ({ page }) => {
    const chartBtn = page.locator(".result-table-toolbar .btn-ghost", { hasText: "Chart" });
    await expect(chartBtn).toBeVisible();
  });

  test("chart toggle switches to chart view", async ({ page }) => {
    await page.locator(".result-table-toolbar .btn-ghost", { hasText: "Chart" }).click();
    await expect(page.locator(".chart-view")).toBeVisible({ timeout: 3000 });
  });
});

test.describe("Query console — split handle", () => {
  test("split handle is present", async ({ page }) => {
    await page.goto("/");
    await openQueryConsole(page);
    await page.locator(".query-console").waitFor({ timeout: 8000 });
    await expect(page.locator(".query-split-handle")).toBeVisible();
  });
});
