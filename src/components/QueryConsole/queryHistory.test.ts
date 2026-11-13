import { describe, it, expect } from "vitest";
import { addHistoryEntry, hasUnsavedSql, needsReplaceConfirm, sortHistoryForDisplay, toggleHistoryPin, type HistoryEntry } from "./queryHistory";

const run = (sql: string, timestamp = 1) => ({ sql, timestamp, executionTimeMs: 1, rowCount: 1 });

describe("query history", () => {
  it("adds new runs at the top with an id", () => {
    let h: HistoryEntry[] = [];
    h = addHistoryEntry(h, run("SELECT 1"));
    h = addHistoryEntry(h, run("SELECT 2"));
    expect(h.map((e) => e.sql)).toEqual(["SELECT 2", "SELECT 1"]);
    expect(h[0].id).not.toBe(h[1].id);
  });

  it("keeps pins across later runs", () => {
    let h = addHistoryEntry([], run("SELECT 1"));
    h = toggleHistoryPin(h, h[0].id);
    h = addHistoryEntry(h, run("SELECT 2"));
    h = addHistoryEntry(h, run("SELECT 3"));
    expect(h.find((e) => e.sql === "SELECT 1")?.pinned).toBe(true);
    expect(sortHistoryForDisplay(h)[0].sql).toBe("SELECT 1");
  });

  it("re-running a pinned query keeps it pinned (and its id)", () => {
    let h = addHistoryEntry([], run("SELECT 1"));
    const id = h[0].id;
    h = toggleHistoryPin(h, id);
    h = addHistoryEntry(h, run("SELECT 2"));
    h = addHistoryEntry(h, run("  SELECT 1  ", 5));
    expect(h).toHaveLength(2);
    expect(h[0]).toMatchObject({ id, pinned: true, timestamp: 5 });
  });

  it("caps only unpinned entries", () => {
    let h = addHistoryEntry([], run("pinned"));
    h = toggleHistoryPin(h, h[0].id);
    for (let i = 0; i < 10; i++) h = addHistoryEntry(h, run(`q${i}`), 3);
    expect(h.filter((e) => !e.pinned).map((e) => e.sql)).toEqual(["q9", "q8", "q7"]);
    expect(h.some((e) => e.sql === "pinned" && e.pinned)).toBe(true);
  });

  it("unpins", () => {
    let h = addHistoryEntry([], run("SELECT 1"));
    h = toggleHistoryPin(h, h[0].id);
    h = toggleHistoryPin(h, h[0].id);
    expect(h[0].pinned).toBe(false);
  });

  it("orders pinned first, otherwise newest first", () => {
    let h: HistoryEntry[] = [];
    for (const s of ["a", "b", "c"]) h = addHistoryEntry(h, run(s));
    h = toggleHistoryPin(h, h.find((e) => e.sql === "a")!.id);
    expect(sortHistoryForDisplay(h).map((e) => e.sql)).toEqual(["a", "c", "b"]);
  });
});

describe("unsaved console SQL", () => {
  it("an untitled console is dirty once something other than the default is typed", () => {
    expect(hasUnsavedSql("SELECT 1;", null)).toBe(false);
    expect(hasUnsavedSql("  ", null)).toBe(false);
    expect(hasUnsavedSql("SELECT * FROM users", null)).toBe(true);
  });

  it("a loaded saved query is dirty when the text differs", () => {
    expect(hasUnsavedSql("SELECT 2", "SELECT 2")).toBe(false);
    expect(hasUnsavedSql("SELECT 3", "SELECT 2")).toBe(true);
  });

  it("asks before replacing unsaved text that isn't in history", () => {
    expect(needsReplaceConfirm("SELECT 'draft'", null, "SELECT 1", [])).toBe(true);
    expect(needsReplaceConfirm("SELECT 'draft'", null, "SELECT 1", ["SELECT 'draft'"])).toBe(false);
    expect(needsReplaceConfirm("SELECT 1;", null, "SELECT 2", [])).toBe(false);
    expect(needsReplaceConfirm("SELECT 'x'", null, "SELECT 'x'", [])).toBe(false);
    expect(needsReplaceConfirm("SELECT edited", "SELECT saved", "SELECT other", [])).toBe(true);
  });
});
