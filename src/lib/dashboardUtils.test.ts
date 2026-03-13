import { describe, it, expect } from "vitest";
import {
  computeRate,
  formatVal,
  makeLabels,
  formatDuration,
  formatQueryDuration,
  cacheHitClass,
  speedClass,
  filterSessions,
} from "./dashboardUtils";
import type { DatabaseStats } from "./tauri";

function makeStats(overrides: Partial<DatabaseStats> = {}): DatabaseStats {
  return {
    active_connections: 0,
    idle_connections: 0,
    idle_in_transaction: 0,
    total_connections: 0,
    xact_commit: 0,
    xact_rollback: 0,
    tup_inserted: 0,
    tup_updated: 0,
    tup_deleted: 0,
    tup_fetched: 0,
    blks_read: 0,
    blks_hit: 0,
    timestamp_ms: 0,
    ...overrides,
  };
}

describe("computeRate", () => {
  it("returns zeroed rates when dt is zero", () => {
    const prev = makeStats({ timestamp_ms: 1000 });
    const cur = makeStats({ timestamp_ms: 1000 });
    const rate = computeRate(prev, cur);
    expect(rate.tps_commit).toBe(0);
    expect(rate.tps_rollback).toBe(0);
    expect(rate.tup_ins).toBe(0);
    expect(rate.blks_hit).toBe(0);
  });

  it("returns zeroed rates when dt is negative", () => {
    const prev = makeStats({ timestamp_ms: 2000 });
    const cur = makeStats({ timestamp_ms: 1000 });
    const rate = computeRate(prev, cur);
    expect(rate.tps_commit).toBe(0);
  });

  it("computes correct rates for 1-second interval", () => {
    const prev = makeStats({ timestamp_ms: 0, xact_commit: 100, xact_rollback: 10 });
    const cur = makeStats({ timestamp_ms: 1000, xact_commit: 110, xact_rollback: 12 });
    const rate = computeRate(prev, cur);
    expect(rate.tps_commit).toBe(10);
    expect(rate.tps_rollback).toBe(2);
  });

  it("computes correct rates for 2-second interval", () => {
    const prev = makeStats({ timestamp_ms: 0, tup_inserted: 0 });
    const cur = makeStats({ timestamp_ms: 2000, tup_inserted: 100 });
    const rate = computeRate(prev, cur);
    expect(rate.tup_ins).toBe(50);
  });

  it("clamps negative deltas to zero", () => {
    const prev = makeStats({ timestamp_ms: 0, xact_commit: 200 });
    const cur = makeStats({ timestamp_ms: 1000, xact_commit: 100 });
    const rate = computeRate(prev, cur);
    expect(rate.tps_commit).toBe(0);
  });

  it("handles fractional second intervals", () => {
    const prev = makeStats({ timestamp_ms: 0, blks_hit: 0 });
    const cur = makeStats({ timestamp_ms: 500, blks_hit: 100 });
    const rate = computeRate(prev, cur);
    expect(rate.blks_hit).toBe(200);
  });
});

describe("formatVal", () => {
  it("formats zero", () => {
    expect(formatVal(0)).toBe("0");
  });

  it("formats small positive", () => {
    expect(formatVal(0.5)).toBe("0.50");
    expect(formatVal(0.05)).toBe("0.05");
  });

  it("formats values >= 1 and < 100", () => {
    expect(formatVal(1)).toBe("1.0");
    expect(formatVal(50.5)).toBe("50.5");
    expect(formatVal(99.9)).toBe("99.9");
  });

  it("formats values >= 100 and < 1000", () => {
    expect(formatVal(100)).toBe("100");
    expect(formatVal(999)).toBe("999");
    expect(formatVal(500.7)).toBe("501");
  });

  it("formats thousands", () => {
    expect(formatVal(1000)).toBe("1.0K");
    expect(formatVal(1500)).toBe("1.5K");
    expect(formatVal(999999)).toBe("1000.0K");
  });

  it("formats millions", () => {
    expect(formatVal(1000000)).toBe("1.0M");
    expect(formatVal(2500000)).toBe("2.5M");
  });
});

describe("makeLabels", () => {
  it("returns empty array for count 0", () => {
    expect(makeLabels(0)).toEqual([]);
  });

  it("returns ['now'] for count 1", () => {
    expect(makeLabels(1)).toEqual(["now"]);
  });

  it("last element is always 'now'", () => {
    const labels = makeLabels(10);
    expect(labels[labels.length - 1]).toBe("now");
  });

  it("has correct length", () => {
    expect(makeLabels(60).length).toBe(60);
  });

  it("shows time labels at 10-second intervals", () => {
    const labels = makeLabels(30);
    expect(labels[labels.length - 1]).toBe("now");
    expect(labels[labels.length - 6]).toBe("10s ago");
    expect(labels[labels.length - 11]).toBe("20s ago");
  });

  it("uses empty strings for non-interval points", () => {
    const labels = makeLabels(10);
    expect(labels[labels.length - 2]).toBe("");
    expect(labels[labels.length - 3]).toBe("");
  });
});

describe("formatDuration", () => {
  it("returns '-' for null", () => {
    expect(formatDuration(null)).toBe("-");
  });

  it("formats milliseconds", () => {
    expect(formatDuration(500)).toBe("500ms");
    expect(formatDuration(0)).toBe("0ms");
    expect(formatDuration(999)).toBe("999ms");
  });

  it("formats seconds", () => {
    expect(formatDuration(1000)).toBe("1.0s");
    expect(formatDuration(5500)).toBe("5.5s");
    expect(formatDuration(59999)).toBe("60.0s");
  });

  it("formats minutes", () => {
    expect(formatDuration(60000)).toBe("1.0m");
    expect(formatDuration(120000)).toBe("2.0m");
    expect(formatDuration(90000)).toBe("1.5m");
  });

  it("rounds small ms values", () => {
    expect(formatDuration(1.7)).toBe("2ms");
    expect(formatDuration(0.4)).toBe("0ms");
  });
});

describe("formatQueryDuration", () => {
  it("formats sub-millisecond as microseconds", () => {
    expect(formatQueryDuration(0.5)).toBe("500µs");
    expect(formatQueryDuration(0.001)).toBe("1µs");
  });

  it("formats milliseconds", () => {
    expect(formatQueryDuration(1)).toBe("1.0ms");
    expect(formatQueryDuration(500.5)).toBe("500.5ms");
  });

  it("formats seconds", () => {
    expect(formatQueryDuration(1000)).toBe("1.00s");
    expect(formatQueryDuration(5432)).toBe("5.43s");
  });

  it("formats minutes", () => {
    expect(formatQueryDuration(60000)).toBe("1.0m");
    expect(formatQueryDuration(150000)).toBe("2.5m");
  });
});

describe("cacheHitClass", () => {
  it("returns good for >= 90", () => {
    expect(cacheHitClass(90)).toBe("qs-cache-good");
    expect(cacheHitClass(100)).toBe("qs-cache-good");
    expect(cacheHitClass(95.5)).toBe("qs-cache-good");
  });

  it("returns warn for 70-89", () => {
    expect(cacheHitClass(70)).toBe("qs-cache-warn");
    expect(cacheHitClass(89.9)).toBe("qs-cache-warn");
  });

  it("returns bad for < 70", () => {
    expect(cacheHitClass(69.9)).toBe("qs-cache-bad");
    expect(cacheHitClass(0)).toBe("qs-cache-bad");
    expect(cacheHitClass(50)).toBe("qs-cache-bad");
  });
});

describe("speedClass", () => {
  it("returns slow for >= 5000", () => {
    expect(speedClass(5000)).toBe("qs-speed-slow");
    expect(speedClass(10000)).toBe("qs-speed-slow");
  });

  it("returns warn for 1000-4999", () => {
    expect(speedClass(1000)).toBe("qs-speed-warn");
    expect(speedClass(4999)).toBe("qs-speed-warn");
  });

  it("returns empty for < 1000", () => {
    expect(speedClass(999)).toBe("");
    expect(speedClass(0)).toBe("");
    expect(speedClass(500)).toBe("");
  });
});

describe("filterSessions", () => {
  const sessions = [
    { pid: "100", user: "admin", database: "mydb", query: "SELECT 1", state: "active", client_addr: "192.168.1.1" },
    { pid: "200", user: "readonly", database: "analytics", query: "SELECT * FROM logs", state: "idle", client_addr: "10.0.0.5" },
    { pid: "300", user: "admin", database: "mydb", query: "UPDATE users SET name='test'", state: "active", client_addr: null },
  ];

  it("returns all items when search is empty", () => {
    expect(filterSessions(sessions, "")).toEqual(sessions);
    expect(filterSessions(sessions, "  ")).toEqual(sessions);
  });

  it("filters by pid", () => {
    expect(filterSessions(sessions, "100")).toHaveLength(1);
    expect(filterSessions(sessions, "100")[0].pid).toBe("100");
  });

  it("filters by user", () => {
    const result = filterSessions(sessions, "admin");
    expect(result).toHaveLength(2);
  });

  it("filters by database", () => {
    const result = filterSessions(sessions, "analytics");
    expect(result).toHaveLength(1);
  });

  it("filters by query text", () => {
    const result = filterSessions(sessions, "UPDATE");
    expect(result).toHaveLength(1);
    expect(result[0].pid).toBe("300");
  });

  it("filters by state", () => {
    const result = filterSessions(sessions, "idle");
    expect(result).toHaveLength(1);
    expect(result[0].pid).toBe("200");
  });

  it("filters by client_addr", () => {
    const result = filterSessions(sessions, "192.168");
    expect(result).toHaveLength(1);
    expect(result[0].pid).toBe("100");
  });

  it("handles null client_addr gracefully", () => {
    const result = filterSessions(sessions, "10.0.0");
    expect(result).toHaveLength(1);
    expect(result[0].pid).toBe("200");
  });

  it("is case-insensitive", () => {
    expect(filterSessions(sessions, "ADMIN")).toHaveLength(2);
    expect(filterSessions(sessions, "SELECT")).toHaveLength(2);
  });

  it("returns empty array when nothing matches", () => {
    expect(filterSessions(sessions, "nonexistent")).toHaveLength(0);
  });
});
