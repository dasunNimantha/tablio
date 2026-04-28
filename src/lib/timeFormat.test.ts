import { describe, it, expect } from "vitest";
import { formatRelativeTime } from "./timeFormat";

/**
 * `now` reference used across the deterministic relative-time tests.
 * Picked to be UTC-aligned so the maths in the deltas is obvious.
 */
const NOW = Date.parse("2026-04-28T18:00:00Z");

describe("formatRelativeTime — null/empty handling", () => {
  it("returns null for null", () => {
    expect(formatRelativeTime(null, NOW)).toBeNull();
  });

  it("returns null for undefined", () => {
    expect(formatRelativeTime(undefined, NOW)).toBeNull();
  });

  it("returns null for the empty string", () => {
    expect(formatRelativeTime("", NOW)).toBeNull();
  });
});

describe("formatRelativeTime — Postgres timestamp normalisation", () => {
  it("parses microsecond precision with `+00` timezone (the production case)", () => {
    const ts = "2026-04-28 09:00:14.374922+00";
    // 9 hours before NOW => "9h ago"
    expect(formatRelativeTime(ts, NOW)).toBe("9h ago");
  });

  it("parses second precision (no fractional seconds) with `+00` timezone", () => {
    const ts = "2026-04-28 09:00:14+00";
    expect(formatRelativeTime(ts, NOW)).toBe("9h ago");
  });

  it("parses microsecond precision with full `+HH:MM` timezone", () => {
    // 18:00 UTC == 23:30 IST. 23:30-23:30 = "just now".
    const ts = "2026-04-28 23:30:00.000000+05:30";
    expect(formatRelativeTime(ts, NOW)).toBe("just now");
  });

  it("parses negative timezone offsets", () => {
    // 18:00 UTC == 10:00 PDT (-08:00). 10:00-10:00 = "just now".
    const ts = "2026-04-28 10:00:00.123456-08";
    expect(formatRelativeTime(ts, NOW)).toBe("just now");
  });

  it("returns the raw string when parsing fails", () => {
    expect(formatRelativeTime("not a date", NOW)).toBe("not a date");
  });
});

describe("formatRelativeTime — relative buckets", () => {
  const at = (msAgo: number): string => new Date(NOW - msAgo).toISOString();

  it("'just now' for <= 5s in the past", () => {
    expect(formatRelativeTime(at(0), NOW)).toBe("just now");
    expect(formatRelativeTime(at(5_000), NOW)).toBe("just now");
  });

  it("'Ns ago' for 6s..59s", () => {
    expect(formatRelativeTime(at(6_000), NOW)).toBe("6s ago");
    expect(formatRelativeTime(at(59_000), NOW)).toBe("59s ago");
  });

  it("'Nm ago' for 1m..59m", () => {
    expect(formatRelativeTime(at(60_000), NOW)).toBe("1m ago");
    expect(formatRelativeTime(at(59 * 60_000), NOW)).toBe("59m ago");
  });

  it("'Nh ago' for 1h..23h", () => {
    expect(formatRelativeTime(at(60 * 60_000), NOW)).toBe("1h ago");
    expect(formatRelativeTime(at(9 * 60 * 60_000), NOW)).toBe("9h ago");
    expect(formatRelativeTime(at(23 * 60 * 60_000), NOW)).toBe("23h ago");
  });

  it("'Nd ago' for 1d..29d", () => {
    expect(formatRelativeTime(at(24 * 60 * 60_000), NOW)).toBe("1d ago");
    expect(formatRelativeTime(at(29 * 24 * 60 * 60_000), NOW)).toBe("29d ago");
  });

  it("falls back to a calendar date for >= 30d", () => {
    const v = formatRelativeTime(at(45 * 24 * 60 * 60_000), NOW);
    // The exact text depends on the host locale, but it must NOT be "Nd ago"
    // and it must contain a digit (the day-of-month).
    expect(v).toBeTruthy();
    expect(v).not.toMatch(/d ago$/);
    expect(v).toMatch(/\d/);
  });

  it("collapses future timestamps (clock skew) to 'just now'", () => {
    const future = new Date(NOW + 10_000).toISOString();
    expect(formatRelativeTime(future, NOW)).toBe("just now");
  });
});
