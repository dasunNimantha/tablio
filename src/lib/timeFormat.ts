/**
 * Format helpers for time values returned by database backends.
 *
 * These live in their own module (separate from the components that use
 * them) so they can be unit-tested without spinning up React or jsdom.
 */

/** Format a Postgres-style timestamp string as a compact relative label.
 *
 *  Postgres prints `timestamptz` values as
 *      "YYYY-MM-DD HH:MM:SS.ffffff+TZ"
 *  e.g. "2026-04-28 08:30:14.374922+00".
 *
 *  V8's `Date.parse` rejects two parts of that format:
 *    - microsecond precision (it only supports milliseconds), and
 *    - bare two-digit timezone offsets (it requires `+HH:MM` or `Z`).
 *
 *  So we normalise first, then bucket the resulting delta into one of
 *  the friendly labels: "just now", "Ns ago", "Nm ago", "Nh ago",
 *  "Nd ago", or a calendar date (`Apr 28`) for anything older than ~30
 *  days. If the value is unparseable, the original string is returned
 *  unchanged so the caller still has *something* to render.
 *
 *  An optional `now` parameter exists for testability — production
 *  callers should leave it default (Date.now()).
 */
export function formatRelativeTime(
  ts: string | null | undefined,
  now: number = Date.now()
): string | null {
  if (!ts) return null;

  // Normalise Postgres's wire format into something Date can parse on
  // every browser:
  //   "2026-04-28 08:30:14.374922+00"  ->  "2026-04-28T08:30:14.374Z"
  //   1. Replace the space with 'T'.
  //   2. Truncate fractional seconds to 3 digits (JS only supports ms).
  //   3. Expand a bare "+HH" / "-HH" timezone to "+HH:00" / "-HH:00".
  //   4. Treat "+00:00" / "-00:00" as 'Z' for clarity.
  let iso = ts.replace(" ", "T");
  iso = iso.replace(/(\.\d{3})\d+/, "$1");
  iso = iso.replace(/([+-]\d{2})$/, "$1:00");
  iso = iso.replace(/[+-]00:00$/, "Z");

  const d = new Date(iso);
  if (isNaN(d.getTime())) return ts;

  const diffMs = now - d.getTime();
  // Future timestamps (clock skew between client and DB server) just
  // collapse to "just now" rather than rendering a confusing negative.
  if (diffMs < 0) return "just now";

  const sec = Math.round(diffMs / 1000);
  if (sec < 60) return sec <= 5 ? "just now" : `${sec}s ago`;
  const min = Math.round(sec / 60);
  if (min < 60) return `${min}m ago`;
  const hr = Math.round(min / 60);
  if (hr < 24) return `${hr}h ago`;
  const day = Math.round(hr / 24);
  if (day < 30) return `${day}d ago`;
  return d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}
