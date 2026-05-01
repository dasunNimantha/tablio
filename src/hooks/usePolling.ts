import { useEffect, useRef } from "react";

/**
 * Run `callback` once on mount and then every `intervalMs`, with two
 * guarantees that vanilla `setInterval(fetch, ms)` does not give you:
 *
 *   1. **Visibility-aware:** ticks are skipped while
 *      `document.hidden` is true, so a minimized window or a
 *      backgrounded tab cannot pile up a backlog of in-flight
 *      `fetch`/IPC requests over hours of idle. We re-run `callback`
 *      immediately when the window becomes visible again so the user
 *      doesn't see stale data.
 *
 *   2. **Re-entrancy-safe:** if the previous `callback` is still
 *      running when the next tick fires, the new tick is dropped
 *      rather than overlapping. Without this, a single slow IPC
 *      response (a stuck DB query, a paused tunnel) would let calls
 *      queue up linearly until the renderer's main thread fell
 *      behind on event processing — the exact failure mode that
 *      froze v0.3.1.
 *
 * Pass `enabled = false` to suspend polling without unmounting (used
 * by Activity dashboard's pause button). Setting `intervalMs <= 0`
 * also suspends and is the conventional way for callers to expose
 * "off" in their own UI.
 *
 * `callback` may be sync or async; its return value is ignored.
 * The hook intentionally does not pass it any arguments — the
 * caller's closure already captures everything it needs.
 */
export function usePolling(
  callback: () => void | Promise<void>,
  intervalMs: number,
  enabled: boolean = true,
): void {
  // Hold the latest callback in a ref so the interval doesn't have to
  // tear down + rebuild every time the parent re-renders (which would
  // be every keystroke for components with a search box).
  const cbRef = useRef(callback);
  useEffect(() => {
    cbRef.current = callback;
  }, [callback]);

  useEffect(() => {
    if (!enabled || intervalMs <= 0) return;

    let alive = true;
    let inFlight = false;

    const tick = () => {
      if (!alive || inFlight) return;
      if (typeof document !== "undefined" && document.hidden) return;
      inFlight = true;
      Promise.resolve(cbRef.current())
        .catch(() => {
          // The callback owns its own error UX; we just need to
          // keep `inFlight` honest.
        })
        .finally(() => {
          inFlight = false;
        });
    };

    tick();
    const id = setInterval(tick, intervalMs);
    const onVisibility = () => {
      if (!document.hidden) tick();
    };
    if (typeof document !== "undefined") {
      document.addEventListener("visibilitychange", onVisibility);
    }

    return () => {
      alive = false;
      clearInterval(id);
      if (typeof document !== "undefined") {
        document.removeEventListener("visibilitychange", onVisibility);
      }
    };
  }, [enabled, intervalMs]);
}
