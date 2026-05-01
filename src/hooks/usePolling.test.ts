import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { renderHook, act } from "@testing-library/react";
import { usePolling } from "./usePolling";

describe("usePolling", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    // Default to "visible" for every test; individual tests flip it.
    Object.defineProperty(document, "hidden", {
      configurable: true,
      get: () => false,
    });
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  function flushMicrotasks() {
    // Fake timers don't drain promise microtasks; we have to do it
    // manually to observe the inFlight reset between ticks.
    return Promise.resolve();
  }

  it("invokes the callback once on mount and then every interval", async () => {
    const cb = vi.fn().mockResolvedValue(undefined);
    renderHook(() => usePolling(cb, 1000));

    expect(cb).toHaveBeenCalledTimes(1);

    // Drain microtasks so the initial call's .finally() clears
    // inFlight before the next interval tick fires; vi.useFakeTimers
    // does not drain promises between timer firings.
    await act(async () => {
      await flushMicrotasks();
      await vi.advanceTimersByTimeAsync(1000);
    });
    expect(cb).toHaveBeenCalledTimes(2);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(2500);
    });
    expect(cb).toHaveBeenCalledTimes(4);
  });

  it("does not start the interval when enabled=false", async () => {
    const cb = vi.fn().mockResolvedValue(undefined);
    renderHook(() => usePolling(cb, 1000, false));

    expect(cb).not.toHaveBeenCalled();
    await act(async () => {
      vi.advanceTimersByTime(5000);
      await flushMicrotasks();
    });
    expect(cb).not.toHaveBeenCalled();
  });

  it("does not start the interval for non-positive intervalMs", async () => {
    const cb = vi.fn().mockResolvedValue(undefined);
    renderHook(() => usePolling(cb, 0));
    expect(cb).not.toHaveBeenCalled();
    await act(async () => {
      vi.advanceTimersByTime(5000);
      await flushMicrotasks();
    });
    expect(cb).not.toHaveBeenCalled();
  });

  it("skips ticks while document.hidden is true", async () => {
    Object.defineProperty(document, "hidden", {
      configurable: true,
      get: () => true,
    });

    const cb = vi.fn().mockResolvedValue(undefined);
    renderHook(() => usePolling(cb, 1000));

    // Initial tick is also gated on hidden.
    expect(cb).not.toHaveBeenCalled();
    await act(async () => {
      vi.advanceTimersByTime(5000);
      await flushMicrotasks();
    });
    expect(cb).not.toHaveBeenCalled();
  });

  it("resumes immediately when the page becomes visible again", async () => {
    let hidden = true;
    Object.defineProperty(document, "hidden", {
      configurable: true,
      get: () => hidden,
    });

    const cb = vi.fn().mockResolvedValue(undefined);
    renderHook(() => usePolling(cb, 1000));

    expect(cb).not.toHaveBeenCalled();
    await act(async () => {
      vi.advanceTimersByTime(5000);
      await flushMicrotasks();
    });
    expect(cb).not.toHaveBeenCalled();

    // Flip to visible and dispatch the event.
    hidden = false;
    await act(async () => {
      document.dispatchEvent(new Event("visibilitychange"));
      await flushMicrotasks();
    });
    expect(cb).toHaveBeenCalledTimes(1);
  });

  it("drops a tick that fires while the previous call is still pending", async () => {
    let resolveCb: (() => void) | null = null;
    const cb = vi.fn(
      () =>
        new Promise<void>((resolve) => {
          resolveCb = resolve;
        }),
    );

    renderHook(() => usePolling(cb, 1000));
    // Initial tick fires and is held pending.
    expect(cb).toHaveBeenCalledTimes(1);

    // Three full intervals pass while the first call is still in
    // flight; none of them should queue another fetch.
    await act(async () => {
      vi.advanceTimersByTime(3000);
      await flushMicrotasks();
    });
    expect(cb).toHaveBeenCalledTimes(1);

    // Resolve the in-flight promise; the *next* tick should fire.
    await act(async () => {
      resolveCb?.();
      await flushMicrotasks();
    });
    await act(async () => {
      vi.advanceTimersByTime(1000);
      await flushMicrotasks();
    });
    expect(cb).toHaveBeenCalledTimes(2);
  });

  it("keeps polling after the callback rejects (errors are swallowed by the hook)", async () => {
    const cb = vi
      .fn()
      .mockRejectedValueOnce(new Error("boom"))
      .mockResolvedValue(undefined);

    renderHook(() => usePolling(cb, 1000));
    // Initial tick triggers the rejection.
    expect(cb).toHaveBeenCalledTimes(1);
    await act(async () => {
      await flushMicrotasks();
    });

    await act(async () => {
      vi.advanceTimersByTime(1000);
      await flushMicrotasks();
    });
    expect(cb).toHaveBeenCalledTimes(2);
  });

  it("clears the interval and removes the visibility listener on unmount", async () => {
    const cb = vi.fn().mockResolvedValue(undefined);
    const removeSpy = vi.spyOn(document, "removeEventListener");
    const { unmount } = renderHook(() => usePolling(cb, 1000));

    unmount();
    await act(async () => {
      vi.advanceTimersByTime(10000);
      await flushMicrotasks();
    });

    // Initial tick already fired; no further calls should land after
    // unmount.
    expect(cb).toHaveBeenCalledTimes(1);
    expect(removeSpy).toHaveBeenCalledWith(
      "visibilitychange",
      expect.any(Function),
    );
  });

  it("uses the LATEST callback even though the interval is not torn down on rerender", async () => {
    const a = vi.fn().mockResolvedValue(undefined);
    const b = vi.fn().mockResolvedValue(undefined);
    const { rerender } = renderHook(
      ({ cb }) => usePolling(cb, 1000),
      { initialProps: { cb: a } },
    );

    expect(a).toHaveBeenCalledTimes(1);
    await act(async () => {
      await flushMicrotasks();
    });

    rerender({ cb: b });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(1000);
    });
    expect(b).toHaveBeenCalledTimes(1);
    // `a` is no longer invoked even though we never reset the timer.
    expect(a).toHaveBeenCalledTimes(1);
  });
});
