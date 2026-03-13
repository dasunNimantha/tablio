import { describe, it, expect, beforeEach, vi, afterEach } from "vitest";
import { useToastStore } from "./toastStore";

describe("toastStore", () => {
  beforeEach(() => {
    useToastStore.setState({ toasts: [] });
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("starts with empty toasts", () => {
    expect(useToastStore.getState().toasts).toHaveLength(0);
  });

  it("adds a toast with default type 'success'", () => {
    useToastStore.getState().addToast("Hello");
    const toasts = useToastStore.getState().toasts;
    expect(toasts).toHaveLength(1);
    expect(toasts[0].message).toBe("Hello");
    expect(toasts[0].type).toBe("success");
    expect(toasts[0].id).toBeTruthy();
  });

  it("adds a toast with specified type", () => {
    useToastStore.getState().addToast("Error!", "error");
    expect(useToastStore.getState().toasts[0].type).toBe("error");
  });

  it("adds an info toast", () => {
    useToastStore.getState().addToast("Info", "info");
    expect(useToastStore.getState().toasts[0].type).toBe("info");
  });

  it("can add multiple toasts", () => {
    useToastStore.getState().addToast("First");
    useToastStore.getState().addToast("Second");
    useToastStore.getState().addToast("Third");
    expect(useToastStore.getState().toasts).toHaveLength(3);
  });

  it("each toast has a unique id", () => {
    useToastStore.getState().addToast("A");
    useToastStore.getState().addToast("B");
    const ids = useToastStore.getState().toasts.map((t) => t.id);
    expect(new Set(ids).size).toBe(2);
  });

  it("removes toast by id", () => {
    useToastStore.getState().addToast("Keep");
    useToastStore.getState().addToast("Remove");
    const removeId = useToastStore.getState().toasts[1].id;
    useToastStore.getState().removeToast(removeId);
    expect(useToastStore.getState().toasts).toHaveLength(1);
    expect(useToastStore.getState().toasts[0].message).toBe("Keep");
  });

  it("removeToast is a no-op for unknown id", () => {
    useToastStore.getState().addToast("Keep");
    useToastStore.getState().removeToast("nonexistent");
    expect(useToastStore.getState().toasts).toHaveLength(1);
  });

  it("auto-removes toast after 3 seconds", () => {
    useToastStore.getState().addToast("Temporary");
    expect(useToastStore.getState().toasts).toHaveLength(1);
    vi.advanceTimersByTime(3000);
    expect(useToastStore.getState().toasts).toHaveLength(0);
  });

  it("does not remove toast before 3 seconds", () => {
    useToastStore.getState().addToast("Temporary");
    vi.advanceTimersByTime(2999);
    expect(useToastStore.getState().toasts).toHaveLength(1);
  });

  it("handles multiple toasts auto-expiring at different times", () => {
    useToastStore.getState().addToast("First");
    vi.advanceTimersByTime(1000);
    useToastStore.getState().addToast("Second");
    vi.advanceTimersByTime(2000);
    expect(useToastStore.getState().toasts).toHaveLength(1);
    expect(useToastStore.getState().toasts[0].message).toBe("Second");
    vi.advanceTimersByTime(1000);
    expect(useToastStore.getState().toasts).toHaveLength(0);
  });
});
