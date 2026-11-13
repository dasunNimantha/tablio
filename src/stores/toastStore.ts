import { create } from "zustand";

export interface Toast {
  id: string;
  message: string;
  type: "success" | "error" | "info";
}

/** How long a toast stays up, by type. Errors usually need to be read
 *  (and often copied), so they linger much longer than confirmations. */
export const TOAST_DURATION_MS: Record<Toast["type"], number> = {
  success: 3000,
  info: 3000,
  error: 10000,
};

interface ToastState {
  toasts: Toast[];
  addToast: (message: string, type?: Toast["type"]) => void;
  removeToast: (id: string) => void;
  /** Stop the auto-dismiss countdown (pointer hover / keyboard focus). */
  pauseToast: (id: string) => void;
  /** Resume the countdown with whatever time was left when paused. */
  resumeToast: (id: string) => void;
}

interface Timer {
  handle: ReturnType<typeof setTimeout> | null;
  remaining: number;
  startedAt: number;
}

const timers = new Map<string, Timer>();

export const useToastStore = create<ToastState>((set, get) => {
  const schedule = (id: string, ms: number) => {
    const handle = setTimeout(() => {
      timers.delete(id);
      set((s) => ({ toasts: s.toasts.filter((t) => t.id !== id) }));
    }, ms);
    timers.set(id, { handle, remaining: ms, startedAt: Date.now() });
  };

  return {
    toasts: [],

    addToast: (message, type = "success") => {
      const id = crypto.randomUUID();
      set((s) => ({ toasts: [...s.toasts, { id, message, type }] }));
      schedule(id, TOAST_DURATION_MS[type]);
    },

    removeToast: (id) => {
      const t = timers.get(id);
      if (t?.handle) clearTimeout(t.handle);
      timers.delete(id);
      set((s) => ({ toasts: s.toasts.filter((t) => t.id !== id) }));
    },

    pauseToast: (id) => {
      const t = timers.get(id);
      if (!t || !t.handle) return;
      clearTimeout(t.handle);
      t.handle = null;
      t.remaining = Math.max(0, t.remaining - (Date.now() - t.startedAt));
    },

    resumeToast: (id) => {
      const t = timers.get(id);
      if (!t || t.handle) return;
      if (!get().toasts.some((x) => x.id === id)) {
        timers.delete(id);
        return;
      }
      // Give the reader at least a moment after they move away.
      schedule(id, Math.max(t.remaining, 1500));
    },
  };
});
