import {
  createContext,
  useContext,
  useEffect,
  useId,
  useRef,
  useState,
  type FormEvent,
  type ReactNode,
  type RefObject,
} from "react";
import { createPortal } from "react-dom";

/**
 * Shared accessible modal shell used by every dialog in the app.
 *
 * - `role="dialog"` + `aria-modal` + `aria-labelledby` / `aria-label`
 * - Escape closes the topmost modal (unless `busy` or `closeOnEscape=false`)
 * - Tab / Shift+Tab are trapped inside the dialog
 * - On open, focus moves to `initialFocusRef`, an element marked
 *   `data-autofocus`, the first form field, the primary button, or the
 *   dialog itself (in that order) unless something inside already has focus
 * - On close, focus returns to the element that had it when the modal opened
 * - Overlay clicks close the modal unless `busy` or `closeOnOverlayClick=false`
 *   (callers pass `false` while the form holds unsaved work)
 */
export interface ModalProps {
  onClose: () => void;
  children: ReactNode;
  /** Classes for the dialog element (default `"dialog"`). */
  className?: string;
  /** Classes for the backdrop (default `"dialog-overlay"`). */
  overlayClassName?: string;
  /** id of the element that names the dialog. */
  labelledBy?: string;
  /** Accessible name when there's no visible title element. */
  label?: string;
  describedBy?: string;
  role?: "dialog" | "alertdialog";
  /** While true, Escape and overlay clicks are ignored. */
  busy?: boolean;
  closeOnEscape?: boolean;
  closeOnOverlayClick?: boolean;
  initialFocusRef?: RefObject<HTMLElement | null>;
  /** Render into document.body instead of in place. */
  portal?: boolean;
  /** Render the dialog element as a <form> (for submit-on-Enter prompts). */
  as?: "div" | "form";
  onSubmit?: (e: FormEvent<HTMLFormElement>) => void;
  testId?: string;
}

const TABBABLE = [
  "a[href]",
  "button:not([disabled])",
  "input:not([disabled]):not([type='hidden'])",
  "select:not([disabled])",
  "textarea:not([disabled])",
  "[tabindex]:not([tabindex='-1'])",
  "[contenteditable='true']",
].join(",");

function isVisible(el: HTMLElement): boolean {
  if (typeof el.checkVisibility === "function") {
    return el.checkVisibility({ visibilityProperty: true });
  }
  // No layout information (jsdom): fall back to computed styles.
  for (let n: HTMLElement | null = el; n; n = n.parentElement) {
    const cs = getComputedStyle(n);
    if (cs.display === "none") return false;
    if (n === el && cs.visibility === "hidden") return false;
  }
  return true;
}

export function getTabbable(root: HTMLElement): HTMLElement[] {
  return Array.from(root.querySelectorAll<HTMLElement>(TABBABLE)).filter(
    (el) => el.tabIndex >= 0 && isVisible(el) && !el.closest("[inert]"),
  );
}

function pickInitialFocus(root: HTMLElement): HTMLElement {
  const marked = root.querySelector<HTMLElement>("[data-autofocus]");
  if (marked && isVisible(marked)) return marked;
  const tabbable = getTabbable(root);
  const field = tabbable.find((el) =>
    el.matches("input:not([type='checkbox']):not([type='radio']):not([readonly]), select, textarea"),
  );
  if (field) return field;
  const primary = tabbable.find((el) => el.matches(".btn-primary"));
  if (primary) return primary;
  const notClose = tabbable.find((el) => !el.closest(".dialog-header, [data-modal-close]"));
  return notClose ?? root;
}

/**
 * Open modals. The topmost is the deepest-nested one (a ConfirmDialog
 * inside a dialog), and among equals the most recently opened; only it
 * reacts to Escape / Tab. Nesting depth comes from context because child
 * effects run before their parent's, so mount order alone is inverted.
 */
interface StackEntry {
  token: symbol;
  depth: number;
  seq: number;
}
const modalStack: StackEntry[] = [];
let modalSeq = 0;
const ModalDepth = createContext(0);

function topModal(): symbol | undefined {
  let top: StackEntry | undefined;
  for (const e of modalStack) {
    if (!top || e.depth > top.depth || (e.depth === top.depth && e.seq > top.seq)) top = e;
  }
  return top?.token;
}

export function Modal({
  onClose,
  children,
  className = "dialog",
  overlayClassName = "dialog-overlay",
  labelledBy,
  label,
  describedBy,
  role = "dialog",
  busy = false,
  closeOnEscape = true,
  closeOnOverlayClick = true,
  initialFocusRef,
  portal = false,
  as = "div",
  onSubmit,
  testId,
}: ModalProps) {
  const dialogRef = useRef<HTMLElement | null>(null);
  const depth = useContext(ModalDepth) + 1;
  const [token] = useState(() => Symbol("modal"));
  // Capture the opener during the first render: by the time effects run,
  // autoFocus children may already have taken focus.
  const [opener] = useState<Element | null>(() =>
    typeof document !== "undefined" ? document.activeElement : null,
  );
  const pointerDownOnOverlay = useRef(false);

  // Keep latest values in refs so the document listener stays stable.
  const stateRef = useRef({ onClose, busy, closeOnEscape });
  stateRef.current = { onClose, busy, closeOnEscape };

  useEffect(() => {
    modalStack.push({ token, depth, seq: ++modalSeq });
    const dialog = dialogRef.current;
    const nestedOpen = modalStack.some((e) => e.depth > depth);
    if (dialog && !nestedOpen && !dialog.contains(document.activeElement)) {
      const target = initialFocusRef?.current ?? pickInitialFocus(dialog);
      target.focus({ preventScroll: false });
    }

    const onKeyDown = (e: KeyboardEvent) => {
      if (topModal() !== token) return;
      const el = dialogRef.current;
      if (!el) return;
      if (e.key === "Escape") {
        if (e.defaultPrevented) return;
        const { busy, closeOnEscape, onClose } = stateRef.current;
        e.preventDefault();
        e.stopPropagation();
        if (!busy && closeOnEscape) onClose();
        return;
      }
      if (e.key !== "Tab") return;
      const items = getTabbable(el);
      if (items.length === 0) {
        e.preventDefault();
        el.focus();
        return;
      }
      const first = items[0];
      const last = items[items.length - 1];
      const active = document.activeElement as HTMLElement | null;
      const inside = !!active && el.contains(active);
      if (e.shiftKey) {
        if (!inside || active === first || active === el) {
          e.preventDefault();
          last.focus();
        }
      } else if (!inside || active === last) {
        e.preventDefault();
        first.focus();
      }
    };
    // Focus that escapes (e.g. a click on the backdrop of a portal) is
    // pulled back into the topmost dialog.
    const onFocusIn = (e: FocusEvent) => {
      if (topModal() !== token) return;
      const el = dialogRef.current;
      const t = e.target as Node | null;
      if (!el || !t || el.contains(t)) return;
      // Portalled popovers (custom selects, menus) opened from inside the
      // dialog are allowed to take focus.
      if ((t as HTMLElement).closest?.("[data-modal-popover], .cs-dropdown, .context-menu")) return;
      el.focus();
    };
    window.addEventListener("keydown", onKeyDown);
    window.addEventListener("focusin", onFocusIn);
    return () => {
      window.removeEventListener("keydown", onKeyDown);
      window.removeEventListener("focusin", onFocusIn);
      const i = modalStack.findIndex((e) => e.token === token);
      if (i >= 0) modalStack.splice(i, 1);
      if (opener instanceof HTMLElement && opener.isConnected && opener !== document.body) {
        opener.focus({ preventScroll: true });
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const fallbackId = useId();
  const dialogProps = {
    ref: (el: HTMLElement | null) => {
      dialogRef.current = el;
    },
    className,
    role,
    "aria-modal": true as const,
    "aria-labelledby": labelledBy,
    "aria-label": labelledBy ? undefined : label,
    "aria-describedby": describedBy,
    "aria-busy": busy || undefined,
    tabIndex: -1,
    "data-testid": testId,
    "data-modal": fallbackId,
  };

  const dialog =
    as === "form" ? (
      <form {...dialogProps} onSubmit={onSubmit} noValidate>
        {children}
      </form>
    ) : (
      <div {...dialogProps}>{children}</div>
    );

  const tree = (
    <div
      className={overlayClassName}
      onMouseDown={(e) => {
        pointerDownOnOverlay.current = e.target === e.currentTarget;
      }}
      onClick={(e) => {
        const fromOverlay = pointerDownOnOverlay.current && e.target === e.currentTarget;
        pointerDownOnOverlay.current = false;
        if (fromOverlay && !busy && closeOnOverlayClick) onClose();
      }}
    >
      {dialog}
    </div>
  );

  const wrapped = <ModalDepth.Provider value={depth}>{tree}</ModalDepth.Provider>;
  return portal ? createPortal(wrapped, document.body) : wrapped;
}
