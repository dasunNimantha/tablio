import { useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { ChevronRight } from "lucide-react";
import { readZoomFactor } from "../../lib/zoom";
import { handleMenuArrowKeys, menuItems } from "./menuKeys";

export interface MenuEntry {
  label: string;
  action?: () => void;
  /** Renders a "Move to ▸" style submenu. */
  children?: MenuEntry[];
  separator?: boolean;
}

interface Props {
  /** Position in CSS px (already divided by the zoom factor). */
  x: number;
  y: number;
  items: MenuEntry[];
  onClose: () => void;
  /** Element to focus when the menu closes (usually the tree row it was opened from). */
  returnFocusTo?: HTMLElement | null;
  label?: string;
}

/**
 * Keyboard-operable context menu (`role="menu"`): focus starts on the first
 * item; Up/Down/Home/End move; Enter/Space activate; Right/Left open and
 * close the submenu; Escape or Tab closes and returns focus to the opener.
 */
export function ContextMenu({ x, y, items, onClose, returnFocusTo, label }: Props) {
  const menuRef = useRef<HTMLDivElement>(null);
  const subRefs = useRef<Record<number, HTMLDivElement | null>>({});
  const triggerRefs = useRef<Record<number, HTMLButtonElement | null>>({});
  const [openSub, setOpenSub] = useState<number | null>(null);
  const focusSubOnOpen = useRef(false);

  const close = () => {
    onClose();
    if (returnFocusTo?.isConnected) returnFocusTo.focus();
  };

  useEffect(() => {
    const menu = menuRef.current;
    if (menu) menuItems(menu)[0]?.focus();
  }, []);

  useEffect(() => {
    if (openSub == null || !focusSubOnOpen.current) return;
    focusSubOnOpen.current = false;
    const sub = subRefs.current[openSub];
    if (sub) menuItems(sub)[0]?.focus();
  }, [openSub]);

  const activate = (item: MenuEntry) => {
    // Close (and restore focus) first so a dialog the action opens
    // records the tree row, not a vanished menu item, as its opener.
    close();
    item.action?.();
  };

  const onMenuKeyDown = (e: React.KeyboardEvent<HTMLDivElement>) => {
    const target = e.target as HTMLElement;
    const inSub = target.closest("[role='menu']") !== menuRef.current;
    if (e.key === "Escape") {
      e.preventDefault();
      e.stopPropagation();
      if (inSub && openSub != null) {
        const i = openSub;
        setOpenSub(null);
        triggerRefs.current[i]?.focus();
      } else {
        close();
      }
      return;
    }
    if (e.key === "Tab") {
      e.preventDefault();
      close();
      return;
    }
    if (inSub && e.key === "ArrowLeft" && openSub != null) {
      e.preventDefault();
      e.stopPropagation();
      const i = openSub;
      setOpenSub(null);
      triggerRefs.current[i]?.focus();
      return;
    }
    const menu = inSub ? (target.closest("[role='menu']") as HTMLElement) : menuRef.current;
    if (menu) handleMenuArrowKeys(e, menu);
  };

  return createPortal(
    <>
      <div className="context-backdrop" onClick={close} onContextMenu={(e) => { e.preventDefault(); close(); }} />
      <div
        className="context-menu"
        role="menu"
        aria-label={label}
        style={{ left: x, top: y }}
        onKeyDown={onMenuKeyDown}
        ref={(el) => {
          menuRef.current = el;
          if (!el) return;
          const z = readZoomFactor();
          const cssVh = window.innerHeight / z;
          const cssVw = window.innerWidth / z;
          if (y + el.offsetHeight > cssVh) {
            el.style.top = `${Math.max(4, cssVh - el.offsetHeight)}px`;
          }
          if (x + el.offsetWidth > cssVw) {
            el.style.left = `${Math.max(4, cssVw - el.offsetWidth)}px`;
          }
        }}
      >
        {items.map((item, i) =>
          item.separator || item.label === "---" ? (
            <div key={i} className="context-menu-separator" role="separator" />
          ) : item.children ? (
            <div
              key={i}
              className="context-menu-submenu"
              role="none"
              onMouseLeave={() => setOpenSub((cur) => (cur === i ? null : cur))}
            >
              <button
                ref={(el) => {
                  triggerRefs.current[i] = el;
                }}
                className="context-menu-submenu-trigger"
                role="menuitem"
                aria-haspopup="menu"
                aria-expanded={openSub === i}
                tabIndex={-1}
                onClick={() => {
                  focusSubOnOpen.current = true;
                  setOpenSub(i);
                }}
                onKeyDown={(e) => {
                  if (e.key === "ArrowRight" || e.key === "Enter" || e.key === " ") {
                    e.preventDefault();
                    e.stopPropagation();
                    focusSubOnOpen.current = true;
                    setOpenSub(i);
                  }
                }}
              >
                {item.label}
                <ChevronRight size={12} aria-hidden="true" />
              </button>
              <div
                ref={(el) => {
                  subRefs.current[i] = el;
                }}
                className={`context-menu-submenu-content${openSub === i ? " open" : ""}`}
                role="menu"
                aria-label={item.label}
              >
                {item.children.map((child, j) =>
                  child.separator || child.label === "---" ? (
                    <div key={j} className="context-menu-separator" role="separator" />
                  ) : (
                    <button key={j} role="menuitem" tabIndex={-1} onClick={() => activate(child)}>
                      {child.label}
                    </button>
                  ),
                )}
              </div>
            </div>
          ) : (
            <button key={i} role="menuitem" tabIndex={-1} onClick={() => activate(item)}>
              {item.label}
            </button>
          ),
        )}
      </div>
    </>,
    document.body,
  );
}
