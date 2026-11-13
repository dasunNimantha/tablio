import { useEffect, useRef, type ReactNode, type RefObject } from "react";
import { handleMenuArrowKeys, menuItems } from "./menuKeys";

interface Props {
  label: string;
  className?: string;
  /** Button that opened the menu; focus returns there on Escape / Tab. */
  triggerRef: RefObject<HTMLElement | null>;
  onClose: () => void;
  children: ReactNode;
}

/**
 * Anchored popover menu (status-bar theme / settings menus): `role="menu"`,
 * focus starts on the checked or first item, arrows / Home / End move,
 * Escape and Tab close it and return focus to the trigger.
 */
export function PopoverMenu({ label, className = "theme-picker-popover", triggerRef, onClose, children }: Props) {
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const menu = ref.current;
    if (!menu) return;
    const items = menuItems(menu);
    (items.find((el) => el.getAttribute("aria-checked") === "true") ?? items[0])?.focus();
  }, []);

  return (
    <div
      ref={ref}
      className={className}
      role="menu"
      aria-label={label}
      onKeyDown={(e) => {
        if (e.key === "Escape") {
          e.preventDefault();
          e.stopPropagation();
          onClose();
          triggerRef.current?.focus();
          return;
        }
        if (e.key === "Tab") {
          onClose();
          return;
        }
        if (ref.current) handleMenuArrowKeys(e, ref.current);
      }}
    >
      {children}
    </div>
  );
}
