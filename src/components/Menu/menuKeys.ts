import type { KeyboardEvent as ReactKeyboardEvent } from "react";

/** Enabled menu items that belong directly to `menu` (not to a nested submenu). */
export function menuItems(menu: HTMLElement): HTMLElement[] {
  return Array.from(menu.querySelectorAll<HTMLElement>("[role='menuitem'], [role='menuitemradio']")).filter(
    (el) => el.closest("[role='menu']") === menu && !el.hasAttribute("disabled") && el.getAttribute("aria-disabled") !== "true",
  );
}

/**
 * Arrow-key navigation for a `role="menu"` element: Up/Down wrap, Home/End
 * jump. Returns true when the key was handled. Enter/Space are left to the
 * native button activation.
 */
export function handleMenuArrowKeys(e: ReactKeyboardEvent<HTMLElement>, menu: HTMLElement): boolean {
  const items = menuItems(menu);
  if (items.length === 0) return false;
  const current = items.indexOf(document.activeElement as HTMLElement);
  let next: number | null = null;
  switch (e.key) {
    case "ArrowDown":
      next = current < 0 ? 0 : (current + 1) % items.length;
      break;
    case "ArrowUp":
      next = current < 0 ? items.length - 1 : (current - 1 + items.length) % items.length;
      break;
    case "Home":
      next = 0;
      break;
    case "End":
      next = items.length - 1;
      break;
    default:
      return false;
  }
  e.preventDefault();
  e.stopPropagation();
  items[next].focus();
  return true;
}
