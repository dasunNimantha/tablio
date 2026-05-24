import { useState, useRef, useEffect, useMemo, useLayoutEffect } from "react";
import { createPortal } from "react-dom";
import { readZoomFactor } from "../../lib/zoom";
import "./CustomSelect.css";

interface Option {
  value: string;
  label: string;
}

interface Props {
  value: string;
  options: Option[];
  onChange: (value: string) => void;
  className?: string;
  searchable?: boolean;
  placeholder?: string;
  /**
   * Render the dropdown via a portal anchored to the trigger's
   * viewport position instead of inlining it next to the trigger.
   *
   * Enable this whenever the select sits inside an `overflow: hidden`
   * or `overflow: auto` container (modal dialogs, sticky toolbars,
   * sidebar panels) so the dropdown can extend past the clipping
   * boundary. Off by default to preserve the simpler in-flow
   * positioning that every existing call site relies on.
   */
  portal?: boolean;
}

/** Computed { top, left, width } for the portalised dropdown,
 *  measured relative to the viewport. `null` while closed so we
 *  don't pay the layout read for an unmounted popover. */
interface PortalPosition {
  top: number;
  left: number;
  width: number;
  openUpward: boolean;
}

const DROPDOWN_MAX_HEIGHT = 240;
const TRIGGER_GAP = 3;

export function CustomSelect({
  value,
  options,
  onChange,
  className,
  searchable,
  placeholder,
  portal,
}: Props) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const [portalPos, setPortalPos] = useState<PortalPosition | null>(null);
  const ref = useRef<HTMLDivElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const searchRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (!open) return;
    const close = (e: MouseEvent) => {
      // Click anywhere outside the trigger OR the portalised
      // dropdown closes the popover. We can't rely on `ref.current`
      // alone in portal mode because the dropdown is no longer a
      // descendant of the wrapper.
      const insideTrigger = ref.current?.contains(e.target as Node);
      const insideDropdown = dropdownRef.current?.contains(
        e.target as Node,
      );
      if (!insideTrigger && !insideDropdown) setOpen(false);
    };
    document.addEventListener("mousedown", close);
    return () => document.removeEventListener("mousedown", close);
  }, [open]);

  useEffect(() => {
    if (open && searchable) {
      setSearch("");
      const timer = setTimeout(() => searchRef.current?.focus(), 0);
      return () => clearTimeout(timer);
    }
  }, [open, searchable]);

  // Compute portal coordinates anchored to the trigger. Flips
  // upward when there isn't enough space below; recomputes on
  // scroll / resize so the popover tracks the trigger.
  useLayoutEffect(() => {
    if (!open || !portal || !ref.current) {
      setPortalPos(null);
      return;
    }

    const measure = () => {
      const trigger = ref.current?.getBoundingClientRect();
      if (!trigger) return;
      // The dropdown is portalised into <body>, which on Chromium
      // carries the app's CSS `zoom` (see src/lib/zoom.ts). A
      // `position: fixed` element inside a zoom-scaled parent is
      // ALSO scaled, while `getBoundingClientRect` already returns
      // visually-zoomed coordinates → without compensation we'd
      // end up double-zoomed and the popover would drift from the
      // trigger at any zoom != 100%. Scaling the rect down by the
      // current zoom factor cancels the second multiplication that
      // the zoomed parent will apply on render.
      const z = readZoomFactor();
      const top = trigger.top / z;
      const bottom = trigger.bottom / z;
      const left = trigger.left / z;
      const width = trigger.width / z;
      const viewportHeight = window.innerHeight / z;
      const spaceBelow = viewportHeight - bottom - TRIGGER_GAP;
      const spaceAbove = top - TRIGGER_GAP;
      const openUpward =
        spaceBelow < DROPDOWN_MAX_HEIGHT && spaceAbove > spaceBelow;
      setPortalPos({
        top: openUpward ? top - TRIGGER_GAP : bottom + TRIGGER_GAP,
        left,
        width,
        openUpward,
      });
    };

    measure();
    // Track scroll on any ancestor + viewport resize so the
    // dropdown stays glued to the trigger.
    const update = () => measure();
    window.addEventListener("scroll", update, true);
    window.addEventListener("resize", update);
    return () => {
      window.removeEventListener("scroll", update, true);
      window.removeEventListener("resize", update);
    };
  }, [open, portal]);

  const selected = options.find((o) => o.value === value);

  const filtered = useMemo(() => {
    if (!searchable || !search) return options;
    const q = search.toLowerCase();
    return options.filter((o) => o.label.toLowerCase().includes(q));
  }, [options, search, searchable]);

  const dropdownBody = (
    <div
      ref={dropdownRef}
      className={`cs-dropdown${portal ? " cs-dropdown--portal" : ""}${
        portalPos?.openUpward ? " cs-dropdown--upward" : ""
      }`}
      style={
        portal && portalPos
          ? {
              position: "fixed",
              top: portalPos.openUpward ? undefined : portalPos.top,
              // `portalPos.top` is already in zoom-compensated px
              // (see measure() in the layout effect), so we
              // compare against the zoom-compensated viewport
              // height to keep the upward-flip anchor consistent.
              bottom: portalPos.openUpward
                ? window.innerHeight / readZoomFactor() - portalPos.top
                : undefined,
              left: portalPos.left,
              width: portalPos.width,
            }
          : undefined
      }
    >
      {searchable && (
        <div className="cs-search-wrapper">
          <input
            ref={searchRef}
            className="cs-search"
            placeholder="Search..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Escape") {
                setOpen(false);
              }
              if (e.key === "Enter" && filtered.length === 1) {
                onChange(filtered[0].value);
                setOpen(false);
              }
            }}
          />
        </div>
      )}
      <div className="cs-options">
        {filtered.map((opt) => (
          <div
            key={opt.value}
            className={`cs-option ${opt.value === value ? "cs-option-selected" : ""}`}
            onClick={() => {
              onChange(opt.value);
              setOpen(false);
            }}
          >
            {opt.label}
          </div>
        ))}
        {searchable && filtered.length === 0 && (
          <div className="cs-no-results">No matches</div>
        )}
      </div>
    </div>
  );

  return (
    <div className={`cs-wrapper ${className || ""}`} ref={ref}>
      <button
        type="button"
        className="cs-trigger"
        onClick={() => setOpen((v) => !v)}
      >
        <span
          className={`cs-value ${!selected && placeholder ? "cs-placeholder" : ""}`}
        >
          {selected?.label ?? placeholder ?? value}
        </span>
        <svg
          className="cs-chevron"
          width="10"
          height="10"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2.5"
        >
          <path d="m6 9 6 6 6-6" />
        </svg>
      </button>
      {open &&
        (portal
          ? createPortal(dropdownBody, document.body)
          : dropdownBody)}
    </div>
  );
}
