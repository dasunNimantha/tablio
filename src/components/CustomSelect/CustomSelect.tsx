import { useState, useRef, useEffect, useMemo } from "react";
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
}

export function CustomSelect({ value, options, onChange, className, searchable, placeholder }: Props) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");
  const ref = useRef<HTMLDivElement>(null);
  const searchRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (!open) return;
    const close = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
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

  const selected = options.find((o) => o.value === value);

  const filtered = useMemo(() => {
    if (!searchable || !search) return options;
    const q = search.toLowerCase();
    return options.filter((o) => o.label.toLowerCase().includes(q));
  }, [options, search, searchable]);

  return (
    <div className={`cs-wrapper ${className || ""}`} ref={ref}>
      <button
        type="button"
        className="cs-trigger"
        onClick={() => setOpen((v) => !v)}
      >
        <span className={`cs-value ${!selected && placeholder ? "cs-placeholder" : ""}`}>
          {selected?.label ?? placeholder ?? value}
        </span>
        <svg className="cs-chevron" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
          <path d="m6 9 6 6 6-6" />
        </svg>
      </button>
      {open && (
        <div className="cs-dropdown">
          {searchable && (
            <div className="cs-search-wrapper">
              <input
                ref={searchRef}
                className="cs-search"
                placeholder="Search..."
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Escape") { setOpen(false); }
                  if (e.key === "Enter" && filtered.length === 1) {
                    onChange(filtered[0].value);
                    setOpen(false);
                  }
                }}
              />
            </div>
          )}
          {filtered.map((opt) => (
            <div
              key={opt.value}
              className={`cs-option ${opt.value === value ? "cs-option-selected" : ""}`}
              onClick={() => { onChange(opt.value); setOpen(false); }}
            >
              {opt.label}
            </div>
          ))}
          {searchable && filtered.length === 0 && (
            <div className="cs-no-results">No matches</div>
          )}
        </div>
      )}
    </div>
  );
}
