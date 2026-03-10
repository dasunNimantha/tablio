import { useState, useRef, useEffect } from "react";
import { Download } from "lucide-react";
import "./ExportMenu.css";

interface Props {
  onExport: (format: "csv" | "json" | "sql") => void;
}

export function ExportMenu({ onExport }: Props) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    if (open) document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, [open]);

  return (
    <div className="export-menu-wrapper" ref={ref}>
      <button
        className="btn-ghost"
        onClick={() => setOpen(!open)}
        title="Export Data"
      >
        <Download size={14} /> Export
      </button>
      {open && (
        <div className="export-menu-dropdown">
          <button
            onClick={() => {
              onExport("csv");
              setOpen(false);
            }}
          >
            Export as CSV
          </button>
          <button
            onClick={() => {
              onExport("json");
              setOpen(false);
            }}
          >
            Export as JSON
          </button>
          <button
            onClick={() => {
              onExport("sql");
              setOpen(false);
            }}
          >
            Export as SQL
          </button>
        </div>
      )}
    </div>
  );
}
