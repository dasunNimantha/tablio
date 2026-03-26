import { useState, useRef, useEffect, useCallback, useMemo } from "react";
import { Columns3, Eye, EyeOff, GripVertical, RotateCcw } from "lucide-react";
import type { ColumnInfo } from "../../lib/tauri";
import "./ColumnOrganizer.css";

export interface ColumnSettings {
  order: string[];
  hidden: Set<string>;
}

interface Props {
  columns: ColumnInfo[];
  settings: ColumnSettings;
  onChange: (settings: ColumnSettings) => void;
}

function getStorageKey(connectionId: string, database: string, schema: string, table: string) {
  return `tablio-cols:${connectionId}:${database}:${schema}:${table}`;
}

export function loadColumnSettings(
  connectionId: string,
  database: string,
  schema: string,
  table: string
): ColumnSettings | null {
  try {
    const raw = localStorage.getItem(getStorageKey(connectionId, database, schema, table));
    if (!raw) return null;
    const data = JSON.parse(raw);
    return {
      order: data.order || [],
      hidden: new Set(data.hidden || []),
    };
  } catch {
    return null;
  }
}

export function saveColumnSettings(
  connectionId: string,
  database: string,
  schema: string,
  table: string,
  settings: ColumnSettings
) {
  try {
    localStorage.setItem(
      getStorageKey(connectionId, database, schema, table),
      JSON.stringify({ order: settings.order, hidden: Array.from(settings.hidden) })
    );
  } catch {}
}

export function applyColumnSettings(
  columns: ColumnInfo[],
  settings: ColumnSettings | null
): { visibleIndices: number[] } {
  if (!settings || (settings.order.length === 0 && settings.hidden.size === 0)) {
    return { visibleIndices: columns.map((_, i) => i) };
  }

  const pkIndices: number[] = [];
  const nameToIdx = new Map<string, number>();
  columns.forEach((c, i) => {
    nameToIdx.set(c.name, i);
    if (c.is_primary_key) pkIndices.push(i);
  });

  const ordered: number[] = [...pkIndices];
  const pkNames = new Set(columns.filter((c) => c.is_primary_key).map((c) => c.name));

  if (settings.order.length > 0) {
    for (const name of settings.order) {
      if (pkNames.has(name)) continue;
      const idx = nameToIdx.get(name);
      if (idx !== undefined && !settings.hidden.has(name)) {
        ordered.push(idx);
      }
    }
  }

  for (let i = 0; i < columns.length; i++) {
    if (!ordered.includes(i) && !settings.hidden.has(columns[i].name) && !pkNames.has(columns[i].name)) {
      ordered.push(i);
    }
  }

  return { visibleIndices: ordered };
}

export function ColumnOrganizer({ columns, settings, onChange }: Props) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  const listRef = useRef<HTMLDivElement>(null);
  const draggingRef = useRef(false);
  const scrollTimerRef = useRef<number | null>(null);
  const [dragIdx, setDragIdx] = useState<number | null>(null);
  const [dragOverIdx, setDragOverIdx] = useState<number | null>(null);

  useEffect(() => {
    if (!open) return;
    const handleClick = (e: MouseEvent) => {
      if (draggingRef.current) return;
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", handleClick);
    return () => {
      document.removeEventListener("mousedown", handleClick);
      if (scrollTimerRef.current) {
        cancelAnimationFrame(scrollTimerRef.current);
        scrollTimerRef.current = null;
      }
    };
  }, [open]);

  const pkColumns = columns.filter((c) => c.is_primary_key);
  const pkNames = new Set(pkColumns.map((c) => c.name));

  const nonPkOrder = useMemo(() => {
    const nonPk = columns.filter((c) => !c.is_primary_key);
    if (settings.order.length === 0) return nonPk.map((c) => c.name);

    const ordered: string[] = [];
    const nonPkNames = new Set(nonPk.map((c) => c.name));

    for (const name of settings.order) {
      if (nonPkNames.has(name)) ordered.push(name);
    }
    for (const c of nonPk) {
      if (!ordered.includes(c.name)) ordered.push(c.name);
    }
    return ordered;
  }, [columns, settings.order]);

  const toggleVisibility = (name: string) => {
    const next = new Set(settings.hidden);
    if (next.has(name)) {
      next.delete(name);
    } else {
      next.add(name);
    }
    onChange({ ...settings, hidden: next });
  };

  const handleDragStart = useCallback((e: React.DragEvent, idx: number) => {
    draggingRef.current = true;
    setDragIdx(idx);
    e.dataTransfer.effectAllowed = "move";
    e.dataTransfer.setData("text/plain", String(idx));

    const ghost = document.createElement("div");
    ghost.textContent = nonPkOrder[idx];
    ghost.style.cssText = "position:fixed;top:-1000px;padding:6px 12px;background:var(--bg-surface,#fff);border:1px solid rgba(0,0,0,0.15);border-radius:6px;font-size:13px;font-family:inherit;color:var(--text-primary,#111);box-shadow:0 2px 8px rgba(0,0,0,0.12);white-space:nowrap;";
    document.body.appendChild(ghost);
    e.dataTransfer.setDragImage(ghost, 0, 0);
    requestAnimationFrame(() => document.body.removeChild(ghost));
  }, [nonPkOrder]);

  const handleDragOver = useCallback((e: React.DragEvent, idx: number) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = "move";
    setDragOverIdx(idx);
  }, []);

  const handleDrop = useCallback((dropIdx: number) => {
    if (dragIdx === null || dragIdx === dropIdx) {
      setDragIdx(null);
      setDragOverIdx(null);
      draggingRef.current = false;
      return;
    }

    const newOrder = [...nonPkOrder];
    const [moved] = newOrder.splice(dragIdx, 1);
    newOrder.splice(dropIdx, 0, moved);

    onChange({ ...settings, order: [...pkColumns.map((c) => c.name), ...newOrder] });
    setDragIdx(null);
    setDragOverIdx(null);
    draggingRef.current = false;
  }, [dragIdx, nonPkOrder, pkColumns, settings, onChange]);

  const handleDragEnd = useCallback(() => {
    setDragIdx(null);
    setDragOverIdx(null);
    draggingRef.current = false;
    if (scrollTimerRef.current) {
      cancelAnimationFrame(scrollTimerRef.current);
      scrollTimerRef.current = null;
    }
  }, []);

  const handleListDragOver = useCallback((e: React.DragEvent) => {
    const list = listRef.current;
    if (!list) return;
    const rect = list.getBoundingClientRect();
    const edgeSize = 40;
    const y = e.clientY;

    if (scrollTimerRef.current) {
      cancelAnimationFrame(scrollTimerRef.current);
      scrollTimerRef.current = null;
    }

    if (y < rect.top + edgeSize) {
      const intensity = Math.min(1, Math.max(0, 1 - (y - rect.top) / edgeSize));
      const speed = Math.max(2, intensity * 12);
      const tick = () => {
        if (list.scrollTop <= 0) { scrollTimerRef.current = null; return; }
        list.scrollTop -= speed;
        scrollTimerRef.current = requestAnimationFrame(tick);
      };
      scrollTimerRef.current = requestAnimationFrame(tick);
    } else if (y > rect.bottom - edgeSize) {
      const intensity = Math.min(1, Math.max(0, 1 - (rect.bottom - y) / edgeSize));
      const speed = Math.max(2, intensity * 12);
      const maxScroll = list.scrollHeight - list.clientHeight;
      const tick = () => {
        if (list.scrollTop >= maxScroll) { scrollTimerRef.current = null; return; }
        list.scrollTop += speed;
        scrollTimerRef.current = requestAnimationFrame(tick);
      };
      scrollTimerRef.current = requestAnimationFrame(tick);
    }
  }, []);

  const handleReset = () => {
    onChange({ order: [], hidden: new Set() });
  };

  const handleHideAll = () => {
    const allNonPk = columns.filter((c) => !c.is_primary_key).map((c) => c.name);
    onChange({ ...settings, hidden: new Set(allNonPk) });
  };

  const allNonPkHidden = columns
    .filter((c) => !c.is_primary_key)
    .every((c) => settings.hidden.has(c.name));

  const nonPkNames = new Set(columns.filter((c) => !c.is_primary_key).map((c) => c.name));
  const hiddenCount = Array.from(settings.hidden).filter((n) => nonPkNames.has(n)).length;

  return (
    <div className="col-organizer-wrapper" ref={ref}>
      <button
        className={`btn-ghost ${hiddenCount > 0 ? "active-filter" : ""}`}
        onClick={() => setOpen((v) => !v)}
        title="Organize Columns"
      >
        <Columns3 size={14} /> Columns{hiddenCount > 0 ? ` (${columns.length - pkNames.size - hiddenCount}/${columns.length - pkNames.size})` : ""}
      </button>
      {open && (
        <div className="col-organizer-dropdown">
          <div className="col-organizer-header">
            <span>Columns</span>
            <div className="col-organizer-header-actions">
              <button
                className="col-organizer-hide-all"
                onClick={allNonPkHidden ? handleReset : handleHideAll}
                title={allNonPkHidden ? "Show all columns" : "Hide all except primary key"}
              >
                {allNonPkHidden ? "Show All" : "Hide All"}
              </button>
              <button
                className="btn-icon col-organizer-reset"
                onClick={handleReset}
                title="Reset to default"
              >
                <RotateCcw size={12} />
              </button>
            </div>
          </div>
          <div className="col-organizer-list" ref={listRef} onDragOver={handleListDragOver}>
            {pkColumns.map((col) => (
              <div key={col.name} className="col-organizer-item col-organizer-pk">
                <span className="col-organizer-grip-spacer" />
                <Eye size={13} className="col-organizer-eye-locked" />
                <span className="col-organizer-name">{col.name}</span>
                <span className="col-organizer-badge">PK</span>
              </div>
            ))}
            {nonPkOrder.map((name, idx) => {
              const isHidden = settings.hidden.has(name);
              const isDragging = dragIdx === idx;
              const isDragOver = dragOverIdx === idx && dragIdx !== idx;

              return (
                <div
                  key={name}
                  className={[
                    "col-organizer-item",
                    isDragging && "col-organizer-dragging",
                    isDragOver && "col-organizer-dragover",
                    isHidden && "col-organizer-hidden",
                  ].filter(Boolean).join(" ")}
                  draggable
                  onDragStart={(e) => handleDragStart(e, idx)}
                  onDragOver={(e) => handleDragOver(e, idx)}
                  onDrop={() => handleDrop(idx)}
                  onDragEnd={handleDragEnd}
                >
                  <span className="col-organizer-grip">
                    <GripVertical size={12} />
                  </span>
                  <button
                    className="btn-icon col-organizer-eye"
                    onClick={() => toggleVisibility(name)}
                    title={isHidden ? "Show column" : "Hide column"}
                  >
                    {isHidden ? <EyeOff size={13} /> : <Eye size={13} />}
                  </button>
                  <span className="col-organizer-name">{name}</span>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
