import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api } from "../../lib/tauri";
import type { ColumnInfo, ForeignKeyInfo, TableInfo } from "../../lib/tauri";
import {
  Loader2,
  ZoomIn,
  ZoomOut,
  Maximize2,
  RefreshCw,
  Search,
  Link2,
  Link2Off,
} from "lucide-react";
import "./ERDView.css";

const TABLE_WIDTH = 240;
const ROW_HEIGHT = 20;
const HEADER_HEIGHT = 30;
const PAD = 24;
const GRID_GAP = 100;
const MIN_ZOOM = 0.3;
const MAX_ZOOM = 8;

interface TableNode {
  name: string;
  columns: ColumnInfo[];
  foreignKeys: ForeignKeyInfo[];
  x: number;
  y: number;
}

interface Props {
  connectionId: string;
  database: string;
  schema: string;
}

function layoutTables(
  details: Array<{
    table: TableInfo;
    columns: ColumnInfo[];
    foreignKeys: ForeignKeyInfo[];
  }>,
): TableNode[] {
  const n = details.length;
  if (n === 0) return [];
  const colCount = Math.ceil(Math.sqrt(n)) || 1;
  const rowCount = Math.ceil(n / colCount);
  const rowHeights: number[] = new Array(rowCount).fill(0);
  for (let i = 0; i < n; i++) {
    const r = Math.floor(i / colCount);
    const h = HEADER_HEIGHT + ROW_HEIGHT * details[i].columns.length;
    rowHeights[r] = Math.max(rowHeights[r], h);
  }
  const rowY: number[] = [];
  let yAcc = PAD;
  for (let r = 0; r < rowCount; r++) {
    rowY[r] = yAcc;
    yAcc += rowHeights[r] + GRID_GAP;
  }
  return details.map((d, i) => {
    const c = i % colCount;
    const r = Math.floor(i / colCount);
    return {
      name: d.table.name,
      columns: d.columns,
      foreignKeys: d.foreignKeys,
      x: PAD + c * (TABLE_WIDTH + GRID_GAP),
      y: rowY[r],
    };
  });
}

export function ERDView({ connectionId, database, schema }: Props) {
  const [nodes, setNodes] = useState<TableNode[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [zoom, setZoom] = useState(1);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const [tableDragging, setTableDragging] = useState<string | null>(null);
  const [panDragging, setPanDragging] = useState(false);
  const [showEdges, setShowEdges] = useState(true);
  const [filterQuery, setFilterQuery] = useState("");
  const loadGenRef = useRef(0);

  const loadSchema = useCallback(async () => {
    const gen = ++loadGenRef.current;
    setLoading(true);
    setError(null);
    try {
      const tables = await api.listTables(connectionId, database, schema);
      if (gen !== loadGenRef.current) return;
      const baseTables = tables.filter(
        (t) => t.table_type === "BASE TABLE" || t.table_type === "TABLE",
      );
      const details = await Promise.all(
        baseTables.map(async (t) => {
          const [columns, foreignKeys] = await Promise.all([
            api.listColumns(connectionId, database, schema, t.name),
            api.listForeignKeys(connectionId, database, schema, t.name),
          ]);
          return { table: t, columns, foreignKeys };
        }),
      );
      if (gen !== loadGenRef.current) return;
      setNodes(layoutTables(details));
    } catch (e) {
      if (gen !== loadGenRef.current) return;
      setError(String(e));
    } finally {
      if (gen === loadGenRef.current) setLoading(false);
    }
  }, [connectionId, database, schema]);

  useEffect(() => {
    loadSchema();
  }, [loadSchema]);

  const getTableHeight = (colCount: number) => HEADER_HEIGHT + colCount * ROW_HEIGHT;

  const getColumnY = (table: TableNode, columnIndex: number) =>
    table.y + HEADER_HEIGHT + columnIndex * ROW_HEIGHT + ROW_HEIGHT / 2;

  const findColumnIndex = (table: TableNode, columnName: string) =>
    table.columns.findIndex((c) => c.name === columnName);

  const visibleNodeSet = useMemo(() => {
    const q = filterQuery.trim().toLowerCase();
    if (!q) return null;
    return new Set(nodes.filter((n) => n.name.toLowerCase().includes(q)).map((n) => n.name));
  }, [nodes, filterQuery]);

  const handleWheel = (e: React.WheelEvent) => {
    e.preventDefault();
    const delta = e.deltaY > 0 ? -0.1 : 0.1;
    setZoom((z) => Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, z + delta)));
  };

  const handleResetView = () => {
    setZoom(1);
    setPan({ x: 0, y: 0 });
  };

  const handleFitView = useCallback(() => {
    if (nodes.length === 0) return;
    let minX = Infinity;
    let minY = Infinity;
    let maxX = -Infinity;
    let maxY = -Infinity;
    for (const n of nodes) {
      const nh = HEADER_HEIGHT + ROW_HEIGHT * n.columns.length;
      minX = Math.min(minX, n.x);
      minY = Math.min(minY, n.y);
      maxX = Math.max(maxX, n.x + TABLE_WIDTH);
      maxY = Math.max(maxY, n.y + nh);
    }
    const vbW = Math.max(800, ...nodes.map((n) => n.x + TABLE_WIDTH + PAD));
    const vbH = Math.max(600, ...nodes.map((n) => n.y + HEADER_HEIGHT + ROW_HEIGHT * n.columns.length + PAD));
    const bw = maxX - minX + PAD * 2;
    const bh = maxY - minY + PAD * 2;
    const inset = 0.1;
    const z = Math.min((vbW * (1 - 2 * inset)) / bw, (vbH * (1 - 2 * inset)) / bh, MAX_ZOOM);
    const clamped = Math.max(MIN_ZOOM, Math.min(MAX_ZOOM, z));
    const cx = (minX + maxX) / 2;
    const cy = (minY + maxY) / 2;
    setZoom(clamped);
    setPan({
      x: vbW / 2 - clamped * cx,
      y: vbH / 2 - clamped * cy,
    });
  }, [nodes]);

  const dragRef = useRef<{
    type: "table" | "pan";
    tableName?: string;
    mouseX: number;
    mouseY: number;
    nodeX: number;
    nodeY: number;
  } | null>(null);
  const zoomRef = useRef(zoom);
  zoomRef.current = zoom;

  const handleTableMouseDown = (e: React.MouseEvent, tableName: string) => {
    e.stopPropagation();
    if (e.button !== 0) return;
    const node = nodes.find((n) => n.name === tableName);
    if (!node) return;
    dragRef.current = {
      type: "table",
      tableName,
      mouseX: e.clientX,
      mouseY: e.clientY,
      nodeX: node.x,
      nodeY: node.y,
    };
    setTableDragging(tableName);
    window.addEventListener("mousemove", onDragMove);
    window.addEventListener("mouseup", onDragEnd);
  };

  const handleCanvasMouseDown = (e: React.MouseEvent) => {
    if (e.button !== 0 || (e.target as SVGElement).closest(".erd-table-rect")) return;
    dragRef.current = {
      type: "pan",
      mouseX: e.clientX,
      mouseY: e.clientY,
      nodeX: pan.x,
      nodeY: pan.y,
    };
    setPanDragging(true);
    window.addEventListener("mousemove", onDragMove);
    window.addEventListener("mouseup", onDragEnd);
  };

  const onDragMove = useCallback((e: MouseEvent) => {
    const d = dragRef.current;
    if (!d) return;
    if (d.type === "table" && d.tableName) {
      const dx = (e.clientX - d.mouseX) / zoomRef.current;
      const dy = (e.clientY - d.mouseY) / zoomRef.current;
      setNodes((prev) =>
        prev.map((n) =>
          n.name === d.tableName ? { ...n, x: d.nodeX + dx, y: d.nodeY + dy } : n,
        ),
      );
    } else if (d.type === "pan") {
      setPan({ x: d.nodeX + (e.clientX - d.mouseX), y: d.nodeY + (e.clientY - d.mouseY) });
    }
  }, []);

  const onDragEnd = useCallback(() => {
    dragRef.current = null;
    setTableDragging(null);
    setPanDragging(false);
    window.removeEventListener("mousemove", onDragMove);
    window.removeEventListener("mouseup", onDragEnd);
  }, [onDragMove]);

  useEffect(() => {
    return () => {
      window.removeEventListener("mousemove", onDragMove);
      window.removeEventListener("mouseup", onDragEnd);
    };
  }, [onDragMove, onDragEnd]);

  if (loading) {
    return (
      <div className="erd-view erd-loading">
        <Loader2 className="spin" size={32} />
        <span>Loading schema…</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="erd-view erd-error">
        <p>{error}</p>
        <button type="button" onClick={loadSchema}>
          Retry
        </button>
      </div>
    );
  }

  const svgWidth = Math.max(
    800,
    ...nodes.map((n) => n.x + TABLE_WIDTH + PAD),
  );
  const svgHeight = Math.max(
    600,
    ...nodes.map((n) => n.y + getTableHeight(n.columns.length) + PAD),
  );

  const edgeVisible = (from: string, to: string) => {
    if (!visibleNodeSet) return true;
    return visibleNodeSet.has(from) && visibleNodeSet.has(to);
  };

  return (
    <div className="erd-view">
      <div className="erd-toolbar">
        <div className="erd-toolbar-search">
          <Search size={14} className="erd-search-icon" />
          <input
            type="search"
            className="erd-filter-input"
            placeholder="Filter tables…"
            value={filterQuery}
            onChange={(e) => setFilterQuery(e.target.value)}
            aria-label="Filter tables"
          />
        </div>
        <div className="erd-toolbar-spacer" />
        <div className="erd-zoom">
          <button
            type="button"
            onClick={() => setZoom((z) => Math.max(MIN_ZOOM, z - 0.2))}
            title="Zoom out"
          >
            <ZoomOut size={16} />
          </button>
          <span>{Math.round(zoom * 100)}%</span>
          <button
            type="button"
            onClick={() => setZoom((z) => Math.min(MAX_ZOOM, z + 0.2))}
            title="Zoom in"
          >
            <ZoomIn size={16} />
          </button>
        </div>
        <button type="button" onClick={handleFitView} title="Fit diagram to view">
          <Maximize2 size={16} />
        </button>
        <button type="button" onClick={handleResetView} title="Reset zoom and pan">
          1:1
        </button>
        <button
          type="button"
          className={showEdges ? "erd-toggle-active" : ""}
          onClick={() => setShowEdges((v) => !v)}
          title={showEdges ? "Hide relationship lines" : "Show relationship lines"}
        >
          {showEdges ? <Link2 size={16} /> : <Link2Off size={16} />}
        </button>
        <button type="button" onClick={loadSchema} title="Reload schema">
          <RefreshCw size={16} />
        </button>
      </div>
      <div
        className="erd-canvas"
        onWheel={handleWheel}
        style={{
          cursor: tableDragging ? "grabbing" : panDragging ? "grabbing" : "default",
        }}
      >
        <svg
          className="erd-svg"
          width="100%"
          height="100%"
          viewBox={`0 0 ${svgWidth} ${svgHeight}`}
          preserveAspectRatio="xMidYMid meet"
        >
          <defs>
            <marker
              id="erd-crow-foot"
              markerWidth="8"
              markerHeight="8"
              refX="7"
              refY="4"
              orient="auto"
            >
              <path
                d="M0,0 L0,8 M4,0 L4,8"
                className="erd-edge-marker"
                strokeWidth="1.2"
                fill="none"
              />
            </marker>
            <marker
              id="erd-one-line"
              markerWidth="6"
              markerHeight="8"
              refX="0"
              refY="4"
              orient="auto"
            >
              <path d="M0,0 L0,8" className="erd-edge-marker" strokeWidth="1.2" fill="none" />
            </marker>
            <pattern
              id="erd-grid"
              width={24}
              height={24}
              patternUnits="userSpaceOnUse"
            >
              <path d="M 24 0 L 0 0 0 24" className="erd-grid-line" fill="none" />
            </pattern>
          </defs>
          <g transform={`translate(${pan.x},${pan.y}) scale(${zoom})`}>
            <rect
              width={svgWidth}
              height={svgHeight}
              fill="url(#erd-grid)"
              onMouseDown={handleCanvasMouseDown}
              className="erd-grid-bg"
              style={{ cursor: panDragging ? "grabbing" : "grab" }}
            />
            {showEdges &&
              nodes.map((fromNode) =>
                fromNode.foreignKeys.map((fk) => {
                  const toNode = nodes.find((n) => n.name === fk.referenced_table);
                  if (!toNode) return null;
                  if (!edgeVisible(fromNode.name, toNode.name)) return null;
                  const fromColIdx = findColumnIndex(fromNode, fk.column);
                  const toColIdx = findColumnIndex(toNode, fk.referenced_column);
                  if (fromColIdx < 0 || toColIdx < 0) return null;
                  const x1 = fromNode.x + TABLE_WIDTH;
                  const y1 = getColumnY(fromNode, fromColIdx);
                  const x2 = toNode.x;
                  const y2 = getColumnY(toNode, toColIdx);
                  const mid = (x1 + x2) / 2;
                  const path = `M ${x1} ${y1} C ${mid} ${y1}, ${mid} ${y2}, ${x2} ${y2}`;
                  return (
                    <path
                      key={`${fromNode.name}-${fk.column}-${fk.referenced_table}`}
                      d={path}
                      className="erd-edge"
                      fill="none"
                      markerStart="url(#erd-crow-foot)"
                      markerEnd="url(#erd-one-line)"
                    />
                  );
                }),
              )}
            {nodes.map((node) => {
              const dimmed =
                visibleNodeSet !== null && !visibleNodeSet.has(node.name);
              return (
                <g
                  key={node.name}
                  transform={`translate(${node.x}, ${node.y})`}
                  onMouseDown={(e) => handleTableMouseDown(e, node.name)}
                  style={{ cursor: "grab" }}
                  className={dimmed ? "erd-table-dimmed" : undefined}
                  opacity={dimmed ? 0.35 : 1}
                >
                  <rect
                    width={TABLE_WIDTH}
                    height={getTableHeight(node.columns.length)}
                    rx={8}
                    ry={8}
                    className="erd-table-rect"
                  />
                  <text x={12} y={20} className="erd-table-name">
                    {node.name}
                  </text>
                  {node.columns.map((col, i) => (
                    <g key={col.name} transform={`translate(0, ${HEADER_HEIGHT + i * ROW_HEIGHT})`}>
                      {col.is_primary_key && (
                        <rect x={6} y={3} width={12} height={12} rx={2} className="erd-key-pk" />
                      )}
                      {node.foreignKeys.some((fk) => fk.column === col.name) &&
                        !col.is_primary_key && (
                          <rect x={6} y={3} width={12} height={12} rx={2} className="erd-key-fk" />
                        )}
                      <text x={24} y={15} className="erd-col-name">
                        {col.name}
                      </text>
                      <text
                        x={TABLE_WIDTH - 12}
                        y={15}
                        className="erd-col-type"
                        textAnchor="end"
                      >
                        {col.data_type}
                      </text>
                    </g>
                  ))}
                </g>
              );
            })}
          </g>
        </svg>
      </div>
    </div>
  );
}
