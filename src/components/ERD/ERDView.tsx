import { useCallback, useEffect, useRef, useState } from "react";
import { api } from "../../lib/tauri";
import type { TableInfo, ColumnInfo, ForeignKeyInfo } from "../../lib/tauri";
import { Loader2, ZoomIn, ZoomOut, Maximize2 } from "lucide-react";
import "./ERDView.css";

const TABLE_WIDTH = 220;
const ROW_HEIGHT = 22;
const HEADER_HEIGHT = 32;
const PAD = 12;
const GRID_GAP = 80;

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

export function ERDView({ connectionId, database, schema }: Props) {
  const [nodes, setNodes] = useState<TableNode[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [zoom, setZoom] = useState(1);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const [tableDragging, setTableDragging] = useState<string | null>(null);
  const [tableDragStart, setTableDragStart] = useState({ mouseX: 0, mouseY: 0, nodeX: 0, nodeY: 0 });
  const [panDragging, setPanDragging] = useState(false);
  const [panStart, setPanStart] = useState({ x: 0, y: 0 });
  const containerRef = useRef<HTMLDivElement>(null);

  const loadSchema = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const tables = await api.listTables(connectionId, database, schema);
      const baseTables = tables.filter(
        (t) => t.table_type === "BASE TABLE" || t.table_type === "TABLE"
      );
      const details = await Promise.all(
        baseTables.map(async (t) => {
          const [columns, foreignKeys] = await Promise.all([
            api.listColumns(connectionId, database, schema, t.name),
            api.listForeignKeys(connectionId, database, schema, t.name),
          ]);
          return { table: t, columns, foreignKeys };
        })
      );
      const cols = Math.ceil(Math.sqrt(baseTables.length)) || 1;
      const tableNodes: TableNode[] = details.map((d, i) => ({
        name: d.table.name,
        columns: d.columns,
        foreignKeys: d.foreignKeys,
        x: PAD + (i % cols) * (TABLE_WIDTH + GRID_GAP),
        y: PAD + Math.floor(i / cols) * (HEADER_HEIGHT + ROW_HEIGHT * d.columns.length + GRID_GAP),
      }));
      setNodes(tableNodes);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
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

  const handleWheel = (e: React.WheelEvent) => {
    e.preventDefault();
    const delta = e.deltaY > 0 ? -0.1 : 0.1;
    setZoom((z) => Math.min(3, Math.max(0.3, z + delta)));
  };

  const handleResetView = () => {
    setZoom(1);
    setPan({ x: 0, y: 0 });
  };

  const handleTableMouseDown = (e: React.MouseEvent, tableName: string) => {
    e.stopPropagation();
    if (e.button !== 0) return;
    const node = nodes.find((n) => n.name === tableName);
    if (!node) return;
    setTableDragging(tableName);
    setTableDragStart({ mouseX: e.clientX, mouseY: e.clientY, nodeX: node.x, nodeY: node.y });
  };

  const handleCanvasMouseDown = (e: React.MouseEvent) => {
    if (e.button !== 0 || (e.target as SVGElement).closest(".erd-table-rect")) return;
    setPanDragging(true);
    setPanStart({ x: e.clientX - pan.x, y: e.clientY - pan.y });
  };

  const handleMouseMove = useCallback(
    (e: MouseEvent) => {
      if (tableDragging) {
        const node = nodes.find((n) => n.name === tableDragging);
        if (!node) return;
        const dx = (e.clientX - tableDragStart.mouseX) / zoom;
        const dy = (e.clientY - tableDragStart.mouseY) / zoom;
        setNodes((prev) =>
          prev.map((n) =>
            n.name === tableDragging
              ? { ...n, x: tableDragStart.nodeX + dx, y: tableDragStart.nodeY + dy }
              : n
          )
        );
      } else if (panDragging) {
        setPan({ x: e.clientX - panStart.x, y: e.clientY - panStart.y });
      }
    },
    [tableDragging, tableDragStart, panDragging, panStart, zoom, nodes]
  );

  const handleMouseUp = useCallback(() => {
    setTableDragging(null);
    setPanDragging(false);
  }, []);

  useEffect(() => {
    if (!tableDragging && !panDragging) return;
    window.addEventListener("mousemove", handleMouseMove);
    window.addEventListener("mouseup", handleMouseUp);
    return () => {
      window.removeEventListener("mousemove", handleMouseMove);
      window.removeEventListener("mouseup", handleMouseUp);
    };
  }, [tableDragging, panDragging, handleMouseMove, handleMouseUp]);

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
    ...nodes.map((n) => n.x + TABLE_WIDTH + PAD)
  );
  const svgHeight = Math.max(
    600,
    ...nodes.map((n) => n.y + getTableHeight(n.columns.length) + PAD)
  );

  return (
    <div className="erd-view" ref={containerRef}>
      <div className="erd-toolbar">
        <div className="erd-zoom">
          <button
            type="button"
            onClick={() => setZoom((z) => Math.max(0.3, z - 0.2))}
            title="Zoom out"
          >
            <ZoomOut size={16} />
          </button>
          <span>{Math.round(zoom * 100)}%</span>
          <button
            type="button"
            onClick={() => setZoom((z) => Math.min(3, z + 0.2))}
            title="Zoom in"
          >
            <ZoomIn size={16} />
          </button>
        </div>
        <button type="button" onClick={handleResetView} title="Reset view">
          <Maximize2 size={16} />
        </button>
        <button type="button" onClick={loadSchema}>
          Refresh
        </button>
      </div>
      <div
        className="erd-canvas"
        onWheel={handleWheel}
        style={{ cursor: tableDragging ? "grabbing" : panDragging ? "grabbing" : "default" }}
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
              id="crow-foot"
              markerWidth="10"
              markerHeight="10"
              refX="9"
              refY="5"
              orient="auto"
            >
              <path d="M0,0 L0,10 M5,0 L5,10" stroke="#64748b" strokeWidth="1.5" fill="none" />
            </marker>
            <marker
              id="one-line"
              markerWidth="10"
              markerHeight="10"
              refX="0"
              refY="5"
              orient="auto"
            >
              <path d="M0,0 L0,10" stroke="#64748b" strokeWidth="1.5" fill="none" />
            </marker>
          </defs>
          <g transform={`translate(${pan.x},${pan.y}) scale(${zoom})`}>
            {/* Grid */}
            <pattern
              id="grid"
              width={20}
              height={20}
              patternUnits="userSpaceOnUse"
            >
              <path d="M 20 0 L 0 0 0 20" fill="none" stroke="#e2e8f0" strokeWidth="0.5" />
            </pattern>
            <rect
              width={svgWidth}
              height={svgHeight}
              fill="url(#grid)"
              onMouseDown={handleCanvasMouseDown}
              style={{ cursor: panDragging ? "grabbing" : "grab" }}
            />
            {/* Relationship lines */}
            {nodes.map((fromNode) =>
              fromNode.foreignKeys.map((fk) => {
                const toNode = nodes.find((n) => n.name === fk.referenced_table);
                if (!toNode) return null;
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
                    fill="none"
                    stroke="#64748b"
                    strokeWidth="1.5"
                    markerStart="url(#crow-foot)"
                    markerEnd="url(#one-line)"
                  />
                );
              })
            )}
            {/* Table boxes */}
            {nodes.map((node) => (
              <g
                key={node.name}
                transform={`translate(${node.x}, ${node.y})`}
                onMouseDown={(e) => handleTableMouseDown(e, node.name)}
                style={{ cursor: "grab" }}
              >
                <rect
                  width={TABLE_WIDTH}
                  height={getTableHeight(node.columns.length)}
                  rx={6}
                  ry={6}
                  className="erd-table-rect"
                />
                <text x={PAD} y={20} className="erd-table-name">
                  {node.name}
                </text>
                {node.columns.map((col, i) => (
                  <g key={col.name} transform={`translate(0, ${HEADER_HEIGHT + i * ROW_HEIGHT})`}>
                    {col.is_primary_key && (
                      <rect x={6} y={4} width={14} height={14} rx={2} className="erd-key-pk" />
                    )}
                    {node.foreignKeys.some((fk) => fk.column === col.name) && !col.is_primary_key && (
                      <rect x={6} y={4} width={14} height={14} rx={2} className="erd-key-fk" />
                    )}
                    <text x={28} y={16} className="erd-col-name">
                      {col.name}
                    </text>
                    <text x={TABLE_WIDTH - PAD} y={16} className="erd-col-type" textAnchor="end">
                      {col.data_type}
                    </text>
                  </g>
                ))}
              </g>
            ))}
          </g>
        </svg>
      </div>
    </div>
  );
}
