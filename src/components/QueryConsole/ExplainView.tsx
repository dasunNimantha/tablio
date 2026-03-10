import { useState } from "react";
import { ExplainResult, ExplainNode } from "../../lib/tauri";
import { Clock, ChevronRight, ChevronDown, Code, BarChart3 } from "lucide-react";
import "./ExplainView.css";

interface Props {
  result: ExplainResult;
}

export function ExplainView({ result }: Props) {
  const [showRaw, setShowRaw] = useState(false);

  const maxCost = getMaxCost(result.plan);

  return (
    <div className="explain-view">
      <div className="explain-header">
        <div className="explain-header-left">
          <Clock size={12} />
          <span>{result.execution_time_ms}ms</span>
        </div>
        <div className="explain-header-right">
          <button
            className={`btn-ghost ${!showRaw ? "active" : ""}`}
            onClick={() => setShowRaw(false)}
          >
            <BarChart3 size={12} /> Visual
          </button>
          <button
            className={`btn-ghost ${showRaw ? "active" : ""}`}
            onClick={() => setShowRaw(true)}
          >
            <Code size={12} /> Raw
          </button>
        </div>
      </div>
      {showRaw ? (
        <div className="explain-raw">
          <pre>{result.raw_text}</pre>
        </div>
      ) : (
        <div className="explain-tree">
          <ExplainNodeCard node={result.plan} maxCost={maxCost} depth={0} />
        </div>
      )}
    </div>
  );
}

function ExplainNodeCard({
  node,
  maxCost,
  depth,
}: {
  node: ExplainNode;
  maxCost: number;
  depth: number;
}) {
  const [expanded, setExpanded] = useState(true);
  const hasChildren = node.children.length > 0;
  const costRatio = maxCost > 0 ? node.total_cost / maxCost : 0;
  const costColor = getCostColor(costRatio);

  return (
    <div className="explain-node" style={{ marginLeft: depth > 0 ? 24 : 0 }}>
      <div
        className="explain-node-card"
        onClick={() => hasChildren && setExpanded(!expanded)}
      >
        <div className="explain-node-header">
          {hasChildren && (
            <span className="explain-chevron">
              {expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
            </span>
          )}
          <span className="explain-node-type">{node.node_type}</span>
          {node.relation && (
            <span className="explain-node-relation">on {node.relation}</span>
          )}
        </div>

        <div className="explain-node-stats">
          <div className="explain-stat">
            <span className="explain-stat-label">Cost</span>
            <span className="explain-stat-value" style={{ color: costColor }}>
              {node.startup_cost.toFixed(2)}..{node.total_cost.toFixed(2)}
            </span>
          </div>
          <div className="explain-stat">
            <span className="explain-stat-label">Est. Rows</span>
            <span className="explain-stat-value">
              {node.rows_estimated.toLocaleString()}
            </span>
          </div>
          {node.rows_actual !== null && (
            <div className="explain-stat">
              <span className="explain-stat-label">Actual Rows</span>
              <span className="explain-stat-value">
                {node.rows_actual.toLocaleString()}
              </span>
            </div>
          )}
          {node.actual_time_ms !== null && (
            <div className="explain-stat">
              <span className="explain-stat-label">Time</span>
              <span className="explain-stat-value">
                {node.actual_time_ms.toFixed(3)}ms
              </span>
            </div>
          )}
          {node.width > 0 && (
            <div className="explain-stat">
              <span className="explain-stat-label">Width</span>
              <span className="explain-stat-value">{node.width}</span>
            </div>
          )}
        </div>

        {node.filter && (
          <div className="explain-node-filter">
            <span className="explain-stat-label">Filter: </span>
            <code>{node.filter}</code>
          </div>
        )}

        <div
          className="explain-cost-bar"
          style={{ width: `${Math.max(costRatio * 100, 2)}%`, background: costColor }}
        />
      </div>

      {expanded &&
        hasChildren &&
        node.children.map((child, i) => (
          <ExplainNodeCard
            key={i}
            node={child}
            maxCost={maxCost}
            depth={depth + 1}
          />
        ))}
    </div>
  );
}

function getMaxCost(node: ExplainNode): number {
  let max = node.total_cost;
  for (const child of node.children) {
    max = Math.max(max, getMaxCost(child));
  }
  return max;
}

function getCostColor(ratio: number): string {
  if (ratio < 0.25) return "#15db95";
  if (ratio < 0.5) return "#fad83b";
  if (ratio < 0.75) return "#ff8d21";
  return "#ff5d59";
}
