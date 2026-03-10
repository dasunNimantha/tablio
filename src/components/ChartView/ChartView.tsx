import { useMemo, useState } from "react";
import {
  Bar,
  Line,
  Pie,
  Scatter,
} from "react-chartjs-2";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  ArcElement,
  Tooltip,
  Legend,
  Title,
} from "chart.js";
import "./ChartView.css";

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  ArcElement,
  Tooltip,
  Legend,
  Title
);

const CHART_COLORS = [
  "#6d9eff",
  "#34d399",
  "#fbbf24",
  "#f87171",
  "#a78bfa",
  "#4ad0ff",
  "#fab387",
  "#94e2d5",
];

type ChartType = "bar" | "line" | "pie" | "scatter";

interface Props {
  columns: string[];
  rows: unknown[][];
}

function isNumeric(value: unknown): boolean {
  if (value === null || value === undefined) return false;
  const num = Number(value);
  return !Number.isNaN(num) && value !== "";
}

function getNumericColumnIndices(columns: string[], rows: unknown[][]): number[] {
  return columns
    .map((_, colIndex) => {
      const values = rows.map((row) => row[colIndex]);
      const numericCount = values.filter(isNumeric).length;
      return { colIndex, numericCount };
    })
    .filter(({ numericCount }) => numericCount > 0)
    .sort((a, b) => b.numericCount - a.numericCount)
    .map(({ colIndex }) => colIndex);
}

export function ChartView({ columns, rows }: Props) {
  const [chartType, setChartType] = useState<ChartType>("bar");
  const [xColumn, setXColumn] = useState<string>("");
  const [yColumns, setYColumns] = useState<string[]>([]);

  const numericColumnIndices = useMemo(
    () => getNumericColumnIndices(columns, rows),
    [columns, rows]
  );

  const numericColumnNames = useMemo(
    () => numericColumnIndices.map((i) => columns[i]),
    [columns, numericColumnIndices]
  );

  const xColumnIndex = useMemo(
    () => (xColumn ? columns.indexOf(xColumn) : -1),
    [columns, xColumn]
  );

  const yColumnIndices = useMemo(
    () =>
      yColumns
        .map((c) => columns.indexOf(c))
        .filter((i) => i >= 0),
    [columns, yColumns]
  );

  const chartData = useMemo(() => {
    if (rows.length === 0 || columns.length === 0) return null;

    if (chartType === "pie") {
      const labelCol = xColumnIndex >= 0 ? xColumnIndex : 0;
      const valueCol = yColumnIndices[0] ?? numericColumnIndices[0] ?? (columns.length > 1 ? 1 : 0);
      const pairs = rows
        .map((r) => ({
          label: String(r[labelCol] ?? "(empty)"),
          value: isNumeric(r[valueCol]) ? Number(r[valueCol]) : 0,
        }))
        .filter((p) => p.value !== 0);
      return {
        labels: pairs.map((p) => p.label),
        datasets: [
          {
            data: pairs.map((p) => p.value),
            backgroundColor: CHART_COLORS.concat(CHART_COLORS).slice(0, pairs.length),
            borderColor: "transparent",
            borderWidth: 1,
          },
        ],
      };
    }

    if (chartType === "scatter") {
      const xCol = xColumnIndex >= 0 ? xColumnIndex : 0;
      const yCol = yColumnIndices[0] ?? numericColumnIndices[0] ?? 0;
      const points = rows
        .filter((r) => isNumeric(r[xCol]) && isNumeric(r[yCol]))
        .map((r) => ({
          x: Number(r[xCol]),
          y: Number(r[yCol]),
        }));
      return {
        datasets: [
          {
            label: columns[yCol],
            data: points,
            backgroundColor: CHART_COLORS[0],
            borderColor: CHART_COLORS[0],
            borderWidth: 1,
          },
        ],
      };
    }

    const labelCol = xColumnIndex >= 0 ? xColumnIndex : 0;
    const labels = rows.map((r) => String(r[labelCol] ?? ""));

    const yCols = yColumnIndices.length
      ? yColumnIndices
      : numericColumnIndices.length
        ? numericColumnIndices.slice(0, 1)
        : columns.length > 1 ? [1] : [0];

    const datasets = yCols.map((colIdx, i) => {
      const values = rows.map((r) => {
        const v = r[colIdx];
        return isNumeric(v) ? Number(v) : 0;
      });
      return {
        label: columns[colIdx],
        data: values,
        backgroundColor: CHART_COLORS[i % CHART_COLORS.length] + "80",
        borderColor: CHART_COLORS[i % CHART_COLORS.length],
        borderWidth: 1,
      };
    });

    return {
      labels,
      datasets,
    };
  }, [chartType, rows, columns, xColumnIndex, yColumnIndices, numericColumnIndices]);

  const chartOptions = useMemo(() => {
    const style = getComputedStyle(document.documentElement);
    const textMuted = style.getPropertyValue("--text-muted").trim() || "#a1a1aa";
    const gridColor = style.getPropertyValue("--border").trim() || "rgba(255,255,255,0.08)";

    return {
      responsive: true,
      maintainAspectRatio: false,
      backgroundColor: "transparent",
      plugins: {
        legend: {
          labels: { color: textMuted },
        },
        title: { display: false },
      },
      scales:
        chartType !== "pie"
          ? {
              x: {
                grid: { color: gridColor },
                ticks: { color: textMuted },
              },
              y: {
                grid: { color: gridColor },
                ticks: { color: textMuted },
              },
            }
          : undefined,
    };
  }, [chartType]);

  const hasData = rows.length > 0 && columns.length > 0;

  if (!hasData) {
    return (
      <div className="chart-view">
        <div className="chart-empty">No data to display. Run a query first.</div>
      </div>
    );
  }

  const handleYColumnsChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const selected = Array.from(
      e.target.selectedOptions,
      (opt) => opt.value
    );
    setYColumns(selected);
  };

  return (
    <div className="chart-view">
      <div className="chart-controls">
        <div className="chart-type-buttons">
          {(["bar", "line", "pie", "scatter"] as const).map((type) => (
            <button
              key={type}
              type="button"
              className={`chart-type-btn ${chartType === type ? "active" : ""}`}
              onClick={() => setChartType(type)}
            >
              {type.charAt(0).toUpperCase() + type.slice(1)}
            </button>
          ))}
        </div>
        <div className="chart-axis-selectors">
          <label>
            <span className="chart-axis-label">X / Labels</span>
            <select
              value={xColumn}
              onChange={(e) => setXColumn(e.target.value)}
            >
              <option value="">— Auto —</option>
              {columns.map((c) => (
                <option key={c} value={c}>
                  {c}
                </option>
              ))}
            </select>
          </label>
          {chartType !== "pie" && chartType !== "scatter" && (
            <label>
              <span className="chart-axis-label">Y / Values</span>
              <select
                multiple
                value={yColumns}
                onChange={handleYColumnsChange}
                className="chart-y-select"
              >
                {numericColumnNames.map((c) => (
                  <option key={c} value={c}>
                    {c}
                  </option>
                ))}
              </select>
            </label>
          )}
        </div>
      </div>
      <div className="chart-canvas">
        {chartData && (
          <>
            {chartType === "bar" && (
              <Bar data={chartData as any} options={chartOptions as any} />
            )}
            {chartType === "line" && (
              <Line data={chartData as any} options={chartOptions as any} />
            )}
            {chartType === "pie" && (
              <Pie data={chartData as any} options={chartOptions as any} />
            )}
            {chartType === "scatter" && (
              <Scatter data={chartData as any} options={chartOptions as any} />
            )}
          </>
        )}
      </div>
    </div>
  );
}
