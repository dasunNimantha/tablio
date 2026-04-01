import { useEffect, useState, useRef, useCallback, useMemo } from "react";
import { api, DatabaseStats } from "../../lib/tauri";
import { computeRate, formatVal, makeLabels, type RatePoint } from "../../lib/dashboardUtils";
import { chartDevicePixelRatio, chartFontFamily } from "../../lib/chartRendering";
import { Loader2, Activity, ArrowUpDown, Database, HardDrive } from "lucide-react";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
  Legend,
} from "chart.js";

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Filler, Tooltip, Legend);

interface Props {
  connectionId: string;
  paused: boolean;
}

const MAX_POINTS = 60;

interface DatasetConfig {
  label: string;
  color: string;
  data: number[];
}

function getThemeColors() {
  const isLight = document.documentElement.getAttribute("data-theme") === "light";

  return {
    grid: isLight ? "rgba(0,0,0,0.08)" : "rgba(255,255,255,0.1)",
    tick: isLight ? "rgba(0,0,0,0.12)" : "rgba(255,255,255,0.15)",
    border: isLight ? "rgba(0,0,0,0.12)" : "rgba(255,255,255,0.15)",
    label: isLight ? "rgba(0,0,0,0.5)" : "rgba(255,255,255,0.55)",
    crosshair: isLight ? "rgba(0,0,0,0.15)" : "rgba(255,255,255,0.2)",
    tooltipBg: isLight ? "rgba(255,255,255,0.99)" : "rgba(30,30,38,0.98)",
    tooltipTitle: isLight ? "rgba(0,0,0,0.72)" : "rgba(230,232,240,0.95)",
    tooltipBody: isLight ? "rgba(0,0,0,0.92)" : "rgba(255,255,255,1)",
    tooltipBorder: isLight ? "rgba(0,0,0,0.14)" : "rgba(255,255,255,0.22)",
    pointHoverBorder: isLight ? "#333" : "#fff",
  };
}

interface ChartCardProps {
  title: string;
  icon: React.ReactNode;
  datasets: DatasetConfig[];
  unit?: string;
}

function applyDpr(chart: ChartJS) {
  const next = chartDevicePixelRatio();
  if (chart.options.devicePixelRatio !== next) {
    chart.options.devicePixelRatio = next;
    chart.resize();
  }
}

function ChartCard({ title, icon, datasets, unit }: ChartCardProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const chartRef = useRef<ChartJS | null>(null);
  const unitRef = useRef(unit);
  unitRef.current = unit;
  const datasetsRef = useRef(datasets);
  datasetsRef.current = datasets;

  const labels = useMemo(() => {
    const maxLen = Math.max(...datasets.map((d) => d.data.length), 0);
    return makeLabels(Math.max(maxLen, MAX_POINTS));
  }, [datasets]);
  const labelsRef = useRef(labels);
  labelsRef.current = labels;

  const syncChartData = useCallback(() => {
    const chart = chartRef.current;
    if (!chart) return;
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const curLabels = labelsRef.current;
    const curDatasets = datasetsRef.current;

    const makeGradient = (color: string) => {
      const m = color.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
      const [r, g, b] = m ? [m[1], m[2], m[3]] : ["130", "130", "150"];
      const h = canvas.clientHeight || 200;
      const grad = ctx.createLinearGradient(0, 0, 0, h);
      grad.addColorStop(0, `rgba(${r},${g},${b},0.25)`);
      grad.addColorStop(0.6, `rgba(${r},${g},${b},0.08)`);
      grad.addColorStop(1, `rgba(${r},${g},${b},0.01)`);
      return grad;
    };

    const tc = getThemeColors();
    chart.data.labels = curLabels;

    while (chart.data.datasets.length > curDatasets.length) chart.data.datasets.pop();

    curDatasets.forEach((ds, i) => {
      const padded = new Array(Math.max(0, curLabels.length - ds.data.length)).fill(null).concat(ds.data);
      if (chart.data.datasets[i]) {
        chart.data.datasets[i].data = padded;
        chart.data.datasets[i].backgroundColor = makeGradient(ds.color) as any;
      } else {
        chart.data.datasets.push({
          label: ds.label,
          data: padded,
          borderColor: ds.color,
          backgroundColor: makeGradient(ds.color),
          borderWidth: 2,
          pointRadius: 0,
          pointHoverRadius: 5,
          pointHoverBackgroundColor: ds.color,
          pointHoverBorderColor: tc.pointHoverBorder,
          pointHoverBorderWidth: 2,
          tension: 0.4,
          fill: true,
        } as any);
      }
    });

    const opts = chart.options;
    const ff = chartFontFamily();
    const tickFont = { size: 12, family: ff, weight: 400 as const };

    for (const axis of ["x", "y"] as const) {
      const scale = (opts.scales as any)?.[axis];
      if (!scale) continue;
      if (scale.grid) { scale.grid.color = tc.grid; scale.grid.tickColor = tc.tick; }
      if (scale.ticks) { scale.ticks.color = tc.label; scale.ticks.font = tickFont; }
      if (scale.border) { scale.border.color = tc.border; }
    }

    if (opts.plugins?.tooltip) {
      Object.assign(opts.plugins.tooltip, {
        backgroundColor: tc.tooltipBg, titleColor: tc.tooltipTitle,
        bodyColor: tc.tooltipBody, borderColor: tc.tooltipBorder,
        titleFont: { size: 13, weight: 500 as const, family: ff },
        bodyFont: { size: 13, weight: 400 as const, family: ff },
      } as any);
    }

    applyDpr(chart);
    chart.update("none");
  }, []);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d", { alpha: true });
    if (!ctx) return;

    const tc = getThemeColors();
    const ff = chartFontFamily();
    const tickFont = { size: 12, family: ff, weight: 400 as const };

    chartRef.current = new ChartJS(ctx, {
      type: "line",
      data: { labels: [], datasets: [] },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        devicePixelRatio: chartDevicePixelRatio(),
        animation: false,
        interaction: { mode: "index", intersect: false },
        hover: { mode: "index", intersect: false },
        layout: { padding: { left: 2, right: 6, top: 4, bottom: 2 } },
        scales: {
          x: {
            grid: { color: tc.grid, drawTicks: true, tickLength: 4, tickColor: tc.tick },
            ticks: {
              color: tc.label,
              font: tickFont,
              maxRotation: 0,
              autoSkip: true,
              autoSkipPadding: 12,
              padding: 6,
            },
            border: { display: true, color: tc.border },
          },
          y: {
            beginAtZero: true,
            grid: { color: tc.grid, drawTicks: true, tickLength: 4, tickColor: tc.tick },
            ticks: {
              color: tc.label,
              font: tickFont,
              maxTicksLimit: 5,
              callback: (val) => formatVal(Number(val)),
              padding: 6,
            },
            border: { display: true, color: tc.border },
          },
        },
        plugins: {
          legend: { display: false },
          tooltip: {
            enabled: true,
            backgroundColor: tc.tooltipBg, titleColor: tc.tooltipTitle, bodyColor: tc.tooltipBody, borderColor: tc.tooltipBorder,
            borderWidth: 1,
            padding: { top: 12, bottom: 12, left: 16, right: 16 },
            titleFont: { size: 13, weight: 500 as const, family: ff },
            bodyFont: { size: 13, weight: 400 as const, family: ff },
            bodySpacing: 8, displayColors: true, boxWidth: 11, boxHeight: 11, boxPadding: 8,
            cornerRadius: 8, caretSize: 0, usePointStyle: true,
            callbacks: {
              title: (items) => {
                if (!items.length) return "";
                const label = items[0].label;
                return label === "now" ? "Current" : label || "";
              },
              label: (item) => {
                const val = item.parsed.y ?? 0;
                const suffix = unitRef.current ? ` ${unitRef.current}` : "";
                return ` ${item.dataset.label}:  ${formatVal(val)}${suffix}`;
              },
            },
          },
        },
      },
      plugins: [{
        id: "crosshair",
        afterDraw: (chart) => {
          const tooltip = chart.tooltip;
          if (!tooltip || !tooltip.opacity) return;
          const x = tooltip.caretX;
          const yScale = chart.scales.y;
          const drawCtx = chart.ctx;
          drawCtx.save();
          drawCtx.beginPath();
          drawCtx.setLineDash([4, 4]);
          drawCtx.strokeStyle = tc.crosshair;
          drawCtx.lineWidth = 1;
          drawCtx.moveTo(x, yScale.top);
          drawCtx.lineTo(x, yScale.bottom);
          drawCtx.stroke();
          drawCtx.restore();
        },
      }],
    });

    syncChartData();

    const chart = chartRef.current;
    const parent = canvas.parentElement;
    const ro =
      parent && typeof ResizeObserver !== "undefined"
        ? new ResizeObserver(() => { if (chart) { applyDpr(chart); syncChartData(); } })
        : null;
    if (parent && ro) ro.observe(parent);
    const onWinResize = () => chart && applyDpr(chart);
    window.addEventListener("resize", onWinResize);

    return () => {
      window.removeEventListener("resize", onWinResize);
      ro?.disconnect();
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
      }
    };
  }, [syncChartData]);

  useEffect(() => {
    syncChartData();
  }, [datasets, labels, syncChartData]);

  return (
    <div className="chart-card">
      <div className="chart-card-header">
        <div className="chart-card-title-row">
          <span className="chart-card-icon">{icon}</span>
          <span className="chart-card-title">{title}</span>
        </div>
        <div className="chart-card-legend">
          {datasets.map((ds) => {
            const current = ds.data.length > 0 ? ds.data[ds.data.length - 1] : 0;
            return (
              <span key={ds.label} className="chart-legend-item">
                <span className="chart-legend-dot" style={{ background: ds.color }} />
                <span className="chart-legend-label">{ds.label}</span>
                <span className="chart-legend-value">{formatVal(current)}{unit ? ` ${unit}` : ""}</span>
              </span>
            );
          })}
        </div>
      </div>
      <div className="chart-canvas-wrapper">
        <canvas ref={canvasRef} />
      </div>
    </div>
  );
}

export function DashboardOverview({ connectionId, paused }: Props) {
  const [snapshots, setSnapshots] = useState<DatabaseStats[]>([]);
  const [rates, setRates] = useState<RatePoint[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const fetchStats = useCallback(async () => {
    try {
      const stats = await api.getDatabaseStats({ connection_id: connectionId });
      setSnapshots((prev) => {
        const next = [...prev, stats];
        if (next.length > MAX_POINTS + 1) next.splice(0, next.length - MAX_POINTS - 1);

        if (next.length >= 2) {
          const newRate = computeRate(next[next.length - 2], next[next.length - 1]);
          setRates((prevRates) => {
            const r = [...prevRates, newRate];
            if (r.length > MAX_POINTS) r.splice(0, r.length - MAX_POINTS);
            return r;
          });
        }

        return next;
      });
      setError(null);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, [connectionId]);

  useEffect(() => {
    fetchStats();
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [fetchStats]);

  useEffect(() => {
    if (intervalRef.current) clearInterval(intervalRef.current);
    if (!paused) {
      intervalRef.current = setInterval(fetchStats, 2000);
    }
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [paused, fetchStats]);

  const latest = snapshots.length > 0 ? snapshots[snapshots.length - 1] : null;
  const prevRate = rates.length > 0 ? rates[rates.length - 1] : null;

  const connDatasets: DatasetConfig[] = useMemo(() => [
    { label: "Active", color: "rgb(61, 214, 140)", data: snapshots.map((s) => s.active_connections) },
    { label: "Idle", color: "rgb(140, 140, 160)", data: snapshots.map((s) => s.idle_connections) },
    { label: "Idle in Tx", color: "rgb(245, 183, 49)", data: snapshots.map((s) => s.idle_in_transaction) },
  ], [snapshots]);

  const tpsDatasets: DatasetConfig[] = useMemo(() => [
    { label: "Commits", color: "rgb(61, 214, 140)", data: rates.map((r) => r.tps_commit) },
    { label: "Rollbacks", color: "rgb(240, 100, 100)", data: rates.map((r) => r.tps_rollback) },
  ], [rates]);

  const tupDatasets: DatasetConfig[] = useMemo(() => [
    { label: "Fetched", color: "rgb(82, 132, 224)", data: rates.map((r) => r.tup_fetch) },
    { label: "Inserted", color: "rgb(61, 214, 140)", data: rates.map((r) => r.tup_ins) },
    { label: "Updated", color: "rgb(245, 183, 49)", data: rates.map((r) => r.tup_upd) },
    { label: "Deleted", color: "rgb(240, 100, 100)", data: rates.map((r) => r.tup_del) },
  ], [rates]);

  const ioDatasets: DatasetConfig[] = useMemo(() => [
    { label: "Buffer Hits", color: "rgb(61, 214, 140)", data: rates.map((r) => r.blks_hit) },
    { label: "Disk Reads", color: "rgb(240, 100, 100)", data: rates.map((r) => r.blks_read) },
  ], [rates]);

  if (loading) {
    return (
      <div className="activity-loading">
        <Loader2 size={24} className="spin" />
        <span>Loading statistics...</span>
      </div>
    );
  }

  return (
    <div className="dashboard-sub-content">
      {error && <div className="activity-error">{error}</div>}

      {latest && (
        <div className="overview-stats-bar">
          <div className="overview-stat">
            <span className="overview-stat-value">{latest.total_connections}</span>
            <span className="overview-stat-label">Total</span>
          </div>
          <div className="overview-stat">
            <span className="overview-stat-value success">{latest.active_connections}</span>
            <span className="overview-stat-label">Active</span>
          </div>
          <div className="overview-stat">
            <span className="overview-stat-value">{latest.idle_connections}</span>
            <span className="overview-stat-label">Idle</span>
          </div>
          <div className="overview-stat">
            <span className="overview-stat-value warning">{latest.idle_in_transaction}</span>
            <span className="overview-stat-label">Idle in Tx</span>
          </div>
          <div className="overview-stat-divider" />
          <div className="overview-stat">
            <span className="overview-stat-value">{prevRate ? formatVal(prevRate.tps_commit + prevRate.tps_rollback) : "—"}</span>
            <span className="overview-stat-label">TPS</span>
          </div>
          <div className="overview-stat">
            <span className="overview-stat-value">{prevRate ? formatVal(prevRate.tup_fetch + prevRate.tup_ins + prevRate.tup_upd + prevRate.tup_del) : "—"}</span>
            <span className="overview-stat-label">Tuples/s</span>
          </div>
        </div>
      )}

      <div className="chart-grid">
        <ChartCard title="Connections" icon={<Activity size={14} />} datasets={connDatasets} />
        <ChartCard title="Transactions / sec" icon={<ArrowUpDown size={14} />} datasets={tpsDatasets} unit="/s" />
        <ChartCard title="Tuples / sec" icon={<Database size={14} />} datasets={tupDatasets} unit="/s" />
        <ChartCard title="Block I/O / sec" icon={<HardDrive size={14} />} datasets={ioDatasets} unit="/s" />
      </div>
    </div>
  );
}
