/**
 * Chart.js draws to canvas. When the app uses CSS `zoom` on `html`, the bitmap is
 * scaled up without extra backing-store pixels, so axis labels and tooltips look jagged.
 */
export function getUiZoomFactor(): number {
  const raw = String(document.documentElement.style.zoom || "").trim().replace("%", "");
  const z = parseFloat(raw || "100");
  return Number.isFinite(z) && z > 0 ? z / 100 : 1;
}

export function chartDevicePixelRatio(): number {
  const dpr = window.devicePixelRatio || 1;
  return Math.min(4, dpr * getUiZoomFactor());
}

export function chartFontFamily(): string {
  const v = getComputedStyle(document.documentElement).getPropertyValue("--font-sans").trim();
  return v || '"Fira Sans", system-ui, sans-serif';
}
