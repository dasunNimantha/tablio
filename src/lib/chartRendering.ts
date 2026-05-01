import { readZoomFactor } from "./zoom";

/**
 * Chart.js draws to canvas. When the app uses CSS `zoom`, the bitmap is
 * scaled up without extra backing-store pixels, so axis labels and tooltips
 * look jagged. The actual element holding `zoom` is webview-dependent (see
 * `src/lib/zoom.ts`), so we go through the helper.
 */
export function getUiZoomFactor(): number {
  return readZoomFactor();
}

export function chartDevicePixelRatio(): number {
  const dpr = window.devicePixelRatio || 1;
  return Math.min(4, dpr * getUiZoomFactor());
}

export function chartFontFamily(): string {
  const v = getComputedStyle(document.documentElement).getPropertyValue("--font-sans").trim();
  return v || '"Fira Sans", system-ui, sans-serif';
}
