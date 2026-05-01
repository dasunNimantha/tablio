/**
 * Cross-webview zoom helpers.
 *
 * Tablio uses CSS `zoom` for app-wide UI scaling. The two webviews we ship
 * to handle `zoom` differently:
 *
 *   - WebKit2GTK (Linux): `zoom` on `<html>` auto-compensates viewport
 *     references, so `100vh`, `100%`, and friends still resolve to the
 *     full window. Putting `zoom` on `<body>` does not give the same
 *     auto-compensation, so we keep it on `<html>` there.
 *
 *   - Chromium / WebView2 (Windows): `zoom` does not expand the element's
 *     own layout box and `vh` units stay anchored to the unscaled
 *     viewport, so `zoom < 100%` leaves an empty band along the bottom
 *     and right. We work around that by zooming `<body>` and
 *     compensating its layout box in CSS via the `.zoom-compensated`
 *     class.
 *
 * The two webviews are distinguished by user agent: Chromium-based
 * webviews expose "Chrome" (WebView2 also adds "Edg"); WebKit2GTK does
 * not.
 */

let cachedTarget: HTMLElement | null = null;
let cachedIsChromium: boolean | null = null;

function detectChromium(): boolean {
  if (typeof navigator === "undefined") return false;
  return /Chrome|Edg/.test(navigator.userAgent);
}

export function isChromiumWebview(): boolean {
  if (cachedIsChromium === null) cachedIsChromium = detectChromium();
  return cachedIsChromium;
}

/**
 * The element CSS `zoom` and `--app-zoom` are written to (and read from).
 *
 * Chromium needs zoom on `<body>` (paired with `.zoom-compensated`); WebKit
 * needs it on `<html>` so its auto-compensation kicks in.
 */
export function getZoomTarget(): HTMLElement {
  if (cachedTarget) return cachedTarget;
  cachedTarget = isChromiumWebview() ? document.body : document.documentElement;
  return cachedTarget;
}

/** Reads the current zoom factor (e.g. 1.1 for 110%) regardless of which element holds it. */
export function readZoomFactor(): number {
  const raw = getZoomTarget().style.zoom;
  const n = parseFloat(raw || "100");
  return Number.isFinite(n) && n > 0 ? n / 100 : 1;
}
