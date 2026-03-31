/**
 * Monaco's defineTheme uses Color.fromHex(), which returns red for any value that
 * is not valid hex (e.g. rgba() from CSS variables). Resolve to #rrggbb / #rrggbbaa.
 */
let probe: HTMLSpanElement | null = null;

function ensureProbe(): HTMLSpanElement {
  if (!probe) {
    probe = document.createElement("span");
    probe.style.cssText = "position:absolute;left:-9999px;visibility:hidden;pointer-events:none;";
    document.body.appendChild(probe);
  }
  return probe;
}

export function cssColorToMonacoHex(cssValue: string, fallback = "#808080"): string {
  const v = cssValue.trim();
  if (!v) return fallback.match(/^#[0-9a-fA-F]{6}([0-9a-fA-F]{2})?$/) ? fallback : "#808080";

  if (/^#[0-9a-fA-F]{3}$/.test(v)) {
    const [, a, b, c] = v;
    return `#${a}${a}${b}${b}${c}${c}`.toLowerCase();
  }
  if (/^#[0-9a-fA-F]{6}$/.test(v) || /^#[0-9a-fA-F]{8}$/.test(v)) {
    return v.toLowerCase();
  }

  const el = ensureProbe();
  el.style.color = v;
  const rgb = getComputedStyle(el).color;
  const m = rgb.match(/rgba?\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*(?:,\s*([\d.]+)\s*)?\)/);
  if (!m) {
    return /^#[0-9a-fA-F]{6}([0-9a-fA-F]{2})?$/.test(fallback) ? fallback.toLowerCase() : "#808080";
  }
  const r = Math.min(255, parseInt(m[1], 10));
  const g = Math.min(255, parseInt(m[2], 10));
  const b = Math.min(255, parseInt(m[3], 10));
  const a = m[4] !== undefined ? Math.round(parseFloat(m[4]) * 255) : 255;
  const h = (n: number) => n.toString(16).padStart(2, "0");
  if (a >= 255) return `#${h(r)}${h(g)}${h(b)}`;
  return `#${h(r)}${h(g)}${h(b)}${h(a)}`;
}

/** 6-digit hex for accent + optional 2-digit alpha suffix (e.g. "33"). */
export function selectionBackgroundFromAccent(accentCss: string, fallback = "#6398ff"): string {
  const hex = cssColorToMonacoHex(accentCss, fallback);
  const base = hex.length === 9 ? hex.slice(0, 7) : hex;
  return `${base}33`;
}
