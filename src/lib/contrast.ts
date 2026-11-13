/** WCAG 2.x contrast helpers (used by theme tests and tooling). */
export interface Rgba {
  r: number;
  g: number;
  b: number;
  a: number;
}

export function parseColor(input: string): Rgba {
  const s = input.trim().toLowerCase();
  let m = s.match(/^#([0-9a-f]{3}|[0-9a-f]{6}|[0-9a-f]{8})$/);
  if (m) {
    let hex = m[1];
    if (hex.length === 3) hex = hex.split("").map((c) => c + c).join("");
    const n = (i: number) => parseInt(hex.slice(i, i + 2), 16);
    return { r: n(0), g: n(2), b: n(4), a: hex.length === 8 ? n(6) / 255 : 1 };
  }
  m = s.match(/^rgba?\(([^)]+)\)$/);
  if (m) {
    const parts = m[1].split(/[\s,/]+/).filter(Boolean).map(Number);
    return { r: parts[0], g: parts[1], b: parts[2], a: parts[3] ?? 1 };
  }
  throw new Error(`Unsupported colour: ${input}`);
}

/** Paints `top` over the opaque `bottom`. */
export function composite(top: Rgba, bottom: Rgba): Rgba {
  const a = top.a;
  return {
    r: top.r * a + bottom.r * (1 - a),
    g: top.g * a + bottom.g * (1 - a),
    b: top.b * a + bottom.b * (1 - a),
    a: 1,
  };
}

function channel(c: number): number {
  const v = c / 255;
  return v <= 0.03928 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4;
}

export function luminance(c: Rgba): number {
  return 0.2126 * channel(c.r) + 0.7152 * channel(c.g) + 0.0722 * channel(c.b);
}

export function contrastRatio(fg: Rgba, bg: Rgba): number {
  const f = fg.a < 1 ? composite(fg, bg) : fg;
  const l1 = luminance(f);
  const l2 = luminance(bg);
  const [hi, lo] = l1 > l2 ? [l1, l2] : [l2, l1];
  return (hi + 0.05) / (lo + 0.05);
}

export function toHex(c: Rgba): string {
  const h = (n: number) => Math.round(Math.max(0, Math.min(255, n))).toString(16).padStart(2, "0");
  return `#${h(c.r)}${h(c.g)}${h(c.b)}`;
}

/** Linear mix of two opaque colours (t = 0 → a, t = 1 → b). */
export function mix(a: Rgba, b: Rgba, t: number): Rgba {
  return { r: a.r + (b.r - a.r) * t, g: a.g + (b.g - a.g) * t, b: a.b + (b.b - a.b) * t, a: 1 };
}
