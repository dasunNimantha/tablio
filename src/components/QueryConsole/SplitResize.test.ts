import { describe, it, expect } from "vitest";

function clampEditorHeight(pct: number): number {
  return Math.max(15, Math.min(85, pct));
}

function computeNewHeight(
  startHeight: number,
  startY: number,
  currentY: number,
  totalH: number,
  zoom: number
): number {
  const dy = (currentY - startY) / zoom;
  return clampEditorHeight(startHeight + (dy / totalH) * 100);
}

function parseZoom(zoomStr: string): number {
  return parseFloat(zoomStr || "100") / 100;
}

function getCssVarFallback(value: string, fallback: string): string {
  return value.trim() || fallback;
}

describe("SplitResize editor height clamping", () => {
  it("clamps below minimum to 15%", () => {
    expect(clampEditorHeight(10)).toBe(15);
    expect(clampEditorHeight(0)).toBe(15);
    expect(clampEditorHeight(-5)).toBe(15);
  });

  it("clamps above maximum to 85%", () => {
    expect(clampEditorHeight(90)).toBe(85);
    expect(clampEditorHeight(100)).toBe(85);
    expect(clampEditorHeight(999)).toBe(85);
  });

  it("preserves values in range", () => {
    expect(clampEditorHeight(15)).toBe(15);
    expect(clampEditorHeight(50)).toBe(50);
    expect(clampEditorHeight(85)).toBe(85);
    expect(clampEditorHeight(45)).toBe(45);
  });

  it("handles fractional values", () => {
    expect(clampEditorHeight(45.5)).toBe(45.5);
    expect(clampEditorHeight(14.9)).toBe(15);
    expect(clampEditorHeight(85.1)).toBe(85);
  });
});

describe("SplitResize height computation", () => {
  it("increases height when dragging down", () => {
    const result = computeNewHeight(50, 100, 200, 1000, 1);
    expect(result).toBe(60);
  });

  it("decreases height when dragging up", () => {
    const result = computeNewHeight(50, 200, 100, 1000, 1);
    expect(result).toBe(40);
  });

  it("clamps to minimum when dragging far up", () => {
    const result = computeNewHeight(50, 200, 0, 200, 1);
    expect(result).toBe(15);
  });

  it("clamps to maximum when dragging far down", () => {
    const result = computeNewHeight(50, 0, 500, 200, 1);
    expect(result).toBe(85);
  });

  it("accounts for zoom factor", () => {
    const noZoom = computeNewHeight(50, 100, 200, 1000, 1);
    const doubleZoom = computeNewHeight(50, 100, 200, 1000, 2);
    expect(doubleZoom).toBeLessThan(noZoom);
    expect(doubleZoom).toBe(55);
  });

  it("returns start height when no movement", () => {
    expect(computeNewHeight(45, 100, 100, 1000, 1)).toBe(45);
  });

  it("handles very small containers", () => {
    const result = computeNewHeight(50, 0, 10, 50, 1);
    expect(result).toBe(70);
  });
});

describe("SplitResize zoom parsing", () => {
  it("parses percentage string", () => {
    expect(parseZoom("100")).toBe(1);
    expect(parseZoom("110")).toBe(1.1);
    expect(parseZoom("200")).toBe(2);
    expect(parseZoom("50")).toBe(0.5);
  });

  it("defaults to 1 for empty string", () => {
    expect(parseZoom("")).toBe(1);
  });

  it("handles decimal zoom values", () => {
    expect(parseZoom("150.5")).toBeCloseTo(1.505);
  });
});

describe("getCssVar fallback", () => {
  it("returns value when present", () => {
    expect(getCssVarFallback("#1e1e1e", "#000")).toBe("#1e1e1e");
  });

  it("returns fallback for empty string", () => {
    expect(getCssVarFallback("", "#000")).toBe("#000");
  });

  it("returns fallback for whitespace-only", () => {
    expect(getCssVarFallback("   ", "#000")).toBe("#000");
  });

  it("trims whitespace from value", () => {
    expect(getCssVarFallback("  #1e1e1e  ", "#000")).toBe("#1e1e1e");
  });
});
