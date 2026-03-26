import { describe, it, expect } from "vitest";

function clampZoom(level: number): number {
  return Math.min(200, Math.max(50, Math.round(level)));
}

function computeAppZoomVar(clamped: number): string {
  return String(clamped / 100);
}

function computeZoomStyle(clamped: number): string {
  return `${clamped}%`;
}

function zoomStep(current: number, direction: "in" | "out", step = 10): number {
  const next = direction === "in" ? current + step : current - step;
  return clampZoom(next);
}

describe("App zoom clamping", () => {
  it("clamps below minimum to 50", () => {
    expect(clampZoom(10)).toBe(50);
    expect(clampZoom(0)).toBe(50);
    expect(clampZoom(-100)).toBe(50);
  });

  it("clamps above maximum to 200", () => {
    expect(clampZoom(300)).toBe(200);
    expect(clampZoom(201)).toBe(200);
    expect(clampZoom(999)).toBe(200);
  });

  it("preserves values within range", () => {
    expect(clampZoom(50)).toBe(50);
    expect(clampZoom(100)).toBe(100);
    expect(clampZoom(110)).toBe(110);
    expect(clampZoom(150)).toBe(150);
    expect(clampZoom(200)).toBe(200);
  });

  it("rounds fractional values", () => {
    expect(clampZoom(110.4)).toBe(110);
    expect(clampZoom(110.5)).toBe(111);
    expect(clampZoom(99.9)).toBe(100);
  });
});

describe("App --app-zoom CSS variable", () => {
  it("converts 100% zoom to 1", () => {
    expect(computeAppZoomVar(100)).toBe("1");
  });

  it("converts 50% zoom to 0.5", () => {
    expect(computeAppZoomVar(50)).toBe("0.5");
  });

  it("converts 200% zoom to 2", () => {
    expect(computeAppZoomVar(200)).toBe("2");
  });

  it("converts 110% zoom to 1.1", () => {
    expect(computeAppZoomVar(110)).toBe("1.1");
  });

  it("converts 75% zoom to 0.75", () => {
    expect(computeAppZoomVar(75)).toBe("0.75");
  });
});

describe("App zoom style string", () => {
  it("formats as percentage", () => {
    expect(computeZoomStyle(100)).toBe("100%");
    expect(computeZoomStyle(110)).toBe("110%");
    expect(computeZoomStyle(50)).toBe("50%");
    expect(computeZoomStyle(200)).toBe("200%");
  });
});

describe("App zoom step", () => {
  it("increases zoom by step", () => {
    expect(zoomStep(100, "in")).toBe(110);
    expect(zoomStep(190, "in")).toBe(200);
  });

  it("decreases zoom by step", () => {
    expect(zoomStep(100, "out")).toBe(90);
    expect(zoomStep(60, "out")).toBe(50);
  });

  it("clamps at upper boundary", () => {
    expect(zoomStep(200, "in")).toBe(200);
    expect(zoomStep(195, "in")).toBe(200);
  });

  it("clamps at lower boundary", () => {
    expect(zoomStep(50, "out")).toBe(50);
    expect(zoomStep(55, "out")).toBe(50);
  });

  it("supports custom step size", () => {
    expect(zoomStep(100, "in", 25)).toBe(125);
    expect(zoomStep(100, "out", 25)).toBe(75);
  });
});
