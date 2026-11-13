import { describe, it, expect } from "vitest";
import { defaultZoomPercent } from "./zoom";

describe("defaultZoomPercent", () => {
  it("is 100% on Windows and 110% elsewhere", () => {
    expect(defaultZoomPercent({ userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", platform: "Win32" })).toBe(100);
    expect(defaultZoomPercent({ userAgent: "Mozilla/5.0 (X11; Linux x86_64)", platform: "Linux x86_64" })).toBe(110);
    expect(defaultZoomPercent({ userAgent: "Mozilla/5.0 (Macintosh)", platform: "MacIntel" })).toBe(110);
  });
});
