import { describe, it, expect } from "vitest";
import { themes, getThemeById } from "./themes";
import { contrastRatio, parseColor } from "./contrast";

describe("themes", () => {
  it("has at least one theme", () => {
    expect(themes.length).toBeGreaterThan(0);
  });

  it("has both dark and light themes", () => {
    const groups = new Set(themes.map((t) => t.group));
    expect(groups.has("dark")).toBe(true);
    expect(groups.has("light")).toBe(true);
  });

  it("every theme has unique id", () => {
    const ids = themes.map((t) => t.id);
    expect(new Set(ids).size).toBe(ids.length);
  });

  it("every theme has required CSS variables", () => {
    const required = [
      "--bg-primary",
      "--bg-secondary",
      "--text-primary",
      "--text-secondary",
      "--accent",
      "--border",
      "--error",
      "--success",
      "--warning",
    ];
    for (const theme of themes) {
      for (const key of required) {
        expect(theme.vars).toHaveProperty(key);
      }
    }
  });

  it("every theme has a non-empty name", () => {
    for (const theme of themes) {
      expect(theme.name.length).toBeGreaterThan(0);
    }
  });

  it("every theme group is either dark or light", () => {
    for (const theme of themes) {
      expect(["dark", "light"]).toContain(theme.group);
    }
  });
});

describe("getThemeById", () => {
  it("returns the correct theme for a valid id", () => {
    const theme = getThemeById("dark");
    expect(theme.id).toBe("dark");
    expect(theme.name).toBe("Default Dark");
  });

  it("returns dracula theme", () => {
    const theme = getThemeById("dracula");
    expect(theme.id).toBe("dracula");
    expect(theme.group).toBe("dark");
  });

  it("returns light theme", () => {
    const theme = getThemeById("light");
    expect(theme.id).toBe("light");
    expect(theme.group).toBe("light");
  });

  it("falls back to first theme for unknown id", () => {
    const theme = getThemeById("nonexistent");
    expect(theme.id).toBe(themes[0].id);
  });

  it("falls back to first theme for empty string", () => {
    const theme = getThemeById("");
    expect(theme.id).toBe(themes[0].id);
  });
});

describe("theme text contrast (WCAG AA)", () => {
  const BACKGROUNDS = ["--bg-primary", "--bg-secondary", "--bg-tertiary", "--bg-header", "--bg-surface", "--bg-elevated"];
  for (const theme of themes) {
    for (const token of ["--text-primary", "--text-secondary", "--text-muted", "--cell-null"]) {
      it(`${theme.id}: ${token} is at least 4.5:1 on every background`, () => {
        const fg = parseColor(theme.vars[token]);
        for (const bg of BACKGROUNDS) {
          const ratio = contrastRatio(fg, parseColor(theme.vars[bg]));
          expect(ratio, `${token} on ${bg}`).toBeGreaterThanOrEqual(4.5);
        }
      });
    }
  }
});

describe("contrastRatio", () => {
  it("matches the WCAG reference values", () => {
    expect(contrastRatio(parseColor("#000"), parseColor("#fff"))).toBeCloseTo(21, 5);
    expect(contrastRatio(parseColor("#777777"), parseColor("#ffffff"))).toBeCloseTo(4.48, 2);
    expect(contrastRatio(parseColor("rgba(0,0,0,0.5)"), parseColor("#ffffff"))).toBeCloseTo(3.95, 1);
  });
});
