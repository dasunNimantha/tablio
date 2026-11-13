import { describe, it, expect } from "vitest";
import { rolePasswordValue } from "./RoleManager";

describe("rolePasswordValue", () => {
  it("keeps leading and trailing spaces in passwords", () => {
    expect(rolePasswordValue("  secret ")).toBe("  secret ");
    expect(rolePasswordValue(" ")).toBe(" ");
  });

  it("treats only an empty field as no password", () => {
    expect(rolePasswordValue("")).toBeNull();
  });
});
