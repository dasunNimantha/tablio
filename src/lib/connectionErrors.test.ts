import { describe, it, expect } from "vitest";
import { formatConnectFailure, formatConnectionError } from "./connectionErrors";

const pg = { db_type: "postgres" as const, user: "alice", ssh_host: "", name: "Prod", database: "app" };

describe("formatConnectionError", () => {
  it("explains password failures", () => {
    expect(formatConnectionError('password authentication failed for user "alice"', pg)).toMatch(
      /Database authentication failed for PostgreSQL user "alice"/,
    );
  });

  it("labels SSH failures with the bastion", () => {
    expect(
      formatConnectionError(new Error("ssh handshake failed"), { ...pg, ssh_host: "bastion" }),
    ).toBe("SSH tunnel failed for bastion: ssh handshake failed");
  });

  it("falls back to the raw message", () => {
    expect(formatConnectionError("connection refused", pg)).toBe("connection refused");
    expect(formatConnectionError("", pg)).toBe("Connection failed.");
  });
});

describe("formatConnectFailure", () => {
  it("names the connection", () => {
    expect(formatConnectFailure("connection refused", pg)).toBe(
      'Couldn\'t connect to "Prod". connection refused',
    );
  });
});
