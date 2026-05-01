import { describe, it, expect } from "vitest";
import {
  api,
  parseSshHostKeyMismatch,
  SSH_HOST_KEY_MISMATCH_PREFIX,
} from "./tauri";

describe("parseSshHostKeyMismatch", () => {
  const validPayload = {
    host: "bastion.example.com",
    port: 22,
    fingerprint: "SHA256:abc",
    knownHostsPath: "/home/u/.tablio/known_hosts",
  };

  it("parses a string error with the magic prefix", () => {
    const err = `${SSH_HOST_KEY_MISMATCH_PREFIX}${JSON.stringify(validPayload)}`;
    expect(parseSshHostKeyMismatch(err)).toEqual(validPayload);
  });

  it("parses an Error instance whose message carries the prefix", () => {
    const err = new Error(
      `${SSH_HOST_KEY_MISMATCH_PREFIX}${JSON.stringify(validPayload)}`,
    );
    expect(parseSshHostKeyMismatch(err)).toEqual(validPayload);
  });

  it("returns null for unrelated string errors", () => {
    expect(parseSshHostKeyMismatch("connection refused")).toBeNull();
    expect(parseSshHostKeyMismatch(new Error("auth failed"))).toBeNull();
  });

  it("returns null when the JSON body is malformed", () => {
    expect(
      parseSshHostKeyMismatch(`${SSH_HOST_KEY_MISMATCH_PREFIX}{not json`),
    ).toBeNull();
  });

  it("returns null when required fields are missing or wrong type", () => {
    const badPort = `${SSH_HOST_KEY_MISMATCH_PREFIX}${JSON.stringify({
      ...validPayload,
      port: "22",
    })}`;
    expect(parseSshHostKeyMismatch(badPort)).toBeNull();

    const noHost = `${SSH_HOST_KEY_MISMATCH_PREFIX}${JSON.stringify({
      port: 22,
      fingerprint: "x",
      knownHostsPath: "/y",
    })}`;
    expect(parseSshHostKeyMismatch(noHost)).toBeNull();
  });

  it("handles plain objects with a message property", () => {
    const obj = {
      message: `${SSH_HOST_KEY_MISMATCH_PREFIX}${JSON.stringify(validPayload)}`,
    };
    expect(parseSshHostKeyMismatch(obj)).toEqual(validPayload);
  });

  it("returns null for non-string non-error inputs", () => {
    expect(parseSshHostKeyMismatch(undefined)).toBeNull();
    expect(parseSshHostKeyMismatch(null)).toBeNull();
    expect(parseSshHostKeyMismatch(42)).toBeNull();
  });
});

describe("xlsx mock-mode commands", () => {
  // The dialog renders against these mocks during e2e and during
  // vitest. Pin the contract so a future mock refactor doesn't quietly
  // strip a field the dialog reads.

  it("parseXlsxWorkbook returns sheet names + a populated default sheet", async () => {
    const preview = await api.parseXlsxWorkbook(new Uint8Array([1, 2, 3]));
    expect(preview.sheet_names.length).toBeGreaterThan(0);
    expect(preview.default_sheet).toBe(preview.sheet_names[0]);
    expect(preview.sheet.name).toBe(preview.default_sheet);
    expect(preview.sheet.headers.length).toBeGreaterThan(0);
    expect(preview.sheet.inferred_types.length).toBe(
      preview.sheet.headers.length,
    );
  });

  it("parseXlsxSheet echoes the requested sheet name + returns typed rows", async () => {
    const payload = await api.parseXlsxSheet(new Uint8Array([0]), "Other");
    expect(payload.name).toBe("Other");
    expect(payload.headers.length).toBe(payload.inferred_types.length);
    expect(payload.rows.length).toBeGreaterThan(0);
    // Type fidelity sanity: numbers stay numbers, bools stay bools.
    const first = payload.rows[0] as unknown[];
    expect(typeof first[0]).toBe("number");
    expect(typeof first[2]).toBe("boolean");
  });
});
