import { describe, it, expect } from "vitest";
import {
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
