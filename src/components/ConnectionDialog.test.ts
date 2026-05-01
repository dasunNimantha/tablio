import { describe, expect, it } from "vitest";
import {
  normalizeConnectionForm,
  validateConnectionForm,
} from "./ConnectionDialog";
import type { ConnectionConfig } from "../lib/tauri";

const baseConfig: ConnectionConfig = {
  id: "id-1",
  name: "My DB",
  db_type: "postgres",
  host: "db.example.com",
  port: 5432,
  user: "alice",
  password: "secret",
  database: "appdb",
  color: "#89b4fa",
  ssl: false,
  trust_server_cert: false,
  group: null,
  ssh_enabled: false,
  ssh_host: "",
  ssh_port: 22,
  ssh_user: "",
  ssh_password: "",
  ssh_key_path: "",
  ssh_auth_method: "password",
  ssh_prompt_passphrase: false,
};

describe("normalizeConnectionForm — SSH fields", () => {
  it("clears every SSH field when ssh_enabled is false", () => {
    const dirty: ConnectionConfig = {
      ...baseConfig,
      ssh_enabled: false,
      ssh_host: "  bastion ",
      ssh_user: " alice ",
      ssh_password: "leftover",
      ssh_key_path: "/tmp/leftover",
      ssh_auth_method: "identityfile",
      ssh_prompt_passphrase: true,
    };
    const out = normalizeConnectionForm(dirty);
    expect(out.ssh_enabled).toBe(false);
    expect(out.ssh_host).toBe("");
    expect(out.ssh_user).toBe("");
    expect(out.ssh_password).toBe("");
    expect(out.ssh_key_path).toBe("");
    expect(out.ssh_auth_method).toBe("password");
    expect(out.ssh_prompt_passphrase).toBe(false);
  });

  it("trims ssh_host and ssh_user when SSH is enabled", () => {
    const out = normalizeConnectionForm({
      ...baseConfig,
      ssh_enabled: true,
      ssh_host: "  bastion.example.com  ",
      ssh_user: " alice ",
      ssh_password: "p",
      ssh_auth_method: "password",
    });
    expect(out.ssh_host).toBe("bastion.example.com");
    expect(out.ssh_user).toBe("alice");
  });

  it("clears ssh_key_path when auth = password", () => {
    const out = normalizeConnectionForm({
      ...baseConfig,
      ssh_enabled: true,
      ssh_host: "h",
      ssh_user: "u",
      ssh_password: "p",
      ssh_key_path: "/tmp/key",
      ssh_auth_method: "password",
    });
    expect(out.ssh_key_path).toBe("");
  });

  it("trims ssh_key_path when auth = identityfile", () => {
    const out = normalizeConnectionForm({
      ...baseConfig,
      ssh_enabled: true,
      ssh_host: "h",
      ssh_user: "u",
      ssh_key_path: "  /home/alice/.ssh/id_ed25519  ",
      ssh_auth_method: "identityfile",
    });
    expect(out.ssh_key_path).toBe("/home/alice/.ssh/id_ed25519");
  });

  it("blanks the stored passphrase when prompt-for-passphrase is on", () => {
    const out = normalizeConnectionForm({
      ...baseConfig,
      ssh_enabled: true,
      ssh_host: "h",
      ssh_user: "u",
      ssh_password: "should-not-be-saved",
      ssh_key_path: "/k",
      ssh_auth_method: "identityfile",
      ssh_prompt_passphrase: true,
    });
    expect(out.ssh_password).toBe("");
    expect(out.ssh_prompt_passphrase).toBe(true);
  });

  it("ignores prompt-for-passphrase when auth = password", () => {
    const out = normalizeConnectionForm({
      ...baseConfig,
      ssh_enabled: true,
      ssh_host: "h",
      ssh_user: "u",
      ssh_password: "secret-bastion-password",
      ssh_auth_method: "password",
      ssh_prompt_passphrase: true,
    });
    expect(out.ssh_prompt_passphrase).toBe(false);
    expect(out.ssh_password).toBe("secret-bastion-password");
  });

  it("falls back to ssh_port = 22 when missing", () => {
    const out = normalizeConnectionForm({
      ...baseConfig,
      ssh_enabled: true,
      ssh_host: "h",
      ssh_user: "u",
      ssh_password: "p",
      ssh_port: undefined as unknown as number,
    });
    expect(out.ssh_port).toBe(22);
  });
});

describe("validateConnectionForm — SSH rules", () => {
  it("requires ssh_host, ssh_user and ssh_password when SSH + password auth", () => {
    const errors = validateConnectionForm(
      {
        ...baseConfig,
        ssh_enabled: true,
        ssh_host: "",
        ssh_user: "",
        ssh_password: "",
        ssh_auth_method: "password",
      },
      [],
    );
    expect(errors.ssh_host).toBeTruthy();
    expect(errors.ssh_user).toBeTruthy();
    expect(errors.ssh_password).toBeTruthy();
    // No identity-file path is required in password mode.
    expect(errors.ssh_key_path).toBeUndefined();
  });

  it("requires ssh_key_path when SSH + identity-file auth", () => {
    const errors = validateConnectionForm(
      {
        ...baseConfig,
        ssh_enabled: true,
        ssh_host: "h",
        ssh_user: "u",
        ssh_password: "",
        ssh_key_path: "",
        ssh_auth_method: "identityfile",
      },
      [],
    );
    expect(errors.ssh_key_path).toBeTruthy();
    // ssh_password is the (optional) passphrase in this mode; not required.
    expect(errors.ssh_password).toBeUndefined();
  });

  it("rejects out-of-range SSH ports", () => {
    const errors = validateConnectionForm(
      {
        ...baseConfig,
        ssh_enabled: true,
        ssh_host: "h",
        ssh_user: "u",
        ssh_password: "p",
        ssh_port: 70000,
      },
      [],
    );
    expect(errors.ssh_port).toBeTruthy();
  });

  it("does not enforce SSH validation when SSH is disabled", () => {
    const errors = validateConnectionForm(
      {
        ...baseConfig,
        ssh_enabled: false,
        ssh_host: "",
        ssh_user: "",
        ssh_password: "",
      },
      [],
    );
    expect(errors.ssh_host).toBeUndefined();
    expect(errors.ssh_user).toBeUndefined();
    expect(errors.ssh_password).toBeUndefined();
  });

  it("ignores SSH for SQLite connections", () => {
    const errors = validateConnectionForm(
      {
        ...baseConfig,
        db_type: "sqlite",
        host: "",
        user: "",
        database: "/tmp/db.sqlite",
        ssh_enabled: true,
        ssh_host: "",
        ssh_user: "",
      },
      [],
    );
    expect(errors.ssh_host).toBeUndefined();
    expect(errors.ssh_user).toBeUndefined();
  });

  it("still validates DB host/user alongside SSH validation for postgres", () => {
    const errors = validateConnectionForm(
      {
        ...baseConfig,
        host: "",
        user: "",
        ssh_enabled: true,
        ssh_host: "",
        ssh_user: "",
        ssh_password: "",
      },
      [],
    );
    expect(errors.host).toBeTruthy();
    expect(errors.user).toBeTruthy();
    expect(errors.ssh_host).toBeTruthy();
    expect(errors.ssh_user).toBeTruthy();
  });
});
