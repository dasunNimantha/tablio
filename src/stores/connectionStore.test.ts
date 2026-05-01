import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  api,
  SSH_HOST_KEY_MISMATCH_PREFIX,
  type ConnectionConfig,
} from "../lib/tauri";
import {
  resolveSshPassphrase,
  useConnectionStore,
  withHostKeyMismatchRetry,
} from "./connectionStore";

function baseConfig(overrides: Partial<ConnectionConfig> = {}): ConnectionConfig {
  return {
    id: "conn-1",
    name: "Test DB",
    db_type: "postgres",
    host: "db.example",
    port: 5432,
    user: "u",
    password: "p",
    database: "appdb",
    color: "#000",
    ssl: false,
    trust_server_cert: false,
    ssh_enabled: false,
    ssh_host: "",
    ssh_port: 22,
    ssh_user: "",
    ssh_password: "",
    ssh_key_path: "",
    ssh_auth_method: "password",
    ssh_prompt_passphrase: false,
    ...overrides,
  };
}

function resetStore() {
  useConnectionStore.setState({
    connections: [],
    activeConnections: new Set(),
    loading: false,
    error: null,
    pendingPassphrasePrompt: null,
    pendingHostKeyMismatch: null,
  });
}

/**
 * Spin the event loop until `selector` returns a non-null value or the
 * timeout expires. Used to wait for a prompt to appear when the connect
 * helper has more than one microtask hop before raising it (e.g. after
 * the resolveSshPassphrase / withHostKeyMismatchRetry refactor).
 */
async function waitForPending<T>(
  selector: () => T | null,
  timeoutMs = 1000,
): Promise<T> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const v = selector();
    if (v) return v;
    await new Promise((r) => setTimeout(r, 0));
  }
  throw new Error("timed out waiting for pending prompt to be raised");
}

describe("connectionStore.connectTo — SSH passphrase prompt", () => {
  beforeEach(() => {
    resetStore();
    vi.spyOn(api, "connect").mockResolvedValue("ok");
    vi.spyOn(api, "forgetKnownHost").mockResolvedValue(undefined);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("does NOT raise the prompt when SSH is disabled", async () => {
    const cfg = baseConfig({
      ssh_enabled: false,
      ssh_auth_method: "identityfile",
      ssh_prompt_passphrase: true,
    });
    await useConnectionStore.getState().connectTo(cfg);
    expect(useConnectionStore.getState().pendingPassphrasePrompt).toBeNull();
    expect(api.connect).toHaveBeenCalledTimes(1);
  });

  it("does NOT raise the prompt for password auth even with the flag set", async () => {
    const cfg = baseConfig({
      ssh_enabled: true,
      ssh_host: "bastion",
      ssh_auth_method: "password",
      ssh_prompt_passphrase: true,
    });
    await useConnectionStore.getState().connectTo(cfg);
    expect(useConnectionStore.getState().pendingPassphrasePrompt).toBeNull();
    expect(api.connect).toHaveBeenCalledTimes(1);
  });

  it("surfaces the prompt and threads the entered passphrase into ssh_password", async () => {
    const cfg = baseConfig({
      ssh_enabled: true,
      ssh_host: "bastion",
      ssh_auth_method: "identityfile",
      ssh_key_path: "/keys/id_ed25519",
      ssh_prompt_passphrase: true,
      ssh_password: "",
    });

    const promise = useConnectionStore.getState().connectTo(cfg);
    const pending = await waitForPending(
      () => useConnectionStore.getState().pendingPassphrasePrompt,
    );
    expect(pending.connectionName).toBe("Test DB");
    expect(pending.keyPath).toBe("/keys/id_ed25519");

    pending.resolve("super-secret");
    await promise;

    expect(useConnectionStore.getState().pendingPassphrasePrompt).toBeNull();
    expect(api.connect).toHaveBeenCalledTimes(1);
    const passed = (api.connect as ReturnType<typeof vi.fn>).mock
      .calls[0][0] as ConnectionConfig;
    expect(passed.ssh_password).toBe("super-secret");
    expect(passed.ssh_prompt_passphrase).toBe(false);
    // Original config object must not be mutated.
    expect(cfg.ssh_password).toBe("");
    expect(cfg.ssh_prompt_passphrase).toBe(true);
    expect(useConnectionStore.getState().isConnected("conn-1")).toBe(true);
  });

  it("rejecting the prompt aborts the connect and never calls api.connect", async () => {
    const cfg = baseConfig({
      ssh_enabled: true,
      ssh_host: "bastion",
      ssh_auth_method: "identityfile",
      ssh_key_path: "/keys/id_ed25519",
      ssh_prompt_passphrase: true,
    });

    const promise = useConnectionStore.getState().connectTo(cfg);
    const pending = await waitForPending(
      () => useConnectionStore.getState().pendingPassphrasePrompt,
    );
    pending.reject(new Error("user cancelled"));

    await expect(promise).rejects.toThrow("user cancelled");
    expect(api.connect).not.toHaveBeenCalled();
    expect(useConnectionStore.getState().pendingPassphrasePrompt).toBeNull();
    expect(useConnectionStore.getState().isConnected("conn-1")).toBe(false);
  });
});

describe("connectionStore.connectTo — SSH host-key mismatch", () => {
  const mismatchPayload = {
    host: "bastion.example",
    port: 22,
    fingerprint: "SHA256:newkey",
    knownHostsPath: "/home/u/.tablio/known_hosts",
  };
  const mismatchError = new Error(
    `${SSH_HOST_KEY_MISMATCH_PREFIX}${JSON.stringify(mismatchPayload)}`,
  );

  beforeEach(() => {
    resetStore();
    vi.spyOn(api, "forgetKnownHost").mockResolvedValue(undefined);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("propagates non-mismatch errors without raising the prompt", async () => {
    vi.spyOn(api, "connect").mockRejectedValueOnce(
      new Error("connection refused"),
    );
    const cfg = baseConfig({ ssh_enabled: true, ssh_host: "bastion" });
    await expect(
      useConnectionStore.getState().connectTo(cfg),
    ).rejects.toThrow("connection refused");
    expect(useConnectionStore.getState().pendingHostKeyMismatch).toBeNull();
    expect(api.forgetKnownHost).not.toHaveBeenCalled();
  });

  it("on Forget & retry: forgets the recorded key and re-invokes api.connect once", async () => {
    const connectMock = vi
      .spyOn(api, "connect")
      .mockRejectedValueOnce(mismatchError)
      .mockResolvedValueOnce("ok");

    const cfg = baseConfig({ ssh_enabled: true, ssh_host: "bastion" });
    const promise = useConnectionStore.getState().connectTo(cfg);
    const pending = await waitForPending(
      () => useConnectionStore.getState().pendingHostKeyMismatch,
    );
    expect(pending.connectionName).toBe("Test DB");
    expect(pending.info).toEqual(mismatchPayload);

    pending.resolve(true);
    await promise;

    expect(api.forgetKnownHost).toHaveBeenCalledExactlyOnceWith(
      mismatchPayload.host,
      mismatchPayload.port,
      mismatchPayload.fingerprint,
    );
    expect(connectMock).toHaveBeenCalledTimes(2);
    expect(useConnectionStore.getState().pendingHostKeyMismatch).toBeNull();
    expect(useConnectionStore.getState().isConnected("conn-1")).toBe(true);
  });

  it("on Cancel: throws the original mismatch error and does NOT forget or retry", async () => {
    const connectMock = vi
      .spyOn(api, "connect")
      .mockRejectedValueOnce(mismatchError);

    const cfg = baseConfig({ ssh_enabled: true, ssh_host: "bastion" });
    const promise = useConnectionStore.getState().connectTo(cfg);
    const pending = await waitForPending(
      () => useConnectionStore.getState().pendingHostKeyMismatch,
    );
    pending.resolve(false);

    await expect(promise).rejects.toBe(mismatchError);
    expect(api.forgetKnownHost).not.toHaveBeenCalled();
    expect(connectMock).toHaveBeenCalledTimes(1);
    expect(useConnectionStore.getState().pendingHostKeyMismatch).toBeNull();
    expect(useConnectionStore.getState().isConnected("conn-1")).toBe(false);
  });

  it("does NOT retry a second time if the new key also mismatches (retry budget = 1)", async () => {
    const secondMismatch = new Error(
      `${SSH_HOST_KEY_MISMATCH_PREFIX}${JSON.stringify({
        ...mismatchPayload,
        fingerprint: "SHA256:secondkey",
      })}`,
    );
    const connectMock = vi
      .spyOn(api, "connect")
      .mockRejectedValueOnce(mismatchError)
      .mockRejectedValueOnce(secondMismatch);

    const cfg = baseConfig({ ssh_enabled: true, ssh_host: "bastion" });
    const promise = useConnectionStore.getState().connectTo(cfg);
    const pending = await waitForPending(
      () => useConnectionStore.getState().pendingHostKeyMismatch,
    );
    pending.resolve(true);

    await expect(promise).rejects.toBe(secondMismatch);
    expect(connectMock).toHaveBeenCalledTimes(2);
    expect(api.forgetKnownHost).toHaveBeenCalledTimes(1);
  });
});

describe("resolveSshPassphrase", () => {
  beforeEach(() => {
    resetStore();
  });

  it("returns the original config unchanged when SSH is disabled", async () => {
    const cfg = baseConfig({
      ssh_enabled: false,
      ssh_auth_method: "identityfile",
      ssh_prompt_passphrase: true,
      ssh_password: "kept",
    });
    const out = await resolveSshPassphrase(cfg);
    expect(out).toBe(cfg);
    expect(useConnectionStore.getState().pendingPassphrasePrompt).toBeNull();
  });

  it("returns the original config unchanged for password auth", async () => {
    const cfg = baseConfig({
      ssh_enabled: true,
      ssh_host: "bastion",
      ssh_auth_method: "password",
      ssh_prompt_passphrase: true,
    });
    const out = await resolveSshPassphrase(cfg);
    expect(out).toBe(cfg);
    expect(useConnectionStore.getState().pendingPassphrasePrompt).toBeNull();
  });

  it("returns the original config unchanged when prompt-for-passphrase is off", async () => {
    const cfg = baseConfig({
      ssh_enabled: true,
      ssh_host: "bastion",
      ssh_auth_method: "identityfile",
      ssh_prompt_passphrase: false,
      ssh_password: "stored",
    });
    const out = await resolveSshPassphrase(cfg);
    expect(out).toBe(cfg);
    expect(useConnectionStore.getState().pendingPassphrasePrompt).toBeNull();
  });

  it("raises the prompt and returns a fresh config with the entered passphrase", async () => {
    const cfg = baseConfig({
      ssh_enabled: true,
      ssh_host: "bastion",
      ssh_auth_method: "identityfile",
      ssh_key_path: "/keys/id_ed25519",
      ssh_prompt_passphrase: true,
      ssh_password: "",
    });
    const promise = resolveSshPassphrase(cfg);
    const pending = await waitForPending(
      () => useConnectionStore.getState().pendingPassphrasePrompt,
    );
    pending.resolve("entered");
    const out = await promise;
    expect(out).not.toBe(cfg);
    expect(out.ssh_password).toBe("entered");
    expect(out.ssh_prompt_passphrase).toBe(false);
    // Original config object stays untouched.
    expect(cfg.ssh_password).toBe("");
    expect(cfg.ssh_prompt_passphrase).toBe(true);
  });

  it("rejects when the user cancels the prompt", async () => {
    const cfg = baseConfig({
      ssh_enabled: true,
      ssh_host: "bastion",
      ssh_auth_method: "identityfile",
      ssh_key_path: "/keys/id_ed25519",
      ssh_prompt_passphrase: true,
    });
    const promise = resolveSshPassphrase(cfg);
    const pending = await waitForPending(
      () => useConnectionStore.getState().pendingPassphrasePrompt,
    );
    pending.reject(new Error("cancelled"));
    await expect(promise).rejects.toThrow("cancelled");
  });
});

describe("withHostKeyMismatchRetry", () => {
  const mismatchPayload = {
    host: "bastion",
    port: 22,
    fingerprint: "SHA256:fp",
    knownHostsPath: "/known_hosts",
  };
  const mismatchError = new Error(
    `${SSH_HOST_KEY_MISMATCH_PREFIX}${JSON.stringify(mismatchPayload)}`,
  );

  beforeEach(() => {
    resetStore();
    vi.spyOn(api, "forgetKnownHost").mockResolvedValue(undefined);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("returns the action's value on a clean run without raising any prompt", async () => {
    const action = vi.fn().mockResolvedValue("ok");
    const out = await withHostKeyMismatchRetry("Test DB", action);
    expect(out).toBe("ok");
    expect(action).toHaveBeenCalledTimes(1);
    expect(useConnectionStore.getState().pendingHostKeyMismatch).toBeNull();
    expect(api.forgetKnownHost).not.toHaveBeenCalled();
  });

  it("propagates non-mismatch errors and never raises the prompt", async () => {
    const err = new Error("boom");
    const action = vi.fn().mockRejectedValue(err);
    await expect(
      withHostKeyMismatchRetry("Test DB", action),
    ).rejects.toBe(err);
    expect(action).toHaveBeenCalledTimes(1);
    expect(useConnectionStore.getState().pendingHostKeyMismatch).toBeNull();
    expect(api.forgetKnownHost).not.toHaveBeenCalled();
  });

  it("on Forget & retry: forgets the recorded key and re-runs the action once", async () => {
    const action = vi
      .fn<() => Promise<string>>()
      .mockRejectedValueOnce(mismatchError)
      .mockResolvedValueOnce("retried");
    const promise = withHostKeyMismatchRetry("My Conn", action);
    const pending = await waitForPending(
      () => useConnectionStore.getState().pendingHostKeyMismatch,
    );
    expect(pending.connectionName).toBe("My Conn");
    expect(pending.info).toEqual(mismatchPayload);
    pending.resolve(true);
    const out = await promise;
    expect(out).toBe("retried");
    expect(action).toHaveBeenCalledTimes(2);
    expect(api.forgetKnownHost).toHaveBeenCalledExactlyOnceWith(
      mismatchPayload.host,
      mismatchPayload.port,
      mismatchPayload.fingerprint,
    );
  });

  it("on Cancel: rethrows the original mismatch and does not forget", async () => {
    const action = vi.fn().mockRejectedValueOnce(mismatchError);
    const promise = withHostKeyMismatchRetry("My Conn", action);
    const pending = await waitForPending(
      () => useConnectionStore.getState().pendingHostKeyMismatch,
    );
    pending.resolve(false);
    await expect(promise).rejects.toBe(mismatchError);
    expect(action).toHaveBeenCalledTimes(1);
    expect(api.forgetKnownHost).not.toHaveBeenCalled();
  });
});
