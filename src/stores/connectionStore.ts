import { create } from "zustand";
import {
  api,
  ConnectionConfig,
  parseSshHostKeyMismatch,
  type SshHostKeyMismatchInfo,
} from "../lib/tauri";

/**
 * In-flight request for the user to enter an SSH key passphrase. The
 * `PassphrasePrompt` component subscribes to this and resolves/rejects
 * the connect call. The passphrase is never persisted.
 */
export interface PendingPassphrasePrompt {
  /** Display name shown in the prompt UI. */
  connectionName: string;
  /** Identity-file path shown in the prompt UI. */
  keyPath: string;
  /** Caller resolves with the entered passphrase (empty string is allowed). */
  resolve: (passphrase: string) => void;
  /** Caller rejects to cancel the connect attempt. */
  reject: (reason: Error) => void;
}

/**
 * In-flight host-key-mismatch confirmation. The `HostKeyMismatchPrompt`
 * subscribes to this; "Forget & retry" calls `resolve(true)` so the
 * caller forgets the recorded key and attempts the connect again, while
 * Cancel calls `resolve(false)` to give up.
 */
export interface PendingHostKeyMismatchPrompt {
  connectionName: string;
  info: SshHostKeyMismatchInfo;
  resolve: (forgetAndRetry: boolean) => void;
}

interface ConnectionState {
  connections: ConnectionConfig[];
  activeConnections: Set<string>;
  loading: boolean;
  error: string | null;
  pendingPassphrasePrompt: PendingPassphrasePrompt | null;
  pendingHostKeyMismatch: PendingHostKeyMismatchPrompt | null;

  loadConnections: () => Promise<void>;
  addConnection: (config: ConnectionConfig) => Promise<void>;
  updateConnection: (config: ConnectionConfig) => Promise<void>;
  removeConnection: (id: string) => Promise<void>;
  connectTo: (config: ConnectionConfig) => Promise<void>;
  disconnectFrom: (id: string) => Promise<void>;
  isConnected: (id: string) => boolean;
}

export const useConnectionStore = create<ConnectionState>((set, get) => ({
  connections: [],
  activeConnections: new Set(),
  loading: false,
  error: null,
  pendingPassphrasePrompt: null,
  pendingHostKeyMismatch: null,

  loadConnections: async () => {
    set({ loading: true, error: null });
    try {
      const connections = await api.loadConnections();
      set({ connections, loading: false });
    } catch (e) {
      set({ error: String(e), loading: false });
    }
  },

  addConnection: async (config) => {
    try {
      await api.saveConnection(config);
      set((s) => ({ connections: [...s.connections, config] }));
    } catch (e) {
      set({ error: String(e) });
    }
  },

  updateConnection: async (config) => {
    try {
      await api.saveConnection(config);
      set((s) => ({
        connections: s.connections.map((c) => (c.id === config.id ? config : c)),
      }));
    } catch (e) {
      set({ error: String(e) });
    }
  },

  removeConnection: async (id) => {
    try {
      await api.deleteConnection(id);
      set((s) => ({
        connections: s.connections.filter((c) => c.id !== id),
      }));
    } catch (e) {
      set({ error: String(e) });
    }
  },

  connectTo: async (config) => {
    try {
      const effectiveConfig = await resolveSshPassphrase(config);
      await withHostKeyMismatchRetry(config.name, () =>
        api.connect(effectiveConfig),
      );

      set((s) => {
        const next = new Set(s.activeConnections);
        next.add(config.id);
        return { activeConnections: next };
      });
    } catch (e) {
      set({ error: String(e) });
      throw e;
    }
  },

  disconnectFrom: async (id) => {
    try {
      await api.disconnect(id);
      set((s) => {
        const next = new Set(s.activeConnections);
        next.delete(id);
        return { activeConnections: next };
      });
    } catch (e) {
      set({ error: String(e) });
    }
  },

  isConnected: (id) => get().activeConnections.has(id),
}));

/**
 * Surface the passphrase prompt for SSH identity-file connections that
 * opted out of persisting the passphrase, and return a config with the
 * just-entered passphrase injected into `ssh_password` (and the prompt
 * flag cleared so the backend uses the supplied value verbatim).
 *
 * For every other case the original config is returned unchanged.
 *
 * Exported so any caller that initiates an SSH connect — currently
 * `connectTo` and the dialog's "Test Connection" button — can share
 * the same UX without re-implementing the modal lifecycle.
 */
export async function resolveSshPassphrase(
  config: ConnectionConfig,
): Promise<ConnectionConfig> {
  if (
    !config.ssh_enabled ||
    config.ssh_auth_method !== "identityfile" ||
    !config.ssh_prompt_passphrase
  ) {
    return config;
  }
  const passphrase = await new Promise<string>((resolve, reject) => {
    useConnectionStore.setState({
      pendingPassphrasePrompt: {
        connectionName: config.name,
        keyPath: config.ssh_key_path ?? "",
        resolve: (p) => {
          useConnectionStore.setState({ pendingPassphrasePrompt: null });
          resolve(p);
        },
        reject: (err) => {
          useConnectionStore.setState({ pendingPassphrasePrompt: null });
          reject(err);
        },
      },
    });
  });
  return {
    ...config,
    ssh_password: passphrase,
    ssh_prompt_passphrase: false,
  };
}

/**
 * Run `action`; if it fails with a structured `ssh_host_key_mismatch`
 * error, surface the prompt and — when the user confirms — forget the
 * recorded key and call `action` exactly once more. Any non-mismatch
 * error or a Cancel decision propagates the original error.
 *
 * Use this around any backend call that can open an SSH session
 * (`api.connect`, `api.testConnection`, `api.backupDatabase`,
 * `api.restoreDatabase`, `api.dumpAndRestore`) so users get a
 * consistent Forget & Retry experience instead of a raw
 * `ssh_host_key_mismatch:{...}` string.
 *
 * `connectionName` is shown to the user in the modal — keep it stable
 * (no IDs) so the message is meaningful.
 */
export async function withHostKeyMismatchRetry<T>(
  connectionName: string,
  action: () => Promise<T>,
): Promise<T> {
  try {
    return await action();
  } catch (err) {
    const mismatch = parseSshHostKeyMismatch(err);
    if (!mismatch) throw err;
    const forgetAndRetry = await new Promise<boolean>((resolve) => {
      useConnectionStore.setState({
        pendingHostKeyMismatch: {
          connectionName,
          info: mismatch,
          resolve: (decision) => {
            useConnectionStore.setState({ pendingHostKeyMismatch: null });
            resolve(decision);
          },
        },
      });
    });
    if (!forgetAndRetry) throw err;
    await api.forgetKnownHost(
      mismatch.host,
      mismatch.port,
      mismatch.fingerprint,
    );
    return action();
  }
}
