import { create } from "zustand";
import { api, ConnectionConfig } from "../lib/tauri";

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

interface ConnectionState {
  connections: ConnectionConfig[];
  activeConnections: Set<string>;
  loading: boolean;
  error: string | null;
  pendingPassphrasePrompt: PendingPassphrasePrompt | null;

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
      // If the user opted to be prompted for the SSH key passphrase rather
      // than persisting it, surface a one-shot modal request. The stored
      // `ssh_password` is empty in this case; we inject a transient one
      // for this connect call only and clear the prompt flag so the
      // backend takes the supplied passphrase.
      let effectiveConfig = config;
      if (
        config.ssh_enabled &&
        config.ssh_auth_method === "identityfile" &&
        config.ssh_prompt_passphrase
      ) {
        const passphrase = await new Promise<string>((resolve, reject) => {
          set({
            pendingPassphrasePrompt: {
              connectionName: config.name,
              keyPath: config.ssh_key_path ?? "",
              resolve: (p) => {
                set({ pendingPassphrasePrompt: null });
                resolve(p);
              },
              reject: (err) => {
                set({ pendingPassphrasePrompt: null });
                reject(err);
              },
            },
          });
        });
        effectiveConfig = {
          ...config,
          ssh_password: passphrase,
          ssh_prompt_passphrase: false,
        };
      }

      await api.connect(effectiveConfig);
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
