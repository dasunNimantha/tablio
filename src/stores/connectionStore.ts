import { create } from "zustand";
import { api, ConnectionConfig } from "../lib/tauri";

interface ConnectionState {
  connections: ConnectionConfig[];
  activeConnections: Set<string>;
  loading: boolean;
  error: string | null;

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
      await api.connect(config);
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
