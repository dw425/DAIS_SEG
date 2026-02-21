import { create } from 'zustand';

export interface AuthUser {
  id: string;
  email: string;
  name: string;
  role: string;
  is_active: boolean;
  created_at: string | null;
}

export interface AuthState {
  user: AuthUser | null;
  token: string | null;
  refreshToken: string | null;
  isLoading: boolean;
  error: string | null;

  // Actions
  login: (email: string, password: string) => Promise<void>;
  register: (email: string, name: string, password: string) => Promise<void>;
  logout: () => void;
  refreshAuth: () => Promise<void>;
  setFromStorage: () => void;
  clearError: () => void;
}

const API_BASE = '/api';

export const useAuthStore = create<AuthState>((set, get) => ({
  user: null,
  token: null,
  refreshToken: null,
  isLoading: false,
  error: null,

  login: async (email: string, password: string) => {
    set({ isLoading: true, error: null });
    try {
      const res = await fetch(`${API_BASE}/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password }),
      });
      if (!res.ok) {
        const body = await res.json().catch(() => ({ detail: 'Login failed' }));
        throw new Error(body.detail || `Login failed (${res.status})`);
      }
      const data = await res.json();
      set({
        user: data.user,
        token: data.access_token,
        refreshToken: data.refresh_token,
        isLoading: false,
      });
      localStorage.setItem('etl_auth_token', data.access_token);
      localStorage.setItem('etl_refresh_token', data.refresh_token);
      localStorage.setItem('etl_auth_user', JSON.stringify(data.user));
    } catch (err) {
      set({ isLoading: false, error: (err as Error).message });
      throw err;
    }
  },

  register: async (email: string, name: string, password: string) => {
    set({ isLoading: true, error: null });
    try {
      const res = await fetch(`${API_BASE}/auth/register`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, name, password }),
      });
      if (!res.ok) {
        const body = await res.json().catch(() => ({ detail: 'Registration failed' }));
        throw new Error(body.detail || `Registration failed (${res.status})`);
      }
      const data = await res.json();
      set({
        user: data.user,
        token: data.access_token,
        refreshToken: data.refresh_token,
        isLoading: false,
      });
      localStorage.setItem('etl_auth_token', data.access_token);
      localStorage.setItem('etl_refresh_token', data.refresh_token);
      localStorage.setItem('etl_auth_user', JSON.stringify(data.user));
    } catch (err) {
      set({ isLoading: false, error: (err as Error).message });
      throw err;
    }
  },

  logout: () => {
    set({ user: null, token: null, refreshToken: null, error: null });
    localStorage.removeItem('etl_auth_token');
    localStorage.removeItem('etl_refresh_token');
    localStorage.removeItem('etl_auth_user');
  },

  refreshAuth: async () => {
    const { refreshToken } = get();
    if (!refreshToken) return;

    try {
      const res = await fetch(`${API_BASE}/auth/refresh`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ refresh_token: refreshToken }),
      });
      if (!res.ok) {
        get().logout();
        return;
      }
      const data = await res.json();
      set({
        user: data.user,
        token: data.access_token,
        refreshToken: data.refresh_token,
      });
      localStorage.setItem('etl_auth_token', data.access_token);
      localStorage.setItem('etl_refresh_token', data.refresh_token);
      localStorage.setItem('etl_auth_user', JSON.stringify(data.user));
    } catch {
      console.warn('Token refresh failed, clearing session');
      set({ token: null, user: null, refreshToken: null });
      localStorage.removeItem('etl_auth_token');
      localStorage.removeItem('etl_refresh_token');
      localStorage.removeItem('etl_auth_user');
    }
  },

  setFromStorage: () => {
    const token = localStorage.getItem('etl_auth_token');
    const refreshToken = localStorage.getItem('etl_refresh_token');
    const userStr = localStorage.getItem('etl_auth_user');
    if (token && userStr) {
      try {
        const user = JSON.parse(userStr);
        set({ user, token, refreshToken });
      } catch {
        // Invalid stored data, clear it
        localStorage.removeItem('etl_auth_token');
        localStorage.removeItem('etl_refresh_token');
        localStorage.removeItem('etl_auth_user');
      }
    }
  },

  clearError: () => set({ error: null }),
}));
