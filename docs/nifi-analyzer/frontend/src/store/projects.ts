import { create } from 'zustand';
import { useAuthStore } from './auth';

export interface Project {
  id: string;
  name: string;
  description: string;
  platform: string;
  owner_id: string;
  created_at: string | null;
  updated_at: string | null;
  run_count: number;
}

export interface ProjectsState {
  projects: Project[];
  isLoading: boolean;
  error: string | null;

  fetchProjects: () => Promise<void>;
  createProject: (name: string, description: string, platform: string) => Promise<Project>;
  updateProject: (id: string, data: Partial<Pick<Project, 'name' | 'description' | 'platform'>>) => Promise<void>;
  deleteProject: (id: string) => Promise<void>;
  clearError: () => void;
}

const API_BASE = '/api';

function authHeaders(): Record<string, string> {
  const token = useAuthStore.getState().token;
  return token ? { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' } : { 'Content-Type': 'application/json' };
}

export const useProjectsStore = create<ProjectsState>((set) => ({
  projects: [],
  isLoading: false,
  error: null,

  fetchProjects: async () => {
    set({ isLoading: true, error: null });
    try {
      const res = await fetch(`${API_BASE}/projects`, { headers: authHeaders() });
      if (!res.ok) throw new Error(`Failed to fetch projects (${res.status})`);
      const data = await res.json();
      set({ projects: data.projects, isLoading: false });
    } catch (err) {
      set({ isLoading: false, error: (err as Error).message });
    }
  },

  createProject: async (name: string, description: string, platform: string) => {
    set({ isLoading: true, error: null });
    try {
      const res = await fetch(`${API_BASE}/projects`, {
        method: 'POST',
        headers: authHeaders(),
        body: JSON.stringify({ name, description, platform }),
      });
      if (!res.ok) {
        const body = await res.json().catch(() => ({ detail: 'Create failed' }));
        throw new Error(body.detail || `Create failed (${res.status})`);
      }
      const data = await res.json();
      set((state) => ({
        projects: [data.project, ...state.projects],
        isLoading: false,
      }));
      return data.project;
    } catch (err) {
      set({ isLoading: false, error: (err as Error).message });
      throw err;
    }
  },

  updateProject: async (id: string, data: Partial<Pick<Project, 'name' | 'description' | 'platform'>>) => {
    set({ isLoading: true, error: null });
    try {
      const res = await fetch(`${API_BASE}/projects/${id}`, {
        method: 'PUT',
        headers: authHeaders(),
        body: JSON.stringify(data),
      });
      if (!res.ok) throw new Error(`Update failed (${res.status})`);
      const result = await res.json();
      set((state) => ({
        projects: state.projects.map((p) => (p.id === id ? result.project : p)),
        isLoading: false,
      }));
    } catch (err) {
      set({ isLoading: false, error: (err as Error).message });
    }
  },

  deleteProject: async (id: string) => {
    set({ isLoading: true, error: null });
    try {
      const res = await fetch(`${API_BASE}/projects/${id}`, {
        method: 'DELETE',
        headers: authHeaders(),
      });
      if (!res.ok) throw new Error(`Delete failed (${res.status})`);
      set((state) => ({
        projects: state.projects.filter((p) => p.id !== id),
        isLoading: false,
      }));
    } catch (err) {
      set({ isLoading: false, error: (err as Error).message });
    }
  },

  clearError: () => set({ error: null }),
}));
