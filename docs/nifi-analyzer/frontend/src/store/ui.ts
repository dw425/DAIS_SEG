import { create } from 'zustand';

export type StepStatus = 'idle' | 'running' | 'done' | 'error';

export interface ErrorEntry {
  id: string;
  message: string;
  severity: 'info' | 'warning' | 'error';
  timestamp: number;
  stack?: string;
}

export interface ErrorModalData {
  step: number;
  stepName: string;
  message: string;
  status?: number;
  stack?: string;
}

export interface UIState {
  activeStep: number;             // 0-9: steps 1-8 + summary + admin
  stepStatuses: StepStatus[];
  sidebarMode: boolean;           // true = sidebar, false = tabs
  errors: ErrorEntry[];
  progress: number;               // 0-100
  toasts: ToastEntry[];
  searchOpen: boolean;
  settingsOpen: boolean;
  helpOpen: boolean;
  pipelineRunning: boolean;       // true while runAll is executing
  errorModal: ErrorModalData | null;  // non-null = show error modal

  // Actions
  setActiveStep: (step: number) => void;
  setStepStatus: (step: number, status: StepStatus) => void;
  setSidebarMode: (mode: boolean) => void;
  addError: (error: Omit<ErrorEntry, 'id' | 'timestamp'>) => void;
  clearErrors: () => void;
  removeError: (id: string) => void;
  setProgress: (progress: number) => void;
  addToast: (toast: Omit<ToastEntry, 'id' | 'timestamp'>) => void;
  removeToast: (id: string) => void;
  setSearchOpen: (open: boolean) => void;
  setSettingsOpen: (open: boolean) => void;
  setHelpOpen: (open: boolean) => void;
  setPipelineRunning: (running: boolean) => void;
  setErrorModal: (data: ErrorModalData | null) => void;
  resetUI: () => void;
}

export interface ToastEntry {
  id: string;
  message: string;
  type: 'success' | 'error' | 'info' | 'warning';
  timestamp: number;
}

let nextId = 1;
const uid = () => String(nextId++);

const INITIAL_STATUSES: StepStatus[] = Array(10).fill('idle');

export const useUIStore = create<UIState>((set) => ({
  activeStep: 0,
  stepStatuses: [...INITIAL_STATUSES],
  sidebarMode: true,
  errors: [],
  progress: 0,
  toasts: [],
  searchOpen: false,
  settingsOpen: false,
  helpOpen: false,
  pipelineRunning: false,
  errorModal: null,

  setActiveStep: (step) => set({ activeStep: step }),

  setStepStatus: (step, status) =>
    set((state) => {
      const statuses = [...state.stepStatuses];
      statuses[step] = status;
      // Compute progress: each 'done' step = 12.5%, partial credit for running
      const doneCount = statuses.filter((s) => s === 'done').length;
      const runningCount = statuses.filter((s) => s === 'running').length;
      const progress = Math.min(100, Math.round(((doneCount + runningCount * 0.5) / 8) * 100));
      return { stepStatuses: statuses, progress };
    }),

  setSidebarMode: (mode) => set({ sidebarMode: mode }),

  addError: (error) =>
    set((state) => ({
      errors: [...state.errors, { ...error, id: uid(), timestamp: Date.now() }],
    })),

  clearErrors: () => set({ errors: [] }),

  removeError: (id) =>
    set((state) => ({
      errors: state.errors.filter((e) => e.id !== id),
    })),

  setProgress: (progress) => set({ progress }),

  addToast: (toast) =>
    set((state) => ({
      toasts: [...state.toasts, { ...toast, id: uid(), timestamp: Date.now() }],
    })),

  removeToast: (id) =>
    set((state) => ({
      toasts: state.toasts.filter((t) => t.id !== id),
    })),

  setSearchOpen: (open) => set({ searchOpen: open }),
  setSettingsOpen: (open) => set({ settingsOpen: open }),
  setHelpOpen: (open) => set({ helpOpen: open }),
  setPipelineRunning: (running) => set({ pipelineRunning: running }),
  setErrorModal: (data) => set({ errorModal: data }),

  resetUI: () =>
    set({
      activeStep: 0,
      stepStatuses: [...INITIAL_STATUSES],
      errors: [],
      progress: 0,
      toasts: [],
      searchOpen: false,
      settingsOpen: false,
      helpOpen: false,
      pipelineRunning: false,
      errorModal: null,
    }),
}));
