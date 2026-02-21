import { create } from 'zustand';

export interface SettingsState {
  apiBaseUrl: string;
  darkMode: boolean;
  autoAdvance: boolean;
  defaultCatalog: string;
  defaultSchema: string;
  defaultScope: string;
  sidebarMode: boolean;

  get: <K extends keyof Omit<SettingsState, 'get' | 'set' | 'resetDefaults'>>(key: K) => SettingsState[K];
  set: <K extends keyof Omit<SettingsState, 'get' | 'set' | 'resetDefaults'>>(key: K, value: SettingsState[K]) => void;
  resetDefaults: () => void;
}

const STORAGE_KEY = 'etl-migration-settings';

const DEFAULTS = {
  apiBaseUrl: '/api',
  darkMode: true,
  autoAdvance: true,
  defaultCatalog: '',
  defaultSchema: '',
  defaultScope: '',
  sidebarMode: true,
};

type SettingsData = typeof DEFAULTS;

function loadFromStorage(): Partial<SettingsData> {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw) return JSON.parse(raw) as Partial<SettingsData>;
  } catch (err) {
    console.warn('Settings: failed to load from localStorage', err);
  }
  return {};
}

function saveToStorage(state: SettingsData) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  } catch (err) {
    console.warn('Settings: failed to save to localStorage', err);
  }
}

export const useSettings = create<SettingsState>((setState, getState) => ({
  ...DEFAULTS,
  ...loadFromStorage(),

  get: (key) => getState()[key],
  set: (key, value) => {
    setState({ [key]: value } as Partial<SettingsState>);
    const s = getState();
    saveToStorage({
      apiBaseUrl: s.apiBaseUrl,
      darkMode: s.darkMode,
      autoAdvance: s.autoAdvance,
      defaultCatalog: s.defaultCatalog,
      defaultSchema: s.defaultSchema,
      defaultScope: s.defaultScope,
      sidebarMode: s.sidebarMode,
    });
  },

  resetDefaults: () => {
    setState(DEFAULTS);
    saveToStorage(DEFAULTS);
  },
}));
