import { useEffect, useRef } from 'react';
import { usePipelineStore, type PipelineState } from '../store/pipeline';

const STORAGE_KEY = 'etl-migration-session';
const SESSION_VERSION = '1.0.0';

interface SessionData {
  version: string;
  timestamp: number;
  state: Partial<PipelineState>;
}

const PERSISTED_KEYS: (keyof PipelineState)[] = [
  'parsed', 'analysis', 'assessment', 'notebook', 'report',
  'finalReport', 'validation', 'valueAnalysis', 'fileName', 'fileSize', 'platform',
];

function serialize(state: PipelineState): SessionData {
  const partial: Record<string, unknown> = {};
  for (const key of PERSISTED_KEYS) {
    partial[key] = state[key];
  }
  return {
    version: SESSION_VERSION,
    timestamp: Date.now(),
    state: partial as Partial<PipelineState>,
  };
}

export function clearSession() {
  try {
    localStorage.removeItem(STORAGE_KEY);
  } catch (err) {
    console.warn('Session persistence: failed to clear localStorage', err);
  }
}

/**
 * Persists pipeline state to localStorage with 500ms debounce.
 * On mount, hydrates from saved session if version matches.
 */
export function useSessionPersistence() {
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const hydratedRef = useRef(false);

  // Hydrate on mount
  useEffect(() => {
    if (hydratedRef.current) return;
    hydratedRef.current = true;

    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (!raw) return;
      const data: SessionData = JSON.parse(raw);
      if (data.version !== SESSION_VERSION) {
        clearSession();
        return;
      }

      const store = usePipelineStore.getState();
      const s = data.state;
      if (s.parsed) store.setParsed(s.parsed);
      if (s.analysis) store.setAnalysis(s.analysis);
      if (s.assessment) store.setAssessment(s.assessment);
      if (s.notebook) store.setNotebook(s.notebook);
      if (s.report) store.setReport(s.report);
      if (s.finalReport) store.setFinalReport(s.finalReport);
      if (s.validation) store.setValidation(s.validation);
      if (s.valueAnalysis) store.setValueAnalysis(s.valueAnalysis);
      if (s.fileName && s.fileSize) store.setFile(s.fileName, s.fileSize);
      if (s.platform) store.setPlatform(s.platform);
    } catch (err) {
      console.warn('Session persistence: failed to hydrate from localStorage', err);
      clearSession();
    }
  }, []);

  // Subscribe to store changes and debounce-persist
  useEffect(() => {
    const unsub = usePipelineStore.subscribe((state) => {
      if (timerRef.current) clearTimeout(timerRef.current);
      timerRef.current = setTimeout(() => {
        try {
          localStorage.setItem(STORAGE_KEY, JSON.stringify(serialize(state)));
        } catch (err) {
          console.warn('Session persistence: failed to save to localStorage', err);
        }
      }, 500);
    });

    return () => {
      unsub();
      if (timerRef.current) clearTimeout(timerRef.current);
    };
  }, []);
}
