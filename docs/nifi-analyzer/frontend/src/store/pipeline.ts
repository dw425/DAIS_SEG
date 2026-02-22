import { create } from 'zustand';
import type {
  ParseResult,
  AnalysisResult,
  AssessmentResult,
  NotebookResult,
  MigrationReport,
  FinalReport,
  ValidationResult,
  ValueAnalysis,
  DeepAnalysisResult,
  LineageGraph,
} from '../types/pipeline';
import type { FullROIReport } from '../types/roi';
import { useUIStore } from './ui';

export interface PipelineState {
  platform: string | null;
  parsed: ParseResult | null;
  analysis: AnalysisResult | null;
  deepAnalysis: DeepAnalysisResult | null;
  assessment: AssessmentResult | null;
  notebook: NotebookResult | null;
  report: MigrationReport | null;
  finalReport: FinalReport | null;
  validation: ValidationResult | null;
  valueAnalysis: ValueAnalysis | null;
  roiReport: FullROIReport | null;
  lineage: LineageGraph | null;
  fileName: string | null;
  fileSize: number;

  // Actions
  setPlatform: (platform: string | null) => void;
  setParsed: (parsed: ParseResult | null) => void;
  setAnalysis: (analysis: AnalysisResult | null) => void;
  setDeepAnalysis: (deepAnalysis: DeepAnalysisResult | null) => void;
  setAssessment: (assessment: AssessmentResult | null) => void;
  setNotebook: (notebook: NotebookResult | null) => void;
  setReport: (report: MigrationReport | null) => void;
  setFinalReport: (finalReport: FinalReport | null) => void;
  setValidation: (validation: ValidationResult | null) => void;
  setValueAnalysis: (valueAnalysis: ValueAnalysis | null) => void;
  setROIReport: (roiReport: FullROIReport | null) => void;
  setLineage: (lineage: LineageGraph | null) => void;
  setFile: (name: string, size: number) => void;
  resetAll: () => void;
}

const initialState = {
  platform: null,
  parsed: null,
  analysis: null,
  deepAnalysis: null,
  assessment: null,
  notebook: null,
  report: null,
  finalReport: null,
  validation: null,
  valueAnalysis: null,
  roiReport: null,
  lineage: null,
  fileName: null,
  fileSize: 0,
};

export const usePipelineStore = create<PipelineState>((set) => ({
  ...initialState,

  setPlatform: (platform) => set({ platform }),
  setParsed: (parsed) => set({ parsed }),
  setAnalysis: (analysis) => set({ analysis }),
  setDeepAnalysis: (deepAnalysis) => set({ deepAnalysis }),
  setAssessment: (assessment) => set({ assessment }),
  setNotebook: (notebook) => set({ notebook }),
  setReport: (report) => set({ report }),
  setFinalReport: (finalReport) => set({ finalReport }),
  setValidation: (validation) => set({ validation }),
  setValueAnalysis: (valueAnalysis) => set({ valueAnalysis }),
  setROIReport: (roiReport) => set({ roiReport }),
  setLineage: (lineage) => set({ lineage }),
  setFile: (name, size) => set({ fileName: name, fileSize: size }),
  resetAll: () => {
    set(initialState);
    // Cascade reset: also clear UI step statuses and progress so stale
    // "done" badges and progress bar don't persist after a full reset.
    useUIStore.getState().resetUI();
  },
}));
