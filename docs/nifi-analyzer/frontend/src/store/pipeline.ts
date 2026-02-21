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
} from '../types/pipeline';

export interface PipelineState {
  platform: string | null;
  parsed: ParseResult | null;
  analysis: AnalysisResult | null;
  assessment: AssessmentResult | null;
  notebook: NotebookResult | null;
  report: MigrationReport | null;
  finalReport: FinalReport | null;
  validation: ValidationResult | null;
  valueAnalysis: ValueAnalysis | null;
  fileName: string | null;
  fileSize: number;

  // Actions
  setPlatform: (platform: string | null) => void;
  setParsed: (parsed: ParseResult | null) => void;
  setAnalysis: (analysis: AnalysisResult | null) => void;
  setAssessment: (assessment: AssessmentResult | null) => void;
  setNotebook: (notebook: NotebookResult | null) => void;
  setReport: (report: MigrationReport | null) => void;
  setFinalReport: (finalReport: FinalReport | null) => void;
  setValidation: (validation: ValidationResult | null) => void;
  setValueAnalysis: (valueAnalysis: ValueAnalysis | null) => void;
  setFile: (name: string, size: number) => void;
  resetAll: () => void;
}

const initialState = {
  platform: null,
  parsed: null,
  analysis: null,
  assessment: null,
  notebook: null,
  report: null,
  finalReport: null,
  validation: null,
  valueAnalysis: null,
  fileName: null,
  fileSize: 0,
};

export const usePipelineStore = create<PipelineState>((set) => ({
  ...initialState,

  setPlatform: (platform) => set({ platform }),
  setParsed: (parsed) => set({ parsed }),
  setAnalysis: (analysis) => set({ analysis }),
  setAssessment: (assessment) => set({ assessment }),
  setNotebook: (notebook) => set({ notebook }),
  setReport: (report) => set({ report }),
  setFinalReport: (finalReport) => set({ finalReport }),
  setValidation: (validation) => set({ validation }),
  setValueAnalysis: (valueAnalysis) => set({ valueAnalysis }),
  setFile: (name, size) => set({ fileName: name, fileSize: size }),
  resetAll: () => set(initialState),
}));
