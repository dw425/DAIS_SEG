import { useCallback } from 'react';
import { usePipelineStore } from '../store/pipeline';
import { useUIStore } from '../store/ui';
import * as api from '../api/client';
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

/**
 * Hook that manages sequential pipeline execution,
 * updating both pipeline data store and UI step statuses.
 */
export function usePipeline() {
  const pipeline = usePipelineStore();
  const ui = useUIStore();

  const runStep = useCallback(
    async <T>(
      stepIndex: number,
      fn: () => Promise<T>,
      onSuccess: (result: T) => void,
    ): Promise<T | null> => {
      ui.setStepStatus(stepIndex, 'running');
      ui.setActiveStep(stepIndex);
      try {
        const result = await fn();
        onSuccess(result);
        ui.setStepStatus(stepIndex, 'done');
        ui.addToast({ message: `Step ${stepIndex + 1} completed`, type: 'success' });
        return result;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        ui.setStepStatus(stepIndex, 'error');
        ui.addError({ message: msg, severity: 'error', stack: err instanceof Error ? err.stack : undefined });
        ui.addToast({ message: `Step ${stepIndex + 1} failed: ${msg}`, type: 'error' });
        return null;
      }
    },
    [ui],
  );

  /** Step 1: Parse */
  const runParse = useCallback(
    async (file: File) => {
      pipeline.setFile(file.name, file.size);
      return runStep<ParseResult>(0, () => api.parseFlow(file) as Promise<ParseResult>, (r) => {
        pipeline.setParsed(r);
        if (r.platform) pipeline.setPlatform(r.platform);
      });
    },
    [pipeline, runStep],
  );

  /** Step 2: Analyze */
  const runAnalyze = useCallback(async () => {
    if (!pipeline.parsed) return null;
    return runStep<AnalysisResult>(1, () => api.analyzeFlow(pipeline.parsed) as Promise<AnalysisResult>, (r) => pipeline.setAnalysis(r));
  }, [pipeline, runStep]);

  /** Step 3: Assess */
  const runAssess = useCallback(async () => {
    if (!pipeline.parsed || !pipeline.analysis) return null;
    return runStep<AssessmentResult>(2, () => api.assessFlow(pipeline.parsed, pipeline.analysis) as Promise<AssessmentResult>, (r) => pipeline.setAssessment(r));
  }, [pipeline, runStep]);

  /** Step 4: Convert / Generate Notebook */
  const runConvert = useCallback(async () => {
    if (!pipeline.parsed || !pipeline.assessment) return null;
    return runStep<NotebookResult>(3, () => api.generateNotebook({ parsed: pipeline.parsed, assessment: pipeline.assessment }) as Promise<NotebookResult>, (r) => pipeline.setNotebook(r));
  }, [pipeline, runStep]);

  /** Step 5: Migration Report */
  const runReport = useCallback(async () => {
    if (!pipeline.assessment) return null;
    return runStep<MigrationReport>(4, () => api.generateReport({ assessment: pipeline.assessment, analysis: pipeline.analysis }) as Promise<MigrationReport>, (r) => pipeline.setReport(r));
  }, [pipeline, runStep]);

  /** Step 6: Final Report */
  const runFinalReport = useCallback(async () => {
    return runStep<FinalReport>(5, () => api.generateReport({ type: 'final', parsed: pipeline.parsed, analysis: pipeline.analysis, assessment: pipeline.assessment, report: pipeline.report }) as Promise<FinalReport>, (r) => pipeline.setFinalReport(r));
  }, [pipeline, runStep]);

  /** Step 7: Validate */
  const runValidate = useCallback(async () => {
    return runStep<ValidationResult>(6, () => api.validateNotebook({ notebook: pipeline.notebook, parsed: pipeline.parsed }) as Promise<ValidationResult>, (r) => pipeline.setValidation(r));
  }, [pipeline, runStep]);

  /** Step 8: Value Analysis */
  const runValueAnalysis = useCallback(async () => {
    return runStep<ValueAnalysis>(7, () => api.generateReport({ type: 'value', parsed: pipeline.parsed, analysis: pipeline.analysis, assessment: pipeline.assessment }) as Promise<ValueAnalysis>, (r) => pipeline.setValueAnalysis(r));
  }, [pipeline, runStep]);

  /** Run all steps sequentially */
  const runAll = useCallback(
    async (file: File) => {
      const parsed = await runParse(file);
      if (!parsed) return;
      const analysis = await runAnalyze();
      if (!analysis) return;
      const assessment = await runAssess();
      if (!assessment) return;
      await runConvert();
      await runReport();
      await runFinalReport();
      await runValidate();
      await runValueAnalysis();
      ui.setActiveStep(8); // Summary
      ui.addToast({ message: 'Pipeline complete!', type: 'success' });
    },
    [runParse, runAnalyze, runAssess, runConvert, runReport, runFinalReport, runValidate, runValueAnalysis, ui],
  );

  return {
    runParse,
    runAnalyze,
    runAssess,
    runConvert,
    runReport,
    runFinalReport,
    runValidate,
    runValueAnalysis,
    runAll,
  };
}
