import { useCallback, useRef, useEffect } from 'react';
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
  DeepAnalysisResult,
} from '../types/pipeline';
import type { FullROIReport } from '../types/roi';

const STEP_NAMES = [
  'Parse Flow',
  'Analyze',
  'Assess & Map',
  'Convert',
  'Migration Report',
  'Final Report',
  'Validate',
  'Value Analysis',
];

/** Watchdog check interval: 300 seconds */
const WATCHDOG_INTERVAL_MS = 300_000;

/**
 * Hook that manages sequential pipeline execution,
 * updating both pipeline data store and UI step statuses.
 * Includes a watchdog that checks backend liveness every 300s.
 *
 * V6: Uses /analyze/deep for 6-pass analysis and extracts deepAnalysis
 * from generation results.
 */
export function usePipeline() {
  const pipeline = usePipelineStore();
  const ui = useUIStore();
  const getLatest = usePipelineStore.getState;
  const watchdogRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const currentStepRef = useRef<number>(-1);
  const heartbeatFailureCount = useRef<number>(0);

  // Clean up watchdog on unmount
  useEffect(() => {
    return () => {
      if (watchdogRef.current) {
        clearInterval(watchdogRef.current);
      }
    };
  }, []);

  /** Start the watchdog timer for a running step */
  const startWatchdog = useCallback((stepIndex: number) => {
    // Clear any existing watchdog
    if (watchdogRef.current) {
      clearInterval(watchdogRef.current);
      watchdogRef.current = null;
    }
    currentStepRef.current = stepIndex;

    watchdogRef.current = setInterval(async () => {
      const idx = currentStepRef.current;
      if (idx < 0) return; // No step running

      const currentStatus = useUIStore.getState().stepStatuses[idx];
      if (currentStatus !== 'running') {
        // Step finished — stop watchdog
        if (watchdogRef.current) {
          clearInterval(watchdogRef.current);
          watchdogRef.current = null;
        }
        return;
      }

      try {
        const hb = await api.heartbeat();
        heartbeatFailureCount.current = 0;
        if (hb.active) {
          // Backend is still processing — show "taking longer" toast
          useUIStore.getState().addToast({
            message: `Step ${idx + 1} (${STEP_NAMES[idx]}) is taking longer than expected but is still running (${hb.elapsedSeconds}s elapsed, ${hb.processorCount} processors).`,
            type: 'warning',
          });
        } else {
          // Backend says NOT active but frontend thinks step is still running
          useUIStore.getState().addToast({
            message: `Watchdog: backend reports idle but Step ${idx + 1} response pending. The request may still be in transit.`,
            type: 'warning',
          });
        }
      } catch {
        heartbeatFailureCount.current += 1;
        console.warn(`Watchdog: heartbeat failure #${heartbeatFailureCount.current} for step ${idx + 1}`);

        if (heartbeatFailureCount.current >= 5) {
          // Too many consecutive failures — mark step as errored
          useUIStore.getState().setStepStatus(idx, 'error');
          useUIStore.getState().addError({
            message: `Step ${idx + 1} (${STEP_NAMES[idx]}): Backend unreachable after 5 consecutive heartbeat failures.`,
            severity: 'error',
          });
          if (watchdogRef.current) {
            clearInterval(watchdogRef.current);
            watchdogRef.current = null;
          }
        } else {
          useUIStore.getState().addToast({
            message: `Watchdog: cannot reach backend health endpoint. Step ${idx + 1} may have failed.`,
            type: 'warning',
          });
        }
      }
    }, WATCHDOG_INTERVAL_MS);
  }, []);

  const stopWatchdog = useCallback(() => {
    currentStepRef.current = -1;
    if (watchdogRef.current) {
      clearInterval(watchdogRef.current);
      watchdogRef.current = null;
    }
  }, []);

  const runStep = useCallback(
    async <T>(
      stepIndex: number,
      fn: () => Promise<T>,
      onSuccess: (result: T) => void,
    ): Promise<T | null> => {
      ui.setStepStatus(stepIndex, 'running');
      startWatchdog(stepIndex);

      try {
        const result = await fn();
        stopWatchdog();
        onSuccess(result);
        ui.setStepStatus(stepIndex, 'done');
        if (!useUIStore.getState().pipelineRunning && stepIndex < 7) {
          ui.setActiveStep(stepIndex + 1);
        }
        return result;
      } catch (err) {
        stopWatchdog();
        const msg = err instanceof Error ? err.message : String(err);
        ui.setStepStatus(stepIndex, 'error');
        ui.addError({
          message: `Step ${stepIndex + 1} (${STEP_NAMES[stepIndex]}): ${msg}`,
          severity: 'error',
          stack: err instanceof Error ? err.stack : undefined,
        });

        const statusMatch = msg.match(/\b(\d{3})\b/);
        const status = statusMatch ? parseInt(statusMatch[1], 10) : undefined;

        ui.setErrorModal({
          step: stepIndex + 1,
          stepName: STEP_NAMES[stepIndex],
          message: msg,
          status,
          stack: err instanceof Error ? err.stack : undefined,
        });

        return null;
      }
    },
    [ui, startWatchdog, stopWatchdog],
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

  /** Step 2: Analyze — uses V6 deep analysis endpoint */
  const runAnalyze = useCallback(async () => {
    const latest = getLatest();
    if (!latest.parsed) return null;
    return runStep<AnalysisResult>(
      1,
      () => api.deepAnalyzeFlow(getLatest().parsed) as Promise<AnalysisResult>,
      (r) => {
        pipeline.setAnalysis(r);
        // Extract deep analysis from the combined response
        if (r.deepAnalysis) {
          pipeline.setDeepAnalysis(r.deepAnalysis as DeepAnalysisResult);
        }
      },
    );
  }, [pipeline, runStep, getLatest]);

  /** Step 3: Assess */
  const runAssess = useCallback(async () => {
    const latest = getLatest();
    if (!latest.parsed || !latest.analysis) return null;
    return runStep<AssessmentResult>(2, () => {
      const s = getLatest();
      return api.assessFlow(s.parsed, s.analysis) as Promise<AssessmentResult>;
    }, (r) => pipeline.setAssessment(r));
  }, [pipeline, runStep, getLatest]);

  /** Step 4: Convert / Generate Notebook — uses V6 generator */
  const runConvert = useCallback(async () => {
    const latest = getLatest();
    if (!latest.parsed || !latest.assessment) return null;
    return runStep<NotebookResult>(3, () => {
      const s = getLatest();
      return api.generateNotebook({ parsed: s.parsed, assessment: s.assessment }) as Promise<NotebookResult>;
    }, (r) => {
      pipeline.setNotebook(r);
      // V6 generation also returns deep analysis — update store if present
      if (r.deepAnalysis && !getLatest().deepAnalysis) {
        pipeline.setDeepAnalysis(r.deepAnalysis as DeepAnalysisResult);
      }
    });
  }, [pipeline, runStep, getLatest]);

  /** Step 5: Migration Report */
  const runReport = useCallback(async () => {
    const latest = getLatest();
    if (!latest.assessment) return null;
    return runStep<MigrationReport>(4, () => {
      const s = getLatest();
      return api.generateReport({ parsed: s.parsed, assessment: s.assessment, analysis: s.analysis }) as Promise<MigrationReport>;
    }, (r) => pipeline.setReport(r));
  }, [pipeline, runStep, getLatest]);

  /** Step 6: Final Report */
  const runFinalReport = useCallback(async () => {
    return runStep<FinalReport>(5, () => {
      const s = getLatest();
      return api.generateReport({ type: 'final', parsed: s.parsed, analysis: s.analysis, assessment: s.assessment, report: s.report }) as Promise<FinalReport>;
    }, (r) => pipeline.setFinalReport(r));
  }, [pipeline, runStep, getLatest]);

  /** Step 7: Validate */
  const runValidate = useCallback(async () => {
    return runStep<ValidationResult>(6, () => {
      const s = getLatest();
      return api.validateNotebook({ notebook: s.notebook, parsed: s.parsed }) as Promise<ValidationResult>;
    }, (r) => pipeline.setValidation(r));
  }, [pipeline, runStep, getLatest]);

  /** Step 8: Value Analysis */
  const runValueAnalysis = useCallback(async () => {
    return runStep<ValueAnalysis>(7, () => {
      const s = getLatest();
      return api.generateReport({ type: 'value', parsed: s.parsed, analysis: s.analysis, assessment: s.assessment }) as Promise<ValueAnalysis>;
    }, (r) => pipeline.setValueAnalysis(r));
  }, [pipeline, runStep, getLatest]);

  /** ROI Report */
  const runROI = useCallback(async () => {
    if (!pipeline.parsed) return null;
    try {
      const result = await api.generateROIReport({
        parsed: pipeline.parsed,
        analysis: pipeline.analysis,
        assessment: pipeline.assessment,
      });
      pipeline.setROIReport(result);
      ui.addToast({ message: 'ROI report generated', type: 'success' });
      return result;
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      ui.addToast({ message: `ROI report failed: ${msg}`, type: 'error' });
      return null;
    }
  }, [pipeline, ui]);

  /** Run all steps sequentially — stays on progress view until done */
  const runAll = useCallback(
    async (file: File) => {
      ui.setPipelineRunning(true);
      const parsed = await runParse(file);
      if (!parsed) { ui.setPipelineRunning(false); return; }
      const analysis = await runAnalyze();
      if (!analysis) { ui.setPipelineRunning(false); return; }
      const assessment = await runAssess();
      if (!assessment) { ui.setPipelineRunning(false); return; }
      await runConvert();
      await runReport();
      await runFinalReport();
      await runValidate();
      await runValueAnalysis();
      ui.setPipelineRunning(false);
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
    runROI,
    runAll,
  };
}
