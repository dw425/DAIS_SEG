import { describe, it, expect, beforeEach } from 'vitest';
import { usePipelineStore } from '../../store/pipeline';

describe('PipelineStore', () => {
  beforeEach(() => {
    usePipelineStore.getState().resetAll();
  });

  it('should start with null state', () => {
    const state = usePipelineStore.getState();
    expect(state.parsed).toBeNull();
    expect(state.analysis).toBeNull();
    expect(state.assessment).toBeNull();
    expect(state.notebook).toBeNull();
    expect(state.report).toBeNull();
    expect(state.platform).toBeNull();
    expect(state.fileName).toBeNull();
    expect(state.fileSize).toBe(0);
  });

  it('should set and clear platform', () => {
    usePipelineStore.getState().setPlatform('nifi');
    expect(usePipelineStore.getState().platform).toBe('nifi');

    usePipelineStore.getState().setPlatform(null);
    expect(usePipelineStore.getState().platform).toBeNull();
  });

  it('should set parsed result', () => {
    const parsed = {
      platform: 'nifi',
      version: '1.0',
      processors: [],
      connections: [],
      processGroups: [],
      controllerServices: [],
      metadata: {},
      warnings: [],
      parameterContexts: [],
    };
    usePipelineStore.getState().setParsed(parsed);
    expect(usePipelineStore.getState().parsed).toEqual(parsed);
  });

  it('should set file info', () => {
    usePipelineStore.getState().setFile('test.json', 1024);
    expect(usePipelineStore.getState().fileName).toBe('test.json');
    expect(usePipelineStore.getState().fileSize).toBe(1024);
  });

  it('should set analysis result', () => {
    const analysis = {
      dependencyGraph: {},
      externalSystems: [],
      cycles: [],
      cycleClassifications: [],
      taskClusters: [],
      backpressureConfigs: [],
      flowMetrics: {},
      securityFindings: [],
      stages: [],
    };
    usePipelineStore.getState().setAnalysis(analysis);
    expect(usePipelineStore.getState().analysis).toEqual(analysis);
  });

  it('should set assessment result', () => {
    const assessment = {
      mappings: [],
      packages: [],
      unmappedCount: 0,
    };
    usePipelineStore.getState().setAssessment(assessment);
    expect(usePipelineStore.getState().assessment).toEqual(assessment);
  });

  it('should set notebook result', () => {
    const notebook = { cells: [], workflow: {} };
    usePipelineStore.getState().setNotebook(notebook);
    expect(usePipelineStore.getState().notebook).toEqual(notebook);
  });

  it('should set validation result', () => {
    const validation = {
      overallScore: 85,
      scores: [],
      gaps: [],
      errors: [],
    };
    usePipelineStore.getState().setValidation(validation);
    expect(usePipelineStore.getState().validation).toEqual(validation);
  });

  it('should reset all state', () => {
    usePipelineStore.getState().setPlatform('nifi');
    usePipelineStore.getState().setFile('test.json', 1024);
    usePipelineStore.getState().resetAll();

    const state = usePipelineStore.getState();
    expect(state.platform).toBeNull();
    expect(state.fileName).toBeNull();
    expect(state.fileSize).toBe(0);
    expect(state.parsed).toBeNull();
  });
});
