// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';

// The notebook generator has many deep dependencies, so we mock them
vi.mock('../src/generators/lineage-builder.js', () => ({
  buildDataFrameLineage: vi.fn(() => ({})),
}));
vi.mock('../src/generators/cell-builders/imports-cell.js', () => ({
  collectSmartImports: vi.fn(() => ({ code: 'from pyspark.sql import *' })),
}));
vi.mock('../src/generators/topo-sort.js', () => ({
  topologicalSortMappings: vi.fn((mappings) => mappings),
}));
vi.mock('../src/generators/full-properties.js', () => ({
  extractFullProperties: vi.fn(() => ({})),
}));
vi.mock('../src/generators/cell-builders/header-cell.js', () => ({
  buildHeaderCell: vi.fn((opts) => ({ type: 'md', label: 'Header', source: `# ${opts.flowName}`, role: 'config' })),
}));
vi.mock('../src/generators/cell-builders/config-cell.js', () => ({
  buildConfigCell: vi.fn(() => ({ type: 'code', label: 'Config', source: '# config', role: 'config' })),
}));
vi.mock('../src/generators/cell-builders/schema-cell.js', () => ({
  buildSchemaCell: vi.fn(() => null),
}));
vi.mock('../src/generators/cell-builders/processor-cell.js', () => ({
  buildProcessorCell: vi.fn((m) => ({ type: 'code', label: m.name, source: `# ${m.name}`, role: m.role || 'process' })),
}));
vi.mock('../src/generators/cell-builders/execution-report.js', () => ({
  generateExecutionReportCell: vi.fn(() => '# execution report'),
}));
vi.mock('../src/generators/cell-builders/validation-cell.js', () => ({
  generateValidationCell: vi.fn(() => '# validation'),
}));
vi.mock('../src/generators/cycle-loop-generator.js', () => ({
  generateLoopFromCycle: vi.fn(() => null),
}));
vi.mock('../src/generators/placeholder-resolver.js', () => ({
  resolveNotebookPlaceholders: vi.fn((s) => s),
}));
vi.mock('../src/generators/code-validator.js', () => ({
  validateGeneratedCode: vi.fn(() => []),
}));

import { generateDatabricksNotebook } from '../src/generators/notebook-generator.js';

describe('generateDatabricksNotebook', () => {
  it('should generate a notebook with expected structure', () => {
    const mappings = [
      { name: 'GetFile', type: 'GetFile', role: 'source', group: 'ETL', mapped: true },
      { name: 'PutS3', type: 'PutS3Object', role: 'sink', group: 'ETL', mapped: true },
    ];
    const nifi = {
      processors: mappings,
      connections: [{ sourceName: 'GetFile', destinationName: 'PutS3' }],
      processGroups: [{ name: 'ETL Group' }],
    };

    const result = generateDatabricksNotebook(mappings, nifi);
    expect(result.cells).toBeDefined();
    expect(result.cells.length).toBeGreaterThan(0);
    expect(result.flowName).toBe('ETL Group');
    expect(result.metadata).toBeDefined();
    expect(result.metadata.processorCount).toBe(2);
    expect(result.metadata.mappedCount).toBe(2);
  });

  it('should include cells in correct order: header, config, framework, processors, report', () => {
    const mappings = [
      { name: 'Proc1', type: 'GetFile', role: 'source', group: 'Main', mapped: true },
    ];
    const nifi = {
      processors: mappings,
      connections: [],
      processGroups: [],
    };
    const result = generateDatabricksNotebook(mappings, nifi);

    // First cell should be header
    expect(result.cells[0].label).toBe('Header');
    // Second cell should be notebook overview
    expect(result.cells[1].label).toBe('Notebook Overview');
    // Third cell should be environment parameters
    expect(result.cells[2].label).toBe('Environment Parameters');
  });

  it('should handle empty mappings', () => {
    const nifi = { processors: [], connections: [], processGroups: [] };
    const result = generateDatabricksNotebook([], nifi);
    expect(result.cells).toBeDefined();
    expect(result.metadata.processorCount).toBe(0);
    expect(result.metadata.mappedCount).toBe(0);
  });
});
