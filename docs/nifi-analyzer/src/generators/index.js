/**
 * generators/index.js — Phase 6 Generator Orchestrator
 *
 * Top-level entry point that calls the notebook generator and workflow
 * generator, returning both artifacts from a single invocation.
 *
 * @module generators
 */

import { generateDatabricksNotebook } from './notebook-generator.js';
import { generateWorkflowJSON } from './workflow-generator.js';

// Re-export sub-modules for direct imports
export { generateDatabricksNotebook } from './notebook-generator.js';
export { generateWorkflowJSON } from './workflow-generator.js';
export { buildDataFrameLineage } from './lineage-builder.js';
export { topologicalSortMappings } from './topo-sort.js';
export { generateAdaptiveCode } from './adaptive-code.js';
export { generateRelationshipRouting } from './relationship-routing.js';
export { extractFullProperties } from './full-properties.js';
export { generateRetryWrapper } from './retry-wrapper.js';
export { wrapSubprocessAsPandasUDF } from './subprocess-wrapper.js';
export { generateLoopFromCycle } from './cycle-loop-generator.js';
export { wrapBatchSinkForStreaming } from './streaming-wrapper.js';
export { generateAutoRecovery } from './auto-recovery.js';
export { validateGeneratedCode } from './code-validator.js';
export { resolveNotebookPlaceholders } from './placeholder-resolver.js';
export { generateDLQWrapper } from './dlq-wrapper.js';
export { validateNotebookCode } from './databricks-validator.js';
export { scrubGeneratedCode } from './code-scrubber.js';
export { checkRuntimeCompat, generateRuntimeCheck } from './runtime-compat.js';
export { collectSmartImports } from './cell-builders/imports-cell.js';
export { wrapWithErrorFramework } from './cell-builders/error-framework.js';
export { generateExecutionReportCell } from './cell-builders/execution-report.js';
export { generateValidationCell } from './cell-builders/validation-cell.js';
export { buildHeaderCell } from './cell-builders/header-cell.js';
export { buildConfigCell } from './cell-builders/config-cell.js';
export { buildSchemaCell } from './cell-builders/schema-cell.js';
export { buildProcessorCell } from './cell-builders/processor-cell.js';
export { buildPreflightCell } from './cell-builders/preflight-cell.js';

/**
 * Generate both the Databricks notebook and workflow JSON from NiFi flow data.
 *
 * This is the primary entry point for Phase 6 generation. It produces:
 * - A complete Databricks notebook with all 10 improvements
 * - A Databricks Workflow JSON with shared job clusters
 *
 * @param {Object} state — application state
 * @param {Array<Object>} state.mappings — processor mapping objects
 * @param {Object} state.nifi — parsed NiFi flow
 * @param {Object} [state.blueprint] — optional blueprint with .tables[]
 * @param {Object} [state.config] — Databricks config overrides
 * @returns {{ notebook: Object, workflow: Object }}
 */
export function generateNotebookAndWorkflow(state) {
  const { mappings, nifi, blueprint, config } = state;

  const notebook = generateDatabricksNotebook(mappings, nifi, blueprint, config);
  const workflow = generateWorkflowJSON(mappings, nifi, config);

  return { notebook, workflow };
}
