/**
 * reporters/index.js — Report suite orchestrator
 *
 * Aggregates all report generators into a single entry point.
 * Provides generateReportSuite() to run all report generators at once.
 *
 * @module reporters
 */

import { generateMigrationReport } from './migration-report.js';
import { computeComparison } from './comparison-engine.js';
import { buildFinalReportJSON, generateFinalReport } from './final-report.js';
import { sanitizeReportJSON } from './report-sanitizer.js';
import { runValueAnalysis } from './value-analysis.js';
import {
  downloadNotebook,
  downloadWorkflow,
  downloadReport,
  downloadFinalReport,
  downloadValidationReport,
  downloadValueAnalysis,
} from './download-helpers.js';
import {
  exportAsDatabricksNotebook,
  exportAsJupyterNotebook,
  exportWorkflowYAML,
} from './export-formats.js';

export {
  // Report generators
  generateMigrationReport,
  computeComparison,
  buildFinalReportJSON,
  generateFinalReport,
  sanitizeReportJSON,
  runValueAnalysis,

  // Download helpers
  downloadNotebook,
  downloadWorkflow,
  downloadReport,
  downloadFinalReport,
  downloadValidationReport,
  downloadValueAnalysis,

  // Export formats
  exportAsDatabricksNotebook,
  exportAsJupyterNotebook,
  exportWorkflowYAML,
};

/**
 * Run all report generators and return a consolidated result.
 *
 * @param {object} STATE      - Application state
 * @param {object} opts
 * @param {Function} opts.escapeHTML  - HTML escaper function
 * @param {Function} opts.metricsHTML - Metrics card renderer
 * @returns {object} Consolidated report results
 */
export function generateReportSuite(STATE, { escapeHTML, metricsHTML } = {}) {
  const results = {};

  // Migration report
  if (STATE.notebook && STATE.parsed && STATE.parsed._nifi) {
    results.migrationReport = generateMigrationReport(
      STATE.notebook.mappings || STATE.assessment?.mappings || [],
      STATE.parsed._nifi
    );
  }

  // Comparison
  if (STATE.assessment && STATE.parsed && STATE.parsed._nifi) {
    results.comparison = computeComparison(
      STATE.assessment.mappings,
      STATE.parsed._nifi
    );
  }

  // Final report
  if (STATE.parsed) {
    results.finalReport = buildFinalReportJSON(STATE);
  }

  // Value analysis
  if (STATE.parsed && STATE.parsed._nifi && STATE.notebook && escapeHTML) {
    results.valueAnalysis = runValueAnalysis({
      nifi: STATE.parsed._nifi,
      notebook: STATE.notebook,
      escapeHTML,
    });
  }

  return results;
}
