/**
 * reporters/download-helpers.js — File download helpers
 *
 * Extracted from index.html lines 7812-7974.
 * Provides download functions for notebook (.py), workflow (.json),
 * migration report (.md), final report (.json), validation report (.json),
 * and value analysis (.json).
 *
 * @module reporters/download-helpers
 */

import { sanitizeReportJSON } from './report-sanitizer.js';

/**
 * Trigger a browser file download from a Blob.
 * @param {Blob}   blob     - File content
 * @param {string} filename - Suggested filename
 */
function triggerDownload(blob, filename) {
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  a.click();
  URL.revokeObjectURL(a.href);
}

/**
 * Download the generated notebook as a Databricks .py file.
 * @param {object} STATE - Application state (must have STATE.notebook)
 */
export function downloadNotebook(STATE) {
  if (!STATE.notebook) return;
  const pyCells = STATE.notebook.cells.map(c => {
    if (c.type === 'md') {
      return '# MAGIC %md\n' + c.source.split('\n').map(l => '# MAGIC ' + l).join('\n');
    }
    if (c.type === 'sql') {
      return '# MAGIC %sql\n' + c.source.split('\n').map(l => '# MAGIC ' + l).join('\n');
    }
    return c.source;
  });
  const nb = '# Databricks notebook source\n\n' + pyCells.join('\n\n# COMMAND ----------\n\n');
  triggerDownload(new Blob([nb], { type: 'text/plain' }), 'nifi_migration_notebook.py');
}

/**
 * Download the generated workflow as JSON.
 * @param {object} STATE - Application state (must have STATE.notebook.workflow)
 */
export function downloadWorkflow(STATE) {
  if (!STATE.notebook) return;
  triggerDownload(
    new Blob([JSON.stringify(STATE.notebook.workflow, null, 2)], { type: 'application/json' }),
    'databricks_workflow.json'
  );
}

/**
 * Download the migration report as Markdown.
 * @param {object} STATE - Application state (must have STATE.migrationReport)
 */
export function downloadReport(STATE) {
  if (!STATE.migrationReport) return;
  const r = STATE.migrationReport;
  const s = r.summary;
  let md = `# NiFi \u2192 Databricks Migration Report\n\n`;
  md += `## Summary\n| Metric | Value |\n|--------|-------|\n`;
  md += `| Total Processors | ${s.totalProcessors} |\n| Mapped | ${s.mappedProcessors} |\n| Unmapped | ${s.unmappedProcessors} |\n`;
  md += `| Coverage | ${s.coveragePercent}% |\n| Process Groups | ${s.totalProcessGroups} |\n| Effort | ${r.effort} |\n\n`;
  md += `## By Role\n| Role | Mapped | Total | % |\n|------|--------|-------|---|\n`;
  Object.entries(r.byRole).forEach(([role, rd]) => {
    md += `| ${role} | ${rd.mapped} | ${rd.total} | ${rd.total ? Math.round(rd.mapped / rd.total * 100) : 0}% |\n`;
  });
  md += `\n## By Group\n| Group | Mapped | Total | % |\n|-------|--------|-------|---|\n`;
  Object.entries(r.byGroup).forEach(([g, gd]) => {
    md += `| ${g} | ${gd.mapped} | ${gd.total} | ${gd.total ? Math.round(gd.mapped / gd.total * 100) : 0}% |\n`;
  });
  if (r.gaps.length) {
    md += `\n## Gaps\n| Processor | Type | Group | Recommendation |\n|-----------|------|-------|----------------|\n`;
    r.gaps.forEach(g => { md += `| ${g.processor} | ${g.type} | ${g.group || '\u2014'} | ${g.recommendation || 'Manual'} |\n`; });
  }
  if (r.recommendations.length) {
    md += `\n## Recommendations\n`;
    r.recommendations.forEach(rec => { md += `- ${rec}\n`; });
  }
  triggerDownload(new Blob([md], { type: 'text/markdown' }), 'migration_report.md');
}

/**
 * Download the final analysis report as JSON.
 * @param {object} STATE - Application state (must have STATE.finalReport)
 */
export function downloadFinalReport(STATE) {
  if (!STATE.finalReport) return;
  const json = JSON.stringify(sanitizeReportJSON(STATE.finalReport), null, 2);
  triggerDownload(new Blob([json], { type: 'application/json' }), 'nifi_analysis_report.json');
}

/**
 * Download the validation report as JSON.
 * @param {object} STATE - Application state (must have STATE.validation)
 */
export function downloadValidationReport(STATE) {
  if (!STATE.validation) return;
  const json = JSON.stringify(sanitizeReportJSON(STATE.validation), null, 2);
  triggerDownload(new Blob([json], { type: 'application/json' }), 'nifi_validation_report.json');
}

/**
 * Download the value analysis as JSON.
 * @param {object} STATE - Application state (must have STATE.valueAnalysis)
 */
export function downloadValueAnalysis(STATE) {
  if (!STATE.valueAnalysis) return;
  const json = JSON.stringify(STATE.valueAnalysis, null, 2);
  triggerDownload(new Blob([json], { type: 'application/json' }), 'nifi_value_analysis.json');
}
