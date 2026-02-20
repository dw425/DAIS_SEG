/**
 * reporters/download-helpers.js — File download helpers
 *
 * Extracted from index.html lines 7812-7974.
 * Provides download functions for notebook (.py), workflow (.json),
 * migration report (.md), final report (.json), validation report (.json),
 * and value analysis (.json).
 *
 * v2.0.1: Added download validation, timestamped filenames, state snapshots.
 *
 * @module reporters/download-helpers
 */

import { sanitizeReportJSON } from './report-sanitizer.js';

/**
 * Generate a timestamped filename prefix.
 * @param {object} [STATE] - Application state (for flow name)
 * @returns {string} e.g. "nifi_migration_2026-02-20T19-53"
 */
function filePrefix(STATE) {
  const ts = new Date().toISOString().replace(/[:.]/g, '-').substring(0, 16);
  const flowName = STATE?.parsed?.source_name
    ? STATE.parsed.source_name.replace(/\.[^.]+$/, '').replace(/[^a-zA-Z0-9_-]/g, '_').substring(0, 40)
    : 'nifi_migration';
  return `${flowName}_${ts}`;
}

/**
 * Trigger a browser file download from a Blob.
 * @param {Blob}   blob     - File content
 * @param {string} filename - Suggested filename
 * @returns {boolean} true if download was triggered
 */
function triggerDownload(blob, filename) {
  if (!blob || blob.size === 0) {
    console.error('[download] Empty blob, skipping download for:', filename);
    return false;
  }
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  a.click();
  URL.revokeObjectURL(a.href);
  return true;
}

/**
 * Download the generated notebook as a Databricks .py file.
 * @param {object} STATE - Application state (must have STATE.notebook)
 * @returns {boolean} true if download succeeded
 */
export function downloadNotebook(STATE) {
  if (!STATE.notebook) {
    console.warn('[download] No notebook in state — complete Convert step first.');
    return false;
  }
  const cells = STATE.notebook.cells;
  if (!cells || cells.length === 0) {
    console.warn('[download] Notebook has no cells — cannot download empty notebook.');
    return false;
  }
  // Validate cells have content
  const emptyCells = cells.filter(c => !c.source || c.source.trim().length === 0);
  if (emptyCells.length > 0) {
    console.warn(`[download] ${emptyCells.length} empty cells detected in notebook.`);
  }

  const pyCells = cells.map(c => {
    if (c.type === 'md' || c.type === 'markdown' || c.role === 'markdown') {
      return '# MAGIC %md\n' + c.source.split('\n').map(l => '# MAGIC ' + l).join('\n');
    }
    if (c.type === 'sql') {
      return '# MAGIC %sql\n' + c.source.split('\n').map(l => '# MAGIC ' + l).join('\n');
    }
    return c.source;
  });
  const nb = '# Databricks notebook source\n# Generated: ' + new Date().toISOString() + '\n# Cells: ' + cells.length + '\n\n' + pyCells.join('\n\n# COMMAND ----------\n\n');
  return triggerDownload(new Blob([nb], { type: 'text/plain' }), filePrefix(STATE) + '_notebook.py');
}

/**
 * Download the generated workflow as JSON.
 * @param {object} STATE - Application state (must have STATE.notebook.workflow)
 * @returns {boolean} true if download succeeded
 */
export function downloadWorkflow(STATE) {
  if (!STATE.notebook?.workflow) {
    console.warn('[download] No workflow in state — complete Convert step first.');
    return false;
  }
  const workflow = {
    ...STATE.notebook.workflow,
    _meta: {
      generated: new Date().toISOString(),
      tool: 'NiFi Flow Analyzer v2.0.1',
      source: STATE.parsed?.source_name || 'unknown'
    }
  };
  return triggerDownload(
    new Blob([JSON.stringify(workflow, null, 2)], { type: 'application/json' }),
    filePrefix(STATE) + '_workflow.json'
  );
}

/**
 * Download the migration report as Markdown.
 * @param {object} STATE - Application state (must have STATE.migrationReport)
 * @returns {boolean} true if download succeeded
 */
export function downloadReport(STATE) {
  if (!STATE.migrationReport) {
    console.warn('[download] No migration report in state — complete Report step first.');
    return false;
  }
  const r = STATE.migrationReport;
  const s = r.summary;
  let md = `# NiFi \u2192 Databricks Migration Report\n\n`;
  md += `> Generated: ${new Date().toISOString()} | Tool: NiFi Flow Analyzer v2.0.1\n\n`;
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
  if (r.gaps && r.gaps.length) {
    md += `\n## Gaps (${r.gaps.length})\n| Processor | Type | Group | Recommendation |\n|-----------|------|-------|----------------|\n`;
    r.gaps.forEach(g => { md += `| ${g.processor || g.name} | ${g.type} | ${g.group || '\u2014'} | ${g.recommendation || 'Manual'} |\n`; });
  }
  if (r.recommendations && r.recommendations.length) {
    md += `\n## Recommendations\n`;
    r.recommendations.forEach(rec => { md += `- ${rec}\n`; });
  }
  return triggerDownload(new Blob([md], { type: 'text/markdown' }), filePrefix(STATE) + '_migration_report.md');
}

/**
 * Download the final analysis report as JSON.
 * @param {object} STATE - Application state (must have STATE.finalReport)
 * @returns {boolean} true if download succeeded
 */
export function downloadFinalReport(STATE) {
  if (!STATE.finalReport) {
    console.warn('[download] No final report in state — complete Final Report step first.');
    return false;
  }
  const report = {
    ...sanitizeReportJSON(STATE.finalReport),
    _meta: {
      generated: new Date().toISOString(),
      tool: 'NiFi Flow Analyzer v2.0.1',
      source: STATE.parsed?.source_name || 'unknown'
    }
  };
  const json = JSON.stringify(report, null, 2);
  return triggerDownload(new Blob([json], { type: 'application/json' }), filePrefix(STATE) + '_analysis_report.json');
}

/**
 * Download the validation report as JSON.
 * @param {object} STATE - Application state (must have STATE.validation)
 * @returns {boolean} true if download succeeded
 */
export function downloadValidationReport(STATE) {
  if (!STATE.validation) {
    console.warn('[download] No validation in state — complete Validate step first.');
    return false;
  }
  const report = {
    ...sanitizeReportJSON(STATE.validation),
    _meta: {
      generated: new Date().toISOString(),
      tool: 'NiFi Flow Analyzer v2.0.1'
    }
  };
  const json = JSON.stringify(report, null, 2);
  return triggerDownload(new Blob([json], { type: 'application/json' }), filePrefix(STATE) + '_validation_report.json');
}

/**
 * Download the value analysis as JSON.
 * @param {object} STATE - Application state (must have STATE.valueAnalysis)
 * @returns {boolean} true if download succeeded
 */
export function downloadValueAnalysis(STATE) {
  if (!STATE.valueAnalysis) {
    console.warn('[download] No value analysis in state — complete Value Analysis step first.');
    return false;
  }
  const data = typeof STATE.valueAnalysis === 'string'
    ? { html: STATE.valueAnalysis }
    : STATE.valueAnalysis;
  const report = {
    ...data,
    _meta: {
      generated: new Date().toISOString(),
      tool: 'NiFi Flow Analyzer v2.0.1'
    }
  };
  const json = JSON.stringify(report, null, 2);
  return triggerDownload(new Blob([json], { type: 'application/json' }), filePrefix(STATE) + '_value_analysis.json');
}
