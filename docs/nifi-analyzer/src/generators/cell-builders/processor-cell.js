/**
 * generators/cell-builders/processor-cell.js — Per-Processor Cell Builder
 *
 * Builds individual code cells for each processor mapping, incorporating
 * lineage, adaptive code, error framework, auto recovery, relationship
 * routing, and full property comments.
 *
 * Extracted from generateDatabricksNotebook processor loop (index.html ~line 5832-5862).
 *
 * @module generators/cell-builders/processor-cell
 */

import { sanitizeVarName } from '../../utils/string-helpers.js';
import { generateAdaptiveCode } from '../adaptive-code.js';
import { wrapWithErrorFramework } from './error-framework.js';
import { generateAutoRecovery } from '../auto-recovery.js';
import { generateRelationshipRouting } from '../relationship-routing.js';

/**
 * Build a code cell for a single processor mapping.
 *
 * @param {Object} m — processor mapping
 * @param {Object} options
 * @param {Object} options.lineage — DataFrame lineage map
 * @param {string} options.qualifiedSchema — fully qualified schema name
 * @param {Object} options.nifi — parsed NiFi flow
 * @param {Object} options.fullProps — extracted full properties for this processor
 * @param {number} options.cellIndex — cell position index
 * @returns {Object} — cell object { type, label, source, role, processor, procType, confidence, mapped }
 */
export function buildProcessorCell(m, { lineage, qualifiedSchema, nifi, fullProps, cellIndex }) {
  const lbl = `[${m.role.toUpperCase()}] ${m.name} \u2192 ${m.category}`;
  const li = lineage[m.name] || {};
  const inputInfo = (li.inputVars || []).map(v => `${v.varName} (${v.relationship})`).join(', ') || 'none';

  // IMPROVEMENT #4: Adaptive code
  let code = generateAdaptiveCode(m, lineage, qualifiedSchema);

  // IMPROVEMENT #7: Unused property comments
  const fp = fullProps || { unused: {} };
  const unusedComment = Object.keys(fp.unused).length > 0
    ? '\n# Unused NiFi properties:\n' + Object.entries(fp.unused).slice(0, 8).map(([k, v]) => '#   ' + k + ': ' + String(v).substring(0, 80)).join('\n')
    : '';

  // IMPROVEMENT #14 (4.5): Enhanced code comments — processor intent and original NiFi properties
  const intentMap = {
    source: 'DATA INGESTION - Reads data from an external system into the pipeline',
    sink: 'DATA OUTPUT - Writes processed data to a target system or storage',
    transform: 'DATA TRANSFORMATION - Modifies, enriches, or reshapes data in transit',
    route: 'FLOW CONTROL - Routes data to different paths based on conditions',
    process: 'DATA PROCESSING - Performs computation or business logic on data',
    utility: 'UTILITY - Provides supporting functionality (logging, monitoring, etc.)',
  };
  const intentComment = `# INTENT: ${intentMap[m.role] || 'PROCESSING - ' + m.type}`;
  const nifiTypeComment = `# NiFi Processor: ${m.type} (${m.name})`;
  const confidenceComment = `# Migration Confidence: ${Math.round((m.confidence || 0) * 100)}% | Status: ${m.mapped ? 'Mapped' : 'MANUAL IMPLEMENTATION REQUIRED'}`;

  // Original NiFi properties reference
  const allProps = { ...(fp.used || {}), ...(fp.unused || {}) };
  const propsEntries = Object.entries(allProps).slice(0, 12);
  const nifiPropsComment = propsEntries.length > 0
    ? '\n# Original NiFi Properties:\n' + propsEntries.map(([k, v]) => {
        const val = String(v).substring(0, 100);
        return `#   ${k} = ${val}`;
      }).join('\n')
    : '';

  // IMPROVEMENT #8: Relationship routing
  const routing = generateRelationshipRouting(m, nifi, lineage);

  // Build cell with IMPROVEMENT #5 (error framework) + #6 (auto recovery)
  // Use a shallow copy of m so that the adaptive-enhanced code is wrapped by the
  // error framework without mutating the original mapping object.
  let cellCode;
  if (m.mapped && m.code && !m.code.startsWith('# TODO')) {
    const mWithAdaptive = code !== m.code ? { ...m, code } : m;
    cellCode = wrapWithErrorFramework(mWithAdaptive, qualifiedSchema, cellIndex, lineage);
    const recoveryCode = generateAutoRecovery(mWithAdaptive, qualifiedSchema, lineage);
    if (recoveryCode) {
      // Insert recovery code before 'raise _e' so it executes before re-throwing
      cellCode = cellCode.replace(/(\s+raise _e)$/, (match, p1) => recoveryCode + p1);
    }
  } else {
    cellCode = `# ${lbl}\n# ${m.desc}${m.notes ? '  |  ' + m.notes : ''}\n# Input: ${inputInfo}\n${code}`;
  }

  cellCode = `${intentComment}\n${nifiTypeComment}\n${confidenceComment}${nifiPropsComment}\n# Input lineage: ${inputInfo}\n# Output: ${li.outputVar || 'df_' + sanitizeVarName(m.name)}${unusedComment}\n${cellCode}${routing}`;

  return {
    type: 'code',
    label: lbl,
    source: cellCode,
    role: m.role,
    processor: m.name,
    procType: m.type,
    confidence: m.confidence,
    mapped: m.mapped
  };
}
