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

  // IMPROVEMENT #8: Relationship routing
  const routing = generateRelationshipRouting(m, nifi, lineage);

  // Build cell with IMPROVEMENT #5 (error framework) + #6 (auto recovery)
  // Use a shallow copy of m so that the adaptive-enhanced code is wrapped by the
  // error framework without mutating the original mapping object.
  let cellCode;
  if (m.mapped && m.code && !m.code.startsWith('# TODO')) {
    const mWithAdaptive = code !== m.code ? { ...m, code } : m;
    cellCode = wrapWithErrorFramework(mWithAdaptive, qualifiedSchema, cellIndex, lineage);
    cellCode += generateAutoRecovery(mWithAdaptive, qualifiedSchema, lineage);
  } else {
    cellCode = `# ${lbl}\n# ${m.desc}${m.notes ? '  |  ' + m.notes : ''}\n# Input: ${inputInfo}\n${code}`;
  }

  cellCode = `# Input lineage: ${inputInfo}\n# Output: ${li.outputVar || 'df_' + sanitizeVarName(m.name)}${unusedComment}\n${cellCode}${routing}`;

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
