/**
 * generators/lineage-builder.js — DataFrame Lineage Tracker
 *
 * Tracks df_X variable names through cells so downstream processors
 * reference the correct upstream DataFrame variable.
 *
 * Extracted from index.html lines 5403-5431.
 *
 * @module generators/lineage-builder
 */

import { sanitizeVarName } from '../utils/string-helpers.js';

/**
 * Build a DataFrame lineage map for all mappings based on NiFi connections.
 *
 * For each processor mapping, records:
 * - outputVar: the DataFrame variable name this processor produces
 * - inputVars: upstream DataFrame variables feeding into this processor
 * - outputTargets: downstream DataFrame variables this processor feeds
 * - role / type: from the mapping
 *
 * @param {Array<Object>} mappings — processor mapping objects
 * @param {Object} nifi — parsed NiFi flow (processors[], connections[])
 * @returns {Object} — keyed by processor name
 */
export function buildDataFrameLineage(mappings, nifi) {
  const conns = nifi.connections || [];
  const lineage = {};
  const procById = {};
  (nifi.processors || []).forEach(p => { procById[p.id] = p; });
  const downstream = {};
  const upstream = {};
  conns.forEach(c => {
    if (!downstream[c.sourceName]) downstream[c.sourceName] = [];
    downstream[c.sourceName].push({ dest: c.destinationName, rel: c.relationship || 'success' });
    if (!upstream[c.destinationName]) upstream[c.destinationName] = [];
    upstream[c.destinationName].push({ src: c.sourceName, rel: c.relationship || 'success' });
  });
  mappings.forEach(m => {
    const varName = sanitizeVarName(m.name);
    const inputs = (upstream[m.name] || []).map(u => ({
      varName: 'df_' + sanitizeVarName(u.src), procName: u.src, relationship: u.rel
    }));
    const outputs = (downstream[m.name] || []).map(d => ({
      varName: 'df_' + sanitizeVarName(d.dest), procName: d.dest, relationship: d.rel
    }));
    lineage[m.name] = { outputVar: 'df_' + varName, inputVars: inputs, outputTargets: outputs, role: m.role, type: m.type };
  });
  return lineage;
}
