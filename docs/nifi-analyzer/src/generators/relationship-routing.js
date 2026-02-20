/**
 * generators/relationship-routing.js — Connection Relationship Routing
 *
 * Generates code that routes DataFrames along NiFi relationship paths
 * (success, failure, matched, unmatched, etc.).
 *
 * Extracted from index.html lines 5628-5660.
 *
 * @module generators/relationship-routing
 */

import { sanitizeVarName } from '../utils/string-helpers.js';

/**
 * Generate relationship routing code for a processor.
 *
 * When a processor has multiple outgoing connections with different
 * relationship types, this generates DataFrame assignment code that
 * routes data along the appropriate paths.
 *
 * @param {Object} m — processor mapping
 * @param {Object} nifi — parsed NiFi flow
 * @param {Object} lineage — DataFrame lineage map
 * @returns {string} — routing code (empty string if single/no output)
 */
export function generateRelationshipRouting(m, nifi, lineage) {
  const conns = nifi.connections || [];
  const outConns = conns.filter(c => c.sourceName === m.name);
  if (outConns.length <= 1) return '';
  const varName = sanitizeVarName(m.name);
  const relationships = {};
  outConns.forEach(c => {
    const rel = c.relationship || 'success';
    if (!relationships[rel]) relationships[rel] = [];
    relationships[rel].push(c.destinationName);
  });
  if (Object.keys(relationships).length <= 1) return '';
  let routing = '\n# ── Relationship Routing for ' + m.name + ' ──\n';
  Object.entries(relationships).forEach(([rel, dests]) => {
    const destVars = dests.map(d => 'df_' + sanitizeVarName(d));
    if (rel === 'success' || rel === 'matched' || rel === 'valid') {
      routing += '# Route "' + rel + '" -> ' + dests.join(', ') + '\n';
      destVars.forEach(dv => { routing += dv + ' = df_' + varName + '  # success path\n'; });
    } else if (rel === 'failure' || rel === 'unmatched' || rel === 'invalid') {
      routing += '# Route "' + rel + '" -> ' + dests.join(', ') + ' (error path)\n';
    } else {
      routing += '# Route "' + rel + '" -> ' + dests.join(', ') + '\n';
      destVars.forEach(dv => {
        routing += dv + ' = df_' + varName + '_' + sanitizeVarName(rel) + '  # conditional\n';
      });
    }
  });
  return routing;
}
