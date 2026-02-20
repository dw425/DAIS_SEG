/**
 * reporters/comparison-engine.js — Cross-comparison logic
 *
 * Extracted from index.html lines 6006-6033.
 * Computes exact match, functional match, and action conversion rates
 * for NiFi-to-Databricks processor comparisons.
 *
 * @module reporters/comparison-engine
 */

/**
 * Compute cross-comparison metrics between NiFi processors and Databricks mappings.
 *
 * @param {Array}  mappings - Assessment mappings
 * @param {object} nifi     - Parsed NiFi flow object
 * @returns {{exact:{count,total,pct}, functional:{count,total,pct}, actions:{count,total,pct}, rows:Array}}
 */
export function computeComparison(mappings, nifi) {
  const total = mappings.length;
  // Exact match: high-confidence direct 1:1 mappings (conf >= 0.8)
  const exactCount = mappings.filter(m => m.mapped && m.confidence >= 0.8).length;
  // Functional match: any mapped processor (intent preserved regardless of confidence)
  const funcCount = mappings.filter(m => m.mapped).length;
  // Actions converted: connections where BOTH source and destination are mapped
  const conns = nifi.connections || [];
  const mappedNames = new Set(mappings.filter(m => m.mapped).map(m => m.name));
  const totalActions = conns.length;
  const convertedActions = conns.filter(c => mappedNames.has(c.sourceName) && mappedNames.has(c.destinationName)).length;
  // Build comparison rows
  const rows = mappings.map((m, i) => {
    let matchType;
    if (!m.mapped) matchType = 'gap';
    else if (m.confidence >= 0.8) matchType = 'exact';
    else matchType = 'functional';
    return {
      idx: i + 1, name: m.name, type: m.type, group: m.group || '\u2014', role: m.role,
      equiv: m.mapped ? m.desc : '\u2014', category: m.mapped ? m.category : '\u2014',
      matchType, confidence: m.confidence, code: m.code
    };
  });
  return {
    exact: { count: exactCount, total, pct: total ? Math.round(exactCount / total * 100) : 0 },
    functional: { count: funcCount, total, pct: total ? Math.round(funcCount / total * 100) : 0 },
    actions: { count: convertedActions, total: totalActions, pct: totalActions ? Math.round(convertedActions / totalActions * 100) : 0 },
    rows
  };
}
