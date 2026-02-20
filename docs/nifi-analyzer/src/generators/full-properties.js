/**
 * generators/full-properties.js — Full Property Extraction
 *
 * Extracts all NiFi processor properties and classifies them as
 * "used" (standard/recognized) or "unused" (available for comments).
 *
 * Extracted from index.html lines 5607-5627.
 *
 * @module generators/full-properties
 */

/**
 * Extract and classify all properties for a processor mapping.
 *
 * Standard property keys (e.g. "Input Directory", "Table Name", etc.)
 * are classified as "used"; everything else is "unused" and can be
 * surfaced as comments in the generated notebook.
 *
 * @param {Object} m — processor mapping
 * @param {Object} nifi — parsed NiFi flow
 * @returns {{ used: Object, unused: Object, all: Object }}
 */
export function extractFullProperties(m, nifi) {
  const proc = (nifi.processors || []).find(p => p.name === m.name);
  if (!proc) return { used: {}, unused: {}, all: {} };
  const props = proc.properties || {};
  const usedKeys = new Set();
  const standardKeys = [
    'Input Directory', 'File Filter', 'Output Directory', 'Directory',
    'Database Connection Pooling Service', 'SQL select query', 'Table Name',
    'Record Reader', 'Record Writer', 'Kafka Brokers', 'Topic Name', 'Group ID',
    'Routing Strategy', 'JDBC Connection URL', 'Conflict Resolution Strategy',
    'Log Level', 'Log Message', 'Command', 'Command Arguments'
  ];
  Object.entries(props).forEach(([k]) => {
    if (standardKeys.some(sk => k.includes(sk))) usedKeys.add(k);
  });
  const unusedProps = {};
  Object.entries(props).forEach(([k, v]) => { if (!usedKeys.has(k)) unusedProps[k] = v; });
  return {
    used: Object.fromEntries([...usedKeys].map(k => [k, props[k]])),
    unused: unusedProps,
    all: props
  };
}
