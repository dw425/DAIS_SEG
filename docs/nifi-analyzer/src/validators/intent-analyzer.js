/**
 * validators/intent-analyzer.js — Intent preservation analysis
 *
 * Extracted from index.html lines 8277-8346.
 * Compares NiFi processor intents against notebook cell references
 * to verify the PySpark notebook preserves the same data flow intent.
 *
 * @module validators/intent-analyzer
 */

import { classifyNiFiProcessor } from '../mappers/processor-classifier.js';
import { CONFIDENCE_THRESHOLDS } from '../constants/confidence-thresholds.js';

/**
 * Analyze whether each NiFi processor's intent is preserved in the notebook.
 *
 * @param {object} opts
 * @param {Array}  opts.processors      - Parsed NiFi processors
 * @param {Array}  opts.mappings        - Assessment mappings
 * @param {object} opts.mappingByName   - O(1) lookup of mapping by processor name
 * @param {string} opts.allCellTextLower - All notebook cell source joined and lowercased
 * @param {number} [opts.batchSize=100] - Processors per async tick
 * @param {Function} [opts.onProgress]  - (pct, msg) => void
 * @returns {Promise<{nifiIntents:Array, intentMatched:number, intentPartial:number, intentMissing:number, intentGaps:Array, intentScore:number}>}
 */
export async function analyzeIntent({
  processors,
  mappings,
  mappingByName,
  allCellTextLower,
  batchSize = 100,
  onProgress,
}) {
  const nifiIntents = [];

  processors.forEach(p => {
    const role = classifyNiFiProcessor(p.type);
    let intent = '';
    const props = p.properties || {};
    if (role === 'source') {
      const targets = Object.values(props).filter(v => v && typeof v === 'string').join(' ');
      const sysMatch = targets.match(/\b(s3|hdfs|kafka|jdbc|file|ftp|sftp|http)/i);
      intent = 'INGEST data' + (sysMatch ? ' from ' + sysMatch[0].toUpperCase() : '');
    } else if (role === 'sink') {
      intent = 'WRITE/OUTPUT data' + (props['Directory'] ? ' to ' + props['Directory'] : '') + (props['Topic Name'] ? ' to Kafka:' + props['Topic Name'] : '');
    } else if (role === 'transform') {
      intent = 'TRANSFORM data' + (p.type.includes('JSON') ? ' (JSON)' : p.type.includes('SQL') ? ' (SQL)' : p.type.includes('Attribute') ? ' (attributes)' : '');
    } else if (role === 'route') {
      const routeCount = Object.keys(props).filter(k => k !== 'Routing Strategy').length;
      intent = 'ROUTE/BRANCH' + (routeCount > 0 ? ' (' + routeCount + ' conditions)' : '');
    } else if (role === 'process') {
      intent = 'PROCESS data (' + p.type.replace(/^org\.apache\.nifi\.processors?\.\w+\./, '') + ')';
    } else {
      intent = 'UTILITY (' + p.type.replace(/^org\.apache\.nifi\.processors?\.\w+\./, '') + ')';
    }
    nifiIntents.push({ name: p.name, type: p.type, role, intent, props });
  });

  let intentMatched = 0, intentPartial = 0, intentMissing = 0;
  const intentGaps = [];

  for (let bi = 0; bi < nifiIntents.length; bi += batchSize) {
    const batch = nifiIntents.slice(bi, bi + batchSize);
    batch.forEach(ni => {
      const mapping = mappingByName[ni.name];
      if (!mapping) { intentMissing++; intentGaps.push({ proc: ni.name, type: ni.type, intent: ni.intent, issue: 'No mapping found — processor entirely missing from notebook' }); return; }
      if (!mapping.mapped) { intentMissing++; intentGaps.push({ proc: ni.name, type: ni.type, intent: ni.intent, issue: 'Unmapped — no Databricks equivalent generated' }); return; }
      const varName = mapping.name.replace(/[^a-zA-Z0-9]/g, '_').toLowerCase();
      const cellMatch = allCellTextLower.includes(varName) || allCellTextLower.includes(ni.name.toLowerCase());
      if (!cellMatch) { intentPartial++; intentGaps.push({ proc: ni.name, type: ni.type, intent: ni.intent, issue: 'Mapped but no dedicated notebook cell references this processor' }); return; }
      if (mapping.confidence >= CONFIDENCE_THRESHOLDS.MAPPED) { intentMatched++; }
      else { intentPartial++; intentGaps.push({ proc: ni.name, type: ni.type, intent: ni.intent, issue: 'Low confidence (' + Math.round(mapping.confidence * 100) + '%) — intent may not be fully preserved' }); }
    });
    if (onProgress) {
      onProgress(8 + Math.round((bi / nifiIntents.length) * 20), 'Intent analysis: ' + Math.min(bi + batchSize, nifiIntents.length) + '/' + nifiIntents.length + ' processors...');
    }
    await new Promise(r => setTimeout(r, 0));
  }

  const intentScore = nifiIntents.length ? Math.round((intentMatched / nifiIntents.length) * 100) : 0;

  return { nifiIntents, intentMatched, intentPartial, intentMissing, intentGaps, intentScore };
}
