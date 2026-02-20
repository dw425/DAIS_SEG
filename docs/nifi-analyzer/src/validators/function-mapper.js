/**
 * validators/function-mapper.js — NiFi function-to-Databricks mapping
 *
 * Extracted from index.html lines 8532-8608.
 * Maps each NiFi processor's functional capabilities to their
 * Databricks equivalents, with confidence scoring and package detection.
 *
 * @module validators/function-mapper
 */

import { classifyNiFiProcessor } from '../mappers/processor-classifier.js';
import { getProcessorPackages } from '../constants/package-map.js';

/**
 * Map NiFi processor functions to Databricks equivalents.
 *
 * @param {object} opts
 * @param {Array}  opts.mappings      - Assessment mappings
 * @param {object} opts.procByName    - O(1) lookup of processor by name
 * @param {number} [opts.batchSize=100]
 * @param {Function} [opts.onProgress]
 * @returns {Promise<{funcResults:Array, funcMapped:number, funcPartial:number, funcMissing:number, funcScore:number}>}
 */
export async function mapFunctions({
  mappings,
  procByName,
  batchSize = 100,
  onProgress,
}) {
  const funcResults = [];
  let funcMapped = 0, funcPartial = 0, funcMissing = 0;

  for (let bi = 0; bi < mappings.length; bi += batchSize) {
    const batch = mappings.slice(bi, bi + batchSize);
    batch.forEach(m => {
      const proc = procByName[m.name];
      if (!proc) return;
      const role = classifyNiFiProcessor(m.type);
      const props = proc.properties || {};

      const nifiFunctions = [];
      if (role === 'source') nifiFunctions.push('Data ingestion');
      if (role === 'sink') nifiFunctions.push('Data output/write');
      if (role === 'transform') nifiFunctions.push('Data transformation');
      if (role === 'route') nifiFunctions.push('Conditional routing');
      if (role === 'process') nifiFunctions.push('Data processing');
      if (role === 'utility') nifiFunctions.push('Utility');

      if (m.type.includes('SQL') || m.type.includes('Sql')) nifiFunctions.push('SQL execution');
      if (m.type.includes('JSON') || m.type.includes('Json')) nifiFunctions.push('JSON processing');
      if (m.type.includes('Avro')) nifiFunctions.push('Avro serialization');
      if (m.type.includes('CSV') || m.type.includes('Csv')) nifiFunctions.push('CSV processing');
      if (m.type.includes('Kafka')) nifiFunctions.push('Kafka messaging');
      if (m.type.includes('HDFS') || m.type.includes('Hdfs')) nifiFunctions.push('HDFS file operations');
      if (m.type.includes('S3') || m.type.includes('AWS')) nifiFunctions.push('S3/AWS operations');
      if (m.type.includes('Encrypt') || m.type.includes('Hash')) nifiFunctions.push('Encryption/hashing');
      if (m.type.includes('HTTP') || m.type.includes('Http')) nifiFunctions.push('HTTP communication');
      if (m.type.includes('Merge') || m.type.includes('Split')) nifiFunctions.push('Data merge/split');
      if (m.type.includes('Attribute')) nifiFunctions.push('Attribute manipulation');
      if (m.type.includes('Wait') || m.type.includes('Notify')) nifiFunctions.push('Flow coordination');
      if (m.type.includes('Kudu')) nifiFunctions.push('Kudu table operations');

      const elProps = Object.entries(props).filter(([k, v]) => v && String(v).includes('${'));
      if (elProps.length) nifiFunctions.push('NiFi Expression Language (' + elProps.length + ' expressions)');

      let dbxFunctions = [];
      let status = 'missing';
      if (m.mapped && m.confidence >= 0.7) { status = 'mapped'; funcMapped++; if (m.desc) dbxFunctions.push(m.desc); }
      else if (m.mapped) { status = 'partial'; funcPartial++; if (m.desc) dbxFunctions.push(m.desc + ' (low confidence)'); }
      else { funcMissing++; }

      const pkgs = getProcessorPackages(m.type);

      funcResults.push({
        name: m.name, type: m.type, role, nifiFunctions, dbxFunctions, status,
        confidence: m.confidence, packages: pkgs, elCount: elProps.length
      });
    });
    if (onProgress) {
      onProgress(78 + Math.round((bi / mappings.length) * 15), 'Function mapping: ' + Math.min(bi + batchSize, mappings.length) + '/' + mappings.length + '...');
    }
    await new Promise(r => setTimeout(r, 0));
  }

  const funcScore = mappings.length ? Math.round(((funcMapped + funcPartial * 0.5) / mappings.length) * 100) : 0;

  return { funcResults, funcMapped, funcPartial, funcMissing, funcScore };
}
