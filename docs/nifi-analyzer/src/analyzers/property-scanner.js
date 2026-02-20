/**
 * analyzers/property-scanner.js — Deep property inventory scanning
 *
 * Extracted from index.html lines 1427-1674 (Phase B through Phase G
 * of _processProcessorBatch). Builds a comprehensive deepPropertyInventory
 * by scanning every processor property value for file paths, URLs, JDBC URLs,
 * NiFi EL, CRON expressions, credential references, host:port patterns,
 * and data formats.
 *
 * @module analyzers/property-scanner
 */

import { _RE } from '../constants/regex-patterns.js';

/**
 * Scan all processor properties and build a deep property inventory.
 *
 * The returned inventory contains:
 * - filePaths   — file paths found in property values
 * - urls        — HTTP/HTTPS URLs
 * - jdbcUrls    — JDBC connection strings
 * - nifiEL      — NiFi Expression Language references
 * - cronExprs   — CRON expressions (from values and scheduling config)
 * - credentialRefs — properties whose keys match credential patterns
 * - hostPorts   — host:port patterns
 * - dataFormats — Set of detected data formats (avro, parquet, etc.)
 *
 * @param {Array} processors — parsed processors with .properties, .schedulingStrategy, .schedulingPeriod
 * @returns {object} — deepPropertyInventory
 */
export function scanProperties(processors) {
  const deepPropertyInventory = {
    filePaths: {},
    urls: {},
    jdbcUrls: {},
    nifiEL: {},
    cronExprs: {},
    credentialRefs: {},
    hostPorts: {},
    dataFormats: new Set(),
    encodings: new Set(),
  };

  (processors || []).forEach(p => {
    const props = p.properties || {};
    const vals = Object.values(props);
    const keys = Object.keys(props);

    for (let vi = 0; vi < vals.length; vi++) {
      const v = vals[vi];
      if (!v || typeof v !== 'string') continue;
      const vLen = v.length;

      // File paths
      if (vLen > 3 && v.includes('/')) {
        const paths = v.match(_RE.filePath);
        if (paths) paths.forEach(fp => {
          if (fp.length > 3 && !/^\/\//.test(fp)) {
            if (!deepPropertyInventory.filePaths[fp]) deepPropertyInventory.filePaths[fp] = [];
            deepPropertyInventory.filePaths[fp].push({ processor: p.name, group: p.group, property: keys[vi] });
          }
        });
      }

      // URLs
      if (vLen > 8 && /https?:/.test(v)) {
        const urls = v.match(_RE.urlPattern);
        if (urls) urls.forEach(u => {
          if (!deepPropertyInventory.urls[u]) deepPropertyInventory.urls[u] = [];
          deepPropertyInventory.urls[u].push({ processor: p.name, group: p.group, property: keys[vi] });
        });
      }

      // JDBC URLs
      if (vLen > 10 && v.startsWith('jdbc:')) {
        const jdbcs = v.match(_RE.jdbcUrl);
        if (jdbcs) jdbcs.forEach(j => {
          if (!deepPropertyInventory.jdbcUrls[j]) deepPropertyInventory.jdbcUrls[j] = [];
          deepPropertyInventory.jdbcUrls[j].push({ processor: p.name, group: p.group, property: keys[vi] });
        });
      }

      // NiFi Expression Language patterns
      if (vLen > 3 && v.includes('${')) {
        const els = v.match(_RE.nifiEL);
        if (els) els.forEach(el => {
          const inner = el.slice(2, -1);
          if (!deepPropertyInventory.nifiEL[inner]) deepPropertyInventory.nifiEL[inner] = [];
          deepPropertyInventory.nifiEL[inner].push({ processor: p.name, group: p.group, property: keys[vi] });
        });
      }

      // CRON expressions
      if (vLen > 8 && _RE.cronExpr.test(v)) {
        if (!deepPropertyInventory.cronExprs[v.trim()]) deepPropertyInventory.cronExprs[v.trim()] = [];
        deepPropertyInventory.cronExprs[v.trim()].push({ processor: p.name, group: p.group, property: keys[vi] });
      }

      // Credential references
      if (_RE.credentialKey.test(keys[vi])) {
        const sanitized = v.length > 3 ? v.substring(0, 2) + '***' + v.substring(v.length - 1) : '***';
        if (!deepPropertyInventory.credentialRefs[keys[vi]]) deepPropertyInventory.credentialRefs[keys[vi]] = [];
        deepPropertyInventory.credentialRefs[keys[vi]].push({ processor: p.name, group: p.group, value: sanitized });
      }

      // Host:port patterns
      if (vLen > 5) {
        _RE.hostPort.lastIndex = 0;
        let hm;
        while ((hm = _RE.hostPort.exec(v)) !== null) {
          const hp = hm[2] ? hm[1] + ':' + hm[2] : hm[1];
          if (!/\.(css|js|html|png|jpg|gif|svg|ico|woff|ttf|eot)$/i.test(hp) &&
              !/^(www\.|http|example\.com|localhost)/i.test(hm[1])) {
            if (!deepPropertyInventory.hostPorts[hp]) deepPropertyInventory.hostPorts[hp] = [];
            deepPropertyInventory.hostPorts[hp].push({ processor: p.name, group: p.group, property: keys[vi] });
          }
        }
      }

      // Data format detection
      if (_RE.dataFormat.test(v)) {
        const fmts = v.match(_RE.dataFormat);
        if (fmts) fmts.forEach(f => deepPropertyInventory.dataFormats.add(f.toLowerCase()));
      }
    }

    // Scheduling info capture (CRON_DRIVEN processors)
    if (p.schedulingStrategy === 'CRON_DRIVEN' && p.schedulingPeriod) {
      if (!deepPropertyInventory.cronExprs[p.schedulingPeriod]) deepPropertyInventory.cronExprs[p.schedulingPeriod] = [];
      deepPropertyInventory.cronExprs[p.schedulingPeriod].push({ processor: p.name, group: p.group, property: 'schedulingPeriod' });
    }
  });

  return deepPropertyInventory;
}
