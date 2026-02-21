/**
 * generators/cell-builders/imports-cell.js — Smart Import Manager
 *
 * Scans all processor mappings and auto-collects the required imports
 * (PySpark, Python stdlib, Databricks, third-party) into a single
 * consolidated imports cell.
 *
 * Extracted from index.html lines 5432-5472.
 *
 * @module generators/cell-builders/imports-cell
 */

// Known Python standard library modules (not pip-installable)
const _STDLIB_MODULES = new Set([
  'pyspark.sql.functions', 'pyspark.sql.types', 'pyspark.sql.streaming',
  'pyspark.sql.window', 'pyspark.sql', 'pyspark',
  'datetime', 'json', 'logging', 'os', 're', 'hashlib', 'base64',
  'subprocess', 'mimetypes', 'socket', 'smtplib', 'email', 'functools',
  'xml.etree', 'csv', 'io', 'collections', 'math', 'sys', 'time',
  'typing', 'uuid', 'pathlib', 'struct', 'itertools', 'urllib',
]);

/**
 * Collect and consolidate smart imports from all processor mappings.
 *
 * Scans each mapping's .imports array and .code body to detect
 * required libraries. Groups them by category for clean output.
 * Also collects third-party pip packages from imp arrays.
 *
 * @param {Array<Object>} mappings — processor mappings
 * @param {Object} nifi — parsed NiFi flow (unused, reserved for future)
 * @returns {{ code: string, all: Object, pipCell: Object|null }} — formatted import block, raw sets, and optional pip cell
 */
export function collectSmartImports(mappings, nifi) {
  const imports = {
    pyspark: new Set(['from pyspark.sql.functions import *', 'from pyspark.sql.types import *']),
    python: new Set(['from datetime import datetime, timedelta', 'import json', 'import logging']),
    databricks: new Set(),
    thirdParty: new Set()
  };
  const pipPackages = new Set();
  mappings.forEach(m => {
    (m.imports || []).forEach(imp => {
      if (imp.includes('pyspark')) imports.pyspark.add(imp);
      else if (imp.includes('dbutils') || imp.includes('databricks')) imports.databricks.add(imp);
      else imports.python.add(imp);
      // Detect pip-installable packages from import strings
      const modRoot = imp.replace(/^(?:from|import)\s+/, '').split(/[\s.]/)[0];
      if (modRoot && !_STDLIB_MODULES.has(modRoot) && !modRoot.startsWith('pyspark') && !modRoot.startsWith('dbutils') && !modRoot.startsWith('databricks')) {
        pipPackages.add(modRoot);
      }
    });
    if (m.code) {
      if (m.code.includes('requests.')) { imports.thirdParty.add('import requests'); pipPackages.add('requests'); }
      if (m.code.includes('subprocess')) imports.python.add('import subprocess');
      if (m.code.includes('re.')) imports.python.add('import re');
      if (m.code.includes('os.')) imports.python.add('import os');
      if (m.code.includes('hashlib')) imports.python.add('import hashlib');
      if (m.code.includes('base64')) imports.python.add('import base64');
      if (m.code.includes('urllib.parse')) imports.python.add('import urllib.parse');
      if (m.code.includes('xml.etree')) imports.python.add('import xml.etree.ElementTree as ET');
      if (m.code.includes('readStream') || m.code.includes('writeStream'))
        imports.pyspark.add('from pyspark.sql.streaming import StreamingQuery');
      if (m.code.includes('Window'))
        imports.pyspark.add('from pyspark.sql.window import Window');
      if (m.code.includes('paramiko')) pipPackages.add('paramiko');
      if (m.code.includes('geoip2')) pipPackages.add('geoip2');
      if (m.code.includes('lxml')) pipPackages.add('lxml');
      if (m.code.includes('gnupg')) pipPackages.add('python-gnupg');
      if (m.code.includes('tenacity')) pipPackages.add('tenacity');
      if (m.code.includes('tweepy')) pipPackages.add('tweepy');
      if (m.code.includes('BeautifulSoup') || m.code.includes('bs4')) pipPackages.add('beautifulsoup4');
      if (m.code.includes('cryptography')) pipPackages.add('cryptography');
    }
  });
  let code = '# \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\n# IMPORTS \u2014 Auto-collected from all processors\n# \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\n\n';
  code += '# PySpark\n' + [...imports.pyspark].filter(i => !i.startsWith('#')).sort().join('\n');
  const pyImps = [...imports.python].filter(i => !i.startsWith('#')).sort();
  if (pyImps.length) code += '\n\n# Python Standard Library\n' + pyImps.join('\n');
  const dbxImps = [...imports.databricks].filter(i => !i.startsWith('#')).sort();
  if (dbxImps.length) code += '\n\n# Databricks\n' + dbxImps.join('\n');
  const tpImps = [...imports.thirdParty].filter(i => !i.startsWith('#')).sort();
  if (tpImps.length) code += '\n\n# Third-party\n' + tpImps.join('\n');

  // Build pip install cell if there are third-party packages
  let pipCell = null;
  if (pipPackages.size > 0) {
    const sortedPkgs = [...pipPackages].sort();
    pipCell = {
      type: 'code',
      role: 'setup',
      label: 'Install Dependencies',
      source: '# Install required packages\n' + sortedPkgs.map(p => `%pip install ${p}`).join('\n') + '\ndbutils.library.restartPython()'
    };
  }

  return { code, all: imports, pipCell };
}
