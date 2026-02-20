/**
 * validators/index.js — Validation engine orchestrator
 *
 * Extracted from index.html lines 8206-8714.
 * Coordinates all four validation analyses (intent, line, reverse-engineering,
 * function mapping) and aggregates results into an overall validation score.
 *
 * @module validators
 */

import { analyzeIntent } from './intent-analyzer.js';
import { validateLines } from './line-validator.js';
import { checkReverseEngineering } from './reverse-engineering.js';
import { mapFunctions } from './function-mapper.js';
import { generateFeedback } from './accelerator-feedback.js';

/**
 * Run the full validation engine across all four dimensions.
 *
 * @param {object} opts
 * @param {object} opts.nifi           - Parsed NiFi flow object
 * @param {Array}  opts.mappings       - Assessment mappings
 * @param {Array}  opts.cells          - Generated notebook cells
 * @param {object} opts.systems        - Detected external systems map
 * @param {object} [opts.nifiDatabricksMap] - NIFI_DATABRICKS_MAP for feedback
 * @param {Function} [opts.onProgress] - (pct, msg) => void
 * @returns {Promise<object>} Full validation results
 */
export async function runValidationEngine({
  nifi,
  mappings,
  cells,
  systems,
  nifiDatabricksMap,
  onProgress,
}) {
  const progress = onProgress || (() => {});

  progress(2, 'Building lookup indexes...');
  await new Promise(r => setTimeout(r, 0));

  // Build O(1) lookup indexes
  const procByName = {};
  nifi.processors.forEach(p => { procByName[p.name] = p; });
  const mappingByName = {};
  mappings.forEach(m => { mappingByName[m.name] = m; });

  // Pre-index cell text for fast searching
  const cellTextsLower = cells.map(c => (c.source || '').toLowerCase());
  const allCellTextLower = cellTextsLower.join('\n');

  // Fast cell search helper
  function findCellsWithVar(varName) {
    const matches = [];
    for (let i = 0; i < cellTextsLower.length; i++) {
      if (cellTextsLower[i].includes(varName)) matches.push(i);
    }
    return matches;
  }

  progress(5, 'Building connection graph...');
  await new Promise(r => setTimeout(r, 0));

  const connMap = {};
  nifi.connections.forEach(c => {
    if (!connMap[c.sourceName]) connMap[c.sourceName] = [];
    connMap[c.sourceName].push(c);
  });

  // ── ANALYSIS 1: Intent ──
  progress(8, 'Running intent analysis (' + nifi.processors.length + ' processors)...');
  const intentResult = await analyzeIntent({
    processors: nifi.processors,
    mappings,
    mappingByName,
    allCellTextLower,
    onProgress: progress,
  });

  // ── ANALYSIS 2: Line Validation ──
  progress(30, 'Running line validation (' + mappings.length + ' mappings)...');
  const lineResult = await validateLines({
    mappings,
    procByName,
    cellTextsLower,
    findCellsWithVar,
    onProgress: progress,
  });

  // ── ANALYSIS 3: Reverse Engineering ──
  progress(58, 'Running reverse engineering readiness checks...');
  const reResult = await checkReverseEngineering({
    nifi,
    systems,
    allCellTextLower,
    onProgress: progress,
  });

  // ── ANALYSIS 4: Function Mapping ──
  progress(78, 'Running function mapping analysis...');
  const funcResult = await mapFunctions({
    mappings,
    procByName,
    onProgress: progress,
  });

  // ── Overall Score ──
  progress(95, 'Computing overall score...');
  await new Promise(r => setTimeout(r, 0));

  const overallScore = Math.round(
    (intentResult.intentScore + lineResult.lineScore + reResult.reScore + funcResult.funcScore) / 4
  );

  // ── Accelerator Feedback ──
  const lineGapItems = lineResult.lineResults.filter(lr => lr.status !== 'good');
  const feedback = generateFeedback({
    intentGaps: intentResult.intentGaps,
    lineGapItems,
    nifiDatabricksMap,
  });

  // ── ANALYSIS 5: Import Validation ──
  progress(95, 'Checking notebook imports...');
  const missingImports = validateImports(cells);

  progress(100, 'Validation complete!');

  return {
    overallScore,
    intentScore: intentResult.intentScore,
    lineScore: lineResult.lineScore,
    reScore: reResult.reScore,
    funcScore: funcResult.funcScore,
    intentGaps: intentResult.intentGaps,
    nifiIntents: intentResult.nifiIntents,
    intentMatched: intentResult.intentMatched,
    intentPartial: intentResult.intentPartial,
    intentMissing: intentResult.intentMissing,
    lineResults: lineResult.lineResults,
    lineMatched: lineResult.lineMatched,
    lineGaps: lineResult.lineGaps,
    reChecks: reResult.reChecks,
    funcResults: funcResult.funcResults,
    funcMapped: funcResult.funcMapped,
    funcPartial: funcResult.funcPartial,
    funcMissing: funcResult.funcMissing,
    allGaps: feedback.allGaps,
    gapsByType: feedback.gapsByType,
    connMap,
    missingImports,
    timestamp: new Date().toISOString(),
  };
}

/**
 * Scan notebook cells for API usage that lacks a corresponding import.
 * @param {Array} cells - Generated notebook cells
 * @returns {Array<{symbol:string, usedIn:number, suggestion:string}>}
 */
function validateImports(cells) {
  const IMPORT_PATTERNS = [
    { pattern: /\bspark\.read/i, symbol: 'SparkSession', suggestion: 'from pyspark.sql import SparkSession' },
    { pattern: /\bcol\s*\(/i, symbol: 'col', suggestion: 'from pyspark.sql.functions import col' },
    { pattern: /\blit\s*\(/i, symbol: 'lit', suggestion: 'from pyspark.sql.functions import lit' },
    { pattern: /\bwhen\s*\(/i, symbol: 'when', suggestion: 'from pyspark.sql.functions import when' },
    { pattern: /\bexpr\s*\(/i, symbol: 'expr', suggestion: 'from pyspark.sql.functions import expr' },
    { pattern: /\bstruct\s*\(/i, symbol: 'struct', suggestion: 'from pyspark.sql.functions import struct' },
    { pattern: /\barray\s*\(/i, symbol: 'array', suggestion: 'from pyspark.sql.functions import array' },
    { pattern: /\bfrom_json\s*\(/i, symbol: 'from_json', suggestion: 'from pyspark.sql.functions import from_json' },
    { pattern: /\bto_json\s*\(/i, symbol: 'to_json', suggestion: 'from pyspark.sql.functions import to_json' },
    { pattern: /\bStructType\s*\(/i, symbol: 'StructType', suggestion: 'from pyspark.sql.types import StructType' },
    { pattern: /\bStructField\s*\(/i, symbol: 'StructField', suggestion: 'from pyspark.sql.types import StructField' },
    { pattern: /\bStringType\s*\(/i, symbol: 'StringType', suggestion: 'from pyspark.sql.types import StringType' },
    { pattern: /\bIntegerType\s*\(/i, symbol: 'IntegerType', suggestion: 'from pyspark.sql.types import IntegerType' },
    { pattern: /\bpandas_udf/i, symbol: 'pandas_udf', suggestion: 'from pyspark.sql.functions import pandas_udf' },
    { pattern: /\bWindow\./i, symbol: 'Window', suggestion: 'from pyspark.sql.window import Window' },
    { pattern: /\bjson\.loads/i, symbol: 'json', suggestion: 'import json' },
    { pattern: /\bre\./i, symbol: 're', suggestion: 'import re' },
    { pattern: /\bdatetime\./i, symbol: 'datetime', suggestion: 'from datetime import datetime' },
    { pattern: /\bbase64\./i, symbol: 'base64', suggestion: 'import base64' },
    { pattern: /\bhashlib\./i, symbol: 'hashlib', suggestion: 'import hashlib' },
    { pattern: /\bdbutils\.secrets\.get\b/i, symbol: 'dbutils', lib: 'databricks-sdk', note: 'Databricks secrets API' },
    { pattern: /\bpd\.\w+|pandas\.\w+/i, symbol: 'pandas', lib: 'pandas', note: 'Pandas DataFrame operations' },
    { pattern: /\buuid\b/i, symbol: 'uuid', lib: 'uuid', note: 'UUID generation' },
    { pattern: /\bhashlib\b/i, symbol: 'hashlib', lib: 'hashlib', note: 'Hash functions' },
  ];

  const allText = cells.map(c => c.source || '').join('\n');
  const importLines = allText.split('\n').filter(l => /^\s*(import |from .+ import )/.test(l)).join('\n');
  const missing = [];

  for (const { pattern, symbol, suggestion } of IMPORT_PATTERNS) {
    if (pattern.test(allText) && !importLines.includes(symbol)) {
      const usedIn = cells.findIndex(c => pattern.test(c.source || ''));
      missing.push({ symbol, usedIn, suggestion });
    }
  }

  return missing;
}
