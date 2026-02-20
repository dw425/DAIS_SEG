// ================================================================
// sql-extractor.js — Extract SQL table/column metadata from SQL text
// Extracted from monolith lines 1075-1110
// GAP FIX: Impala->Spark SQL dialect conversion
// ================================================================

import { SQL_NOISE } from '../constants/sql-noise.js';

/**
 * Track a SQL table reference with its role (read/write) and context.
 * Filters out noise words, expression-language-only CTE aliases, etc.
 *
 * @param {string} tn - Raw table name
 * @param {string} role - 'read' or 'write'
 * @param {string} proc - Processor name that references the table
 * @param {string} sqlCtx - SQL context snippet
 * @param {Set} sqlTables - Accumulator set of table names
 * @param {Object} sqlTableMeta - Accumulator map of table metadata
 */
export function addSqlTableMeta(tn, role, proc, sqlCtx, sqlTables, sqlTableMeta) {
  if (!tn || tn.length < 2 || SQL_NOISE.has(tn) || SQL_NOISE.has(tn.toLowerCase())) return;
  const cleaned = tn.replace(/^["'`]+|["'`]+$/g, '').trim();
  if (!cleaned || cleaned.startsWith('(')) return;
  // Skip NiFi Expression Language CTE aliases
  if (/^\$\{.*\}$/.test(cleaned) && !/\$\{(external_table|staging_table|prod_table|query|script)/.test(cleaned)) return;
  sqlTables.add(cleaned);
  if (!sqlTableMeta[cleaned]) sqlTableMeta[cleaned] = { readers:[], writers:[], columns: new Set(), sqlContexts:[], parameterized: cleaned.includes('${') };
  if (role === 'read') sqlTableMeta[cleaned].readers.push(proc);
  else sqlTableMeta[cleaned].writers.push(proc);
  if (sqlCtx) sqlTableMeta[cleaned].sqlContexts.push(sqlCtx);
}

/**
 * Extract column names from a SQL statement (SELECT list, WHERE clause, INSERT column list).
 * @param {string} sql - SQL statement text
 * @returns {Set<string>} Set of lowercase column names
 */
export function extractColumnsFromSQL(sql) {
  const cols = new Set();
  // Extract from SELECT columns
  const selMatch = sql.match(/SELECT\s+([\s\S]*?)\s+FROM/i);
  if (selMatch && selMatch[1] && selMatch[1].trim() !== '*') {
    selMatch[1].split(',').forEach(c => {
      const col = c.trim().replace(/.*\s+AS\s+/i,'').replace(/.*\./,'').replace(/[()]/g,'').trim();
      if (col && col !== '*' && col.length < 60 && !/^(count|sum|avg|min|max|nvl|concat|coalesce|cast|case|distinct|group_concat|trim|substr|substring|upper|lower|replace|ifnull|nullif|round|ceil|floor|abs|date_format|current_timestamp|current_date)$/i.test(col.split('(')[0])) cols.add(col.toLowerCase());
    });
  }
  // Extract from WHERE column references
  const whereMatch = sql.match(/WHERE\s+([\s\S]*?)(?:ORDER|GROUP|LIMIT|HAVING|UNION|$)/i);
  if (whereMatch) {
    const parts = whereMatch[1].match(/(\w+)\s*(?:=|<|>|!=|LIKE|IN|IS|BETWEEN)/gi);
    if (parts) parts.forEach(p => {
      const col = p.replace(/\s*(=|<|>|!=|LIKE|IN|IS|BETWEEN).*$/i,'').replace(/.*\./,'').trim();
      if (col && col.length < 50 && !SQL_NOISE.has(col.toLowerCase())) cols.add(col.toLowerCase());
    });
  }
  // Extract from INSERT column list
  const insMatch = sql.match(/INSERT\s+INTO\s+\S+\s*\(([^)]+)\)/i);
  if (insMatch) insMatch[1].split(',').forEach(c => { const col = c.trim(); if (col) cols.add(col.toLowerCase()); });
  return cols;
}

// ================================================================
// GAP FIX: Impala -> Spark SQL dialect conversion
// ================================================================

/**
 * Impala-to-Spark SQL dialect conversion map.
 * Converts Impala-specific statements and functions to Spark SQL equivalents.
 */
const IMPALA_TO_SPARK = [
  // Statement-level conversions
  { pattern: /\bCOMPUTE\s+STATS\s+(\S+)/gi, replacement: 'ANALYZE TABLE $1 COMPUTE STATISTICS' },
  { pattern: /\bCOMPUTE\s+INCREMENTAL\s+STATS\s+(\S+)/gi, replacement: 'ANALYZE TABLE $1 COMPUTE STATISTICS' },
  { pattern: /\bINVALIDATE\s+METADATA\b(?:\s+(\S+))?/gi, replacement: (_, tbl) => tbl ? `REFRESH TABLE ${tbl}` : '-- REFRESH metadata (no Spark equivalent for global invalidate)' },
  { pattern: /\bREFRESH\s+(\S+)/gi, replacement: 'REFRESH TABLE $1' },
  // Function-level conversions
  { pattern: /\bSTRING_AGG\s*\(([^,]+),\s*([^)]+)\)/gi, replacement: 'CONCAT_WS($2, COLLECT_LIST($1))' },
  { pattern: /\bGROUP_CONCAT\s*\(([^,]+),\s*([^)]+)\)/gi, replacement: 'CONCAT_WS($2, COLLECT_LIST($1))' },
  { pattern: /\bGROUP_CONCAT\s*\(([^)]+)\)/gi, replacement: "CONCAT_WS(',', COLLECT_LIST($1))" },
  // Type differences
  { pattern: /\bTINYINT\b/gi, replacement: 'TINYINT' },
  { pattern: /\bSTRING\b/gi, replacement: 'STRING' },
];

/**
 * Convert Impala SQL dialect to Spark SQL.
 * @param {string} sql - Impala SQL statement
 * @returns {string} Spark-compatible SQL
 */
export function convertImpalaSqlToSpark(sql) {
  if (!sql) return sql;
  let result = sql;
  for (const rule of IMPALA_TO_SPARK) {
    result = result.replace(rule.pattern, rule.replacement);
  }
  return result;
}
