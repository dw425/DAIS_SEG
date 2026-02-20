/**
 * generators/adaptive-code.js — Adaptive Process Code
 *
 * Generates adaptive format-detection code for source processors,
 * allowing automatic detection of CSV, JSON, Parquet, etc.
 *
 * Extracted from index.html lines 5512-5534.
 *
 * @module generators/adaptive-code
 */

import { sanitizeVarName } from '../utils/string-helpers.js';

/**
 * Generate adaptive code for a processor mapping.
 *
 * For source processors, prepends a format-detection function that
 * inspects file extensions and magic bytes to auto-detect the input format.
 * Non-source processors return their code unchanged.
 *
 * @param {Object} m — processor mapping
 * @param {Object} lineage — DataFrame lineage map
 * @param {string} qualifiedSchema — fully qualified schema name
 * @returns {string} — the (possibly enhanced) code
 */
export function generateAdaptiveCode(m, lineage, qualifiedSchema) {
  const varName = sanitizeVarName(m.name);
  if (m.role === 'source' && m.mapped && m.code) {
    return '# Adaptive format detection for ' + m.name + '\n' +
      'def _detect_format_' + varName + '(path):\n' +
      '    import os\n' +
      '    ext = os.path.splitext(path)[1].lower() if path else ""\n' +
      '    fmt_map = {".csv":"csv",".tsv":"csv",".json":"json",".jsonl":"json",\n' +
      '               ".parquet":"parquet",".avro":"avro",".orc":"orc",".xml":"xml"}\n' +
      '    if ext in fmt_map: return fmt_map[ext]\n' +
      '    try:\n' +
      '        head = dbutils.fs.head(path, 4)\n' +
      '        if head.startswith("PAR1"): return "parquet"\n' +
      '        if head.startswith("{"): return "json"\n' +
      '    except: pass\n' +
      '    return "csv"\n\n' + m.code;
  }
  return m.code;
}
