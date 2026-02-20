/**
 * generators/cell-builders/validation-cell.js — End-to-End Validation Cell Code
 *
 * Generates the Python cell code that counts source/sink rows,
 * computes data retention, and persists the validation report.
 *
 * Extracted from index.html lines 5718-5763.
 *
 * @module generators/cell-builders/validation-cell
 */

import { sanitizeVarName } from '../../utils/string-helpers.js';

/**
 * Generate the end-to-end validation cell code.
 *
 * The generated code:
 * - Counts rows in source DataFrames and sink DataFrames
 * - Computes data retention percentage
 * - Persists the validation report to __validation_reports
 * - Prints a summary to stdout
 *
 * @param {Array<Object>} mappings — processor mappings
 * @param {string} qualifiedSchema — fully qualified schema name
 * @param {Object} lineage — DataFrame lineage map
 * @returns {string} — Python code for the validation cell
 */
export function generateValidationCell(mappings, qualifiedSchema, lineage) {
  const sourceProcs = mappings.filter(m => m.role === 'source');
  const sinkProcs = mappings.filter(m => m.role === 'sink');
  let srcChecks = sourceProcs.slice(0, 20).map(s => {
    const vn = 'df_' + sanitizeVarName(s.name);
    return 'try: _source_vars["' + s.name + '"] = ' + vn + '.count()\nexcept: _source_vars["' + s.name + '"] = -1';
  }).join('\n');
  let snkChecks = sinkProcs.slice(0, 20).map(s => {
    const vn = 'df_' + sanitizeVarName(s.name);
    return 'try: _sink_vars["' + s.name + '"] = ' + vn + '.count()\nexcept: _sink_vars["' + s.name + '"] = -1';
  }).join('\n');
  return '# \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\n' +
'# END-TO-END VALIDATION\n' +
'# \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\n' +
'import json\n' +
'_source_vars = {}\n_sink_vars = {}\n' +
srcChecks + '\n' + snkChecks + '\n\n' +
'_validation_report = {\n' +
'    "pipeline": "' + qualifiedSchema + '",\n' +
'    "timestamp": datetime.now().isoformat(),\n' +
'    "source_row_counts": _source_vars,\n' +
'    "sink_row_counts": _sink_vars,\n' +
'    "total_source_rows": sum(v for v in _source_vars.values() if v > 0),\n' +
'    "total_sink_rows": sum(v for v in _sink_vars.values() if v > 0)\n' +
'}\n' +
'_src_total = _validation_report["total_source_rows"]\n' +
'_snk_total = _validation_report["total_sink_rows"]\n' +
'if _src_total > 0 and _snk_total > 0:\n' +
'    _retention = round(_snk_total / _src_total * 100, 1)\n' +
'    _validation_report["data_retention_pct"] = _retention\n' +
'    print(f"Data retention: {_retention}%")\n' +
'\n' +
'try:\n' +
'    _val_df = spark.createDataFrame([{"report_json":json.dumps(_validation_report),\n' +
'        "validated_at":datetime.now().isoformat(),\n' +
'        "source_rows":_src_total,"sink_rows":_snk_total}])\n' +
'    _val_df.write.mode("append").saveAsTable("`' + qualifiedSchema + '`.__validation_reports")\n' +
'except: pass\n' +
'\n' +
'print("=" * 60)\n' +
'print("END-TO-END VALIDATION COMPLETE")\n' +
'print("=" * 60)\n' +
'print(json.dumps(_validation_report, indent=2))';
}
