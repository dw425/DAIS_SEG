/**
 * generators/cell-builders/execution-report.js — Execution Report Generator Cell Code
 *
 * Generates the Python cell code that queries __execution_log,
 * builds a JSON report, and persists it to __execution_reports.
 *
 * Extracted from index.html lines 5661-5717.
 *
 * @module generators/cell-builders/execution-report
 */

/**
 * Generate the execution report cell code.
 *
 * The generated code:
 * - Queries __execution_log for all processor results
 * - Builds a JSON report with success/failure/recovered counts
 * - Persists the report to __execution_reports
 * - Prints a summary to stdout
 *
 * @param {Array<Object>} mappings — processor mappings
 * @param {string} qualifiedSchema — fully qualified schema name
 * @returns {string} — Python code for the execution report cell
 */
export function generateExecutionReportCell(mappings, qualifiedSchema) {
  return '# \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\n' +
'# EXECUTION REPORT GENERATOR\n' +
'# \u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\n' +
'import json\n' +
'from datetime import datetime\n' +
'\n' +
'_exec_report = {\n' +
'    "pipeline_name": "' + qualifiedSchema + '",\n' +
'    "generated_at": datetime.now().isoformat(),\n' +
'    "total_processors": ' + mappings.length + ',\n' +
'    "mapped_processors": ' + mappings.filter(m => m.mapped).length + ',\n' +
'    "coverage_pct": ' + Math.round(mappings.filter(m => m.mapped).length / Math.max(mappings.length, 1) * 100) + ',\n' +
'    "processors": []\n' +
'}\n' +
'\n' +
'try:\n' +
'    _exec_rows = spark.sql("""\n' +
'        SELECT processor_name, processor_type, role, status, error_message,\n' +
'               rows_processed, duration, confidence, upstream_procs\n' +
'        FROM ' + qualifiedSchema + '.__execution_log ORDER BY timestamp\n' +
'    """).collect()\n' +
'    for _r in _exec_rows:\n' +
'        _exec_report["processors"].append({\n' +
'            "name": _r.processor_name, "type": _r.processor_type,\n' +
'            "role": _r.role, "status": _r.status,\n' +
'            "error": _r.error_message or "", "rows": _r.rows_processed or 0,\n' +
'            "duration": _r.duration or "", "confidence": _r.confidence or 0\n' +
'        })\n' +
'except Exception as _e:\n' +
'    print(f"[WARN] Could not read execution log: {_e}")\n' +
'\n' +
'_successes = len([p for p in _exec_report["processors"] if p.get("status") == "SUCCESS"])\n' +
'_failures = len([p for p in _exec_report["processors"] if p.get("status") == "FAILED"])\n' +
'_recovered = len([p for p in _exec_report["processors"] if p.get("status") in ("RECOVERED","PASSTHROUGH","DLQ")])\n' +
'_exec_report["summary"] = {"successes":_successes,"failures":_failures,"recovered":_recovered,\n' +
'    "success_rate":round(_successes/max(len(_exec_report["processors"]),1)*100,1)}\n' +
'\n' +
'try:\n' +
'    _report_df = spark.createDataFrame([{"report_json":json.dumps(_exec_report),\n' +
'        "generated_at":datetime.now().isoformat(),\n' +
'        "success_rate":_exec_report["summary"]["success_rate"],\n' +
'        "total_procs":_successes+_failures+_recovered}])\n' +
'    _report_df.write.mode("append").saveAsTable("' + qualifiedSchema + '.__execution_reports")\n' +
'except: pass\n' +
'\n' +
'print("=" * 60)\n' +
'print("EXECUTION REPORT")\n' +
'print("=" * 60)\n' +
'print(f"Successes: {_successes} | Failures: {_failures} | Recovered: {_recovered}")\n' +
'print(f"Success Rate: {_exec_report[\\"summary\\"][\\"success_rate\\"]}%")\n' +
'print("=" * 60)';
}
