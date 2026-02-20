/**
 * generators/subprocess-wrapper.js — subprocess.run() to Pandas UDF wrapper
 *
 * Converts driver-side subprocess.run() calls into distributed Pandas UDF
 * execution when the NiFi processor operates on flowfile content per-row.
 *
 * Extracted from index.html lines 3622-3653.
 *
 * @module generators/subprocess-wrapper
 */

/**
 * Wrap subprocess.run() code in a Pandas UDF for distributed execution.
 *
 * If the processor's Input Source references flowfile content, the subprocess
 * call is wrapped in a @pandas_udf that executes on workers (not driver).
 * Single-execution (non-per-row) code is returned unchanged.
 *
 * @param {string} code — generated PySpark code
 * @param {string} procName — processor name
 * @param {string} varName — sanitized variable name
 * @param {string} inputVar — upstream DataFrame variable name (without df_ prefix)
 * @param {Object} [props] — NiFi processor properties
 * @returns {string} — code with Pandas UDF wrapper (or unchanged)
 */
export function wrapSubprocessAsPandasUDF(code, procName, varName, inputVar, props) {
  if (!code.includes('subprocess.run') && !code.includes('_sp.run')) return code;
  // Detect if this is per-row execution (Input Source = flowfile content)
  const inputSource = (props || {})['Input Source'] || '';
  if (inputSource.toLowerCase().includes('flowfile') || inputSource.toLowerCase().includes('content')) {
    // Per-row: wrap in pandas_udf for distributed execution
    const cmdMatch = code.match(/(?:subprocess\.run|_sp\.run)\(\s*(?:"([^"]+)"|'([^']+)'|\[([^\]]+)\])/);
    const cmd = cmdMatch ? (cmdMatch[1] || cmdMatch[2] || cmdMatch[3]) : 'echo';
    return `# ${procName} — distributed via Pandas UDF (per-row execution)\n` +
      `from pyspark.sql.functions import pandas_udf, col\n` +
      `import pandas as pd\nimport subprocess\n\n` +
      `@pandas_udf("string")\n` +
      `def _exec_cmd_${varName}(rows: pd.Series) -> pd.Series:\n` +
      `    """Execute command per row — runs on workers, not driver"""\n` +
      `    results = []\n` +
      `    for row_val in rows:\n` +
      `        try:\n` +
      `            _r = subprocess.run(${JSON.stringify(cmd)}.split(), input=str(row_val), capture_output=True, text=True, timeout=30)\n` +
      `            results.append(_r.stdout.strip() if _r.returncode == 0 else f"ERROR: {_r.stderr[:100]}")\n` +
      `        except Exception as e:\n` +
      `            results.append(f"ERROR: {str(e)[:100]}")\n` +
      `    return pd.Series(results)\n\n` +
      `df_${varName} = df_${inputVar}.withColumn("_cmd_result", _exec_cmd_${varName}(col("value")))\n` +
      `print(f"[CMD] Distributed execution via Pandas UDF")`;
  }
  // Single-execution: keep driver-side (unchanged)
  return code;
}
