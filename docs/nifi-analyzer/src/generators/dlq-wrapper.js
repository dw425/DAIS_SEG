/**
 * generators/dlq-wrapper.js — Dead-Letter Queue Error Routing Wrapper
 *
 * GAP FIX: Wraps processor code with per-record DLQ routing. Failed records
 * are written to a __dead_letter_queue Delta table while successful records
 * continue downstream. Integrates PHI-safe DLQ writes when PHI fields
 * are detected.
 *
 * Based on index.html FIX #4 (lines 3939-3987), enhanced with PHI detection.
 *
 * @module generators/dlq-wrapper
 */

/**
 * Wrap processor code with dead-letter queue error routing.
 *
 * Only wraps processors that perform real transformations (detected via
 * regex on the code body). Passthrough/logging processors are returned
 * unchanged.
 *
 * When PHI fields are detected (if detectPHIFields and generatePHISafeDLQ
 * are available), DLQ writes are wrapped to redact sensitive fields.
 *
 * @param {string} code — generated PySpark code
 * @param {string} procName — processor name
 * @param {string} varName — sanitized variable name
 * @param {string} inputVar — upstream input variable name (without df_ prefix)
 * @param {string} [qualifiedSchema='nifi_migration'] — fully qualified schema name
 * @param {Object} [options] — optional configuration
 * @param {Function} [options.detectPHIFields] — PHI field detector function
 * @param {Function} [options.generatePHISafeDLQ] — PHI-safe DLQ generator function
 * @param {Object} [options.props] — NiFi processor properties
 * @returns {string} — code with DLQ wrapper (or unchanged)
 */
export function generateDLQWrapper(code, procName, varName, inputVar, qualifiedSchema, options) {
  // Only wrap processors that do real transformations (not passthrough/logging)
  const isTransform = /\.withColumn|regexp_replace|from_json|to_json|\.filter|\.join|\.groupBy|\.agg|spark\.read|requests\.|subprocess\.run|\.format\("jdbc"\)/.test(code);
  if (!isTransform) return code;

  const safeProc = procName.replace(/[^a-zA-Z0-9_]/g, '_');
  const schema = qualifiedSchema || 'nifi_migration';
  const indented = code.split('\n').map(l => '        ' + l).join('\n');
  const indented2 = code.split('\n').map(l => '                ' + l).join('\n').replace(new RegExp(`df_${inputVar}`, 'g'), '_single_df');

  // Check for PHI fields if detector is provided
  const opts = options || {};
  let dlqWriteCode;
  if (opts.detectPHIFields && opts.generatePHISafeDLQ && opts.props) {
    const phiFields = opts.detectPHIFields(opts.props);
    if (phiFields && phiFields.length > 0) {
      dlqWriteCode = opts.generatePHISafeDLQ(procName, schema, phiFields);
    }
  }

  // Default DLQ write code (without PHI redaction)
  if (!dlqWriteCode) {
    dlqWriteCode =
      '        if _failed_records:\n' +
      '            _dlq_df = spark.createDataFrame(_failed_records)\n' +
      '            _dlq_df.write.format("delta").mode("append").saveAsTable("' + schema + '.__dead_letter_queue")\n' +
      '            print(f"[DLQ] ' + procName + ': {len(_failed_records)} records routed to DLQ")';
  }

  return '# ' + procName + ' \u2014 with dead-letter queue routing\n' +
    'from pyspark.sql.functions import lit, current_timestamp, struct, to_json, col\n' +
    'from datetime import datetime\n\n' +
    'def _process_' + safeProc + '(df_input):\n' +
    '    """Process with per-record error routing to DLQ"""\n' +
    '    _success_records = []\n' +
    '    _failed_records = []\n\n' +
    '    try:\n' +
    '        # Attempt batch processing first (fast path)\n' +
    indented + '\n' +
    '        return df_' + varName + '  # Success\n' +
    '    except Exception as _batch_err:\n' +
    '        print(f"[DLQ] Batch failed for ' + procName + ': {_batch_err}")\n' +
    '        print(f"[DLQ] Falling back to per-record processing...")\n\n' +
    '        _rows = df_input.collect()\n' +
    '        for _row in _rows:\n' +
    '            try:\n' +
    '                _single_df = spark.createDataFrame([_row], schema=df_input.schema)\n' +
    indented2 + '\n' +
    '                _success_records.append(df_' + varName + '.collect()[0])\n' +
    '            except Exception as _row_err:\n' +
    '                _failed_records.append({\n' +
    '                    "source_processor": "' + procName + '",\n' +
    '                    "error": str(_row_err)[:500],\n' +
    '                    "record_data": str(_row.asDict())[:1000],\n' +
    '                    "timestamp": str(datetime.now())\n' +
    '                })\n\n' +
    '        # Write failed records to Dead Letter Queue\n' +
    dlqWriteCode + '\n\n' +
    '        if _success_records:\n' +
    '            return spark.createDataFrame(_success_records, schema=df_input.schema)\n' +
    '        else:\n' +
    '            return spark.createDataFrame([], schema=df_input.schema)\n\n' +
    'df_' + varName + ' = _process_' + safeProc + '(df_' + inputVar + ')\n' +
    'print(f"[OK] ' + procName + ': {df_' + varName + '.count()} records processed")';
}
