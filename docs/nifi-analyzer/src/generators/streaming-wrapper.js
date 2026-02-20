/**
 * generators/streaming-wrapper.js — Batch Sink Streaming Guard
 *
 * Converts batch-only DataFrame operations (.toPandas(), .collect(),
 * .write.format(), .saveAsTable(), .count(), .show()) into their
 * streaming-safe equivalents when running in a streaming context.
 *
 * Extracted from index.html lines 3863-3938.
 *
 * @module generators/streaming-wrapper
 */

import {
  _BATCH_SINK_PATTERN
} from '../constants/streaming-types.js';

/**
 * Wrap batch-only sink code for streaming-safe execution.
 *
 * Transforms:
 * - for row in df.collect() -> foreachBatch pattern
 * - .toPandas().to_dict() -> foreachBatch
 * - df.count() -> warning comment
 * - .show() / display() -> warning comment
 * - .write.format() -> .writeStream.format() with checkpoint
 * - .saveAsTable() -> .writeStream...toTable() with checkpoint
 * - Missing checkpointLocation -> auto-inject
 *
 * @param {string} code — generated PySpark code
 * @param {string} procName — processor name
 * @param {string} varName — sanitized variable name
 * @param {string} inputVar — upstream input variable name (without df_ prefix)
 * @param {boolean} isStreamingContext — whether the pipeline is streaming
 * @returns {string} — streaming-safe code (or unchanged if not streaming)
 */
export function wrapBatchSinkForStreaming(code, procName, varName, inputVar, isStreamingContext) {
  if (!isStreamingContext) return code;
  if (!_BATCH_SINK_PATTERN.test(code)) return code;

  let fixed = code;
  // Replace for row in df_X.limit(N).collect() -> foreachBatch pattern
  if (/for\s+row\s+in\s+df_\w+/.test(fixed)) {
    // Note: matches generated code patterns only (no user comments expected)
    const loopStart = fixed.indexOf('for row');
    if (loopStart >= 0) {
      const beforeLoop = fixed.substring(0, loopStart);
      const loopBody = fixed.substring(loopStart);
      const indentedBody = loopBody.split('\n').map(l => '    ' + l).join('\n');
      fixed = beforeLoop +
        '# Streaming-safe sink using foreachBatch\n' +
        'def _process_batch_' + varName + '(batch_df, batch_id):\n' +
        '    """Process each micro-batch (streaming-safe)"""\n' +
        indentedBody.replace(/df_\w+\.(?:limit\(\d+\)\.)?collect\(\)/g, 'batch_df.collect()') + '\n\n' +
        '(df_' + inputVar + '.writeStream\n' +
        '    .foreachBatch(_process_batch_' + varName + ')\n' +
        '    .option("checkpointLocation", "/Volumes/<catalog>/<schema>/data/checkpoints/' + varName + '")\n' +
        '    .trigger(processingTime="10 seconds")\n' +
        '    .start()\n)';
    }
  }
  // Replace .toPandas().to_dict() -> foreachBatch
  if (/\.toPandas\(\)\.to_dict/.test(fixed)) {
    fixed = fixed.replace(
      /df_(\w+)(?:\.limit\(\d+\))?\.toPandas\(\)\.to_dict\(orient="records"\)/g,
      'None  # Resolved in foreachBatch below'
    );
    fixed = '# Streaming-safe: collect via foreachBatch, not .toPandas()\n' +
      'def _process_batch_' + varName + '(batch_df, batch_id):\n' +
      '    _records = batch_df.toPandas().to_dict(orient="records")\n' +
      '    # Process _records here\n' +
      '    return _records\n\n' + fixed;
  }
  // df_X.count() on streaming -> warning
  fixed = fixed.replace(/df_(\w+)\.count\(\)/g, '0  # Cannot call .count() on streaming DataFrame; use watermark/window aggregation');
  // .show() / .display() on streaming -> warning
  fixed = fixed.replace(/df_(\w+)\.show\([^)]*\)/g, '# Cannot call .show() on streaming DataFrame — use display() in notebook');
  fixed = fixed.replace(/display\(df_(\w+)\)/g, '# display() streams automatically in Databricks notebooks');
  // .write.format() -> .writeStream.format() with checkpoint
  if (/\.write\.format\(/.test(fixed) && !/\.writeStream/.test(fixed)) {
    fixed = fixed.replace(
      /(\w+)\.write\.format\(([^)]+)\)((?:\s*\.option\([^)]+\))*)\s*\.(?:save|mode)\(/g,
      function(match, df, fmt, opts) {
        return df + '.writeStream.format(' + fmt + ')' + opts +
          '\n    .option("checkpointLocation", "/Volumes/<catalog>/<schema>/checkpoints/' + varName + '")\n    .trigger(availableNow=True)\n    .start(';
      }
    );
  }
  // .saveAsTable() -> .writeStream...toTable() with checkpoint
  if (/\.write\.(?:mode\([^)]+\)\.)?saveAsTable\(/.test(fixed) && !/\.writeStream/.test(fixed)) {
    fixed = fixed.replace(
      /\.write\.(?:mode\([^)]+\)\.)?saveAsTable\(([^)]+)\)/g,
      '.writeStream\n    .option("checkpointLocation", "/Volumes/<catalog>/<schema>/checkpoints/' + varName + '")\n    .trigger(availableNow=True)\n    .toTable($1)'
    );
  }
  // Auto-inject checkpoint for writeStream that's missing it
  if (/\.writeStream/.test(fixed) && !/checkpointLocation/.test(fixed)) {
    fixed = fixed.replace(
      /\.writeStream/g,
      '.writeStream\n    .option("checkpointLocation", "/Volumes/<catalog>/<schema>/checkpoints/' + varName + '")'
    );
  }

  return fixed;
}
