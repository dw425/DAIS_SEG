/**
 * mappers/streaming-propagator.js -- Streaming context propagation and batch sink wrapping
 *
 * Extracted from index.html lines 3858-3930 and 4008-4027.
 * Tracks which processors are in a streaming context, propagates through connections,
 * and wraps batch operations in foreachBatch when needed.
 */

import {
  _STREAMING_SOURCE_TYPES,
  _BATCH_SINK_PATTERN,
  _BATCH_BREAKING_TYPES
} from '../constants/streaming-types.js';

/**
 * Propagate streaming context through the NiFi connection graph.
 * Starting from streaming source processors, mark all downstream processors
 * as streaming unless they cross a batch-breaking boundary.
 *
 * @param {Array} processors - Array of NiFi processor objects
 * @param {Array} conns - Array of NiFi connection objects
 * @returns {Set<string>} - Set of processor names in streaming context
 */
export function propagateStreamingContext(processors, conns) {
  const streamingProcs = new Set();

  // Seed: mark all streaming source types
  processors.forEach(p => {
    if (_STREAMING_SOURCE_TYPES.test(p.type)) streamingProcs.add(p.name);
  });

  // Build type lookup map
  const procTypeMap = {};
  processors.forEach(p => { procTypeMap[p.name] = p.type; });

  // Propagate through connections (stop at batch-breaking types)
  let changed = true;
  while (changed) {
    changed = false;
    conns.forEach(c => {
      if (streamingProcs.has(c.sourceName) && !streamingProcs.has(c.destinationName)) {
        const destType = procTypeMap[c.destinationName] || '';
        if (_BATCH_BREAKING_TYPES.test(destType)) return; // Streaming stops here
        streamingProcs.add(c.destinationName);
        changed = true;
      }
    });
  }

  return streamingProcs;
}

/**
 * Wrap batch sink operations in foreachBatch when upstream is streaming.
 * Converts .write.format() to .writeStream.format(), .saveAsTable() to .toTable(),
 * and wraps collect()/toPandas() patterns in foreachBatch callbacks.
 *
 * @param {string} code - Generated PySpark code
 * @param {string} procName - Processor name
 * @param {string} varName - Sanitized variable name
 * @param {string} inputVar - Input variable name
 * @param {boolean} isStreamingContext - Whether this processor is in a streaming context
 * @returns {string} - Potentially wrapped code
 */
export function wrapBatchSinkForStreaming(code, procName, varName, inputVar, isStreamingContext) {
  if (!isStreamingContext) return code;
  if (!_BATCH_SINK_PATTERN.test(code)) return code;

  let fixed = code;

  // Replace for row in df_X.limit(N).collect() -> foreachBatch pattern
  if (/for\s+row\s+in\s+df_\w+/.test(fixed)) {
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
        '    .option("checkpointLocation", "/tmp/checkpoints/' + varName + '")\n' +
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
