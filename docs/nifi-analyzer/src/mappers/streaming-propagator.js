/**
 * mappers/streaming-propagator.js -- Streaming context propagation and batch sink wrapping
 *
 * Extracted from index.html lines 3858-3930 and 4008-4027.
 * Tracks which processors are in a streaming context, propagates through connections,
 * and wraps batch operations in foreachBatch when needed.
 */

import {
  _STREAMING_SOURCE_TYPES,
  _BATCH_BREAKING_TYPES
} from '../constants/streaming-types.js';

// Canonical implementation lives in generators/streaming-wrapper.js — re-export here
// so existing imports from this module continue to work.
export { wrapBatchSinkForStreaming } from '../generators/streaming-wrapper.js';

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

