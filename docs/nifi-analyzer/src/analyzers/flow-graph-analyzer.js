/**
 * analyzers/flow-graph-analyzer.js — Analyze NiFi flow graph for dead ends, orphans, cycles
 *
 * Extracted from index.html lines 3464-3528.
 *
 * @module analyzers/flow-graph-analyzer
 */

/**
 * Analyze a NiFi flow graph for structural issues:
 * - Dead ends (processors with no outgoing connections, excluding sinks)
 * - Orphans (processors with no incoming connections, excluding sources)
 * - Circular references (DFS cycle detection)
 * - Disconnected processors (no connections at all)
 *
 * @param {Array} processors — parsed processors
 * @param {Array} connections — parsed connections
 * @returns {{ deadEnds: Array, orphans: Array, circularRefs: Array, disconnected: Array, backpressure: Array }}
 */
export function analyzeFlowGraph(processors, connections) {
  const result = { deadEnds: [], orphans: [], circularRefs: [], disconnected: [], backpressure: [] };
  if (!processors || !connections) return result;

  const procById = {};
  const procByName = {};
  processors.forEach(p => {
    procById[p.id] = p;
    procByName[p.name] = p;
  });

  const outgoing = {};  // procId -> [targetProcIds]
  const incoming = {};  // procId -> [sourceProcIds]
  processors.forEach(p => {
    outgoing[p.id] = [];
    incoming[p.id] = [];
  });

  connections.forEach(c => {
    const srcId = c.sourceId || (c.source && c.source.id);
    const dstId = c.destId || (c.destination && c.destination.id);
    if (srcId && dstId && outgoing[srcId] && incoming[dstId]) {
      outgoing[srcId].push(dstId);
      incoming[dstId].push(srcId);
    }
  });

  // Dead ends — processors with no outgoing connections (excluding sinks)
  const sinkTypes = /^(Put|Log|Publish|Send|Notify|PutEmail|PutSlack)/;
  processors.forEach(p => {
    if (outgoing[p.id] && outgoing[p.id].length === 0 && !sinkTypes.test(p.type)) {
      result.deadEnds.push({ name: p.name, type: p.type, id: p.id });
    }
  });

  // Orphans — processors with no incoming connections (excluding sources)
  const sourceTypes = /^(Get|List|Listen|Consume|Generate|TailFile|HandleHttp)/;
  processors.forEach(p => {
    if (incoming[p.id] && incoming[p.id].length === 0 && !sourceTypes.test(p.type)) {
      result.orphans.push({ name: p.name, type: p.type, id: p.id });
    }
  });

  // Circular references — DFS cycle detection with deduplication
  const visited = new Set();
  const recStack = new Set();
  const seenCycles = new Set();
  function hasCycle(nodeId, path, depth = 0) {
    if (depth > 2000) return;  // guard against stack overflow
    if (recStack.has(nodeId)) {
      const cycle = path.concat(nodeId).map(id => procById[id] ? procById[id].name : id);
      const cycleKey = [...cycle].sort().join('|');
      if (!seenCycles.has(cycleKey)) {
        seenCycles.add(cycleKey);
        result.circularRefs.push({ cycle });
      }
      return true;
    }
    if (visited.has(nodeId)) return false;
    visited.add(nodeId);
    recStack.add(nodeId);
    for (const next of (outgoing[nodeId] || [])) {
      hasCycle(next, path.concat(nodeId), depth + 1);
    }
    recStack.delete(nodeId);
    return false;
  }
  processors.forEach(p => {
    if (!visited.has(p.id)) hasCycle(p.id, []);
  });

  // Disconnected — processors with no connections at all
  processors.forEach(p => {
    if (outgoing[p.id] && outgoing[p.id].length === 0 && incoming[p.id] && incoming[p.id].length === 0) {
      result.disconnected.push({ name: p.name, type: p.type, id: p.id });
    }
  });

  return result;
}
