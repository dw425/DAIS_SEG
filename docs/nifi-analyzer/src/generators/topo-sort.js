/**
 * generators/topo-sort.js — Connection-Aware Cell Ordering (Topological Sort)
 *
 * Sorts processor mappings in topological order based on NiFi connection graph,
 * so that upstream processors appear before downstream ones in the notebook.
 *
 * Extracted from index.html lines 5473-5511.
 *
 * @module generators/topo-sort
 */

/**
 * Sort mappings in topological order using NiFi connections.
 *
 * Uses Kahn's algorithm with a priority tie-breaker based on role
 * (sources first, sinks last). Processors unreachable from the DAG
 * are appended at the end.
 *
 * @param {Array<Object>} mappings — processor mapping objects
 * @param {Object} nifi — parsed NiFi flow (processors[], connections[])
 * @returns {Array<Object>} — topologically sorted mappings
 */
export function topologicalSortMappings(mappings, nifi) {
  const conns = nifi.connections || [];
  const nameToIdx = {};
  mappings.forEach((m, i) => { nameToIdx[m.name] = i; });
  const adj = {};
  const inDegree = {};
  mappings.forEach(m => { adj[m.name] = []; inDegree[m.name] = 0; });
  conns.forEach(c => {
    if (nameToIdx[c.sourceName] !== undefined && nameToIdx[c.destinationName] !== undefined) {
      adj[c.sourceName].push(c.destinationName);
      inDegree[c.destinationName] = (inDegree[c.destinationName] || 0) + 1;
    }
  });
  const queue = [];
  mappings.forEach(m => { if ((inDegree[m.name] || 0) === 0) queue.push(m.name); });
  const sorted = [];
  const visited = new Set();
  const roleOrd = { source: 0, route: 1, transform: 2, process: 3, sink: 4, utility: 5 };
  while (queue.length) {
    queue.sort((a, b) => {
      const ma = mappings[nameToIdx[a]], mb = mappings[nameToIdx[b]];
      return (roleOrd[ma.role] || 3) - (roleOrd[mb.role] || 3);
    });
    const curr = queue.shift();
    if (visited.has(curr)) continue;
    visited.add(curr);
    sorted.push(mappings[nameToIdx[curr]]);
    (adj[curr] || []).forEach(next => {
      inDegree[next]--;
      if (inDegree[next] <= 0 && !visited.has(next)) queue.push(next);
    });
  }
  mappings.forEach(m => { if (!visited.has(m.name)) sorted.push(m); });
  return sorted;
}
