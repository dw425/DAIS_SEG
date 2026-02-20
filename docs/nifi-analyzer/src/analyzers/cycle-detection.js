/**
 * analyzers/cycle-detection.js — Tarjan's SCC algorithm for cycle detection
 *
 * Extracted from index.html lines 6053-6069.
 *
 * @module analyzers/cycle-detection
 */

/**
 * Detect cycles in a directed graph using Tarjan's Strongly Connected
 * Components (SCC) algorithm. Returns SCCs with more than one node,
 * which represent cycles.
 *
 * @param {object} adjacencyMap — { nodeId: [neighborId, ...], ... }
 * @returns {Array<string[]>} — array of SCCs (each is an array of node IDs)
 */
export function detectCyclesSCC(adjacencyMap) {
  let idx = 0;
  const stack = [];
  const onStack = new Set();
  const indices = {};
  const lowlinks = {};
  const sccs = [];

  function sc(v) {
    indices[v] = lowlinks[v] = idx++;
    stack.push(v);
    onStack.add(v);
    for (const w of (adjacencyMap[v] || [])) {
      if (indices[w] === undefined) {
        sc(w);
        lowlinks[v] = Math.min(lowlinks[v], lowlinks[w]);
      } else if (onStack.has(w)) {
        lowlinks[v] = Math.min(lowlinks[v], indices[w]);
      }
    }
    if (lowlinks[v] === indices[v]) {
      const scc = [];
      let w;
      do {
        w = stack.pop();
        onStack.delete(w);
        scc.push(w);
      } while (w !== v);
      if (scc.length > 1) sccs.push(scc);
    }
  }

  for (const v of Object.keys(adjacencyMap)) {
    if (indices[v] === undefined) sc(v);
  }
  return sccs;
}
