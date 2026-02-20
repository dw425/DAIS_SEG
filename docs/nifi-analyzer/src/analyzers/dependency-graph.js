/**
 * analyzers/dependency-graph.js — Build upstream/downstream dependency graph
 *
 * Extracted from index.html lines 2994-3001.
 *
 * @module analyzers/dependency-graph
 */

/**
 * Build a full dependency graph (upstream + downstream) for all processors.
 *
 * @param {object} nifi — parsed NiFi flow (processors[], connections[])
 * @returns {{ downstream: object, upstream: object, fullDownstream: object, fullUpstream: object }}
 */
export function buildDependencyGraph(nifi) {
  const procs = nifi.processors || [];
  const conns = nifi.connections || [];
  const downstream = {};
  const upstream = {};

  procs.forEach(p => {
    downstream[p.name] = [];
    upstream[p.name] = [];
  });

  conns.forEach(c => {
    const src = c.sourceName;
    const dst = c.destinationName;
    if (src && dst) {
      if (!downstream[src]) downstream[src] = [];
      if (!upstream[dst]) upstream[dst] = [];
      if (!downstream[src].includes(dst)) downstream[src].push(dst);
      if (!upstream[dst].includes(src)) upstream[dst].push(src);
    }
  });

  function getChain(graph, start) {
    const result = [];
    const visited = new Set();
    const queue = [...(graph[start] || [])];
    while (queue.length) {
      const node = queue.shift();
      if (visited.has(node)) continue;
      visited.add(node);
      result.push(node);
      (graph[node] || []).forEach(n => {
        if (!visited.has(n)) queue.push(n);
      });
    }
    return result;
  }

  const fullDown = {};
  const fullUp = {};
  procs.forEach(p => {
    fullUp[p.name] = getChain(upstream, p.name);
    fullDown[p.name] = getChain(downstream, p.name);
  });

  return { downstream, upstream, fullDownstream: fullDown, fullUpstream: fullUp };
}
