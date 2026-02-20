/**
 * utils/bfs.js — Breadth-first search utilities for NiFi flow graphs
 *
 * Extracted from index.html lines 6080-6103 and 6902-6929.
 */

/**
 * Find the shortest directed path between two nodes using BFS.
 *
 * @param {Array<{from: string, to: string}>} connections
 * @param {string} startId
 * @param {string} endId
 * @returns {{pathNodes: string[], pathEdgeKeys: string[], found: boolean}}
 */
export function bfsShortestPath(connections, startId, endId) {
  const adj = {};
  connections.forEach(c => {
    if (!adj[c.from]) adj[c.from] = [];
    adj[c.from].push({ to: c.to, key: c.from + '|' + c.to });
  });

  const visited = new Set([startId]);
  const parent = {};
  const queue = [startId];

  while (queue.length) {
    const cur = queue.shift();
    if (cur === endId) {
      const pathNodes = [];
      const pathEdgeKeys = [];
      let n = endId;
      while (n !== startId) {
        pathNodes.unshift(n);
        pathEdgeKeys.unshift(parent[n].key);
        n = parent[n].from;
      }
      pathNodes.unshift(startId);
      return { pathNodes, pathEdgeKeys, found: true };
    }
    for (const nb of (adj[cur] || [])) {
      if (!visited.has(nb.to)) {
        visited.add(nb.to);
        parent[nb.to] = { from: cur, key: nb.key };
        queue.push(nb.to);
      }
    }
  }

  return { pathNodes: [], pathEdgeKeys: [], found: false };
}

/**
 * Find all nodes reachable from a given node (both upstream and downstream).
 *
 * @param {string} nodeId
 * @param {Array<{from: string, to: string}>} connections
 * @returns {{reachNodes: Set<string>, reachEdges: Set<string>}}
 */
export function bfsReachable(nodeId, connections) {
  const reachNodes = new Set([nodeId]);
  const reachEdges = new Set();

  // Downstream
  const queue = [nodeId];
  const visited = new Set([nodeId]);
  while (queue.length) {
    const cur = queue.shift();
    connections.forEach(c => {
      if (c.from === cur && !visited.has(c.to)) {
        visited.add(c.to);
        reachNodes.add(c.to);
        reachEdges.add(c.from + '|' + c.to);
        queue.push(c.to);
      }
    });
  }

  // Upstream
  const queue2 = [nodeId];
  const visited2 = new Set([nodeId]);
  while (queue2.length) {
    const cur = queue2.shift();
    connections.forEach(c => {
      if (c.to === cur && !visited2.has(c.from)) {
        visited2.add(c.from);
        reachNodes.add(c.from);
        reachEdges.add(c.from + '|' + c.to);
        queue2.push(c.from);
      }
    });
  }

  return { reachNodes, reachEdges };
}
