/**
 * ui/tier-diagram/route-trace.js — Progressive route tracing
 *
 * Extracted from index.html lines 6900-7031.
 * Manages multi-select route tracing between nodes.
 */

import { bfsShortestPath } from '../../utils/bfs.js';
import { showPathToast, hidePathToast, flashNoPath } from '../toast.js';

/**
 * BFS all reachable nodes (both directions) from a starting node.
 * Extracted from index.html lines 6902-6928.
 *
 * @param {string} nodeId      — starting node id
 * @param {Array}  connections — connection data array
 * @returns {{ reachNodes: Set<string>, reachEdges: Set<string> }}
 */
function bfsReachable(nodeId, connections) {
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

/**
 * Apply visual styling for route trace state.
 * Extracted from index.html lines 6983-7009.
 *
 * @param {object}      ms        — multi-select state
 * @param {Object}      nodeEls   — map of node id -> DOM element
 * @param {HTMLElement}  container — the diagram container
 */
function applyRouteVisuals(ms, nodeEls, container) {
  Object.entries(nodeEls).forEach(([id, el]) => {
    el.classList.remove('path-selected', 'path-member', 'path-dimmed', 'highlighted', 'dimmed', 'selected');
    if (ms.selected.includes(id)) el.classList.add('path-selected');
    else if (ms.pathNodes.has(id)) el.classList.add('path-member');
    else el.classList.add('path-dimmed');
  });
  const svg = container.querySelector('svg.tier-svg');
  if (svg) {
    svg.querySelectorAll('path[data-from]').forEach(p => {
      const fwd = p.dataset.from + '|' + p.dataset.to;
      const rev = p.dataset.to + '|' + p.dataset.from;
      if (ms.pathEdgeKeys.has(fwd) || ms.pathEdgeKeys.has(rev)) {
        p.setAttribute('opacity', '1');
        p.setAttribute('stroke', '#FACA15');
        p.setAttribute('stroke-width', '3');
        p.setAttribute('filter', 'url(#glow)');
        p.setAttribute('marker-end', 'url(#arrow-white)');
        p.style.transition = 'all 0.2s ease';
      } else {
        p.setAttribute('opacity', '0.04');
        p.removeAttribute('filter');
        p.style.transition = 'all 0.2s ease';
      }
    });
  }
}

/**
 * Start a new route trace from a single node.
 * Extracted from index.html lines 6930-6938.
 *
 * @param {string}      nodeId      — clicked node id
 * @param {object}      ms          — multi-select state object
 * @param {Array}       connections — connection data array
 * @param {Object}      nodeEls     — map of node id -> DOM element
 * @param {HTMLElement}  container   — the diagram container
 */
export function startRouteTrace(nodeId, ms, connections, nodeEls, container) {
  ms.selected = [nodeId];
  ms.active = true;
  const { reachNodes, reachEdges } = bfsReachable(nodeId, connections);
  ms.pathNodes = reachNodes;
  ms.pathEdgeKeys = reachEdges;
  applyRouteVisuals(ms, nodeEls, container);
  showPathToast(ms);
}

/**
 * Add a node to the existing route trace, finding shortest path.
 * Extracted from index.html lines 6941-6981.
 *
 * @param {string}      nodeId      — clicked node id
 * @param {object}      ms          — multi-select state object
 * @param {Array}       connections — connection data array
 * @param {Object}      nodeEls     — map of node id -> DOM element
 * @param {HTMLElement}  container   — the diagram container
 */
export function addToRouteTrace(nodeId, ms, connections, nodeEls, container) {
  if (ms.selected.includes(nodeId)) return;
  ms.selected.push(nodeId);

  if (ms.selected.length === 2) {
    // Two nodes: find the specific path between them
    const a = ms.selected[0], b = ms.selected[1];
    let result = bfsShortestPath(connections, a, b);
    if (!result.found) result = bfsShortestPath(connections, b, a);
    if (!result.found) {
      const bi = connections.flatMap(c => [c, { from: c.to, to: c.from, label: c.label, type: c.type, color: c.color, width: c.width }]);
      result = bfsShortestPath(bi, a, b);
    }
    if (result.found) {
      ms.pathNodes = new Set(result.pathNodes);
      ms.pathEdgeKeys = new Set(result.pathEdgeKeys);
    } else {
      ms.pathNodes = new Set(ms.selected);
      ms.pathEdgeKeys = new Set();
      flashNoPath();
    }
  } else {
    // 3+ nodes: extend from last-but-one to new node
    const prev = ms.selected[ms.selected.length - 2];
    let result = bfsShortestPath(connections, prev, nodeId);
    if (!result.found) result = bfsShortestPath(connections, nodeId, prev);
    if (!result.found) {
      const bi = connections.flatMap(c => [c, { from: c.to, to: c.from, label: c.label, type: c.type, color: c.color, width: c.width }]);
      result = bfsShortestPath(bi, prev, nodeId);
    }
    if (result.found) {
      result.pathNodes.forEach(n => ms.pathNodes.add(n));
      result.pathEdgeKeys.forEach(k => ms.pathEdgeKeys.add(k));
    } else {
      ms.pathNodes.add(nodeId);
      flashNoPath();
    }
  }

  applyRouteVisuals(ms, nodeEls, container);
  showPathToast(ms);
}

/**
 * Clear route trace and restore all visuals.
 * Extracted from index.html lines 7011-7031.
 *
 * @param {object}      ms        — multi-select state object
 * @param {Object}      nodeEls   — map of node id -> DOM element
 * @param {HTMLElement}  container — the diagram container
 */
export function clearRouteTrace(ms, nodeEls, container) {
  ms.selected = [];
  ms.pathNodes = new Set();
  ms.pathEdgeKeys = new Set();
  ms.active = false;
  Object.values(nodeEls).forEach(el => {
    el.classList.remove('path-selected', 'path-member', 'path-dimmed');
  });
  const svg = container.querySelector('svg.tier-svg');
  if (svg) {
    svg.querySelectorAll('path[data-from]').forEach(p => {
      p.setAttribute('opacity', '0.35');
      p.setAttribute('stroke', p.dataset.origColor || '#4B5563');
      p.setAttribute('stroke-width', p.dataset.origWidth || '1.5');
      p.removeAttribute('filter');
      const c = p.dataset.origColor || '';
      const arrowId = c.includes('EF44') ? 'arrow-red'
        : c.includes('F59E') || c.includes('F5') ? 'arrow-amber'
        : c.includes('6366') ? 'arrow-purple'
        : c.includes('3B82') ? 'arrow-blue'
        : c.includes('21C3') ? 'arrow-green'
        : 'arrow-default';
      p.setAttribute('marker-end', `url(#${arrowId})`);
      p.style.transition = '';
    });
  }
  hidePathToast();
}
