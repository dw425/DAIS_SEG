/**
 * ui/tier-diagram/highlight.js — Node hover highlighting
 *
 * Extracted from index.html lines 6759-6836.
 * Traces full upstream + downstream paths and applies visual highlighting.
 *
 * FIX MED: Applies debouncing via utils/debounce to prevent excessive DOM updates.
 */

import { debounce } from '../../utils/debounce.js';

/**
 * Highlight all connected nodes and edges (upstream + downstream BFS).
 * Extracted from index.html lines 6759-6818.
 *
 * @param {string}  nodeId      — the hovered node id
 * @param {Array}   nodes       — all nodes
 * @param {Array}   connections — all connections
 * @param {Object}  nodeEls     — map of node id -> DOM element
 * @param {HTMLElement} container — the diagram container
 */
function _highlightConnected(nodeId, nodes, connections, nodeEls, container) {
  const pathNodes = new Set([nodeId]);
  const pathEdges = new Set();

  // BFS downstream
  function traceDown(id) {
    connections.forEach(c => {
      const key = c.from + '|' + c.to;
      if (c.from === id && !pathEdges.has(key)) {
        pathEdges.add(key);
        pathNodes.add(c.to);
        traceDown(c.to);
      }
    });
  }

  // BFS upstream
  function traceUp(id) {
    connections.forEach(c => {
      const key = c.from + '|' + c.to;
      if (c.to === id && !pathEdges.has(key)) {
        pathEdges.add(key);
        pathNodes.add(c.from);
        traceUp(c.from);
      }
    });
  }

  traceDown(nodeId);
  traceUp(nodeId);

  // Dim all nodes, highlight path nodes
  Object.entries(nodeEls).forEach(([id, el]) => {
    if (pathNodes.has(id)) {
      el.classList.add('highlighted');
      el.classList.remove('dimmed');
      if (id === nodeId) el.classList.add('selected');
    } else {
      el.classList.add('dimmed');
      el.classList.remove('highlighted', 'selected');
    }
  });

  // Highlight SVG paths
  const svg = container.querySelector('svg.tier-svg');
  if (svg) {
    svg.querySelectorAll('path[data-from]').forEach(p => {
      const edgeKey = p.dataset.from + '|' + p.dataset.to;
      if (pathEdges.has(edgeKey)) {
        p.setAttribute('opacity', '1');
        p.setAttribute('stroke', '#FAFAFA');
        p.setAttribute('stroke-width', '3');
        p.setAttribute('filter', 'url(#glow)');
        p.setAttribute('marker-end', 'url(#arrow-white)');
        p.style.transition = 'all 0.2s ease';
      } else {
        p.setAttribute('opacity', '0.08');
        p.removeAttribute('filter');
      }
    });
  }
}

/**
 * Debounced version of highlightConnected to prevent layout thrashing on rapid hover.
 */
export const highlightConnected = debounce(_highlightConnected, 30);

/**
 * Clear all highlight/dim classes and restore SVG paths to defaults.
 * Extracted from index.html lines 6820-6836.
 *
 * @param {Object}      nodeEls   — map of node id -> DOM element
 * @param {HTMLElement}  container — the diagram container
 */
export function clearHighlight(nodeEls, container) {
  Object.values(nodeEls).forEach(el => {
    el.classList.remove('highlighted', 'dimmed', 'selected');
  });
  const svg = container.querySelector('svg.tier-svg');
  if (svg) {
    svg.querySelectorAll('path[data-from]').forEach(p => {
      p.setAttribute('opacity', '0.35');
      p.setAttribute('stroke', p.dataset.origColor || '#4B5563');
      p.setAttribute('stroke-width', p.dataset.origWidth || '1.5');
      p.removeAttribute('filter');
      // Restore original arrow marker
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
}
