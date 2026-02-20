/**
 * ui/tier-diagram/sidebar-filter.js — Sidebar type filter
 *
 * Extracted from index.html lines 7069-7135.
 */

/**
 * Apply sidebar filter: highlight groups that contain the selected processor types.
 * Extracted from index.html lines 7069-7118.
 *
 * @param {{ activeTypes: Set<string> }} sf            — sidebar filter state
 * @param {Object}                       typeToGroups  — map of processor type -> Set of group ids
 * @param {Array}                        nodes         — all nodes
 * @param {Array}                        connections   — all connections
 * @param {Object}                       nodeEls       — map of node id -> DOM element
 * @param {HTMLElement}                   container     — diagram container
 * @param {HTMLElement}                   barsEl        — sidebar bars container
 */
export function applySidebarFilter(sf, typeToGroups, nodes, connections, nodeEls, container, barsEl) {
  const clearBtn = document.getElementById('sidebarClearBtn');
  if (!sf.activeTypes.size) {
    clearSidebarFilter(nodeEls, container);
    barsEl.querySelectorAll('.density-row').forEach(r => r.classList.remove('filter-dimmed'));
    if (clearBtn) clearBtn.style.display = 'none';
    return;
  }
  if (clearBtn) clearBtn.style.display = 'block';

  // Find all groups containing ANY of the selected types
  const matchingGroups = new Set();
  sf.activeTypes.forEach(t => {
    if (typeToGroups[t]) typeToGroups[t].forEach(g => matchingGroups.add(g));
  });

  // Find connections between matching groups
  const matchingEdges = new Set();
  connections.forEach(c => {
    if (matchingGroups.has(c.from) && matchingGroups.has(c.to)) {
      matchingEdges.add(c.from + '|' + c.to);
    }
  });

  // Apply visuals to nodes
  Object.entries(nodeEls).forEach(([id, el]) => {
    el.classList.remove('path-selected', 'path-member', 'path-dimmed', 'highlighted', 'dimmed', 'selected');
    if (matchingGroups.has(id)) {
      el.classList.add('path-member');
    } else {
      el.classList.add('path-dimmed');
    }
  });

  // Apply visuals to SVG edges
  const svg = container.querySelector('svg.tier-svg');
  if (svg) {
    svg.querySelectorAll('path[data-from]').forEach(p => {
      const fwd = p.dataset.from + '|' + p.dataset.to;
      if (matchingEdges.has(fwd)) {
        p.setAttribute('opacity', '0.8');
        p.setAttribute('stroke', p.dataset.origColor || '#4B5563');
        p.setAttribute('stroke-width', p.dataset.origWidth || '1.5');
      } else {
        p.setAttribute('opacity', '0.04');
      }
    });
  }

  // Dim non-active sidebar rows
  barsEl.querySelectorAll('.density-row').forEach(r => {
    if (sf.activeTypes.has(r.dataset.typeName)) r.classList.remove('filter-dimmed');
    else r.classList.add('filter-dimmed');
  });
}

/**
 * Clear sidebar filter, restoring all nodes and edges.
 * Extracted from index.html lines 7120-7135.
 *
 * @param {Object}      nodeEls   — map of node id -> DOM element
 * @param {HTMLElement}  container — diagram container
 */
export function clearSidebarFilter(nodeEls, container) {
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
}
