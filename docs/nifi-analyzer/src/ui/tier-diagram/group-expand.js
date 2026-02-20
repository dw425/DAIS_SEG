/**
 * ui/tier-diagram/group-expand.js — Process group expand/collapse
 *
 * Extracted from index.html lines 6839-6898.
 */

import { classifyNiFiProcessor } from '../../mappers/processor-classifier.js';
import { renderConnections } from './render-connections.js';

/**
 * Role tier colors (duplicated here for node rendering).
 */
const ROLE_TIER_COLORS = {
  source: '#3B82F6', route: '#EAB308', transform: '#A855F7',
  process: '#6366F1', sink: '#21C354', utility: '#808495'
};

/**
 * Toggle expand/collapse of a process group node.
 * Shows individual processors grouped by role when expanded.
 * Extracted from index.html lines 6839-6898.
 *
 * @param {object}      node        — the process group node data
 * @param {HTMLElement}  el          — the node's DOM element
 * @param {HTMLElement}  parentBand  — the tier band element
 * @param {object}      tierData    — the full tier data
 * @param {Object}      nodeEls     — map of node id -> DOM element
 * @param {HTMLElement}  container   — the diagram container
 * @param {HTMLElement}  detailEl    — the detail panel element
 * @param {string}      diagramType — type of diagram
 */
export function toggleGroupExpand(node, el, parentBand, tierData, nodeEls, container, detailEl, diagramType) {
  const subBandId = 'sub_' + node.id;
  const existing = container.querySelector(`[data-sub-band="${subBandId}"]`);

  if (existing) {
    existing.remove();
    el.classList.remove('expanded');
    const ind = el.querySelector('.expand-indicator');
    if (ind) ind.textContent = '\u25B6 expand';
    Object.keys(nodeEls).forEach(k => {
      if (k.startsWith('proc_' + node.name + '|')) delete nodeEls[k];
    });
    requestAnimationFrame(() => renderConnections(container, tierData.connections, nodeEls));
    return;
  }

  el.classList.add('expanded');
  const ind = el.querySelector('.expand-indicator');
  if (ind) ind.textContent = '\u25BC collapse';

  const processors = (node.detail && node.detail.processors) || [];
  const ROLE_ORDER = ['source', 'route', 'transform', 'process', 'sink', 'utility'];
  const ROLE_NAMES = {
    source: 'Sources', route: 'Routing', transform: 'Transforms',
    process: 'Processing', sink: 'Sinks', utility: 'Utility'
  };

  const subBand = document.createElement('div');
  subBand.className = 'tier-sub-band';
  subBand.dataset.subBand = subBandId;

  ROLE_ORDER.forEach(role => {
    const procs = processors.filter(p => classifyNiFiProcessor(p.type) === role);
    if (!procs.length) return;

    const roleLabel = document.createElement('div');
    roleLabel.className = 'tier-band-label';
    roleLabel.style.color = ROLE_TIER_COLORS[role] || '#808495';
    roleLabel.textContent = `${node.name} \u2192 ${ROLE_NAMES[role]} (${procs.length})`;
    subBand.appendChild(roleLabel);

    const nodesDiv = document.createElement('div');
    nodesDiv.className = 'tier-nodes';

    procs.forEach(p => {
      const procEl = document.createElement('div');
      procEl.className = 'tier-node';
      const procId = 'proc_' + node.name + '|' + p.name;
      procEl.dataset.nodeId = procId;
      if (p.state === 'DISABLED' || p.state === 'STOPPED') procEl.style.opacity = '0.5';
      procEl.style.borderTopColor = ROLE_TIER_COLORS[role] || '#808495';
      procEl.style.borderTopWidth = '3px';

      const nameEl = document.createElement('div');
      nameEl.className = 'node-name';
      nameEl.textContent = p.name.length > 20 ? p.name.substring(0, 17) + '...' : p.name;
      nameEl.title = p.name;
      procEl.appendChild(nameEl);

      const metaEl = document.createElement('div');
      metaEl.className = 'node-meta';
      metaEl.textContent = p.type;
      procEl.appendChild(metaEl);

      procEl.addEventListener('click', (e) => {
        e.stopPropagation();
        // Import showNodeDetail dynamically to avoid circular deps
        import('./node-detail.js').then(mod => {
          mod.showNodeDetail(
            {
              name: p.name, type: 'processor', subtype: role,
              meta: p.type, group: node.name, state: p.state,
              propCount: Object.keys(p.properties || {}).length, detail: p
            },
            detailEl, diagramType
          );
        });
      });

      nodesDiv.appendChild(procEl);
      nodeEls[procId] = procEl;
    });

    subBand.appendChild(nodesDiv);
  });

  parentBand.after(subBand);
  requestAnimationFrame(() => renderConnections(container, tierData.connections, nodeEls));
}
