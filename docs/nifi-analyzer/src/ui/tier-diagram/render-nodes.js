/**
 * ui/tier-diagram/render-nodes.js — Node rendering for tier diagram
 *
 * Extracted from index.html lines 6300-6665.
 * Creates DOM elements for each node type (session, table_output, conflict_gate,
 * process_group, generic).
 *
 * FIX MED: Added null checks on node props before accessing.
 */

import { highlightConnected, clearHighlight } from './highlight.js';
import { startRouteTrace, addToRouteTrace } from './route-trace.js';
import { toggleGroupExpand } from './group-expand.js';
import { showNodeDetail } from './node-detail.js';

/**
 * Role tier colors for node borders.
 */
const ROLE_TIER_COLORS = {
  source: '#3B82F6', route: '#EAB308', transform: '#A855F7',
  process: '#6366F1', sink: '#21C354', utility: '#808495'
};

/**
 * Render all nodes into the container, grouped by tier bands.
 * Extracted from index.html lines 6300-6515.
 *
 * @param {HTMLElement}  container   — the diagram container
 * @param {object}      tierData    — full tier data (nodes, connections, tierLabels, etc.)
 * @param {Object}      nodeEls     — mutable map of node id -> DOM element (populated by this fn)
 * @param {object}      _ms         — multi-select state (closure-scoped)
 * @param {HTMLElement}  detailEl    — the detail panel element
 * @param {string}      diagramType — type of diagram
 * @returns {{ sortedTiers: number[] }} metadata about rendered tiers
 */
export function renderNodes(container, tierData, nodeEls, _ms, detailEl, diagramType) {
  const { nodes, connections, tierLabels } = tierData;

  // Group nodes by tier
  const tierGroups = {};
  nodes.forEach(n => {
    if (!tierGroups[n.tier]) tierGroups[n.tier] = [];
    tierGroups[n.tier].push(n);
  });

  const sortedTiers = Object.keys(tierGroups).map(Number).sort((a, b) => a - b);

  sortedTiers.forEach(tier => {
    const config = tierLabels[tier] || { label: `TIER ${tier}`, color: '#808495', bg: 'rgba(128,132,149,0.06)' };
    const band = document.createElement('div');
    band.className = 'tier-band';
    band.style.background = config.bg;
    band.style.borderLeft = `3px solid ${config.color}`;

    const label = document.createElement('div');
    label.className = 'tier-band-label';
    label.style.color = config.color;
    label.textContent = config.label;
    band.appendChild(label);

    const nodesDiv = document.createElement('div');
    nodesDiv.className = 'tier-nodes';

    tierGroups[tier].forEach(node => {
      const el = document.createElement('div');
      el.dataset.nodeId = node.id;
      el.dataset.role = node.subtype || node.dominantRole || '';
      el.dataset.conf = String(node.conf || 0);
      el.dataset.name = node.name || '';
      el.dataset.type = node.procType || node.type || '';

      // -- Session nodes (dependency graph) --
      if (node.type === 'session') {
        el.className = 'tier-node';
        if (node.hasConflict) {
          el.style.borderColor = '#EF4444';
          el.style.borderTopColor = '#EF4444';
        } else if (node.subtype === 'root') {
          el.style.borderTopColor = '#3B82F6';
        } else {
          el.style.borderTopColor = '#6366F1';
        }
        el.style.borderTopWidth = '3px';
        // Sequence number badge
        if (node.seq) {
          const seqEl = document.createElement('div');
          seqEl.className = 'node-seq';
          seqEl.textContent = node.seq;
          if (node.hasConflict) seqEl.style.background = '#EF4444';
          el.appendChild(seqEl);
        }
        // Name
        const nameEl = document.createElement('div');
        nameEl.className = 'node-name';
        const shortName = (node.name || '').replace(/^s_m_(?:Load_|LOAD_)?/i, '');
        nameEl.textContent = shortName.length > 22 ? shortName.substring(0, 19) + '...' : shortName;
        nameEl.title = node.name || '';
        el.appendChild(nameEl);
        // Colored stat badges
        const statsDiv = document.createElement('div');
        statsDiv.className = 'node-stats';
        statsDiv.innerHTML = `<span class="ns ns-tx">${node.srcCount || 0} tx</span><span class="ns ns-ext">${node.tgtCount || 0} ext</span>` + (node.lkpCount ? `<span class="ns ns-lkp">${node.lkpCount} lkp</span>` : '');
        el.appendChild(statsDiv);
        // Conflict badge
        if (node.hasConflict) {
          const badge = document.createElement('div');
          badge.className = 'node-badge red';
          badge.textContent = '!';
          badge.title = (node.conflictDetails || []).map(c => (c.table_name || '') + ': ' + (c.conflict_type || '')).join(', ');
          el.appendChild(badge);
        }
      }
      // -- Table output nodes --
      else if (node.type === 'table_output') {
        el.className = 'tier-node table-output';
        if (node.isConflict) el.style.borderColor = '#EF4444';
        else if (node.isChain) el.style.borderColor = '#F59E0B';
        else el.style.borderColor = '#21C354';
        const icon = document.createElement('div');
        icon.style.cssText = 'font-size:0.8rem;margin-bottom:2px';
        icon.textContent = node.isConflict ? '\u26A0' : node.isChain ? '\u2161' : '\u2713';
        el.appendChild(icon);
        const nameEl = document.createElement('div');
        nameEl.className = 'node-name';
        nameEl.textContent = (node.name || '').length > 20 ? (node.name || '').substring(0, 17) + '...' : (node.name || '');
        nameEl.title = node.name || '';
        el.appendChild(nameEl);
        const cls = document.createElement('div');
        cls.className = 'node-class';
        cls.style.color = node.isConflict ? '#FCA5A5' : node.isChain ? '#FDE68A' : '#86EFAC';
        cls.textContent = node.isConflict ? 'CONFLICT' : node.isChain ? 'CHAIN' : 'INDEPENDENT';
        el.appendChild(cls);
      }
      // -- Conflict gate nodes --
      else if (node.type === 'conflict_gate') {
        el.className = 'tier-node conflict-gate';
        const icon = document.createElement('div');
        icon.style.cssText = 'font-size:1.2rem;margin-bottom:2px';
        icon.textContent = '\u26A0';
        el.appendChild(icon);
        const nameEl = document.createElement('div');
        nameEl.className = 'node-name';
        nameEl.textContent = (node.name || '').length > 25 ? (node.name || '').substring(0, 22) + '...' : (node.name || '');
        nameEl.title = node.name || '';
        el.appendChild(nameEl);
        const metaEl = document.createElement('div');
        metaEl.className = 'node-meta';
        metaEl.textContent = `${node.writerCount || 0}W / ${node.readerCount || 0}R / ${node.lookupCount || 0}L`;
        el.appendChild(metaEl);
        const clsEl = document.createElement('div');
        clsEl.className = 'node-class';
        clsEl.style.color = '#FCA5A5';
        clsEl.textContent = 'CONFLICT';
        el.appendChild(clsEl);
      }
      // -- Process group nodes (NiFi) --
      else if (node.type === 'process_group') {
        el.className = 'tier-node expandable';
        if (node.inCycle) el.classList.add('in-cycle');
        const roleColor = ROLE_TIER_COLORS[node.dominantRole] || '#6366F1';
        el.style.borderTopColor = roleColor;
        el.style.borderTopWidth = '3px';
        el.style.minWidth = '160px';
        el.style.maxWidth = '260px';
        // Store action types for filtering
        if (node.actionTypes && node.actionTypes.length) {
          el.dataset.actions = node.actionTypes.join(',');
        }
        if (node.stage) {
          el.dataset.stage = node.stage;
        }
        // Stage label (top-left corner)
        if (node.stageLabel) {
          const stageEl = document.createElement('div');
          stageEl.className = 'node-stage-label';
          stageEl.style.color = node.stageColor || '#6366F1';
          stageEl.style.borderColor = node.stageColor || '#6366F1';
          stageEl.textContent = node.stageLabel;
          el.appendChild(stageEl);
        }
        // Cycle badge
        if (node.inCycle) {
          const cyBadge = document.createElement('div');
          cyBadge.className = 'cycle-badge';
          cyBadge.textContent = '\u21BB';
          cyBadge.title = 'Cycle: ' + (node.sccMembers || []).filter(g => g !== node.name).join(', ');
          el.appendChild(cyBadge);
        }
        // Name
        const nameEl = document.createElement('div');
        nameEl.className = 'node-name';
        nameEl.textContent = (node.name || '').length > 28 ? (node.name || '').substring(0, 25) + '...' : (node.name || '');
        nameEl.title = node.name || '';
        el.appendChild(nameEl);
        // Processor count badge
        const badge = document.createElement('div');
        badge.className = 'node-badge';
        badge.textContent = node.procCount || 0;
        badge.title = (node.procCount || 0) + ' processors';
        el.appendChild(badge);
        // Subcategory badges (top 3)
        if (node.subcategories && node.subcategories.length) {
          const subDiv = document.createElement('div');
          subDiv.className = 'node-subcategories';
          node.subcategories.slice(0, 3).forEach(sub => {
            const chip = document.createElement('span');
            chip.className = 'ns ns-sub';
            chip.textContent = sub;
            subDiv.appendChild(chip);
          });
          if (node.subcategories.length > 3) {
            const more = document.createElement('span');
            more.className = 'ns ns-sub-more';
            more.textContent = '+' + (node.subcategories.length - 3);
            subDiv.appendChild(more);
          }
          el.appendChild(subDiv);
        }
        // Colored stat badges
        const statsDiv = document.createElement('div');
        statsDiv.className = 'node-stats';
        let statsHtml = '';
        if (node.srcCount) statsHtml += `<span class="ns ns-tx">${node.srcCount} src</span>`;
        if ((node.transformCount || 0) + (node.routeCount || 0)) statsHtml += `<span class="ns" style="background:#A855F7;color:white">${(node.transformCount || 0) + (node.routeCount || 0)} xfm</span>`;
        if (node.processCount) statsHtml += `<span class="ns ns-ext">${node.processCount} proc</span>`;
        if (node.sinkCount) statsHtml += `<span class="ns" style="background:#21C354;color:white">${node.sinkCount} sink</span>`;
        if (node.utilityCount) statsHtml += `<span class="ns" style="background:#808495;color:white">${node.utilityCount} util</span>`;
        statsDiv.innerHTML = statsHtml;
        el.appendChild(statsDiv);
        // Attribute indicators
        if ((node.attrCreates && node.attrCreates.length) || (node.attrReads && node.attrReads.length)) {
          const attrDiv = document.createElement('div');
          attrDiv.className = 'node-attr-indicators';
          if (node.attrCreates.length) {
            const cBadge = document.createElement('span');
            cBadge.className = 'ns ns-attr-create';
            cBadge.textContent = node.attrCreates.length + ' attr\u2191';
            cBadge.title = 'Creates: ' + node.attrCreates.slice(0, 5).join(', ') + (node.attrCreates.length > 5 ? '...' : '');
            attrDiv.appendChild(cBadge);
          }
          if (node.attrReads.length) {
            const rBadge = document.createElement('span');
            rBadge.className = 'ns ns-attr-read';
            rBadge.textContent = node.attrReads.length + ' attr\u2193';
            rBadge.title = 'Reads: ' + node.attrReads.slice(0, 5).join(', ') + (node.attrReads.length > 5 ? '...' : '');
            attrDiv.appendChild(rBadge);
          }
          el.appendChild(attrDiv);
        }
        // Expand indicator
        const expandInd = document.createElement('div');
        expandInd.className = 'expand-indicator';
        expandInd.textContent = '\u25B6 expand';
        el.appendChild(expandInd);
      }
      // -- Generic nodes (NiFi processors, SQL, flat) --
      else {
        el.className = 'tier-node';
        if (node.state === 'DISABLED' || node.state === 'STOPPED') el.style.opacity = '0.5';
        if (node.subtype === 'source') el.style.borderTopColor = '#3B82F6';
        else if (node.subtype === 'sink') el.style.borderTopColor = '#21C354';
        else if (node.subtype === 'route') el.style.borderTopColor = '#EAB308';
        else if (node.subtype === 'transform') el.style.borderTopColor = '#A855F7';
        else if (node.type === 'table') el.style.borderTopColor = '#3B82F6';
        el.style.borderTopWidth = '3px';
        const nameEl = document.createElement('div');
        nameEl.className = 'node-name';
        nameEl.textContent = (node.name || '').length > 25 ? (node.name || '').substring(0, 22) + '...' : (node.name || '');
        nameEl.title = node.name || '';
        el.appendChild(nameEl);
        if (node.meta) {
          const metaEl = document.createElement('div');
          metaEl.className = 'node-meta';
          metaEl.textContent = node.meta;
          el.appendChild(metaEl);
        }
        if (node.rows) {
          const badge = document.createElement('div');
          badge.className = 'node-badge';
          badge.textContent = node.rows >= 1000 ? Math.round(node.rows / 1000) + 'K' : node.rows;
          el.appendChild(badge);
        }
      }

      // Hover: highlight connected (suppressed during multi-select)
      el.addEventListener('mouseenter', () => {
        if (!_ms.active) highlightConnected(node.id, nodes, connections, nodeEls, container);
      });
      el.addEventListener('mouseleave', () => {
        if (!_ms.active) clearHighlight(nodeEls, container);
      });

      // Click: route tracing + expand + detail
      el.addEventListener('click', (e) => {
        e.stopPropagation();
        if (_ms.active && !_ms.selected.includes(node.id)) {
          addToRouteTrace(node.id, _ms, connections, nodeEls, container);
          showNodeDetail(node, detailEl, diagramType);
        } else if (_ms.active && _ms.selected.includes(node.id)) {
          if (node.type === 'process_group' && node.expandable) {
            toggleGroupExpand(node, el, band, tierData, nodeEls, container, detailEl, diagramType);
          }
          showNodeDetail(node, detailEl, diagramType);
        } else {
          startRouteTrace(node.id, _ms, connections, nodeEls, container);
          if (node.type === 'process_group' && node.expandable) {
            toggleGroupExpand(node, el, band, tierData, nodeEls, container, detailEl, diagramType);
          }
          showNodeDetail(node, detailEl, diagramType);
        }
      });

      nodesDiv.appendChild(el);
      nodeEls[node.id] = el;
    });

    band.appendChild(nodesDiv);
    container.appendChild(band);
  });

  return { sortedTiers };
}
