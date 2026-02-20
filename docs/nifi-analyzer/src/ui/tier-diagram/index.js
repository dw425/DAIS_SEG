/**
 * ui/tier-diagram/index.js — Tier diagram entry point
 *
 * Extracted from index.html lines 6267-6664.
 * Orchestrates the full tier diagram rendering: toolbar, nodes, connections,
 * sidebar filter, legend.
 */

import { renderNodes } from './render-nodes.js';
import { renderConnections } from './render-connections.js';
import { clearRouteTrace } from './route-trace.js';
import { applySidebarFilter, clearSidebarFilter } from './sidebar-filter.js';
import { tierFilter, resetTierFilterState } from './filter-toolbar.js';
import { escapeHTML } from '../../security/html-sanitizer.js';

/** Module-level escape handler reference for cleanup between renders. */
let _escapeHandler = null;

/**
 * Render the full tier diagram into the specified container elements.
 * Extracted from index.html lines 6267-6664.
 *
 * @param {object} tierData    — output from buildTierData()
 * @param {string} containerId — DOM id for diagram container
 * @param {string} detailId    — DOM id for detail panel
 * @param {string} legendId    — DOM id for legend
 */
export function renderTierDiagram(tierData, containerId, detailId, legendId) {
  resetTierFilterState();
  const container = document.getElementById(containerId);
  const detailEl = document.getElementById(detailId);
  const legendEl = document.getElementById(legendId);
  if (!container) return;
  container.innerHTML = '';
  container.style.minHeight = '200px';

  const { nodes, connections, tierLabels, diagramType, densityData } = tierData;
  if (!nodes.length) {
    container.innerHTML = '<p style="text-align:center;padding:20px;color:var(--text2)">No nodes to display</p>';
    return;
  }

  // Filter toolbar
  const _tb = document.createElement('div');
  _tb.className = 'filter-toolbar';
  let _tbHtml = '<div class="filter-group"><label>Role:</label>';
  ['all', 'source', 'transform', 'route', 'process', 'sink', 'utility'].forEach(r => {
    const colors = { all: '', source: '#3B82F6', transform: '#A855F7', route: '#EAB308', process: '#6366F1', sink: '#21C354', utility: '#808495' };
    const style = colors[r] ? ' style="border-color:' + colors[r] + ';color:' + colors[r] + '"' : '';
    _tbHtml += '<button class="filter-btn' + (r === 'all' ? ' active' : '') + '"' + style + ' data-filter-role="' + r + '">' + (r === 'all' ? 'All' : r.charAt(0).toUpperCase() + r.slice(1)) + '</button>';
  });
  _tbHtml += '</div><div class="filter-group"><label>Confidence:</label>';
  [{ k: 'all', l: 'All', s: '' }, { k: 'high', l: 'High', s: 'border-color:var(--green);color:var(--green)' }, { k: 'med', l: 'Med', s: 'border-color:var(--amber);color:var(--amber)' }, { k: 'low', l: 'Low', s: 'border-color:var(--red);color:var(--red)' }].forEach(c => {
    _tbHtml += '<button class="filter-btn' + (c.k === 'all' ? ' active' : '') + '"' + (c.s ? ' style="' + c.s + '"' : '') + ' data-filter-conf="' + c.k + '">' + c.l + '</button>';
  });
  _tbHtml += '</div><div class="filter-group"><input class="filter-search" type="text" placeholder="Search processors..."></div>';
  _tb.innerHTML = _tbHtml;

  // FIX CRIT: Use addEventListener instead of inline onclick
  _tb.querySelectorAll('[data-filter-role]').forEach(btn => {
    btn.addEventListener('click', () => {
      btn.parentElement.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      tierFilter(_tb, 'role', btn.dataset.filterRole);
    });
  });
  _tb.querySelectorAll('[data-filter-conf]').forEach(btn => {
    btn.addEventListener('click', () => {
      btn.parentElement.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      tierFilter(_tb, 'conf', btn.dataset.filterConf);
    });
  });
  const searchInput = _tb.querySelector('.filter-search');
  if (searchInput) {
    searchInput.addEventListener('input', () => {
      tierFilter(_tb, 'search', searchInput.value);
    });
  }

  container.appendChild(_tb);

  // Multi-select state (closure-scoped)
  const _ms = { selected: [], pathNodes: new Set(), pathEdgeKeys: new Set(), active: false };
  const nodeEls = {};

  // Render nodes by tier
  renderNodes(container, tierData, nodeEls, _ms, detailEl, diagramType);

  // Render SVG connections after DOM is laid out
  requestAnimationFrame(() => {
    renderConnections(container, connections, nodeEls);
  });

  // Escape clears route trace; click empty space clears
  if (_escapeHandler) document.removeEventListener('keydown', _escapeHandler);
  _escapeHandler = (e) => {
    if (e.key === 'Escape' && _ms.active) clearRouteTrace(_ms, nodeEls, container);
  };
  document.addEventListener('keydown', _escapeHandler);
  container.addEventListener('click', (e) => {
    if ((e.target === container || e.target.classList.contains('tier-band') || e.target.classList.contains('tier-band-label')) && _ms.active) {
      clearRouteTrace(_ms, nodeEls, container);
    }
  });

  // Connection density sidebar — active filter
  const sidebarEl = document.getElementById('tierDensitySidebar');
  const barsEl = document.getElementById('densityBars');
  const _sidebarFilter = { activeTypes: new Set() };

  if (sidebarEl && barsEl && densityData && densityData.length) {
    sidebarEl.classList.remove('hidden');
    const sidebarTitle = sidebarEl.querySelector('h4');
    if (sidebarTitle) sidebarTitle.textContent = diagramType === 'nifi_flow' ? 'Filter by Type' : 'Connection Density';
    barsEl.innerHTML = '';

    if (diagramType === 'nifi_flow') {
      const hint = document.createElement('div');
      hint.className = 'sidebar-filter-hint';
      hint.textContent = 'Click to filter diagram';
      barsEl.appendChild(hint);
    }

    const maxTotal = Math.max(...densityData.map(d => d.total));
    const typeToGroups = {};
    if (diagramType === 'nifi_flow') {
      nodes.forEach(n => {
        if (n.type === 'process_group' && n.detail && n.detail.typeCount) {
          Object.keys(n.detail.typeCount).forEach(t => {
            if (!typeToGroups[t]) typeToGroups[t] = new Set();
            typeToGroups[t].add(n.id);
          });
        }
      });
    }

    densityData.forEach(d => {
      const row = document.createElement('div');
      row.className = 'density-row';
      row.dataset.typeName = d.name;
      if (d.role) {
        const roleColors = { source: '#3B82F6', sink: '#21C354', route: '#EAB308', transform: '#A855F7', process: '#6366F1', utility: '#808495' };
        const barW = Math.max(4, (d.total / maxTotal) * 80);
        row.innerHTML = `<span class="density-bar" style="width:${barW}px;background:${roleColors[d.role] || '#808495'}" title="${d.total}x"></span><span class="density-label" title="${escapeHTML(d.name)}">${escapeHTML(d.name)} (${d.total})</span>`;
      } else {
        const wPct = Math.max(2, (d.writers / maxTotal) * 60);
        const rPct = Math.max(0, (d.readers / maxTotal) * 60);
        const lPct = Math.max(0, (d.lookups / maxTotal) * 60);
        let barsHTML = `<span class="density-bar" style="width:${wPct}px;background:#EF4444" title="${d.writers} writer(s)"></span>`;
        if (d.readers) barsHTML += `<span class="density-bar" style="width:${rPct}px;background:#3B82F6" title="${d.readers} reader(s)"></span>`;
        if (d.lookups) barsHTML += `<span class="density-bar" style="width:${lPct}px;background:#F59E0B" title="${d.lookups} lookup(s)"></span>`;
        row.innerHTML = barsHTML + `<span class="density-label" title="${escapeHTML(d.name)}">${escapeHTML(d.name)}</span>`;
      }

      // Click handler for filter
      if (diagramType === 'nifi_flow') {
        row.addEventListener('click', () => {
          if (_ms.active) clearRouteTrace(_ms, nodeEls, container);
          const typeName = d.name;
          if (_sidebarFilter.activeTypes.has(typeName)) {
            _sidebarFilter.activeTypes.delete(typeName);
            row.classList.remove('filter-active');
          } else {
            _sidebarFilter.activeTypes.add(typeName);
            row.classList.add('filter-active');
          }
          applySidebarFilter(_sidebarFilter, typeToGroups, nodes, connections, nodeEls, container, barsEl);
        });
      }
      barsEl.appendChild(row);
    });

    // Clear filter button
    if (diagramType === 'nifi_flow') {
      const clearBtn = document.createElement('div');
      clearBtn.className = 'sidebar-clear-btn';
      clearBtn.id = 'sidebarClearBtn';
      clearBtn.textContent = '\u2715 Clear filter';
      clearBtn.addEventListener('click', () => {
        _sidebarFilter.activeTypes.clear();
        barsEl.querySelectorAll('.density-row').forEach(r => r.classList.remove('filter-active', 'filter-dimmed'));
        clearSidebarFilter(nodeEls, container);
        clearBtn.style.display = 'none';
      });
      barsEl.appendChild(clearBtn);
    }
  } else if (sidebarEl) {
    sidebarEl.classList.add('hidden');
  }

  // Legend
  if (legendEl) {
    legendEl.innerHTML = '';
    const sessionCount = nodes.filter(n => n.type === 'session').length;
    const tableCount = nodes.filter(n => n.type === 'table_output' || n.type === 'conflict_gate').length;
    if (diagramType === 'dependency_graph') {
      legendEl.innerHTML = [
        `<span>${sessionCount} Sessions</span>`,
        `<span>${tableCount} Tables</span>`,
        `<span style="color:#EF4444">${nodes.filter(n => n.hasConflict || n.type === 'conflict_gate').length} Conflicts</span>`,
        '<span><span class="leg-line" style="background:#6366F1"></span> Dependency</span>',
        '<span><span class="leg-line" style="background:#F59E0B;border-top:2px dashed #F59E0B"></span> Lookup</span>',
        '<span><span class="leg-line" style="background:#EF4444"></span> Conflict</span>',
        '<span><span class="leg-line" style="background:#21C354"></span> Independent</span>',
        '<span><span class="leg-line" style="background:#F59E0B"></span> Chain</span>',
      ].join('');
    } else if (diagramType === 'nifi_flow') {
      const pgCount = nodes.filter(n => n.type === 'process_group').length;
      const procCount = nodes.filter(n => n.type === 'processor').length;
      const cycleCount = tierData.cycleData ? tierData.cycleData.length : 0;
      legendEl.innerHTML = [
        `<span>${pgCount} Process Groups</span>`,
        procCount ? `<span>${procCount} Processors</span>` : '',
        `<span>${connections.length} Connections</span>`,
        cycleCount ? `<span style="color:#EF4444">${cycleCount} Cycle(s)</span>` : '',
        '<span><span class="leg-line" style="background:#3B82F6"></span> Source</span>',
        '<span><span class="leg-line" style="background:#EAB308"></span> Route</span>',
        '<span><span class="leg-line" style="background:#A855F7"></span> Transform</span>',
        '<span><span class="leg-line" style="background:#6366F1"></span> Process</span>',
        '<span><span class="leg-line" style="background:#21C354"></span> Sink</span>',
        '<span><span class="leg-line" style="background:#EF4444;border-top:2px dashed #EF4444"></span> Cycle Edge</span>',
        '<span style="color:var(--text2);font-size:0.7rem">Click nodes to trace route &middot; Esc to clear</span>',
      ].join('');
    } else if (diagramType === 'sql_tables') {
      legendEl.innerHTML = [
        '<span><span class="leg-line" style="background:#3B82F6;border-top:2px solid #3B82F6"></span> Foreign Key</span>',
        `<span>${nodes.length} tables &middot; ${connections.length} relationships</span>`,
      ].join('');
    } else {
      legendEl.innerHTML = [
        `<span>${nodes.length} objects</span>`,
        connections.length ? '<span><span class="leg-line" style="background:#4B5563;border-top:2px dashed #4B5563"></span> Shared columns</span>' : '',
      ].join('');
    }
  }

  const tierContainer = document.getElementById('tierDiagramContainer');
  if (tierContainer) tierContainer.classList.remove('hidden');
}
