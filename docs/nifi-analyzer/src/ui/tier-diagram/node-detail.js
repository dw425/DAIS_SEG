/**
 * ui/tier-diagram/node-detail.js — Node detail panel rendering
 *
 * Extracted from index.html lines 7137-7240.
 * Shows detailed information when a node is clicked.
 */

import { escapeHTML } from '../../security/html-sanitizer.js';
import { isSensitiveProp, maskProperty } from '../../security/sensitive-props.js';

/**
 * Show detailed information for a selected node.
 * Extracted from index.html lines 7137-7240.
 *
 * @param {object}      node        — the node data
 * @param {HTMLElement}  detailEl    — the detail panel DOM element
 * @param {string}      diagramType — type of diagram
 */
export function showNodeDetail(node, detailEl, diagramType) {
  if (!detailEl) return;
  let h = '<div class="node-detail">';
  h += `<h4>${escapeHTML(node.name)}</h4>`;

  if (diagramType === 'nifi_flow' && node.type === 'process_group' && node.detail) {
    const d = node.detail;
    h += `<p><strong>Processors:</strong> ${node.procCount} &middot; <strong>Internal Connections:</strong> ${d.intraConns || 0}</p>`;
    // Role breakdown
    h += '<div style="display:flex;gap:6px;flex-wrap:wrap;margin:8px 0">';
    if (node.srcCount) h += `<span class="ns ns-tx">${node.srcCount} sources</span>`;
    if (node.routeCount) h += `<span class="ns" style="background:#EAB308;color:#000">${node.routeCount} routes</span>`;
    if (node.transformCount) h += `<span class="ns" style="background:#A855F7;color:white">${node.transformCount} transforms</span>`;
    if (node.processCount) h += `<span class="ns ns-ext">${node.processCount} processors</span>`;
    if (node.sinkCount) h += `<span class="ns" style="background:#21C354;color:white">${node.sinkCount} sinks</span>`;
    if (node.utilityCount) h += `<span class="ns" style="background:#808495;color:white">${node.utilityCount} utility</span>`;
    h += '</div>';
    // Processor type breakdown
    if (d.typeCount) {
      const types = Object.entries(d.typeCount).sort((a, b) => b[1] - a[1]);
      h += '<table style="font-size:0.75rem"><thead><tr><th>Processor Type</th><th>Count</th></tr></thead><tbody>';
      types.slice(0, 15).forEach(([t, c]) => {
        h += `<tr><td>${escapeHTML(t)}</td><td>${c}</td></tr>`;
      });
      if (types.length > 15) h += `<tr><td colspan="2" style="color:var(--text2)">+${types.length - 15} more types</td></tr>`;
      h += '</tbody></table>';
    }
    // First few processor names
    if (d.processors && d.processors.length) {
      h += `<p style="margin-top:8px"><strong>Processors (first 10):</strong></p>`;
      h += '<ul style="font-size:0.75rem;margin:4px 0 4px 16px">';
      d.processors.slice(0, 10).forEach(p => {
        h += `<li>${escapeHTML(p.name)} <code style="font-size:0.65rem">${escapeHTML(p.type)}</code></li>`;
      });
      if (d.processors.length > 10) h += `<li style="color:var(--text2)">+${d.processors.length - 10} more</li>`;
      h += '</ul>';
    }
    // Cycle information
    if (node.inCycle && node.sccMembers) {
      h += '<div style="margin:8px 0;padding:8px 12px;border:1px solid #EF4444;border-radius:6px;background:rgba(239,68,68,0.08);font-size:0.8rem">';
      h += '<strong style="color:#EF4444">Circular Dependency Detected</strong><br>';
      h += 'Cycle with: ' + node.sccMembers.filter(g => g !== node.name).map(escapeHTML).join(', ');
      if (node.cycleEdges && node.cycleEdges.length) {
        h += '<br><br><strong>Cycle edges:</strong><br>';
        node.cycleEdges.forEach(ce => {
          h += `${escapeHTML(ce.from)} \u2192 ${escapeHTML(ce.to)} (${ce.count} flow${ce.count > 1 ? 's' : ''})<br>`;
        });
      }
      h += '</div>';
    }
  } else if (diagramType === 'nifi_flow' && node.detail) {
    const p = node.detail;
    h += `<p><strong>Type:</strong> ${escapeHTML(p.type)} <code style="font-size:0.7rem">${escapeHTML(p.fullType || '')}</code></p>`;
    h += `<p><strong>Group:</strong> ${escapeHTML(p.group || '(root)')}</p>`;
    h += `<p><strong>State:</strong> ${escapeHTML(p.state || 'N/A')}</p>`;
    if (p.schedulingStrategy) h += `<p><strong>Scheduling:</strong> ${escapeHTML(p.schedulingStrategy)} / ${escapeHTML(p.schedulingPeriod)}</p>`;
    const propKeys = Object.keys(p.properties || {});
    const sensCount = propKeys.filter(k => isSensitiveProp(k)).length;
    if (propKeys.length) {
      h += `<p><strong>Properties (${propKeys.length}):</strong>${sensCount ? ` <span style="color:#EAB308;font-size:0.75rem">&#x26A0; ${sensCount} sensitive masked</span>` : ''}</p><pre style="max-height:200px;overflow:auto;font-size:0.75rem">`;
      propKeys.slice(0, 20).forEach(k => {
        h += `${escapeHTML(k)}: ${escapeHTML(maskProperty(k, (p.properties[k] || '').substring(0, 100)))}\n`;
      });
      if (propKeys.length > 20) h += `... +${propKeys.length - 20} more\n`;
      h += '</pre>';
    }
  } else if (diagramType === 'dependency_graph' && node.type === 'session' && node.detail) {
    const s = node.detail;
    h += `<p><strong>Sources:</strong> ${s.sources} &middot; <strong>Targets:</strong> ${s.targets} &middot; <strong>Lookups:</strong> ${s.lookups}</p>`;
    if (node.seq) h += `<p><strong>Execution Order:</strong> #${node.seq}</p>`;
    if (s.source_tables && s.source_tables.length) {
      h += `<p style="margin-top:6px"><strong>Source Tables:</strong></p><ul style="font-size:0.8rem;margin:4px 0 4px 16px">`;
      s.source_tables.slice(0, 10).forEach(t => { h += `<li>${escapeHTML(t.name)}</li>`; });
      if (s.source_tables.length > 10) h += `<li style="color:var(--text2)">+${s.source_tables.length - 10} more</li>`;
      h += '</ul>';
    }
    if (s.target_tables && s.target_tables.length) {
      h += `<p><strong>Target Tables:</strong></p><ul style="font-size:0.8rem;margin:4px 0 4px 16px">`;
      s.target_tables.forEach(t => {
        h += `<li>${escapeHTML(t.name)}${t.load_type ? ' <code style="font-size:0.7rem">' + escapeHTML(t.load_type) + '</code>' : ''}</li>`;
      });
      h += '</ul>';
    }
    if (node.hasConflict && node.conflictDetails && node.conflictDetails.length) {
      h += '<div class="alert alert-warn" style="margin:8px 0;padding:8px 12px;font-size:0.8rem"><strong>Conflicts:</strong><br>';
      node.conflictDetails.forEach(c => { h += `${escapeHTML(c.table_name)} &mdash; ${escapeHTML(c.conflict_type)}<br>`; });
      h += '</div>';
    }
  } else if (diagramType === 'dependency_graph' && (node.type === 'table_output' || node.type === 'conflict_gate') && node.detail) {
    const d = node.detail;
    if (d.writers && d.writers.length) h += `<p><strong>Writers:</strong> ${d.writers.map(escapeHTML).join(', ')}</p>`;
    if (d.readers && d.readers.length) h += `<p><strong>Readers:</strong> ${d.readers.map(escapeHTML).join(', ')}</p>`;
    if (d.lookups && d.lookups.length) h += `<p><strong>Lookup Readers:</strong> ${d.lookups.map(escapeHTML).join(', ')}</p>`;
    if (d.conflicts && d.conflicts.length) {
      h += '<div class="alert alert-warn" style="margin:8px 0;padding:8px 12px;font-size:0.8rem"><strong>Conflicts:</strong><br>';
      d.conflicts.forEach(c => {
        h += `${escapeHTML(c.conflict_type)}${c.writers ? ' &mdash; Writers: ' + c.writers.map(escapeHTML).join(', ') : ''}<br>`;
      });
      h += '</div>';
    }
  } else if (node.detail && node.detail.columns) {
    const t = node.detail;
    h += `<p><strong>Schema:</strong> ${escapeHTML(t.schema || 'dbo')} &middot; <strong>Rows:</strong> ${t.row_count}</p>`;
    h += '<table style="font-size:0.75rem"><thead><tr><th>Column</th><th>Type</th><th>PK</th><th>Null</th></tr></thead><tbody>';
    t.columns.slice(0, 15).forEach(c => {
      h += `<tr><td>${escapeHTML(c.name)}</td><td>${escapeHTML(c.data_type)}</td><td>${c.is_primary_key ? 'Y' : ''}</td><td>${c.nullable ? 'Y' : 'N'}</td></tr>`;
    });
    if (t.columns.length > 15) h += `<tr><td colspan="4" style="color:var(--text2)">+${t.columns.length - 15} more columns</td></tr>`;
    h += '</tbody></table>';
    if (t.foreign_keys && t.foreign_keys.length) {
      h += '<p style="margin-top:8px"><strong>Foreign Keys:</strong></p>';
      t.foreign_keys.forEach(fk => {
        h += `<p style="font-size:0.8rem"><code>${escapeHTML(fk.column || fk.fk_column)}</code> &rarr; <code>${escapeHTML(fk.references_table)}(${escapeHTML(fk.references_column)})</code></p>`;
      });
    }
  }
  h += '</div>';
  detailEl.innerHTML = h;
}
