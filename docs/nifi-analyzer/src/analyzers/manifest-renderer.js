/**
 * analyzers/manifest-renderer.js — Render a resource manifest as HTML
 *
 * Extracted from index.html lines 2138-2243.
 *
 * @module analyzers/manifest-renderer
 */

import { escapeHTML } from '../security/html-sanitizer.js';

/**
 * Helper: generate a simple HTML table from headers and rows.
 * Mirrors the tableHTML() function in the monolith (line 7304).
 *
 * @param {string[]} headers
 * @param {Array<Array<string>>} rows
 * @returns {string}
 */
function tableHTML(headers, rows) {
  return `<div class="table-scroll"><table><thead><tr>${headers.map(h => `<th>${h}</th>`).join('')}</tr></thead><tbody>${rows.map(r => `<tr>${r.map(c => `<td>${c ?? ''}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
}

/**
 * Render a resource manifest object as HTML.
 *
 * @param {object} manifest — output of buildResourceManifest()
 * @returns {string} — HTML string
 */
export function renderManifestHTML(manifest) {
  const m = manifest;
  const dirCount = Object.keys(m.directories).length;
  const fileCount = Object.keys(m.files).length;
  const sqlCount = Object.keys(m.sqlTables).length;
  const tokCount = Object.keys(m.tokens).length;
  const sigCount = Object.keys(m.signals).length;

  let h = '<div class="manifest-grid">';
  h += `<div class="manifest-stat"><div class="num">${dirCount}</div><div class="lbl">Directories</div></div>`;
  h += `<div class="manifest-stat"><div class="num">${fileCount}</div><div class="lbl">Files</div></div>`;
  h += `<div class="manifest-stat"><div class="num">${sqlCount}</div><div class="lbl">SQL Tables</div></div>`;
  h += `<div class="manifest-stat"><div class="num">${tokCount}</div><div class="lbl">Tokens</div></div>`;
  h += `<div class="manifest-stat"><div class="num">${sigCount}</div><div class="lbl">Signals</div></div>`;
  h += `<div class="manifest-stat"><div class="num">${m.scripts.length}</div><div class="lbl">Scripts</div></div>`;
  h += `<div class="manifest-stat"><div class="num">${m.httpEndpoints.length}</div><div class="lbl">HTTP APIs</div></div>`;
  h += `<div class="manifest-stat"><div class="num">${m.kafkaTopics.length}</div><div class="lbl">Kafka Topics</div></div>`;
  if (m.clouderaTools && m.clouderaTools.length) {
    h += `<div class="manifest-stat" style="border-top:3px solid #3B82F6"><div class="num" style="color:#3B82F6">${m.clouderaTools.length}</div><div class="lbl">Cloudera Tools</div></div>`;
  }
  h += '</div>';

  // Directories
  if (dirCount) {
    const rows = Object.entries(m.directories).map(([p, d]) => [escapeHTML(p), d.type, d.processors.map(escapeHTML).join(', ')]);
    h += '<h4 style="margin:12px 0 4px">Directories</h4>' + tableHTML(['Path', 'Type', 'Processors'], rows);
  }
  // SQL Tables
  if (sqlCount) {
    const rows = Object.entries(m.sqlTables).map(([n, t]) => [
      escapeHTML(n),
      t.readers.length ? t.readers.map(escapeHTML).join(', ') : '<em>none</em>',
      t.writers.length ? t.writers.map(escapeHTML).join(', ') : '<em>none</em>',
    ]);
    h += '<h4 style="margin:12px 0 4px">SQL Tables</h4>' + tableHTML(['Table', 'Readers', 'Writers'], rows);
  }
  // Signals & Tokens
  if (sigCount || tokCount) {
    const rows = [];
    Object.entries(m.signals).forEach(([n, s]) => rows.push([escapeHTML(n), 'Signal', s.senders.map(escapeHTML).join(', '), s.waiters.map(escapeHTML).join(', '), s.target]));
    Object.entries(m.tokens).forEach(([n, t]) => rows.push([escapeHTML(n), 'Token', t.acquirers.map(escapeHTML).join(', '), t.releasers.map(escapeHTML).join(', '), '\u2014']));
    h += '<h4 style="margin:12px 0 4px">Tokens &amp; Signals</h4>' + tableHTML(['Name', 'Type', 'Producers', 'Consumers', 'Target'], rows);
  }
  // Scripts
  if (m.scripts.length) {
    const rows = m.scripts.map(s => [escapeHTML(s.path), escapeHTML(s.args), escapeHTML(s.processor)]);
    h += '<h4 style="margin:12px 0 4px">Scripts</h4>' + tableHTML(['Path', 'Arguments', 'Processor'], rows);
  }
  // HTTP Endpoints
  if (m.httpEndpoints.length) {
    const rows = m.httpEndpoints.map(e => [escapeHTML(e.url), e.method, escapeHTML(e.processor)]);
    h += '<h4 style="margin:12px 0 4px">HTTP Endpoints</h4>' + tableHTML(['URL', 'Method', 'Processor'], rows);
  }
  // Kafka Topics
  if (m.kafkaTopics.length) {
    const rows = m.kafkaTopics.map(k => [escapeHTML(k.topic), k.direction, escapeHTML(k.brokers), escapeHTML(k.processor)]);
    h += '<h4 style="margin:12px 0 4px">Kafka Topics</h4>' + tableHTML(['Topic', 'Direction', 'Brokers', 'Processor'], rows);
  }
  // DB Connections
  if (m.dbConnections.length) {
    const rows = m.dbConnections.map(d => [escapeHTML(d.name), escapeHTML(d.url || '\u2014'), escapeHTML(d.processor)]);
    h += '<h4 style="margin:12px 0 4px">Database Connections</h4>' + tableHTML(['Service', 'URL', 'Processor'], rows);
  }
  // Cloudera Tools Inventory
  if (m.clouderaTools && m.clouderaTools.length) {
    const toolsByType = {};
    m.clouderaTools.forEach(t => {
      if (!toolsByType[t.tool]) toolsByType[t.tool] = [];
      toolsByType[t.tool].push(t);
    });
    const toolCount = Object.keys(toolsByType).length;
    const priorityLabel = p => p === 1
      ? '<span style="color:#21C354;font-weight:700">Spark SQL</span>'
      : p === 2
        ? '<span style="color:#3B82F6;font-weight:700">PySpark</span>'
        : '<span style="color:#EAB308;font-weight:700">Python/dbutils</span>';
    h += `<h4 style="margin:16px 0 4px;font-size:1rem;border-bottom:1px solid var(--border);padding-bottom:4px">External Systems Inventory (${m.clouderaTools.length} items, ${toolCount} tool types)</h4>`;
    // Summary badges
    h += '<div style="display:flex;gap:8px;flex-wrap:wrap;margin:8px 0">';
    Object.entries(toolsByType).forEach(([tool, items]) => {
      const colors = { Impala: '#3B82F6', HDFS: '#21C354', Kerberos: '#EAB308', Hive: '#FF6B6B', Kudu: '#8B5CF6', Sqoop: '#F97316', Oozie: '#06B6D4', 'Custom JAR': '#EC4899', 'Shell Script': '#6B7280', 'JDBC Driver': '#14B8A6', 'NiFi Cache': '#A78BFA' };
      h += `<span style="display:inline-flex;align-items:center;gap:4px;padding:4px 10px;background:${colors[tool] || 'var(--surface)'}22;border:1px solid ${colors[tool] || 'var(--border)'};border-radius:6px;font-size:0.8rem"><strong>${escapeHTML(tool)}</strong> <span style="color:var(--text2)">${items.length}</span></span>`;
    });
    h += '</div>';
    // Detailed table per tool type
    Object.entries(toolsByType).forEach(([tool, items]) => {
      const rows = items.map(t => [
        escapeHTML(t.subtype || t.tool),
        escapeHTML(t.processor),
        escapeHTML(t.group || '\u2014'),
        `<code style="font-size:0.7rem;word-break:break-all">${escapeHTML((t.command || '').substring(0, 120))}</code>`,
        `<strong>${escapeHTML(t.dbx_equivalent)}</strong>`,
        priorityLabel(t.dbx_priority),
        `<code style="font-size:0.7rem">${escapeHTML((t.dbx_code || '').substring(0, 100))}</code>`,
        `<span style="font-size:0.72rem;color:var(--text2)">${escapeHTML((t.dbx_notes || '').substring(0, 120))}</span>`,
      ]);
      h += '<div class="expander"><div class="expander-header" onclick="this.parentElement.classList.toggle(\'open\')">'
        + `<span><strong>${escapeHTML(tool)}</strong> \u2014 ${items.length} invocation${items.length > 1 ? 's' : ''}</span><span class="expander-arrow">&#9654;</span></div>`
        + '<div class="expander-body"><div class="table-scroll"><table style="font-size:0.75rem"><thead><tr><th>Subtype</th><th>Processor</th><th>Group</th><th>Command</th><th>DBX Equivalent</th><th>Priority</th><th>DBX Code</th><th>Notes</th></tr></thead><tbody>'
        + rows.map(r => '<tr>' + r.map(c => `<td>${c}</td>`).join('') + '</tr>').join('')
        + '</tbody></table></div></div></div>';
    });
  }
  // Warnings
  if (m.warnings.length) {
    h += '<div style="margin-top:12px">' + m.warnings.map(w => `<div class="alert alert-warn" style="margin:4px 0;padding:6px 12px;font-size:0.82rem">${escapeHTML(w)}</div>`).join('') + '</div>';
  }
  // Disconnected processors
  if (m.disconnected.length) {
    const rows = m.disconnected.map(p => [escapeHTML(p.name), escapeHTML(p.type), escapeHTML(p.group || '\u2014'), escapeHTML(p.state || '\u2014')]);
    h += '<h4 style="margin:12px 0 4px;color:var(--red)">Disconnected Processors</h4>' + tableHTML(['Name', 'Type', 'Group', 'State'], rows);
  }
  return h;
}
