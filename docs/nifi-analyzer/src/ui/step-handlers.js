/**
 * ui/step-handlers.js — Main pipeline step handler functions
 *
 * These are the main functions called when users click step buttons:
 * parseInput, runAnalysis, runAssessment, generateNotebook, generateReport.
 */

import { getState, setState } from '../core/state.js';
import { escapeHTML } from '../security/html-sanitizer.js';
import { isSensitiveProp, maskProperty } from '../security/sensitive-props.js';
import { switchTab, setTabStatus, unlockTab } from './tabs.js';
import { getUploadedContent, getUploadedName } from './file-upload.js';
import { parseProgress, parseProgressHide, uiYield } from './progress.js';
import { buildTierData } from './tier-diagram/build-tier-data.js';
import { renderTierDiagram } from './tier-diagram/index.js';

// Parsers
import { parseFlow } from '../parsers/index.js';

// Analyzers
import { buildResourceManifest } from '../analyzers/resource-manifest.js';
import { assembleBlueprint } from '../analyzers/blueprint-assembler.js';
import { buildDependencyGraph } from '../analyzers/dependency-graph.js';
import { detectExternalSystems } from '../analyzers/external-systems.js';

// Mappers
import { classifyNiFiProcessor } from '../mappers/processor-classifier.js';
import { mapNiFiToDatabricksAuto as mapNiFiToDatabricks } from '../mappers/index.js';

// Generators
import { generateDatabricksNotebook } from '../generators/notebook-generator.js';
import { generateWorkflowJSON } from '../generators/workflow-generator.js';

// Reporters
import { generateMigrationReport } from '../reporters/migration-report.js';

// Constants
import { NIFI_DATABRICKS_MAP } from '../constants/nifi-databricks-map.js';
import { ROLE_TIER_COLORS } from '../constants/nifi-role-map.js';
import { getProcessorPackages } from '../constants/package-map.js';

// Config
import { getDbxConfig } from '../core/config.js';

// ================================================================
// HELPER HTML BUILDERS (extracted from index.html lines 7294-7328)
// ================================================================

/**
 * Build a metrics row from label/value items.
 * @param {Array} items
 * @returns {string}
 */
function metricsHTML(items) {
  return '<div class="metrics">' + items.map(item => {
    const l = Array.isArray(item) ? item[0] : item.label;
    const v = Array.isArray(item) ? item[1] : item.value;
    const d = Array.isArray(item) ? item[2] : item.delta;
    const c = Array.isArray(item) ? '' : (item.color || '');
    return `<div class="metric"><div class="label">${l}</div><div class="value"${c ? ' style="color:' + c + '"' : ''}>${v}</div>${d ? `<div class="delta">${d}</div>` : ''}</div>`;
  }).join('') + '</div>';
}

/**
 * Build an HTML table from headers and rows.
 * @param {string[]} headers
 * @param {Array[]}  rows
 * @returns {string}
 */
function tableHTML(headers, rows) {
  return `<div class="table-scroll"><table><thead><tr>${headers.map(h => `<th>${h}</th>`).join('')}</tr></thead><tbody>${rows.map(r => `<tr>${r.map(c => `<td>${c ?? ''}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
}

/**
 * Build an expandable section.
 * @param {string}  title
 * @param {string}  content
 * @param {boolean} open
 * @returns {string}
 */
function expanderHTML(title, content, open = false) {
  return `<div class="expander ${open ? 'open' : ''}"><div class="expander-header" data-expander-toggle><span>${title}</span><span class="expander-arrow">\u25B6</span></div><div class="expander-body">${content}</div></div>`;
}

// Delegate expander toggle clicks via event delegation
if (typeof document !== 'undefined') {
  document.addEventListener('click', (e) => {
    const header = e.target.closest('[data-expander-toggle]');
    if (header) header.parentElement.classList.toggle('open');
  });
}

// ================================================================
// PARSE INPUT — NiFi XML Only
// ================================================================

/**
 * Parse the uploaded or pasted NiFi flow XML.
 * Extracted from index.html lines 7357-7477.
 * Auto-runs all subsequent steps after successful parse.
 */
export async function parseInput() {
  const uploadedContent = getUploadedContent();
  const pasteEl = document.getElementById('pasteInput');
  const raw = uploadedContent || (pasteEl ? pasteEl.value : '');
  if (!raw.trim()) {
    const parseResultsEl = document.getElementById('parseResults');
    if (parseResultsEl) parseResultsEl.innerHTML = '<div class="alert alert-error">Please upload or paste a NiFi flow XML.</div>';
    return;
  }

  const btn = document.getElementById('parseBtn');
  if (btn) btn.disabled = true;
  setTabStatus('load', 'processing');

  const prog = document.getElementById('parseProgress');
  if (prog) prog.style.display = 'flex';
  const bar = document.getElementById('parsePBar');
  const pct = document.getElementById('parsePPct');
  const status = document.getElementById('parsePStatus');
  const updateProg = (p, msg) => {
    if (bar) bar.style.width = p + '%';
    if (pct) pct.textContent = p + '%';
    if (status) status.textContent = msg;
  };

  updateProg(10, 'Cleaning & parsing input...');
  await new Promise(r => setTimeout(r, 50));

  let parsed;
  const uploadedName = getUploadedName();
  try {
    parsed = parseFlow(raw, uploadedName || 'NiFi Flow');
  } catch (e) {
    const parseResultsEl = document.getElementById('parseResults');
    if (parseResultsEl) parseResultsEl.innerHTML = '<div class="alert alert-error">Failed to parse: ' + escapeHTML(e.message) + '</div>';
    if (btn) btn.disabled = false;
    setTabStatus('load', 'ready');
    if (prog) prog.style.display = 'none';
    return;
  }

  updateProg(20, 'Validating flow...');
  await new Promise(r => setTimeout(r, 50));

  if (!parsed || !parsed._nifi || parsed._nifi.processors.length === 0) {
    const parseResultsEl = document.getElementById('parseResults');
    if (parseResultsEl) parseResultsEl.innerHTML = '<div class="alert alert-error">No NiFi processors found. Please provide a valid NiFi flow XML.</div>';
    if (btn) btn.disabled = false;
    setTabStatus('load', 'ready');
    if (prog) prog.style.display = 'none';
    return;
  }

  updateProg(50, 'Processing flow...');
  if (parsed._deferredProcessorWork) {
    const work = parsed._deferredProcessorWork;
    const batchSize = 50;
    for (let i = 0; i < work.processors.length; i += batchSize) {
      work.batchFn(work.processors.slice(i, i + batchSize));
      updateProg(50 + Math.round((i / work.processors.length) * 30), 'Analyzing processor ' + (i + 1) + '/' + work.processors.length + '...');
      await new Promise(r => setTimeout(r, 0));
    }
    work.finalize();
  }

  updateProg(85, 'Building resource manifest...');
  await new Promise(r => setTimeout(r, 50));

  setState({ parsed, manifest: buildResourceManifest(parsed._nifi) });

  updateProg(95, 'Rendering results...');
  const nifi = parsed._nifi;
  const classifyFn = classifyNiFiProcessor;

  let h = '<div class="alert alert-success" style="margin-top:16px">Successfully parsed NiFi flow: <strong>' + escapeHTML(parsed.source_name) + '</strong></div>';
  h += metricsHTML([
    { label: 'Processors', value: nifi.processors.length },
    { label: 'Connections', value: nifi.connections.length },
    { label: 'Process Groups', value: nifi.processGroups.length },
    { label: 'Controller Services', value: nifi.controllerServices.length },
    { label: 'External Systems', value: nifi.clouderaTools.length }
  ]);

  const typeCounts = {};
  nifi.processors.forEach(p => { typeCounts[p.type] = (typeCounts[p.type] || 0) + 1; });
  const typeRows = Object.entries(typeCounts).sort((a, b) => b[1] - a[1]).map(([type, count]) => {
    const role = classifyFn(type);
    const roleColor = ROLE_TIER_COLORS[role] || '#808495';
    return ['<span style="color:' + roleColor + ';font-weight:600">' + escapeHTML(type) + '</span>', count, '<span class="badge" style="background:' + roleColor + '22;color:' + roleColor + '">' + role + '</span>'];
  });
  h += expanderHTML('Processor Types (' + Object.keys(typeCounts).length + ' unique)', tableHTML(['Type', 'Count', 'Role'], typeRows));

  if (parsed.parse_warnings.length) {
    h += parsed.parse_warnings.map(w => '<div class="alert alert-warn" style="margin:4px 0;font-size:0.85rem">' + escapeHTML(w) + '</div>').join('');
  }

  const parseResultsEl = document.getElementById('parseResults');
  if (parseResultsEl) parseResultsEl.innerHTML = h;

  updateProg(100, 'Done!');
  if (btn) btn.disabled = false;
  setTabStatus('load', 'done');
  unlockTab('analyze');

  const analyzeNotReady = document.getElementById('analyzeNotReady');
  const analyzeReady = document.getElementById('analyzeReady');
  if (analyzeNotReady) analyzeNotReady.classList.add('hidden');
  if (analyzeReady) analyzeReady.classList.remove('hidden');
  setTimeout(() => { if (prog) prog.style.display = 'none'; }, 500);

  // Auto-run all steps sequentially
  await new Promise(r => setTimeout(r, 200));
  switchTab('analyze'); runAnalysis();
  await new Promise(r => setTimeout(r, 150));
  switchTab('assess'); runAssessment();
  await new Promise(r => setTimeout(r, 150));
  switchTab('convert'); generateNotebook();
  await new Promise(r => setTimeout(r, 150));
  switchTab('report'); generateReport();
  await new Promise(r => setTimeout(r, 150));
  // Additional steps (reportFinal, validate, value) are called from the
  // monolithic HTML and will be wired up in later extraction phases.
  if (typeof window.generateFinalReport === 'function') {
    switchTab('reportFinal');
    await window.generateFinalReport();
  }
  await new Promise(r => setTimeout(r, 150));
  if (typeof window.runValidation === 'function') {
    switchTab('validate');
    await window.runValidation();
  }
  await new Promise(r => setTimeout(r, 150));
  if (typeof window.runValueAnalysis === 'function') {
    switchTab('value');
    window.runValueAnalysis();
  }
}

// ================================================================
// ANALYZE — Deep Flow Analysis
// ================================================================

/**
 * Run deep flow analysis.
 * Extracted from index.html lines 7483-7590.
 */
export function runAnalysis() {
  const STATE = getState();
  if (!STATE.parsed || !STATE.parsed._nifi) return;
  setTabStatus('analyze', 'processing');

  const nifi = STATE.parsed._nifi;
  const manifest = STATE.manifest;
  const depGraph = buildDependencyGraph(nifi);
  const systems = detectExternalSystems(nifi);
  let h = '';
  const elCount = Object.keys(nifi.deepPropertyInventory.nifiEL || {}).length;
  const sqlCount = nifi.sqlTables ? nifi.sqlTables.length : 0;
  const credCount = Object.keys(nifi.deepPropertyInventory.credentialRefs || {}).length;
  const sysCount = Object.keys(systems).length;
  h += metricsHTML([
    { label: 'Processors', value: nifi.processors.length },
    { label: 'Connections', value: nifi.connections.length },
    { label: 'Process Groups', value: nifi.processGroups.length },
    { label: 'Controller Services', value: nifi.controllerServices.length },
    { label: 'External Systems', value: sysCount },
    { label: 'EL Expressions', value: elCount },
    { label: 'SQL Tables', value: sqlCount },
    { label: 'Credentials', value: credCount }
  ]);

  const blueprint = assembleBlueprint(STATE.parsed);
  const tierData = buildTierData(blueprint, STATE.parsed);
  h += '<hr class="divider"><h3>Flow Visualization</h3>';
  h += '<div id="analysisTierContainer" style="position:relative"></div><div id="analysisTierDetail"></div><div id="analysisTierLegend"></div>';

  // External Systems
  const sysKeys = Object.keys(systems);
  if (sysKeys.length) {
    h += '<hr class="divider"><h3>External Systems &amp; Dependencies (' + sysKeys.length + ')</h3>';
    h += '<p style="color:var(--text2);font-size:0.82rem;margin-bottom:8px">Detected from processor types, JDBC URLs, and properties.</p>';
    sysKeys.forEach(key => {
      const sys = systems[key];
      const procList = sys.processors.map(p => {
        const conf = NIFI_DATABRICKS_MAP[p.type] ? NIFI_DATABRICKS_MAP[p.type].conf : 0;
        const dot = conf >= 0.7 ? 'high' : conf >= 0.3 ? 'med' : 'low';
        return '<span class="conf-dot ' + dot + '"></span>' + escapeHTML(p.name) + ' <span style="color:var(--text2)">(' + p.direction + ')</span>';
      }).join('<br>');
      let body = '<div class="sys-detail-row"><span class="sys-label">Processors:</span><span class="sys-value">' + procList + '</span></div>';
      body += '<div class="sys-detail-row"><span class="sys-label">Databricks:</span><span class="sys-value">' + escapeHTML(sys.dbxApproach) + '</span></div>';
      if (sys.jdbcUrls.length) body += '<div class="sys-detail-row"><span class="sys-label">JDBC:</span><span class="sys-value"><code style="font-size:0.75rem">' + sys.jdbcUrls.map(u => escapeHTML(u)).join('<br>') + '</code></span></div>';
      if (sys.credentials.length) body += '<div class="sys-detail-row"><span class="sys-label">Credentials:</span><span class="sys-value" style="color:var(--amber)">' + sys.credentials.map(c => escapeHTML(c)).join(', ') + '</span></div>';
      if (sys.packages.length) body += '<div class="sys-detail-row"><span class="sys-label">Packages:</span><span class="sys-value"><code>' + sys.packages.join(', ') + '</code></span></div>';
      h += expanderHTML(escapeHTML(sys.name) + ' <span style="color:var(--text2);font-size:0.8rem">(' + sys.processors.length + ' processor' + (sys.processors.length !== 1 ? 's' : '') + ')</span>', body, false);
    });
  }

  // Processor Inventory
  h += '<hr class="divider"><h3>Processor Inventory (' + nifi.processors.length + ')</h3>';
  h += '<p style="color:var(--text2);font-size:0.82rem;margin-bottom:8px">Click to expand for properties, scheduling, and dependencies.</p>';
  nifi.processors.forEach((p) => {
    const role = classifyNiFiProcessor(p.type);
    const roleColor = ROLE_TIER_COLORS[role] || '#808495';
    const mapEntry = NIFI_DATABRICKS_MAP[p.type];
    const conf = mapEntry ? mapEntry.conf : 0;
    const confCls = conf >= 0.7 ? 'high' : conf >= 0.3 ? 'med' : 'low';
    const ups = depGraph.upstream[p.name] || [];
    const downs = depGraph.downstream[p.name] || [];
    const fullUps = depGraph.fullUpstream[p.name] || [];
    const fullDowns = depGraph.fullDownstream[p.name] || [];
    let body = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:8px">';
    body += '<div><strong style="font-size:0.78rem;color:var(--text2)">Type:</strong> ' + escapeHTML(p.type) + '</div>';
    body += '<div><strong style="font-size:0.78rem;color:var(--text2)">Role:</strong> <span style="color:' + roleColor + '">' + role + '</span></div>';
    body += '<div><strong style="font-size:0.78rem;color:var(--text2)">Group:</strong> ' + escapeHTML(p.group || '(root)') + '</div>';
    body += '<div><strong style="font-size:0.78rem;color:var(--text2)">State:</strong> ' + (p.state || 'UNKNOWN') + '</div>';
    body += '<div><strong style="font-size:0.78rem;color:var(--text2)">Scheduling:</strong> ' + (p.schedulingStrategy || '-') + ' / ' + (p.schedulingPeriod || '-') + '</div>';
    body += '<div><strong style="font-size:0.78rem;color:var(--text2)">Confidence:</strong> <span class="conf-dot ' + confCls + '"></span>' + Math.round(conf * 100) + '%</div></div>';
    if (ups.length || downs.length) {
      body += '<div style="display:flex;gap:16px;margin-bottom:8px;font-size:0.8rem">';
      if (ups.length) body += '<div><strong style="color:#3B82F6">Upstream (' + fullUps.length + '):</strong> ' + ups.map(n => escapeHTML(n)).join(', ') + '</div>';
      if (downs.length) body += '<div><strong style="color:#21C354">Downstream (' + fullDowns.length + '):</strong> ' + downs.map(n => escapeHTML(n)).join(', ') + '</div>';
      body += '</div>';
    }
    const props = Object.entries(p.properties || {});
    if (props.length) {
      body += '<table style="width:100%;font-size:0.78rem;border-collapse:collapse">';
      props.forEach(([k, v]) => {
        const masked = isSensitiveProp(k) ? '********' : v;
        const hasEL = v && v.includes('${');
        const dv = hasEL ? String(masked).replace(/\$\{([^}]+)\}/g, '<span class="el-highlight">${$1}</span>') : escapeHTML(String(masked || ''));
        body += '<tr><td style="color:var(--text2);padding:2px 6px;border-bottom:1px solid var(--border);white-space:nowrap">' + escapeHTML(k) + '</td><td style="padding:2px 6px;border-bottom:1px solid var(--border);word-break:break-all">' + dv + '</td></tr>';
      });
      body += '</table>';
    }
    if (mapEntry) body += '<div style="margin-top:8px;padding:8px;background:var(--bg);border-radius:4px;font-size:0.75rem"><strong style="color:var(--green)">Databricks: </strong>' + escapeHTML(mapEntry.desc) + '<br><span style="color:var(--text2)">' + escapeHTML(mapEntry.notes) + '</span></div>';
    const title = '<span class="conf-dot ' + confCls + '"></span><span style="color:' + roleColor + '">[' + role.toUpperCase() + ']</span> ' + escapeHTML(p.name) + ' <span style="color:var(--text2);font-size:0.8rem">' + escapeHTML(p.type) + '</span>';
    h += expanderHTML(title, body, false);
  });

  // Connection Map
  h += '<hr class="divider"><h3>Connection Map (' + nifi.connections.length + ')</h3>';
  if (nifi.connections.length) {
    const connRows = nifi.connections.map(c => [escapeHTML(c.sourceName || c.sourceId), escapeHTML(c.destinationName || c.destinationId), (c.relationships || []).map(r => '<span class="badge" style="background:var(--surface2);font-size:0.7rem">' + escapeHTML(r) + '</span>').join(' '), c.backPressure ? escapeHTML(c.backPressure) : '-']);
    h += tableHTML(['Source', 'Destination', 'Relationships', 'Back Pressure'], connRows);
  }

  // Controller Services
  if (nifi.controllerServices.length) {
    h += '<hr class="divider"><h3>Controller Services (' + nifi.controllerServices.length + ')</h3>';
    h += tableHTML(['Name', 'Type', 'State', 'Properties'], nifi.controllerServices.map(cs => [escapeHTML(cs.name), escapeHTML(cs.type), cs.state || '-', Object.keys(cs.properties || {}).length + ' props']));
  }

  // NiFi Expression Language
  const elEntries = Object.entries(nifi.deepPropertyInventory.nifiEL || {});
  if (elEntries.length) {
    h += '<hr class="divider"><h3>NiFi Expression Language (' + elEntries.length + ')</h3>';
    h += tableHTML(['Expression', 'Used By'], elEntries.slice(0, 50).map(([expr, procs]) => ['<code class="el-highlight">' + escapeHTML(expr.substring(0, 80)) + '</code>', (Array.isArray(procs) ? procs : [procs]).map(p => escapeHTML(String(p))).join(', ')]));
    if (elEntries.length > 50) h += '<p style="color:var(--text2);font-size:0.82rem">... and ' + (elEntries.length - 50) + ' more</p>';
  }

  // Scheduling Summary
  const schedCounts = { TIMER_DRIVEN: 0, CRON_DRIVEN: 0, EVENT_DRIVEN: 0, OTHER: 0 };
  nifi.processors.forEach(p => { const s = (p.schedulingStrategy || '').toUpperCase(); if (s in schedCounts) schedCounts[s]++; else schedCounts.OTHER++; });
  h += '<hr class="divider"><h3>Scheduling Summary</h3>';
  h += metricsHTML([{ label: 'Timer Driven', value: schedCounts.TIMER_DRIVEN }, { label: 'Cron Driven', value: schedCounts.CRON_DRIVEN }, { label: 'Event Driven', value: schedCounts.EVENT_DRIVEN }]);

  const analyzeResultsEl = document.getElementById('analyzeResults');
  if (analyzeResultsEl) analyzeResultsEl.innerHTML = h;

  setState({ analysis: { blueprint, tierData, depGraph, systems } });
  setTimeout(() => { renderTierDiagram(tierData, 'analysisTierContainer', 'analysisTierDetail', 'analysisTierLegend'); }, 50);
  setTabStatus('analyze', 'done');
  unlockTab('assess');
  const assessNotReady = document.getElementById('assessNotReady');
  const assessReady = document.getElementById('assessReady');
  if (assessNotReady) assessNotReady.classList.add('hidden');
  if (assessReady) assessReady.classList.remove('hidden');
}

// ================================================================
// ASSESS — Migration Readiness Assessment
// ================================================================

/**
 * Run migration readiness assessment.
 * Extracted from index.html lines 7596-7696.
 */
export function runAssessment() {
  const STATE = getState();
  if (!STATE.parsed || !STATE.parsed._nifi) return;
  setTabStatus('assess', 'processing');

  const nifi = STATE.parsed._nifi;
  const mappings = mapNiFiToDatabricks(nifi);
  const depGraph = buildDependencyGraph(nifi);
  const systems = detectExternalSystems(nifi);
  const total = mappings.length;
  const autoProcs = mappings.filter(m => m.mapped && m.confidence >= 0.7);
  const manualProcs = mappings.filter(m => m.mapped && m.confidence > 0 && m.confidence < 0.7);
  const unsupportedProcs = mappings.filter(m => !m.mapped || m.confidence === 0);
  const mappedProcs = mappings.filter(m => m.mapped);
  const autoConvertPct = total ? autoProcs.length / total : 0;
  const avgMappedConf = mappedProcs.length ? mappedProcs.reduce((s, m) => s + m.confidence, 0) / mappedProcs.length : 0;
  const coveragePct = total ? mappedProcs.length / total : 0;
  const systemCount = Object.keys(systems).length;
  const systemSimplicity = Math.max(0, 1 - (systemCount / 20));
  const readinessScore = Math.round((autoConvertPct * 50) + (avgMappedConf * 20) + (coveragePct * 20) + (systemSimplicity * 10));
  const effortDays = (autoProcs.length * 0.5) + (manualProcs.length * 2) + (unsupportedProcs.length * 5);
  const effortWeeks = Math.ceil(effortDays / 5);
  const cls = readinessScore >= 75 ? 'green' : readinessScore >= 40 ? 'amber' : 'red';
  const icon = readinessScore >= 75 ? '&#x1F7E2;' : readinessScore >= 40 ? '&#x1F7E1;' : '&#x1F534;';
  const lvl = readinessScore >= 75 ? 'HIGH READINESS' : readinessScore >= 40 ? 'MODERATE READINESS' : 'LOW READINESS';

  let h = '<hr class="divider">';
  h += '<div class="score-big" style="color:var(--' + cls + ')">' + icon + ' ' + lvl + ' &mdash; ' + readinessScore + '%</div>';
  h += metricsHTML([{ label: 'Auto-Convert', value: autoProcs.length, color: 'var(--green)' }, { label: 'Manual', value: manualProcs.length, color: 'var(--amber)' }, { label: 'Unsupported', value: unsupportedProcs.length, color: 'var(--red)' }, { label: 'Effort Est.', value: effortDays.toFixed(0) + ' days (~' + effortWeeks + ' wks)' }]);
  h += metricsHTML([{ label: 'Auto-Convert % (50w)', value: Math.round(autoConvertPct * 100) + '%' }, { label: 'Avg Confidence (20w)', value: Math.round(avgMappedConf * 100) + '%' }, { label: 'Coverage (20w)', value: Math.round(coveragePct * 100) + '%' }, { label: 'Simplicity (10w)', value: Math.round(systemSimplicity * 100) + '%' }]);

  const autoPctBar = total ? (autoProcs.length / total * 100) : 0;
  const manPctBar = total ? (manualProcs.length / total * 100) : 0;
  const unsPctBar = total ? (unsupportedProcs.length / total * 100) : 0;
  h += '<div class="effort-bar">';
  if (autoPctBar > 0) h += '<div class="effort-seg" style="width:' + autoPctBar + '%;background:var(--green)">' + autoProcs.length + ' auto</div>';
  if (manPctBar > 0) h += '<div class="effort-seg" style="width:' + manPctBar + '%;background:var(--amber)">' + manualProcs.length + ' manual</div>';
  if (unsPctBar > 0) h += '<div class="effort-seg" style="width:' + unsPctBar + '%;background:var(--red)">' + unsupportedProcs.length + ' unsupported</div>';
  h += '</div>';

  // Per-Processor Confidence table
  h += '<hr class="divider"><h3>Per-Processor Confidence</h3>';
  const confRows = mappings.map(m => {
    const confCls = m.confidence >= 0.7 ? 'high' : m.confidence >= 0.3 ? 'med' : 'low';
    const ups = depGraph.fullUpstream[m.name] || [];
    const downs = depGraph.fullDownstream[m.name] || [];
    return [escapeHTML(m.name), '<span style="color:' + (ROLE_TIER_COLORS[m.role] || '#808495') + '">' + m.role + '</span>', escapeHTML(m.group), '<span class="conf-dot ' + confCls + '"></span>' + Math.round(m.confidence * 100) + '%', m.mapped ? escapeHTML((m.desc || '').substring(0, 50)) : '<em style="color:var(--red)">' + (m.gapReason || 'Unmapped').substring(0, 50) + '</em>', m.confidence >= 0.7 ? '0.5d' : m.confidence >= 0.3 ? '2d' : '5d', ups.length + ' up / ' + downs.length + ' down'];
  });
  h += tableHTML(['Processor', 'Role', 'Group', 'Confidence', 'Approach', 'Effort', 'Deps'], confRows);

  // Required Packages
  const allPkgs = new Set();
  mappings.forEach(m => { getProcessorPackages(m.type).forEach(p => p.pip.forEach(pkg => allPkgs.add(pkg))); });
  if (allPkgs.size) {
    h += '<hr class="divider"><h3>Required Packages</h3>';
    h += '<pre style="background:var(--bg);padding:12px;border-radius:6px;font-size:0.8rem"># requirements.txt\n';
    [...allPkgs].sort().forEach(pkg => { h += pkg + '\n'; });
    h += '</pre>';
  }

  const assessResultsEl = document.getElementById('assessResults');
  if (assessResultsEl) assessResultsEl.innerHTML = h;

  setState({
    assessment: {
      mappings, readinessScore, autoCount: autoProcs.length,
      manualCount: manualProcs.length, unsupportedCount: unsupportedProcs.length,
      effortDays, systems, depGraph
    }
  });
  setTabStatus('assess', 'done');
  unlockTab('convert');
  const convertNotReady = document.getElementById('convertNotReady');
  const convertReady = document.getElementById('convertReady');
  if (convertNotReady) convertNotReady.classList.add('hidden');
  if (convertReady) convertReady.classList.remove('hidden');
}

// ================================================================
// GENERATE NOTEBOOK
// ================================================================

/**
 * Generate the Databricks notebook.
 * Extracted from index.html lines 7698-7810.
 */
export function generateNotebook() {
  const STATE = getState();
  if (!STATE.parsed || !STATE.parsed._nifi) return;
  setTabStatus('convert', 'processing');

  const nifi = STATE.parsed._nifi;
  const cfg = getDbxConfig();
  const mappings = STATE.assessment ? STATE.assessment.mappings : mapNiFiToDatabricks(nifi);
  const nbResult = generateDatabricksNotebook(mappings, nifi, STATE.analysis ? STATE.analysis.blueprint : assembleBlueprint(STATE.parsed), cfg);
  const cells = nbResult.cells;

  // Package requirements cell
  const _allPkgs = new Set();
  mappings.forEach(m => { getProcessorPackages(m.type).forEach(p => p.pip.forEach(pkg => _allPkgs.add(pkg))); });
  if (_allPkgs.size) {
    cells.unshift({
      type: 'code', role: 'config', label: 'Package Requirements',
      source: '# Install required packages\n' + [..._allPkgs].sort().map(p => '%pip install ' + p).join('\n') + '\ndbutils.library.restartPython()'
    });
  }

  const workflow = generateWorkflowJSON(mappings, nifi, cfg);
  setState({ notebook: { mappings, cells, workflow, config: cfg } });

  let h = '<hr class="divider">';
  h += '<h3>Processor Mapping</h3>';
  const mapRows = mappings.map(m => [
    escapeHTML(m.name),
    `<span style="color:${ROLE_TIER_COLORS[m.role] || '#808495'}">${escapeHTML(m.role)}</span>`,
    escapeHTML(m.group || '\u2014'),
    m.mapped ? escapeHTML(m.desc) : '<em style="color:var(--text2)">No equivalent</em>',
    m.mapped ? `<span class="conf-badge ${m.confidence >= 0.8 ? 'conf-high' : m.confidence >= 0.5 ? 'conf-med' : 'conf-low'}">${Math.round(m.confidence * 100)}%</span>` : '<span class="conf-badge conf-none">\u2014</span>'
  ]);
  h += `<div class="table-scroll"><table class="mapping-table"><thead><tr><th>NiFi Processor</th><th>Role</th><th>Group</th><th>Databricks Equivalent</th><th>Confidence</th></tr></thead><tbody>${mapRows.map(r => `<tr>${r.map(c => `<td>${c}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;

  h += '<hr class="divider"><h3>Generated Notebook</h3>';
  h += '<div class="notebook-preview">';
  cells.forEach((cell, i) => {
    const lbl = cell.label || (cell.type === 'md' ? 'markdown' : 'code');
    const lblClass = cell.role ? 'lb-' + cell.role : 'lb-config';
    const typeTag = cell.type === 'md' ? ' <span style="opacity:0.5">[md]</span>' : cell.type === 'sql' ? ' <span style="opacity:0.5">[sql]</span>' : '';
    const code = cell.source.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    h += `<div class="notebook-cell"><div class="cell-label ${lblClass}">[${i + 1}] ${lbl}${typeTag}</div><pre>${code}</pre></div>`;
  });
  h += '</div>';

  h += '<hr class="divider"><div style="display:flex;gap:8px;flex-wrap:wrap">';
  h += `<button class="btn" onclick="downloadNotebook()">Download .py Notebook</button>`;
  h += `<button class="btn" onclick="downloadWorkflow()">Download Workflow JSON</button>`;
  h += '</div>';

  const notebookResultsEl = document.getElementById('notebookResults');
  if (notebookResultsEl) notebookResultsEl.innerHTML = h;
  setTabStatus('convert', 'done');

  const reportNotReady = document.getElementById('reportNotReady');
  const reportReady = document.getElementById('reportReady');
  if (reportNotReady) reportNotReady.classList.add('hidden');
  if (reportReady) reportReady.classList.remove('hidden');
  unlockTab('report');
}

// ================================================================
// GENERATE REPORT
// ================================================================

/**
 * Generate the migration report.
 * Extracted from index.html lines 7844-7938.
 */
export function generateReport() {
  const STATE = getState();
  if (!STATE.notebook || !STATE.parsed || !STATE.parsed._nifi) return;
  setTabStatus('report', 'processing');

  const nifi = STATE.parsed._nifi;
  const report = generateMigrationReport(STATE.notebook.mappings, nifi);
  setState({ migrationReport: report });
  const s = report.summary;

  let h = '<hr class="divider">';
  const pct = s.coveragePercent;
  const cls = pct >= 85 ? 'green' : pct >= 60 ? 'amber' : 'red';
  const lvl = pct >= 85 ? 'HIGH COVERAGE' : pct >= 60 ? 'PARTIAL COVERAGE' : 'LOW COVERAGE';
  h += `<div class="score-big">${lvl} \u2014 ${pct}%</div>`;
  h += metricsHTML([
    ['Total Processors', s.totalProcessors],
    ['Mapped', s.mappedProcessors],
    ['Unmapped', s.unmappedProcessors],
    ['Process Groups', s.totalProcessGroups],
    ['Connections', s.totalConnections],
    ['Effort', `<span class="badge badge-${report.effort === 'Low' ? 'green' : report.effort === 'Medium' ? 'amber' : 'red'}">${report.effort}</span>`]
  ]);

  // Coverage by Role
  h += '<hr class="divider"><h3>Coverage by Role</h3>';
  const roleOrder = ['source', 'route', 'transform', 'process', 'sink', 'utility'];
  roleOrder.forEach(role => {
    const rd = report.byRole[role];
    if (!rd) return;
    const rpct = rd.total ? Math.round(rd.mapped / rd.total * 100) : 0;
    const rcls = rpct >= 85 ? 'green' : rpct >= 60 ? 'amber' : 'red';
    const color = ROLE_TIER_COLORS[role] || '#808495';
    h += `<div style="margin:8px 0">`;
    h += `<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:2px">`;
    h += `<span style="font-weight:600;color:${color};text-transform:uppercase;font-size:0.85rem">${role}</span>`;
    h += `<span style="font-size:0.85rem;color:var(--text2)">${rd.mapped}/${rd.total} (${rpct}%)</span>`;
    h += `</div>`;
    h += `<div class="progress-bar"><div class="progress-fill ${rcls}" style="width:${rpct}%"></div></div>`;
    h += `</div>`;
  });

  // Gap Analysis
  if (report.gaps && report.gaps.length) {
    h += '<hr class="divider"><h3>Gap Analysis \u2014 Unmapped Processors</h3>';
    report.gaps.forEach(g => {
      h += `<div class="gap-card">`;
      h += `<div class="gap-title">${escapeHTML(g.name)} <span class="gap-meta">${escapeHTML(g.type)} &middot; ${escapeHTML(g.group || 'ungrouped')}</span></div>`;
      h += `<div class="gap-rec">${escapeHTML(g.recommendation || 'Manual implementation required')}</div>`;
      h += `</div>`;
    });
  }

  // Recommendations
  if (report.recommendations && report.recommendations.length) {
    h += '<hr class="divider"><h3>Recommendations</h3>';
    h += '<ul style="margin:0;padding-left:20px">';
    report.recommendations.forEach(r => { h += `<li style="margin:4px 0">${escapeHTML(r)}</li>`; });
    h += '</ul>';
  }

  h += '<hr class="divider"><div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">';
  h += `<button class="btn" onclick="downloadReport()">Download Report (Markdown)</button>`;
  h += '</div>';

  const reportResultsEl = document.getElementById('reportResults');
  if (reportResultsEl) reportResultsEl.innerHTML = h;
  setTabStatus('report', 'done');

  const reportFinalNotReady = document.getElementById('reportFinalNotReady');
  const reportFinalReady = document.getElementById('reportFinalReady');
  if (reportFinalNotReady) reportFinalNotReady.classList.add('hidden');
  if (reportFinalReady) reportFinalReady.classList.remove('hidden');
  unlockTab('reportFinal');
}
