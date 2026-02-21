/**
 * ui/step-handlers.js — Main pipeline step handler functions
 *
 * These are the main functions called when users click step buttons:
 * parseInput, runAnalysis, runAssessment, generateNotebook, generateReport.
 */

import { getState, setState, snapshotState, rollbackState, validatePrerequisites } from '../core/state.js';
import { escapeHTML } from '../security/html-sanitizer.js';
import { isSensitiveProp, maskProperty } from '../security/sensitive-props.js';
import { switchTab, setTabStatus, unlockTab } from './tabs.js';
import { getUploadedContent, getUploadedName, getUploadedBytes } from './file-upload.js';
import { parseProgress, parseProgressHide, uiYield } from './progress.js';
import { buildTierData } from './tier-diagram/build-tier-data.js';
import { renderTierDiagram } from './tier-diagram/index.js';
import { handleError, AppError } from '../core/errors.js';
import { metricsHTML, tableHTML, expanderHTML } from '../utils/dom-helpers.js';

/**
 * Show an error alert in a target element.
 * @param {string} targetId - DOM element ID for error display
 * @param {string} message - Error message
 */
function showStepError(targetId, message) {
  const el = document.getElementById(targetId);
  if (el) el.innerHTML = `<div class="alert alert-error">${escapeHTML(message)}</div>`;
}

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
import { CONFIDENCE_THRESHOLDS } from '../constants/confidence-thresholds.js';
import { ROLE_TIER_COLORS } from '../constants/nifi-role-map.js';
import { getProcessorPackages, PACKAGE_MAP, PROC_PACKAGE_KEYS } from '../constants/package-map.js';

// Config
import { getDbxConfig } from '../core/config.js';

// HTML builders (metricsHTML, tableHTML, expanderHTML) imported from
// ../utils/dom-helpers.js — the canonical, XSS-safe implementations.
// Expander toggle click delegation is also handled in dom-helpers.

// ================================================================
// PARSE HELPERS — Format detection, file size, warnings
// ================================================================

/**
 * Detect file format from filename and content for display purposes.
 * @param {string} filename
 * @param {string} raw - raw text content
 * @param {Uint8Array|null} bytes - binary content
 * @returns {{ format: string, cssClass: string }}
 */
function detectFileFormat(filename, raw, bytes) {
  const lower = (filename || '').toLowerCase();
  if (/\.(xml|xml\.gz)$/i.test(lower)) return { format: 'XML', cssClass: 'fmt-xml' };
  if (/\.(json|json\.gz)$/i.test(lower)) return { format: 'JSON', cssClass: 'fmt-json' };
  if (/\.sql$/i.test(lower)) return { format: 'SQL', cssClass: 'fmt-sql' };
  if (/\.(gz|zip|nar|jar|tgz|tar\.gz|tar)$/i.test(lower)) return { format: 'Archive', cssClass: 'fmt-archive' };
  if (/\.(docx|xlsx)$/i.test(lower)) return { format: 'Document', cssClass: 'fmt-document' };
  // Sniff content
  if (bytes && bytes.length >= 2) {
    if (bytes[0] === 0x1f && bytes[1] === 0x8b) return { format: 'Archive (GZip)', cssClass: 'fmt-archive' };
    if (bytes[0] === 0x50 && bytes[1] === 0x4b) return { format: 'Archive (ZIP)', cssClass: 'fmt-archive' };
  }
  if (typeof raw === 'string' && raw.trim()) {
    const t = raw.trimStart();
    if (t.startsWith('<') || t.startsWith('<?xml')) return { format: 'XML', cssClass: 'fmt-xml' };
    if (t.startsWith('{') || t.startsWith('[')) return { format: 'JSON', cssClass: 'fmt-json' };
  }
  return { format: 'Unknown', cssClass: 'fmt-unknown' };
}

/**
 * Format file size in human-readable units.
 * @param {number} sizeBytes
 * @returns {string}
 */
function formatFileSize(sizeBytes) {
  if (sizeBytes < 1024) return sizeBytes + ' B';
  if (sizeBytes < 1024 * 1024) return (sizeBytes / 1024).toFixed(1) + ' KB';
  return (sizeBytes / (1024 * 1024)).toFixed(1) + ' MB';
}

/**
 * Generate parse error diagnostic HTML with line numbers and suggestions.
 * @param {Error} error
 * @param {string} raw - original content
 * @returns {string} HTML string
 */
function buildParseErrorDiagnostics(error, raw) {
  let h = '<div class="parse-error-detail">';
  const msg = error.message || String(error);

  // Try to extract line number from error
  const lineMatch = msg.match(/line\s+(\d+)/i) || msg.match(/position\s+(\d+)/i);
  if (lineMatch && typeof raw === 'string') {
    const lineNum = parseInt(lineMatch[1], 10);
    const lines = raw.split('\n');
    const start = Math.max(0, lineNum - 3);
    const end = Math.min(lines.length, lineNum + 2);
    h += '<div style="font-size:0.78rem;color:var(--text2);margin-bottom:6px">Near line ' + escapeHTML(String(lineNum)) + ':</div>';
    h += '<pre style="margin:0;padding:8px;background:var(--bg);border-radius:4px;font-size:0.75rem;overflow-x:auto">';
    for (let i = start; i < end; i++) {
      const ln = i + 1;
      const isErrorLine = ln === lineNum;
      const prefix = isErrorLine ? '>>> ' : '    ';
      const style = isErrorLine ? 'color:#fca5a5;font-weight:700' : 'color:var(--text2)';
      h += '<span style="' + style + '">' + escapeHTML(prefix + ln + ' | ' + (lines[i] || '').substring(0, 120)) + '</span>\n';
    }
    h += '</pre>';
  }

  // Suggest common fixes
  const suggestions = [];
  if (/xml/i.test(msg)) {
    if (/unexpected.*eof|premature.*end/i.test(msg)) suggestions.push('File appears truncated. Re-export from NiFi and try again.');
    if (/encoding/i.test(msg)) suggestions.push('Try saving the file as UTF-8 without BOM.');
    if (/entity/i.test(msg)) suggestions.push('Check for unescaped special characters (&, <, >) in property values.');
    if (!suggestions.length) suggestions.push('Ensure this is a valid NiFi template XML or flow.xml.gz export.');
  }
  if (/json/i.test(msg)) {
    if (/unexpected.*token/i.test(msg)) suggestions.push('Check for trailing commas or missing quotes in the JSON.');
    if (/unexpected.*end/i.test(msg)) suggestions.push('File appears truncated. Re-export and try again.');
    if (!suggestions.length) suggestions.push('Ensure this is a valid NiFi Registry JSON export.');
  }
  if (/version/i.test(msg)) {
    suggestions.push('This file may be from an unsupported NiFi version. Supported: NiFi 1.x templates and NiFi Registry JSON.');
  }

  if (suggestions.length) {
    h += '<div class="error-suggestion"><strong>Suggestions:</strong><ul style="margin:4px 0 0 16px">';
    suggestions.forEach(s => { h += '<li>' + escapeHTML(s) + '</li>'; });
    h += '</ul></div>';
  }
  h += '</div>';
  return h;
}

/**
 * Build a flow summary preview card after successful parse.
 * @param {object} nifi - parsed _nifi object
 * @param {string} sourceName
 * @param {function} classifyFn
 * @returns {string} HTML string
 */
function buildFlowSummaryCard(nifi, sourceName, classifyFn) {
  // Count processors by role
  const roleCounts = {};
  const ROLES = ['source', 'route', 'transform', 'process', 'sink', 'utility'];
  ROLES.forEach(r => { roleCounts[r] = 0; });
  nifi.processors.forEach(p => {
    const role = classifyFn(p.type);
    roleCounts[role] = (roleCounts[role] || 0) + 1;
  });
  const maxRoleCount = Math.max(1, ...Object.values(roleCounts));

  // Top 5 processor types
  const typeCounts = {};
  nifi.processors.forEach(p => { typeCounts[p.type] = (typeCounts[p.type] || 0) + 1; });
  const top5 = Object.entries(typeCounts).sort((a, b) => b[1] - a[1]).slice(0, 5);

  // Detect NiFi version from metadata if available
  let nifiVersion = '';
  if (nifi.version) nifiVersion = nifi.version;
  else if (nifi.flowEncodingVersion) nifiVersion = 'Encoding v' + nifi.flowEncodingVersion;

  let h = '<div class="flow-summary-card">';
  h += '<div class="fsc-header">';
  h += '<span class="fsc-title">' + escapeHTML(sourceName) + '</span>';
  if (nifiVersion) h += '<span class="fsc-version">NiFi ' + escapeHTML(nifiVersion) + '</span>';
  h += '</div>';

  // Role distribution mini bars
  h += '<div class="fsc-roles">';
  ROLES.forEach(role => {
    const count = roleCounts[role] || 0;
    if (count === 0) return;
    const color = ROLE_TIER_COLORS[role] || '#808495';
    const pct = Math.round((count / maxRoleCount) * 100);
    h += '<div class="fsc-role-bar">';
    h += '<span style="color:' + color + ';min-width:62px;text-transform:uppercase;font-weight:600">' + role + '</span>';
    h += '<div style="flex:1;min-width:40px;max-width:80px;background:var(--surface2);border-radius:3px;height:6px">';
    h += '<div class="fsc-role-fill" style="width:' + pct + '%;background:' + color + '"></div></div>';
    h += '<span style="color:var(--text2);min-width:20px">' + count + '</span>';
    h += '</div>';
  });
  h += '</div>';

  // Top 5 processor type chips
  if (top5.length) {
    h += '<div class="fsc-chips">';
    top5.forEach(([type, count]) => {
      const role = classifyFn(type);
      const color = ROLE_TIER_COLORS[role] || '#808495';
      h += '<span class="fsc-chip" style="border-color:' + color + '44;color:' + color + '">' + escapeHTML(type) + ' <span style="opacity:0.6">x' + count + '</span></span>';
    });
    h += '</div>';
  }

  // External system count
  if (nifi.clouderaTools && nifi.clouderaTools.length) {
    h += '<div style="margin-top:8px;font-size:0.8rem;color:var(--text2)">External Systems: <strong style="color:var(--amber)">' + nifi.clouderaTools.length + '</strong></div>';
  }

  h += '</div>';
  return h;
}

/**
 * Categorize and render parse warnings with severity grouping.
 * @param {string[]} warnings
 * @returns {string} HTML string
 */
function buildCategorizedWarnings(warnings) {
  if (!warnings || !warnings.length) return '';

  const categories = { critical: [], warning: [], info: [] };
  warnings.forEach(w => {
    const lower = w.toLowerCase();
    if (/error|fail|corrupt|invalid|unsupported|missing required/i.test(lower)) {
      categories.critical.push(w);
    } else if (/warn|deprecated|unknown|unrecognized|skipped|ignored/i.test(lower)) {
      categories.warning.push(w);
    } else {
      categories.info.push(w);
    }
  });

  const severityConfig = {
    critical: { label: 'Critical', icon: '&#x26D4;', css: 'severity-critical' },
    warning: { label: 'Warning', icon: '&#x26A0;', css: 'severity-warning' },
    info: { label: 'Info', icon: '&#x2139;', css: 'severity-info' }
  };

  let h = '<div style="margin-top:12px">';
  h += '<h3 style="margin-bottom:8px">Parse Warnings (' + warnings.length + ')</h3>';

  for (const [sev, items] of Object.entries(categories)) {
    if (!items.length) continue;
    const cfg = severityConfig[sev];
    h += '<div class="parse-warning-group' + (sev === 'critical' ? ' open' : '') + '">';
    h += '<div class="parse-warning-header ' + cfg.css + '" data-expander-toggle>';
    h += '<span>' + cfg.icon + ' ' + cfg.label + '</span>';
    h += '<span class="pw-count">' + items.length + ' item' + (items.length !== 1 ? 's' : '') + '</span>';
    h += '<span class="expander-arrow" style="margin-left:8px">&#9654;</span>';
    h += '</div>';
    h += '<div class="parse-warning-body">';
    items.forEach(w => {
      h += '<div class="parse-warning-item">' + escapeHTML(w) + '</div>';
    });
    h += '</div></div>';
  }
  h += '</div>';
  return h;
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
  const uploadedBytesData = getUploadedBytes();
  const pasteEl = document.getElementById('pasteInput');
  const raw = uploadedContent || (pasteEl ? pasteEl.value : '');
  if (!raw.trim() && !uploadedBytesData) {
    showStepError('parseResults', 'Please upload or paste a NiFi flow file (XML, JSON, SQL, or archive).');
    return;
  }

  const snapshot = snapshotState();
  const btn = document.getElementById('parseBtn');
  if (btn) btn.disabled = true;
  setTabStatus('load', 'processing');

  const prog = document.getElementById('parseProgress');
  if (prog) prog.style.display = 'flex';
  const bar = document.getElementById('parsePBar');
  const pctEl = document.getElementById('parsePPct');
  const status = document.getElementById('parsePStatus');
  const updateProg = (p, msg) => {
    if (bar) bar.style.width = p + '%';
    if (pctEl) pctEl.textContent = p + '%';
    if (status) status.textContent = msg;
  };

  // 1.1 File Format Detection & Validation
  const uploadedName = getUploadedName();
  const fileFormat = detectFileFormat(uploadedName, raw, uploadedBytesData);
  const rawSize = typeof raw === 'string' ? new Blob([raw]).size : (uploadedBytesData ? uploadedBytesData.byteLength : 0);

  updateProg(5, 'Detected format: ' + fileFormat.format + '...');
  await new Promise(r => setTimeout(r, 20));

  // Show format badge and file size in parseResults immediately
  {
    const parseResultsEl = document.getElementById('parseResults');
    if (parseResultsEl) {
      let previewH = '<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin:8px 0">';
      if (uploadedName) previewH += '<span style="font-size:0.9rem">' + escapeHTML(uploadedName) + '</span>';
      previewH += '<span class="format-badge ' + fileFormat.cssClass + '">' + escapeHTML(fileFormat.format) + '</span>';
      if (rawSize > 0) {
        const sizeStr = formatFileSize(rawSize);
        if (rawSize > 50 * 1024 * 1024) {
          previewH += '<span class="file-size-warn">&#x26A0; ' + escapeHTML(sizeStr) + ' (exceeds 50MB limit)</span>';
        } else if (rawSize > 10 * 1024 * 1024) {
          previewH += '<span class="file-size-warn">&#x26A0; ' + escapeHTML(sizeStr) + ' (large file, may be slow)</span>';
        } else {
          previewH += '<span class="file-size-info">' + escapeHTML(sizeStr) + '</span>';
        }
      }
      previewH += '</div>';
      parseResultsEl.innerHTML = previewH;
    }
  }

  // 1.3 Archive Extraction Progress — show extraction status for archive formats
  const isArchive = /archive/i.test(fileFormat.format);
  if (isArchive) {
    updateProg(10, 'Extracting archive contents...');
  } else {
    updateProg(10, 'Cleaning & parsing input...');
  }
  await new Promise(r => setTimeout(r, 20));

  let parsed;

  try {
    parsed = await parseFlow(raw, uploadedName || 'NiFi Flow', { bytes: uploadedBytesData });
  } catch (e) {
    rollbackState(snapshot);
    handleError(new AppError('Parse failed: ' + e.message, { code: 'PARSE_FAILED', phase: 'parse', severity: 'high', cause: e }));
    // 1.2 Parse Error Diagnostics — show error with line numbers and suggestions
    let errorH = '<div class="alert alert-error">Failed to parse: ' + escapeHTML(e.message) + '</div>';
    errorH += buildParseErrorDiagnostics(e, raw);
    // 1.3 Show partial results if any processors were parsed before failure
    if (parsed && parsed._nifi && parsed._nifi.processors && parsed._nifi.processors.length > 0) {
      errorH += '<div class="alert alert-warn" style="margin-top:8px">Partial parse: found ' + parsed._nifi.processors.length + ' processor(s) before failure.</div>';
    }
    const parseResultsEl = document.getElementById('parseResults');
    if (parseResultsEl) parseResultsEl.innerHTML = (parseResultsEl.innerHTML || '') + errorH;
    if (btn) btn.disabled = false;
    setTabStatus('load', 'ready');
    if (prog) prog.style.display = 'none';
    return;
  }

  updateProg(20, 'Validating flow...');
  await new Promise(r => setTimeout(r, 20));

  if (!parsed || !parsed._nifi || parsed._nifi.processors.length === 0) {
    showStepError('parseResults', 'No processors found. Supported formats: NiFi XML/JSON, SQL scripts, .gz/.zip/.nar/.jar archives, .docx/.xlsx documents.');
    if (btn) btn.disabled = false;
    setTabStatus('load', 'ready');
    if (prog) prog.style.display = 'none';
    return;
  }

  updateProg(50, 'Processing flow...');
  if (parsed._deferredProcessorWork) {
    try {
      const work = parsed._deferredProcessorWork;
      const batchSize = 50;
      const totalProcs = work.processors.length;
      for (let i = 0; i < totalProcs; i += batchSize) {
        work.batchFn(work.processors.slice(i, i + batchSize));
        const pct = 50 + Math.round((i / totalProcs) * 30);
        // 1.3 Archive extraction progress — show real-time extraction count
        if (isArchive) {
          updateProg(pct, 'Extracting... ' + Math.min(i + batchSize, totalProcs) + '/' + totalProcs + ' files');
        } else {
          updateProg(pct, 'Analyzing processor ' + (i + 1) + '/' + totalProcs + '...');
        }
        await new Promise(r => setTimeout(r, 0));
      }
      work.finalize();
    } catch (e) {
      handleError(new AppError('Deferred processor work failed', { code: 'DEFERRED_WORK_FAILED', phase: 'parse', cause: e }));
    }
  }

  updateProg(85, 'Building resource manifest...');
  await new Promise(r => setTimeout(r, 20));

  setState({ parsed, manifest: buildResourceManifest(parsed._nifi) });

  updateProg(95, 'Rendering results...');
  const nifi = parsed._nifi;
  const classifyFn = classifyNiFiProcessor;

  // 1.1 Format badge + success message
  let h = '<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin:8px 0">';
  if (uploadedName) h += '<span style="font-size:0.9rem">' + escapeHTML(uploadedName) + '</span>';
  h += '<span class="format-badge ' + fileFormat.cssClass + '">' + escapeHTML(fileFormat.format) + '</span>';
  if (rawSize > 0) h += '<span class="file-size-info">' + escapeHTML(formatFileSize(rawSize)) + '</span>';
  h += '</div>';

  h += '<div class="alert alert-success" style="margin-top:8px">Successfully parsed NiFi flow: <strong>' + escapeHTML(parsed.source_name) + '</strong></div>';

  // 1.4 Flow Summary Preview Card
  h += buildFlowSummaryCard(nifi, parsed.source_name, classifyFn);

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

  // 1.5 Parse Warnings Enhancement — categorized with severity
  if (parsed.parse_warnings.length) {
    h += buildCategorizedWarnings(parsed.parse_warnings);
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

  // Auto-run all steps sequentially with state validation between each step
  try {
    await new Promise(r => setTimeout(r, 50));
    switchTab('analyze'); await runAnalysis();
    if (!getState().analysis) { console.error('[auto-run] Analysis failed, stopping pipeline'); return; }

    await new Promise(r => setTimeout(r, 50));
    switchTab('assess'); await runAssessment();
    if (!getState().assessment) { console.error('[auto-run] Assessment failed, stopping pipeline'); return; }

    await new Promise(r => setTimeout(r, 50));
    switchTab('convert'); await generateNotebook();
    if (!getState().notebook) { console.error('[auto-run] Notebook generation failed, stopping pipeline'); return; }

    await new Promise(r => setTimeout(r, 50));
    switchTab('report'); await generateReport();
    if (!getState().migrationReport) { console.error('[auto-run] Report generation failed, stopping pipeline'); return; }

    await new Promise(r => setTimeout(r, 50));
    if (typeof window.generateFinalReport === 'function') {
      switchTab('reportFinal');
      await window.generateFinalReport();
      if (!getState().finalReport) { console.error('[auto-run] Final report failed, stopping pipeline'); return; }
    }

    await new Promise(r => setTimeout(r, 50));
    if (typeof window.runValidation === 'function') {
      switchTab('validate');
      await window.runValidation();
      if (!getState().validation) { console.error('[auto-run] Validation failed, stopping pipeline'); return; }
    }

    await new Promise(r => setTimeout(r, 50));
    if (typeof window.runValueAnalysis === 'function') {
      switchTab('value');
      await window.runValueAnalysis();
    }
  } catch (e) {
    handleError(new AppError('Auto-run pipeline failed', { code: 'PIPELINE_AUTO_RUN_FAILED', phase: 'pipeline', cause: e }));
  }
}

// ================================================================
// ANALYZE — Deep Flow Analysis
// ================================================================

/**
 * Run deep flow analysis.
 * Extracted from index.html lines 7483-7590.
 */
export async function runAnalysis() {
  const prereq = validatePrerequisites('analyze');
  if (!prereq.ok) {
    showStepError('analyzeResults', 'Prerequisites not met: ' + prereq.missing.join(', ') + '. Please complete earlier steps first.');
    return;
  }
  const STATE = getState();
  const snapshot = snapshotState();
  setTabStatus('analyze', 'processing');

  try {
  const nifi = STATE.parsed._nifi;
  const manifest = STATE.manifest;
  const depGraph = buildDependencyGraph(nifi);
  const systems = detectExternalSystems(nifi);
  let h = '';
  const elCount = Object.keys(nifi.deepPropertyInventory?.nifiEL || {}).length;
  const sqlCount = nifi.sqlTables ? nifi.sqlTables.length : 0;
  const credCount = Object.keys(nifi.deepPropertyInventory?.credentialRefs || {}).length;
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
        const dot = conf >= CONFIDENCE_THRESHOLDS.MAPPED ? 'high' : conf >= CONFIDENCE_THRESHOLDS.PARTIAL ? 'med' : 'low';
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
    const confCls = conf >= CONFIDENCE_THRESHOLDS.MAPPED ? 'high' : conf >= CONFIDENCE_THRESHOLDS.PARTIAL ? 'med' : 'low';
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
        const dv = hasEL ? String(masked).replace(/\$\{([^}]+)\}/g, (m, g) => `<span class="el-highlight">\${${escapeHTML(g)}}</span>`) : escapeHTML(String(masked || ''));
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
  setTimeout(() => {
    try { renderTierDiagram(tierData, 'analysisTierContainer', 'analysisTierDetail', 'analysisTierLegend'); }
    catch (e) { console.error('[analyze] Tier diagram render error:', e); }
  }, 50);
  setTabStatus('analyze', 'done');
  unlockTab('assess');
  const assessNotReady = document.getElementById('assessNotReady');
  const assessReady = document.getElementById('assessReady');
  if (assessNotReady) assessNotReady.classList.add('hidden');
  if (assessReady) assessReady.classList.remove('hidden');
  } catch (e) {
    rollbackState(snapshot);
    handleError(new AppError('Analysis failed: ' + e.message, { code: 'ANALYZE_FAILED', phase: 'analyze', cause: e }));
    showStepError('analyzeResults', 'Analysis failed: ' + e.message);
    setTabStatus('analyze', 'ready');
  }
}

// ================================================================
// ASSESS — Migration Readiness Assessment
// ================================================================

/**
 * Run migration readiness assessment.
 * Extracted from index.html lines 7596-7696.
 */
export async function runAssessment() {
  const prereq = validatePrerequisites('assess');
  if (!prereq.ok) {
    showStepError('assessResults', 'Prerequisites not met: ' + prereq.missing.join(', ') + '. Please complete earlier steps first.');
    return;
  }
  const STATE = getState();
  const snapshot = snapshotState();
  setTabStatus('assess', 'processing');

  try {
  const nifi = STATE.parsed._nifi;
  const mappings = mapNiFiToDatabricks(nifi);
  const depGraph = buildDependencyGraph(nifi);
  const systems = detectExternalSystems(nifi);
  const total = mappings.length;
  const autoProcs = mappings.filter(m => m.mapped && m.confidence >= CONFIDENCE_THRESHOLDS.MAPPED);
  const manualProcs = mappings.filter(m => m.mapped && m.confidence > 0 && m.confidence < CONFIDENCE_THRESHOLDS.MAPPED);
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

  // 3.2 Risk-Weighted Effort Scoring — compute risk per processor
  // Build fan-in / fan-out from depGraph
  const riskMap = {};
  mappings.forEach(m => {
    const fanIn = (depGraph.fullUpstream[m.name] || []).length;
    const fanOut = (depGraph.fullDownstream[m.name] || []).length;
    const connectivity = fanIn * fanOut;
    let risk = 'Low';
    let riskCss = 'risk-low';
    if (connectivity >= 12 || (fanIn >= 4 && fanOut >= 4)) { risk = 'Critical'; riskCss = 'risk-critical'; }
    else if (connectivity >= 6 || fanIn >= 3 || fanOut >= 3) { risk = 'High'; riskCss = 'risk-high'; }
    else if (connectivity >= 2 || fanIn >= 2 || fanOut >= 2) { risk = 'Med'; riskCss = 'risk-med'; }
    riskMap[m.name] = { risk, riskCss, fanIn, fanOut, connectivity };
  });

  // 3.3 Migration Phase Suggestion — auto-assign phases from role
  const phaseMap = {};
  mappings.forEach(m => {
    let phase, phaseCss;
    if (m.role === 'source') { phase = 1; phaseCss = 'phase-1'; }
    else if (m.role === 'sink') { phase = 3; phaseCss = 'phase-3'; }
    else { phase = 2; phaseCss = 'phase-2'; }
    phaseMap[m.name] = { phase, phaseCss };
  });

  // Per-Processor Confidence table with 3.1 score explanation, 3.2 risk, 3.3 phase, 3.4 justification
  h += '<hr class="divider"><h3>Per-Processor Confidence</h3>';
  const confRows = mappings.map(m => {
    const confCls = m.confidence >= CONFIDENCE_THRESHOLDS.MAPPED ? 'high' : m.confidence >= CONFIDENCE_THRESHOLDS.PARTIAL ? 'med' : 'low';
    const ups = depGraph.fullUpstream[m.name] || [];
    const downs = depGraph.fullDownstream[m.name] || [];
    const mapEntry = NIFI_DATABRICKS_MAP[m.type];
    const riskInfo = riskMap[m.name] || { risk: 'Low', riskCss: 'risk-low', fanIn: 0, fanOut: 0 };
    const phaseInfo = phaseMap[m.name] || { phase: 2, phaseCss: 'phase-2' };

    // 3.1 Confidence Score Explanation tooltip
    const templateConf = mapEntry ? Math.round(mapEntry.conf * 100) : 0;
    const propCount = Object.keys((nifi.processors.find(p => p.name === m.name) || {}).properties || {}).length;
    const propScore = Math.min(100, Math.round(propCount > 0 ? 80 : 50));
    const roleScore = m.role === 'source' || m.role === 'sink' ? 90 : m.role === 'transform' ? 85 : 75;
    let confCell = '<span class="conf-dot ' + confCls + '"></span>' + Math.round(m.confidence * 100) + '%';
    confCell += '<span class="info-tooltip-trigger">?<span class="info-tooltip-content">';
    confCell += '<strong>Confidence Breakdown</strong><br>';
    confCell += 'Template: ' + templateConf + '%<br>';
    confCell += 'Props Coverage: ' + propScore + '%<br>';
    confCell += 'Role Match: ' + roleScore + '%';
    confCell += '</span></span>';

    // 3.4 Mapping Justification tooltip
    let approachCell;
    if (m.mapped) {
      approachCell = escapeHTML((m.desc || '').substring(0, 50));
      const justification = mapEntry
        ? 'Mapped via ' + escapeHTML(mapEntry.cat) + ' template. ' + escapeHTML(mapEntry.notes || '')
        : 'Role-based fallback template';
      approachCell += '<span class="info-tooltip-trigger">?<span class="info-tooltip-content">';
      approachCell += '<strong>Why this mapping</strong><br>' + justification;
      approachCell += '</span></span>';
    } else {
      approachCell = '<em style="color:var(--red)">' + escapeHTML((m.gapReason || 'Unmapped').substring(0, 50)) + '</em>';
    }

    // 3.2 Risk column
    const riskCell = '<span class="risk-badge ' + riskInfo.riskCss + '">' + riskInfo.risk + '</span>';

    // 3.3 Phase column
    const phaseCell = '<span class="phase-badge ' + phaseInfo.phaseCss + '">Phase ' + phaseInfo.phase + '</span>';

    return [
      escapeHTML(m.name),
      '<span style="color:' + (ROLE_TIER_COLORS[m.role] || '#808495') + '">' + m.role + '</span>',
      escapeHTML(m.group),
      confCell,
      approachCell,
      riskCell,
      phaseCell,
      m.confidence >= CONFIDENCE_THRESHOLDS.MAPPED ? '0.5d' : m.confidence >= CONFIDENCE_THRESHOLDS.PARTIAL ? '2d' : '5d',
      ups.length + ' up / ' + downs.length + ' down'
    ];
  });
  h += tableHTML(['Processor', 'Role', 'Group', 'Confidence', 'Approach', 'Risk', 'Phase', 'Effort', 'Deps'], confRows);

  // 3.3 Migration Phase Summary
  const phaseCounts = { 1: 0, 2: 0, 3: 0 };
  Object.values(phaseMap).forEach(p => { phaseCounts[p.phase]++; });
  h += '<hr class="divider"><h3>Suggested Migration Phases</h3>';
  h += '<div style="display:flex;gap:16px;flex-wrap:wrap;margin:8px 0">';
  h += '<div style="flex:1;min-width:140px;padding:12px;background:var(--surface);border-radius:8px;border-left:3px solid #60a5fa">';
  h += '<div style="font-weight:700;color:#60a5fa">Phase 1: Sources</div>';
  h += '<div style="font-size:0.85rem;color:var(--text2)">' + phaseCounts[1] + ' processor' + (phaseCounts[1] !== 1 ? 's' : '') + '</div>';
  h += '<div style="font-size:0.78rem;color:var(--text2);margin-top:4px">Ingestion endpoints, file readers, streaming consumers</div>';
  h += '</div>';
  h += '<div style="flex:1;min-width:140px;padding:12px;background:var(--surface);border-radius:8px;border-left:3px solid #c084fc">';
  h += '<div style="font-weight:700;color:#c084fc">Phase 2: Transforms</div>';
  h += '<div style="font-size:0.85rem;color:var(--text2)">' + phaseCounts[2] + ' processor' + (phaseCounts[2] !== 1 ? 's' : '') + '</div>';
  h += '<div style="font-size:0.78rem;color:var(--text2);margin-top:4px">Routing, transformation, processing, enrichment</div>';
  h += '</div>';
  h += '<div style="flex:1;min-width:140px;padding:12px;background:var(--surface);border-radius:8px;border-left:3px solid #86efac">';
  h += '<div style="font-weight:700;color:#86efac">Phase 3: Sinks</div>';
  h += '<div style="font-size:0.85rem;color:var(--text2)">' + phaseCounts[3] + ' processor' + (phaseCounts[3] !== 1 ? 's' : '') + '</div>';
  h += '<div style="font-size:0.78rem;color:var(--text2);margin-top:4px">Output writers, database sinks, notification endpoints</div>';
  h += '</div>';
  h += '</div>';

  // 3.5 Package Dependency Intelligence — per-processor package tracking
  const pkgToProcessors = {};  // pkg name -> Set of processor names
  const pkgToInfo = {};        // pkg name -> PACKAGE_MAP entry
  mappings.forEach(m => {
    const pkgs = getProcessorPackages(m.type);
    pkgs.forEach(pkgInfo => {
      pkgInfo.pip.forEach(pipPkg => {
        if (!pkgToProcessors[pipPkg]) pkgToProcessors[pipPkg] = new Set();
        pkgToProcessors[pipPkg].add(m.name);
        if (!pkgToInfo[pipPkg]) pkgToInfo[pipPkg] = pkgInfo;
      });
    });
  });

  const allPkgs = new Set();
  Object.keys(pkgToProcessors).forEach(p => allPkgs.add(p));

  if (allPkgs.size) {
    h += '<hr class="divider"><h3>Required Packages (' + allPkgs.size + ')</h3>';

    // Check for potential version conflicts (packages that share base names)
    const baseNames = {};
    [...allPkgs].forEach(pkg => {
      const base = pkg.replace(/[^a-zA-Z]/g, '').toLowerCase();
      if (!baseNames[base]) baseNames[base] = [];
      baseNames[base].push(pkg);
    });
    const conflicts = Object.values(baseNames).filter(v => v.length > 1);
    if (conflicts.length) {
      h += '<div class="pkg-warn">&#x26A0; Potential package conflicts detected: ';
      h += conflicts.map(c => c.map(p => '<code>' + escapeHTML(p) + '</code>').join(' vs ')).join('; ');
      h += '. Review versions carefully.</div>';
    }

    // Package cards showing which processors need each package
    h += '<div class="pkg-grid">';
    [...allPkgs].sort().forEach(pkg => {
      const procs = pkgToProcessors[pkg];
      const info = pkgToInfo[pkg];
      h += '<div class="pkg-card">';
      h += '<div class="pkg-name">' + escapeHTML(pkg) + '</div>';
      if (info && info.desc) h += '<div style="font-size:0.72rem;color:var(--text2)">' + escapeHTML(info.desc) + '</div>';
      if (info && info.dbx) h += '<div class="pkg-dbx">' + escapeHTML(info.dbx) + '</div>';
      h += '<div class="pkg-procs">Used by: ' + [...procs].map(p => escapeHTML(p)).join(', ') + '</div>';
      h += '</div>';
    });
    h += '</div>';

    h += '<pre style="background:var(--bg);padding:12px;border-radius:6px;font-size:0.8rem;margin-top:8px"># requirements.txt\n';
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
  } catch (e) {
    rollbackState(snapshot);
    handleError(new AppError('Assessment failed: ' + e.message, { code: 'ASSESS_FAILED', phase: 'assess', cause: e }));
    showStepError('assessResults', 'Assessment failed: ' + e.message);
    setTabStatus('assess', 'ready');
  }
}

// ================================================================
// GENERATE NOTEBOOK
// ================================================================

/**
 * Generate the Databricks notebook.
 * Extracted from index.html lines 7698-7810.
 */
export async function generateNotebook() {
  const prereq = validatePrerequisites('convert');
  if (!prereq.ok) {
    showStepError('notebookResults', 'Prerequisites not met: ' + prereq.missing.join(', ') + '. Please complete earlier steps first.');
    return;
  }
  const STATE = getState();
  const snapshot = snapshotState();
  setTabStatus('convert', 'processing');

  try {
  const nifi = STATE.parsed._nifi;
  const cfg = getDbxConfig();
  // Single source of truth: always use assessment mappings if available
  const mappings = STATE.assessment?.mappings || mapNiFiToDatabricks(nifi);
  const nbResult = generateDatabricksNotebook(mappings, nifi, STATE.analysis ? STATE.analysis.blueprint : assembleBlueprint(STATE.parsed), cfg);
  const cells = nbResult.cells;

  // Package requirements cell
  const _allPkgs = new Set();
  mappings.forEach(m => { getProcessorPackages(m.type).forEach(p => p.pip.forEach(pkg => _allPkgs.add(pkg))); });
  if (_allPkgs.size) {
    cells.unshift({
      type: 'code', role: 'config', label: 'Package Requirements',
      source: '# Install required packages\n' + [..._allPkgs].sort().map(p => '# MAGIC %pip install ' + p).join('\n') + '\ndbutils.library.restartPython()'
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
  h += `<button class="btn" id="dlNotebookBtnInline">Download .py Notebook</button>`;
  h += `<button class="btn" id="dlWorkflowBtnInline">Download Workflow JSON</button>`;
  h += '</div>';

  const notebookResultsEl = document.getElementById('notebookResults');
  if (notebookResultsEl) {
    notebookResultsEl.innerHTML = h;
    const dlNbBtn = document.getElementById('dlNotebookBtnInline');
    if (dlNbBtn) dlNbBtn.addEventListener('click', () => { if (typeof window.downloadNotebook === 'function') window.downloadNotebook(); });
    const dlWfBtn = document.getElementById('dlWorkflowBtnInline');
    if (dlWfBtn) dlWfBtn.addEventListener('click', () => { if (typeof window.downloadWorkflow === 'function') window.downloadWorkflow(); });
  }
  setTabStatus('convert', 'done');

  const reportNotReady = document.getElementById('reportNotReady');
  const reportReady = document.getElementById('reportReady');
  if (reportNotReady) reportNotReady.classList.add('hidden');
  if (reportReady) reportReady.classList.remove('hidden');
  unlockTab('report');
  } catch (e) {
    rollbackState(snapshot);
    handleError(new AppError('Notebook generation failed: ' + e.message, { code: 'GENERATE_FAILED', phase: 'convert', cause: e }));
    showStepError('notebookResults', 'Notebook generation failed: ' + e.message);
    setTabStatus('convert', 'ready');
  }
}

// ================================================================
// GENERATE REPORT
// ================================================================

/**
 * Generate the migration report.
 * Extracted from index.html lines 7844-7938.
 */
export async function generateReport() {
  const prereq = validatePrerequisites('report');
  if (!prereq.ok) {
    showStepError('reportResults', 'Prerequisites not met: ' + prereq.missing.join(', ') + '. Please complete earlier steps first.');
    return;
  }
  const STATE = getState();
  const snapshot = snapshotState();
  setTabStatus('report', 'processing');

  try {
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
    ['Effort', report.effort]
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

  // Gap Analysis with Resolution Playbook (5.1)
  if (report.gapPlaybook && report.gapPlaybook.length) {
    h += '<hr class="divider"><h3>Gap Analysis &amp; Resolution Playbook</h3>';
    report.gapPlaybook.forEach(g => {
      h += `<div class="gap-card" style="margin-bottom:12px">`;
      h += `<div class="gap-title">${escapeHTML(g.name)} <span class="gap-meta">${escapeHTML(g.type)} &middot; ${escapeHTML(g.group || 'ungrouped')}</span></div>`;
      h += `<div class="gap-rec" style="margin-bottom:8px">${escapeHTML(g.recommendation || 'Manual implementation required')}</div>`;
      if (g.alternatives && g.alternatives.length) {
        h += `<div style="margin-top:8px;font-size:0.85rem"><strong>Resolution Alternatives:</strong></div>`;
        g.alternatives.forEach((alt, idx) => {
          const feasColor = alt.feasibility === 'High' ? '#4caf50' : alt.feasibility === 'Medium' ? '#ff9800' : '#f44336';
          h += `<div style="margin:6px 0 6px 12px;padding:8px;background:var(--bg2);border-radius:6px;border-left:3px solid ${feasColor}">`;
          h += `<div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap">`;
          h += `<strong>${idx + 1}. ${escapeHTML(alt.alternative)}</strong>`;
          h += `<span style="font-size:0.8rem;color:${feasColor}">Feasibility: ${escapeHTML(alt.feasibility)} | Effort: ${escapeHTML(alt.effort)}</span>`;
          h += `</div>`;
          if (alt.snippet) {
            h += `<pre style="margin:6px 0 0;padding:6px;background:var(--bg3);border-radius:4px;font-size:0.78rem;overflow-x:auto">${escapeHTML(alt.snippet)}</pre>`;
          }
          h += `</div>`;
        });
      }
      h += `</div>`;
    });
  } else if (report.gaps && report.gaps.length) {
    h += '<hr class="divider"><h3>Gap Analysis</h3>';
    report.gaps.forEach(g => {
      h += `<div class="gap-card">`;
      h += `<div class="gap-title">${escapeHTML(g.name)} <span class="gap-meta">${escapeHTML(g.type)} &middot; ${escapeHTML(g.group || 'ungrouped')}</span></div>`;
      h += `<div class="gap-rec">${escapeHTML(g.recommendation || 'Manual implementation required')}</div>`;
      h += `</div>`;
    });
  }

  // Per-Processor Migration Guides (5.2)
  if (report.processorGuides && report.processorGuides.length) {
    h += '<hr class="divider"><h3>Per-Processor Migration Guides</h3>';
    report.processorGuides.forEach(pg => {
      h += `<div style="margin:8px 0;padding:10px;background:var(--bg2);border-radius:8px;border:1px solid var(--border)">`;
      h += `<div style="font-weight:700;font-size:0.95rem;margin-bottom:6px">${escapeHTML(pg.name)} <span style="color:var(--text2);font-weight:400">(${escapeHTML(pg.type)})</span></div>`;
      h += `<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:0.85rem">`;
      h += `<div><strong>NiFi Behavior:</strong><br>${escapeHTML(pg.nifiDescription)}</div>`;
      h += `<div><strong>Databricks Approach:</strong><br>${escapeHTML(pg.databricksApproach)}</div>`;
      h += `</div>`;
      if (pg.keyDifferences && pg.keyDifferences.length) {
        h += `<div style="margin-top:6px;font-size:0.82rem"><strong>Key Differences:</strong><ul style="margin:4px 0;padding-left:18px">`;
        pg.keyDifferences.forEach(d => { h += `<li>${escapeHTML(d)}</li>`; });
        h += `</ul></div>`;
      }
      if (pg.migrationSteps && pg.migrationSteps.length) {
        h += `<div style="margin-top:4px;font-size:0.82rem"><strong>Migration Steps:</strong><ol style="margin:4px 0;padding-left:18px">`;
        pg.migrationSteps.forEach(ms => { h += `<li>${escapeHTML(ms)}</li>`; });
        h += `</ol></div>`;
      }
      h += `<div style="margin-top:4px;font-size:0.82rem;color:var(--text2)">Estimated effort: ${escapeHTML(pg.estimatedEffort)}</div>`;
      h += `</div>`;
    });
  }

  // Risk & Impact Matrix (5.3)
  if (report.riskMatrix && report.riskMatrix.length) {
    h += '<hr class="divider"><h3>Risk &amp; Impact Matrix</h3>';
    h += `<div class="table-scroll"><table class="mapping-table"><thead><tr>`;
    h += `<th>Processor</th><th>Type</th><th>Role</th><th>Criticality</th><th>Downstream</th><th>Complexity</th><th>Risk Score</th></tr></thead><tbody>`;
    report.riskMatrix.forEach(r => {
      const ratingColor = r.rating === 'red' ? '#f44336' : r.rating === 'amber' ? '#ff9800' : '#4caf50';
      const ratingBg = r.rating === 'red' ? 'rgba(244,67,54,0.08)' : r.rating === 'amber' ? 'rgba(255,152,0,0.08)' : 'rgba(76,175,80,0.08)';
      h += `<tr style="background:${ratingBg}">`;
      h += `<td>${escapeHTML(r.name)}</td>`;
      h += `<td>${escapeHTML(r.type)}</td>`;
      h += `<td>${escapeHTML(r.role)}</td>`;
      h += `<td style="text-align:center">${r.criticalityScore}/5</td>`;
      h += `<td style="text-align:center">${escapeHTML(r.downstreamImpact)}</td>`;
      h += `<td style="text-align:center">${r.complexityScore}/5</td>`;
      h += `<td style="text-align:center;font-weight:700;color:${ratingColor}">${r.totalRiskScore}%</td>`;
      h += `</tr>`;
    });
    h += `</tbody></table></div>`;
    const redCount = report.riskMatrix.filter(r => r.rating === 'red').length;
    const amberCount = report.riskMatrix.filter(r => r.rating === 'amber').length;
    const greenCount = report.riskMatrix.filter(r => r.rating === 'green').length;
    h += `<div style="margin-top:8px;font-size:0.85rem;display:flex;gap:16px">`;
    h += `<span style="color:#f44336;font-weight:600">Critical: ${redCount}</span>`;
    h += `<span style="color:#ff9800;font-weight:600">Moderate: ${amberCount}</span>`;
    h += `<span style="color:#4caf50;font-weight:600">Low: ${greenCount}</span>`;
    h += `</div>`;
  }

  // Timeline & Resource Estimation (5.4)
  if (report.timeline) {
    const tl = report.timeline;
    h += '<hr class="divider"><h3>Timeline &amp; Resource Estimation</h3>';
    h += metricsHTML([
      ['Total Hours', tl.totalHours + 'h'],
      ['Mapped Work', tl.mappedHours + 'h'],
      ['Gap Work', tl.gapHours + 'h'],
      ['Testing', tl.testingHours + 'h'],
      ['Calendar Weeks', tl.totalWeeks + 'w'],
      ['Team Size', tl.recommendedTeamSize],
    ]);
    h += `<div style="margin-top:12px">`;
    const phaseColors = ['#2196F3', '#4CAF50', '#FF9800', '#9C27B0', '#F44336'];
    tl.phases.forEach((phase, i) => {
      const pctWidth = Math.round(phase.weeks / Math.max(tl.totalWeeks, 1) * 100);
      h += `<div style="margin:8px 0">`;
      h += `<div style="display:flex;justify-content:space-between;font-size:0.85rem;margin-bottom:2px">`;
      h += `<strong>${escapeHTML(phase.name)}</strong>`;
      h += `<span style="color:var(--text2)">${phase.weeks} week(s)</span>`;
      h += `</div>`;
      h += `<div class="progress-bar"><div class="progress-fill" style="width:${pctWidth}%;background:${phaseColors[i % phaseColors.length]}"></div></div>`;
      h += `<ul style="margin:4px 0;padding-left:18px;font-size:0.82rem;color:var(--text2)">`;
      phase.tasks.forEach(t => { h += `<li>${escapeHTML(t)}</li>`; });
      h += `</ul></div>`;
    });
    h += `</div>`;
    if (tl.teamComposition) {
      h += `<div style="margin-top:8px;font-size:0.85rem"><strong>Recommended Team:</strong><ul style="margin:4px 0;padding-left:18px">`;
      tl.teamComposition.forEach(tc => { h += `<li>${escapeHTML(tc)}</li>`; });
      h += `</ul></div>`;
    }
  }

  // Migration Success Criteria (5.5)
  if (report.successCriteria) {
    const sc = report.successCriteria;
    h += '<hr class="divider"><h3>Migration Success Criteria</h3>';

    h += `<div style="margin:8px 0"><strong style="font-size:0.9rem">Pipeline-Level Acceptance Criteria</strong></div>`;
    h += `<div class="table-scroll"><table class="mapping-table"><thead><tr><th>Metric</th><th>Target</th><th>Description</th></tr></thead><tbody>`;
    sc.pipelineCriteria.forEach(c => {
      h += `<tr><td style="font-weight:600">${escapeHTML(c.metric)}</td><td>${escapeHTML(c.target)}</td><td style="font-size:0.85rem">${escapeHTML(c.description)}</td></tr>`;
    });
    h += `</tbody></table></div>`;

    h += `<div style="margin:12px 0 8px"><strong style="font-size:0.9rem">Data Comparison Metrics</strong></div>`;
    h += `<div class="table-scroll"><table class="mapping-table"><thead><tr><th>Metric</th><th>Method</th><th>Threshold</th></tr></thead><tbody>`;
    sc.comparisonMetrics.forEach(c => {
      h += `<tr><td style="font-weight:600">${escapeHTML(c.metric)}</td><td style="font-size:0.85rem">${escapeHTML(c.description)}</td><td>${escapeHTML(c.threshold)}</td></tr>`;
    });
    h += `</tbody></table></div>`;

    const shownCriteria = sc.processorCriteria.slice(0, 8);
    if (shownCriteria.length) {
      h += `<div style="margin:12px 0 8px"><strong style="font-size:0.9rem">Processor-Level Acceptance Tests</strong> <span style="font-size:0.82rem;color:var(--text2)">(showing ${shownCriteria.length} of ${sc.processorCriteria.length})</span></div>`;
      shownCriteria.forEach(pc => {
        h += `<div style="margin:4px 0;padding:6px;background:var(--bg2);border-radius:4px;font-size:0.85rem">`;
        h += `<strong>${escapeHTML(pc.name)}</strong> <span style="color:var(--text2)">(${escapeHTML(pc.type)})</span>`;
        h += `<ul style="margin:2px 0;padding-left:16px">`;
        pc.acceptanceTests.forEach(t => { h += `<li>${escapeHTML(t)}</li>`; });
        h += `</ul></div>`;
      });
    }
  }

  // Recommendations
  if (report.recommendations && report.recommendations.length) {
    h += '<hr class="divider"><h3>Recommendations</h3>';
    h += '<ul style="margin:0;padding-left:20px">';
    report.recommendations.forEach(r => { h += `<li style="margin:4px 0">${escapeHTML(r)}</li>`; });
    h += '</ul>';
  }

  h += '<hr class="divider"><div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">';
  h += `<button class="btn" id="dlReportBtnInline">Download Report (Markdown)</button>`;
  h += '</div>';

  const reportResultsEl = document.getElementById('reportResults');
  if (reportResultsEl) {
    reportResultsEl.innerHTML = h;
    const dlRptBtn = document.getElementById('dlReportBtnInline');
    if (dlRptBtn) dlRptBtn.addEventListener('click', () => { if (typeof window.downloadReport === 'function') window.downloadReport(); });
  }
  setTabStatus('report', 'done');

  const reportFinalNotReady = document.getElementById('reportFinalNotReady');
  const reportFinalReady = document.getElementById('reportFinalReady');
  if (reportFinalNotReady) reportFinalNotReady.classList.add('hidden');
  if (reportFinalReady) reportFinalReady.classList.remove('hidden');
  unlockTab('reportFinal');
  } catch (e) {
    rollbackState(snapshot);
    handleError(new AppError('Report generation failed: ' + e.message, { code: 'REPORT_FAILED', phase: 'report', cause: e }));
    showStepError('reportResults', 'Report generation failed: ' + e.message);
    setTabStatus('report', 'ready');
  }
}
