/**
 * main.js — Bootstrap module that wires all imports together
 *
 * Entry point for the Vite-based ES module application.
 * Initializes core infrastructure (state, event bus, config, pipeline, errors),
 * wires UI modules, registers pipeline steps, and binds all DOM event listeners.
 *
 * SECURITY: No inline onclick attributes. All event binding uses addEventListener.
 */

// ================================================================
// 1. CSS Imports (Vite handles these as side-effect imports)
// ================================================================
import '../styles/base.css';
import '../styles/layout.css';
import '../styles/components.css';
import '../styles/metrics.css';
import '../styles/tables.css';
import '../styles/expander.css';
import '../styles/tier-diagram.css';
import '../styles/notebook-preview.css';
import '../styles/forms.css';
import '../styles/charts.css';
import '../styles/validation.css';
import '../styles/simulation.css';
import '../styles/filters.css';
import '../styles/animations.css';
import '../styles/error-panel.css';
import '../styles/json-explorer.css';
import '../styles/progress-viz.css';

// ================================================================
// 2. Core Modules
// ================================================================
import { createStore, getState, setState, resetState, snapshotState, rollbackState } from './core/state.js';
import bus from './core/event-bus.js';
import { loadDbxConfig, saveDbxConfig, getDbxConfig, DBX_CONFIG_DEFAULTS, CLOUD_NODE_TYPES, RUNTIME_VERSIONS } from './core/config.js';
import { handleError, AppError, clearErrorLog, onError, ERROR_CODES } from './core/errors.js';
import { renderErrorPanel, addErrorToPanel, updateErrorBadge } from './ui/error-panel.js';

// ================================================================
// 3. UI Modules
// ================================================================
import { initTabs, setTabStatus, unlockTab } from './ui/tabs.js';
import { escapeHTML } from './security/html-sanitizer.js';
import { metricsHTML } from './utils/dom-helpers.js';
import { initFileUpload, getUploadedContent, getUploadedName, getUploadedBytes } from './ui/file-upload.js';
import { loadSampleFlow, loadSampleFile } from './ui/sample-flows.js';
import {
  parseInput,
  runAnalysis,
  runAssessment,
  generateNotebook,
  generateReport,
} from './ui/step-handlers.js';
import { initJSONExplorer } from './ui/json-explorer.js';
import { initProgressViz, updateProgressViz } from './ui/progress-viz.js';

// ================================================================
// 4. Pipeline Modules
// ================================================================
import { parseFlow } from './parsers/index.js';
import { runAnalysisEngine } from './analyzers/index.js';
import { mapNiFiToDatabricksAuto as mapNiFiToDatabricks } from './mappers/index.js';
import { generateNotebookAndWorkflow } from './generators/index.js';
import { runValidationEngine } from './validators/index.js';
import {
  generateReportSuite,
  generateFinalReport,
  runValueAnalysis,
  downloadNotebook,
  downloadWorkflow,
  downloadReport,
  downloadFinalReport,
  downloadValidationReport,
  downloadValueAnalysis,
  exportAsDatabricksNotebook,
  exportAsJupyterNotebook,
  exportWorkflowYAML,
} from './reporters/index.js';
import { exportAsHTML, exportAsMarkdown, exportProcessorsCSV, exportGapsCSV } from './reporters/multi-format-export.js';
import { NIFI_DATABRICKS_MAP } from './constants/nifi-databricks-map.js';
import { analyzeFlowGraph } from './analyzers/flow-graph-analyzer.js';

// ================================================================
// 5. Validation dimension explanations (7.2)
// ================================================================
const VALIDATION_TOOLTIPS = {
  'Intent Match': 'Measures how well the generated Databricks notebook preserves the original intent of each NiFi processor. Checks that each processor\'s purpose (ingestion, transformation, routing, etc.) is represented in the output code.',
  'Line Coverage': 'Measures the percentage of NiFi processor properties and configurations that have corresponding code in the generated notebook cells. Higher coverage means fewer manual adjustments needed.',
  'Reverse Eng.': 'Evaluates whether the generated code could reconstruct the original NiFi flow behavior, including data flow paths, error handling, and external system connectivity.',
  'Function Map': 'Checks that each NiFi processor function has a corresponding PySpark/Databricks function call. Identifies gaps where NiFi capabilities lack a direct Databricks equivalent.',
};

// ================================================================
// 6. Application Bootstrap
// ================================================================

document.addEventListener('DOMContentLoaded', () => {
  // NOTE: All event listeners below are attached exactly once inside this
  // DOMContentLoaded handler, which itself fires only once. There is no
  // memory-leak risk from repeated listener attachment. If a future refactor
  // moves listener setup into a function called multiple times, use an
  // AbortController to clean up stale listeners before re-attaching.

  // ── 6a. Initialize core infrastructure ──
  const store = createStore();
  const config = loadDbxConfig();

  // Populate config form fields from persisted config on page load
  Object.entries({
    cfgCatalog: config.catalog, cfgSchema: config.schema,
    cfgScope: config.secretScope, cfgCloud: config.cloudProvider,
    cfgNodeType: config.nodeType,
    cfgWorkers: config.numWorkers, cfgWorkspacePath: config.workspacePath,
    cfgComputeType: config.computeType || DBX_CONFIG_DEFAULTS.computeType,
    cfgRuntimeVersion: config.runtimeVersion || DBX_CONFIG_DEFAULTS.runtimeVersion,
  }).forEach(([id, val]) => { const el = document.getElementById(id); if (el) el.value = val ?? ''; });

  // Helper: update the node type <select> options based on selected cloud provider
  function updateNodeTypeOptions(cloud) {
    const nodeTypeEl = document.getElementById('cfgNodeType');
    if (!nodeTypeEl) return;
    const nodes = CLOUD_NODE_TYPES[cloud] || CLOUD_NODE_TYPES.azure;
    const currentVal = nodeTypeEl.value;
    nodeTypeEl.innerHTML = '';
    nodes.forEach(n => {
      const opt = document.createElement('option');
      opt.value = n.value;
      opt.textContent = n.label;
      nodeTypeEl.appendChild(opt);
    });
    // Preserve selection if it exists in new list, otherwise use first
    if (nodes.some(n => n.value === currentVal)) {
      nodeTypeEl.value = currentVal;
    } else {
      nodeTypeEl.value = nodes[0].value;
    }
  }

  // Initialize node type dropdown for current cloud provider
  updateNodeTypeOptions(config.cloudProvider || DBX_CONFIG_DEFAULTS.cloudProvider);
  // Re-set the persisted nodeType after populating options
  const cfgNodeTypeEl = document.getElementById('cfgNodeType');
  if (cfgNodeTypeEl && config.nodeType) cfgNodeTypeEl.value = config.nodeType;

  // Wire cloud provider change to update node types
  const cfgCloudEl = document.getElementById('cfgCloud');
  if (cfgCloudEl) {
    cfgCloudEl.addEventListener('change', () => { updateNodeTypeOptions(cfgCloudEl.value); });
  }

  // Global error handler for uncaught promise rejections
  window.addEventListener('unhandledrejection', (event) => {
    handleError(
      new AppError(event.reason?.message || 'Unhandled promise rejection', {
        code: 'UNHANDLED_REJECTION',
        severity: 'high',
        cause: event.reason,
      })
    );
  });

  // Global error handler for uncaught exceptions
  window.addEventListener('error', (event) => {
    handleError(
      new AppError(event.message || 'Uncaught error', {
        code: 'UNCAUGHT_ERROR',
        severity: 'high',
        context: { filename: event.filename, lineno: event.lineno },
      })
    );
  });

  // ── 6b. Initialize UI modules ──
  initTabs();
  initFileUpload();
  initJSONExplorer();
  initProgressViz();

  // ── 6b-2. Initialize error panel ──
  renderErrorPanel(document.body);
  onError((err) => {
    addErrorToPanel(err);
  });

  // ── 6c. Wire file input change to trigger pipeline ──
  const fileInput = document.getElementById('fileInput');
  if (fileInput) {
    fileInput.addEventListener('change', async () => {
      resetState();
      clearErrorLog();
      updateErrorBadge();
      const errorList = document.getElementById('errorList');
      if (errorList) errorList.innerHTML = '';
      // Wait for FileReader to finish before parsing
      const { handleFile } = await import('./ui/file-upload.js');
      await handleFile();
      parseInput();
    });
  }

  // ── 6d. Wire parse button ──
  const parseBtn = document.getElementById('parseBtn');
  if (parseBtn) {
    parseBtn.addEventListener('click', () => {
      resetState();
      clearErrorLog();
      updateErrorBadge();
      const errorList = document.getElementById('errorList');
      if (errorList) errorList.innerHTML = '';
      parseInput();
    });
  }

  // ── 6e. Wire sample flow buttons ──
  const sampleBtns = document.querySelectorAll('[data-sample-flow]');
  sampleBtns.forEach((btn) => {
    btn.addEventListener('click', () => {
      const flowType = btn.dataset.sampleFlow;
      resetState();
      clearErrorLog();
      updateErrorBadge();
      const errorList = document.getElementById('errorList');
      if (errorList) errorList.innerHTML = '';
      loadSampleFlow(flowType, parseInput);
    });
  });

  // Wire sample file buttons (fetched from URL)
  const sampleFileBtns = document.querySelectorAll('[data-sample-file]');
  sampleFileBtns.forEach((btn) => {
    btn.addEventListener('click', () => {
      const path = btn.dataset.sampleFile;
      const filename = btn.dataset.sampleName || path.split('/').pop();
      resetState();
      clearErrorLog();
      updateErrorBadge();
      const errorList = document.getElementById('errorList');
      if (errorList) errorList.innerHTML = '';
      loadSampleFile(path, filename, parseInput);
    });
  });

  // ── 6f. Wire step buttons (analyze, assess, convert, report) ──
  const analyzeBtn = document.getElementById('analyzeBtn');
  if (analyzeBtn) {
    analyzeBtn.addEventListener('click', async () => {
      analyzeBtn.disabled = true;
      try { await runAnalysis(); } finally { analyzeBtn.disabled = false; updateProgressViz(); }
    });
  }

  const assessBtn = document.getElementById('assessBtn');
  if (assessBtn) {
    assessBtn.addEventListener('click', async () => {
      assessBtn.disabled = true;
      try { await runAssessment(); } finally { assessBtn.disabled = false; updateProgressViz(); }
    });
  }

  const convertBtn = document.getElementById('convertBtn');
  if (convertBtn) {
    convertBtn.addEventListener('click', async () => {
      convertBtn.disabled = true;
      try { await generateNotebook(); } finally { convertBtn.disabled = false; updateProgressViz(); }
    });
  }

  const reportBtn = document.getElementById('reportBtn');
  if (reportBtn) {
    reportBtn.addEventListener('click', async () => {
      reportBtn.disabled = true;
      try { await generateReport(); } finally { reportBtn.disabled = false; updateProgressViz(); }
    });
  }

  // ── 6f-2. Wire step buttons (steps 6-8: final report, validation, value) ──
  const finalReportBtn = document.getElementById('finalReportBtn');
  if (finalReportBtn) {
    finalReportBtn.addEventListener('click', async () => {
      finalReportBtn.disabled = true;
      try { await window.generateFinalReport(); } finally { finalReportBtn.disabled = false; }
    });
  }

  const validateBtn = document.getElementById('validateBtn');
  if (validateBtn) {
    validateBtn.addEventListener('click', async () => {
      validateBtn.disabled = true;
      try { await window.runValidation(); } finally { validateBtn.disabled = false; }
    });
  }

  const valueBtn = document.getElementById('valueBtn');
  if (valueBtn) {
    valueBtn.addEventListener('click', async () => {
      valueBtn.disabled = true;
      try { await window.runValueAnalysis(); } finally { valueBtn.disabled = false; }
    });
  }

  // ── 6g. Wire download buttons ──
  const dlNotebookBtn = document.getElementById('downloadNotebookBtn');
  if (dlNotebookBtn) {
    dlNotebookBtn.addEventListener('click', () => {
      try { downloadNotebook(getState()); } catch (e) { console.error('[download]', e); alert('Download failed: ' + e.message); }
    });
  }

  const dlWorkflowBtn = document.getElementById('downloadWorkflowBtn');
  if (dlWorkflowBtn) {
    dlWorkflowBtn.addEventListener('click', () => {
      try { downloadWorkflow(getState()); } catch (e) { console.error('[download]', e); alert('Download failed: ' + e.message); }
    });
  }

  const dlReportBtn = document.getElementById('downloadReportBtn');
  if (dlReportBtn) {
    dlReportBtn.addEventListener('click', () => {
      try { downloadReport(getState()); } catch (e) { console.error('[download]', e); alert('Download failed: ' + e.message); }
    });
  }

  const dlFinalReportBtn = document.getElementById('downloadFinalReportBtn');
  if (dlFinalReportBtn) {
    dlFinalReportBtn.addEventListener('click', () => {
      try { downloadFinalReport(getState()); } catch (e) { console.error('[download]', e); alert('Download failed: ' + e.message); }
    });
  }

  const dlValidationBtn = document.getElementById('downloadValidationBtn');
  if (dlValidationBtn) {
    dlValidationBtn.addEventListener('click', () => {
      try { downloadValidationReport(getState()); } catch (e) { console.error('[download]', e); alert('Download failed: ' + e.message); }
    });
  }

  const dlValueBtn = document.getElementById('downloadValueBtn');
  if (dlValueBtn) {
    dlValueBtn.addEventListener('click', () => {
      try { downloadValueAnalysis(getState()); } catch (e) { console.error('[download]', e); alert('Download failed: ' + e.message); }
    });
  }

  // ── 6h. Wire export format buttons ──
  const exportDbxBtn = document.getElementById('exportDatabricksBtn');
  if (exportDbxBtn) {
    exportDbxBtn.addEventListener('click', () => {
      const state = getState();
      const cells = state.notebook?.cells || [];
      exportAsDatabricksNotebook(cells);
    });
  }

  const exportJupyterBtn = document.getElementById('exportJupyterBtn');
  if (exportJupyterBtn) {
    exportJupyterBtn.addEventListener('click', () => {
      const state = getState();
      const cells = state.notebook?.cells || [];
      exportAsJupyterNotebook(cells);
    });
  }

  const exportYamlBtn = document.getElementById('exportWorkflowYamlBtn');
  if (exportYamlBtn) {
    exportYamlBtn.addEventListener('click', () => {
      const state = getState();
      if (state.parsed?._nifi) {
        exportWorkflowYAML(state.parsed._nifi, (pgs, conns) => {
          // Simple DAG generation fallback
          return {
            tasks: (pgs || []).map((pg, i) => ({
              task_key: pg.name?.replace(/\s+/g, '_').toLowerCase() || `task_${i}`,
              notebook_task: {
                notebook_path: `/Workspace/Migrations/NiFi/${pg.name || 'task_' + i}`,
              },
              depends_on: i > 0
                ? [{ task_key: (pgs[i - 1].name || `task_${i - 1}`).replace(/\s+/g, '_').toLowerCase() }]
                : [],
            })),
          };
        });
      }
    });
  }

  // ── 6i. Wire config save/load buttons ──
  const cfgSaveBtn = document.getElementById('cfgSaveBtn');
  if (cfgSaveBtn) {
    cfgSaveBtn.addEventListener('click', () => {
      const cfg = getDbxConfig();
      saveDbxConfig(cfg);
      bus.emit('config:saved', cfg);

      // Visual feedback
      const origText = cfgSaveBtn.textContent;
      cfgSaveBtn.textContent = 'Saved!';
      cfgSaveBtn.disabled = true;
      setTimeout(() => {
        cfgSaveBtn.textContent = origText;
        cfgSaveBtn.disabled = false;
      }, 1500);
    });
  }

  const cfgResetBtn = document.getElementById('cfgResetBtn');
  if (cfgResetBtn) {
    cfgResetBtn.addEventListener('click', () => {
      const defaults = { ...DBX_CONFIG_DEFAULTS };
      // Populate form fields from defaults
      const fields = {
        cfgCatalog: defaults.catalog,
        cfgSchema: defaults.schema,
        cfgScope: defaults.secretScope,
        cfgCloud: defaults.cloudProvider,
        cfgComputeType: defaults.computeType,
        cfgRuntimeVersion: defaults.runtimeVersion,
        cfgWorkers: defaults.numWorkers,
        cfgWorkspacePath: defaults.workspacePath,
      };
      Object.entries(fields).forEach(([id, value]) => {
        const el = document.getElementById(id);
        if (el) el.value = value ?? '';
      });
      // Reset node type options for the default cloud provider
      updateNodeTypeOptions(defaults.cloudProvider);
      const nodeTypeEl = document.getElementById('cfgNodeType');
      if (nodeTypeEl) nodeTypeEl.value = defaults.nodeType;
      bus.emit('config:reset', defaults);
    });
  }

  // ── 6j. Wire dynamically-created download buttons in rendered HTML ──
  //
  // Because step-handlers.js renders HTML with onclick attributes for
  // download buttons (legacy pattern), we intercept those via event delegation
  // on the document body. This provides a safety net until the rendered HTML
  // is fully migrated away from inline handlers.
  document.body.addEventListener('click', (e) => {
    // Prefer data-action attribute matching for delegated download buttons
    const actionEl = e.target.closest('[data-action]');
    if (actionEl) {
      const action = actionEl.dataset.action;
      e.preventDefault();
      if (action === 'download-notebook') {
        downloadNotebook(getState());
      } else if (action === 'download-workflow') {
        downloadWorkflow(getState());
      } else if (action === 'download-report-markdown') {
        downloadReport(getState());
      }
      return;
    }

    // Handle remediation copy buttons (7.3)
    const copyBtn = e.target.closest('[data-copy-code]');
    if (copyBtn) {
      e.preventDefault();
      const code = copyBtn.dataset.copyCode;
      navigator.clipboard.writeText(code).then(() => {
        const origText = copyBtn.textContent;
        copyBtn.textContent = 'Copied!';
        setTimeout(() => { copyBtn.textContent = origText; }, 1500);
      }).catch(() => {});
      return;
    }

    // Legacy fallback: match on text content for buttons rendered with
    // inline text labels (will be removed once all HTML templates use data-action)
    const target = e.target.closest('button');
    if (!target) return;

    const text = target.textContent.trim().toLowerCase();

    if (text.includes('download') && text.includes('notebook')) {
      e.preventDefault();
      downloadNotebook(getState());
    } else if (text.includes('download') && text.includes('workflow')) {
      e.preventDefault();
      downloadWorkflow(getState());
    } else if (text.includes('download') && text.includes('report') && text.includes('markdown')) {
      e.preventDefault();
      downloadReport(getState());
    }
  });

  // ── 6m. Expose minimal API on window for legacy compatibility ──
  //
  // Public API: exposed for console debugging and external tool integration
  // Some step-handlers still reference window.* functions during the
  // incremental migration. These will be removed as extraction completes.
  window.downloadNotebook = () => downloadNotebook(getState());
  window.downloadWorkflow = () => downloadWorkflow(getState());
  window.downloadReport = () => downloadReport(getState());
  window.downloadFinalReport = () => downloadFinalReport(getState());
  window.downloadValidationReport = () => downloadValidationReport(getState());
  window.downloadValueAnalysis = () => downloadValueAnalysis(getState());
  window.exportAsDatabricksNotebook = () => {
    const state = getState();
    exportAsDatabricksNotebook(state.notebook?.cells || []);
  };
  window.exportAsJupyterNotebook = () => {
    const state = getState();
    exportAsJupyterNotebook(state.notebook?.cells || []);
  };

  // Expose pipeline modules for legacy step-handlers that use typeof checks
  window.parseFlow = parseFlow;
  window.runAnalysisEngine = runAnalysisEngine;
  window.mapNiFiToDatabricks = mapNiFiToDatabricks;
  window.generateNotebookAndWorkflow = generateNotebookAndWorkflow;
  window.runValidationEngine = runValidationEngine;
  window.generateReportSuite = generateReportSuite;
  window.analyzeFlowGraph = analyzeFlowGraph;

  // ════════════════════════════════════════════════════════════════
  // Steps 6-8: Final Report, Validation, Value Analysis
  // metricsHTML imported from ./utils/dom-helpers.js (XSS-safe version)
  // ════════════════════════════════════════════════════════════════

  // ── STEP 6: Final Report (enhanced with 6.1-6.5) ──
  window.generateFinalReport = async () => {
    const STATE = getState();
    if (!STATE.parsed) return;
    const snapshot = snapshotState();
    setTabStatus('reportFinal', 'processing');
    try {
      const { html, report } = generateFinalReport(STATE, metricsHTML, escapeHTML);
      setState({ finalReport: report });
      const el = document.getElementById('reportFinalResults');
      if (el) el.innerHTML = html;

      // Attach event listener for JSON download button
      const dlBtn = document.getElementById('finalReportDownloadBtn');
      if (dlBtn) {
        dlBtn.addEventListener('click', () => downloadFinalReport(getState()));
      }

      // 6.2 Wire multi-format export button
      const exportBtn = document.getElementById('finalReportExportBtn');
      if (exportBtn) {
        exportBtn.addEventListener('click', () => {
          const fmt = document.getElementById('finalReportFormatSelect');
          if (!fmt || !fmt.value) return;
          const state = getState();
          const rep = state.finalReport;
          if (!rep) return;
          switch (fmt.value) {
            case 'html': exportAsHTML(rep); break;
            case 'markdown': exportAsMarkdown(rep); break;
            case 'csv-processors': exportProcessorsCSV(rep); break;
            case 'csv-gaps': exportGapsCSV(rep); break;
          }
          fmt.value = '';
        });
      }

      setTabStatus('reportFinal', 'done');
      updateProgressViz();
      unlockTab('validate');
      const notReady = document.getElementById('validateNotReady');
      const ready = document.getElementById('validateReady');
      if (notReady) notReady.classList.add('hidden');
      if (ready) ready.classList.remove('hidden');
    } catch (e) {
      rollbackState(snapshot);
      handleError(new AppError('Final report failed: ' + e.message, { code: 'FINAL_REPORT_FAILED', phase: 'reportFinal', cause: e }));
      const el = document.getElementById('reportFinalResults');
      if (el) el.innerHTML = `<div class="alert alert-error">${escapeHTML('Final report failed: ' + e.message)}</div>`;
      setTabStatus('reportFinal', 'ready');
    }
  };

  // ── STEP 7: Validation (enhanced with 7.1-7.4) ──
  window.runValidation = async () => {
    const STATE = getState();
    if (!STATE.parsed || !STATE.parsed._nifi || !STATE.notebook) return;
    const snapshot = snapshotState();
    setTabStatus('validate', 'processing');
    const el = document.getElementById('validateResults');
    try {
      const result = await runValidationEngine({
        nifi: STATE.parsed._nifi,
        mappings: STATE.notebook.mappings || STATE.assessment?.mappings || [],
        cells: STATE.notebook.cells || [],
        systems: STATE.assessment?.systems || {},
        nifiDatabricksMap: NIFI_DATABRICKS_MAP,
        onProgress: (pct, msg) => {
          if (el) el.innerHTML = `<div style="color:var(--text2);padding:16px">${escapeHTML(msg)} (${pct}%)</div>`;
        },
      });
      setState({ validation: result });
      if (el) {
        const score = result.overallScore || 0;
        const cls = score >= 90 ? 'green' : score >= 70 ? 'amber' : 'red';
        let vh = `<hr class="divider"><div class="score-big" style="color:var(--${cls})">Validation Score: ${Math.round(score)}%</div>`;

        // 7.2 — Validation dimension explanations with info tooltips
        vh += '<div class="metrics">';
        const dims = [
          { label: 'Intent Match', value: Math.round(result.intentScore || 0) + '%' },
          { label: 'Line Coverage', value: Math.round(result.lineScore || 0) + '%' },
          { label: 'Reverse Eng.', value: Math.round(result.reScore || 0) + '%' },
          { label: 'Function Map', value: Math.round(result.funcScore || 0) + '%' },
        ];
        dims.forEach(dim => {
          const tooltip = VALIDATION_TOOLTIPS[dim.label] || '';
          vh += `<div class="metric"><div class="label">${escapeHTML(dim.label)}`;
          if (tooltip) {
            vh += `<span class="info-tooltip"><span class="tooltip-icon">?</span><span class="tooltip-text">${escapeHTML(tooltip)}</span></span>`;
          }
          vh += `</div><div class="value">${escapeHTML(dim.value)}</div></div>`;
        });
        vh += '</div>';

        // 7.1 — Severity-based gap ranking
        if (result.allGaps && result.allGaps.length) {
          // Classify and sort gaps by severity
          const classifiedGaps = result.allGaps.map(g => {
            let severity = 'LOW';
            const issue = (g.issue || g.gap || g.reason || g.message || '').toLowerCase();
            if (issue.includes('no mapping') || issue.includes('no databricks') || issue.includes('missing') || issue.includes('unmapped')) {
              severity = 'CRITICAL';
            } else if (issue.includes('no notebook cell') || issue.includes('no cell')) {
              severity = 'HIGH';
            } else if (issue.includes('low property') || issue.includes('coverage')) {
              severity = 'MEDIUM';
            }
            return { ...g, severity };
          });

          const severityOrder = { CRITICAL: 0, HIGH: 1, MEDIUM: 2, LOW: 3 };
          classifiedGaps.sort((a, b) => severityOrder[a.severity] - severityOrder[b.severity]);

          // Count by severity
          const sevCounts = { CRITICAL: 0, HIGH: 0, MEDIUM: 0, LOW: 0 };
          classifiedGaps.forEach(g => { sevCounts[g.severity]++; });

          vh += '<hr class="divider"><h3>Gaps (' + classifiedGaps.length + ')</h3>';
          vh += '<div style="display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap">';
          if (sevCounts.CRITICAL > 0) vh += `<span class="severity-badge severity-critical">Critical: ${sevCounts.CRITICAL}</span>`;
          if (sevCounts.HIGH > 0) vh += `<span class="severity-badge severity-high">High: ${sevCounts.HIGH}</span>`;
          if (sevCounts.MEDIUM > 0) vh += `<span class="severity-badge severity-medium">Medium: ${sevCounts.MEDIUM}</span>`;
          if (sevCounts.LOW > 0) vh += `<span class="severity-badge severity-low">Low: ${sevCounts.LOW}</span>`;
          vh += '</div>';

          const PAGE = 50;
          const pages = Math.ceil(classifiedGaps.length / PAGE);
          for (let p = 0; p < pages; p++) {
            const s = p * PAGE, e = Math.min(s + PAGE, classifiedGaps.length);
            const title = pages > 1 ? `Gaps ${s + 1}-${e}` : 'All Gaps';
            vh += `<div class="expander ${p === 0 ? 'open' : ''}"><div class="expander-header" data-expander-toggle><span>${title}</span><span class="expander-arrow">\u25B6</span></div><div class="expander-body">`;
            vh += '<ul style="margin:0;padding-left:20px;font-size:0.85rem;list-style:none">';
            classifiedGaps.slice(s, e).forEach(g => {
              const sevClass = 'severity-' + g.severity.toLowerCase();
              const sevBadge = `<span class="severity-badge ${sevClass}" style="margin-right:6px">${g.severity}</span>`;
              const roleTag = g.role ? '<span class="ns ns-' + escapeHTML(g.role) + '" style="font-size:0.7rem;margin-right:4px">' + escapeHTML(g.role) + '</span>' : '';
              const confTag = g.confidence != null ? ' <span style="opacity:0.6;font-size:0.75rem">(' + Math.round((g.confidence || 0) * 100) + '%)</span>' : '';
              vh += '<li style="margin:6px 0;padding:6px;background:var(--surface);border-radius:4px">' + sevBadge + roleTag + '<strong>' + escapeHTML(g.proc || g.processor || g.name || 'Unknown') + '</strong>' + confTag + ': ' + escapeHTML(g.issue || g.gap || g.reason || g.message || 'Unknown gap');

              // 7.3 — Remediation code suggestion for unmapped processors
              if (g.severity === 'CRITICAL' || g.severity === 'HIGH') {
                const procType = (g.type || '').split('.').pop() || 'UnknownProcessor';
                const mapEntry = NIFI_DATABRICKS_MAP[procType];
                if (mapEntry && mapEntry.tpl) {
                  vh += `<div class="remediation-block" style="margin-top:6px">`;
                  vh += `<div class="remediation-header"><span>Suggested skeleton code</span><button class="remediation-copy-btn" data-copy-code="${escapeHTML(mapEntry.tpl)}">Copy</button></div>`;
                  vh += `<div class="remediation-code">${escapeHTML(mapEntry.tpl)}</div>`;
                  vh += '</div>';
                } else {
                  // Generic skeleton for unmapped processors
                  const skeleton = `# TODO: Implement ${procType} equivalent\n# NiFi processor: ${escapeHTML(g.proc || g.name || 'Unknown')}\n# Original intent: ${escapeHTML(g.issue || g.gap || '')}\ndf = spark.read.format("delta").load("...")\n# Add transformation logic here\ndf.write.format("delta").mode("append").saveAsTable("catalog.schema.table")`;
                  vh += `<div class="remediation-block" style="margin-top:6px">`;
                  vh += `<div class="remediation-header"><span>Generic skeleton</span><button class="remediation-copy-btn" data-copy-code="${escapeHTML(skeleton)}">Copy</button></div>`;
                  vh += `<div class="remediation-code">${escapeHTML(skeleton)}</div>`;
                  vh += '</div>';
                }
              }
              vh += '</li>';
            });
            vh += '</ul></div></div>';
          }
        }

        // 7.3 — Missing imports with exact import lines
        if (result.missingImports && result.missingImports.length) {
          vh += '<hr class="divider"><h3>Missing Imports (' + result.missingImports.length + ')</h3>';
          result.missingImports.forEach(mi => {
            vh += `<div class="remediation-block" style="margin:6px 0">`;
            vh += `<div class="remediation-header"><span><code>${escapeHTML(mi.symbol || '')}</code> needed by cell ${escapeHTML(String(mi.usedIn || ''))}</span><button class="remediation-copy-btn" data-copy-code="${escapeHTML(mi.suggestion || '')}">Copy</button></div>`;
            vh += `<div class="remediation-code">${escapeHTML(mi.suggestion || '')}</div>`;
            vh += '</div>';
          });
        }

        // 7.4 — Show validation errors if any
        if (result.validationErrors && result.validationErrors.length) {
          vh += '<hr class="divider"><h3>Validation Warnings (' + result.validationErrors.length + ')</h3>';
          vh += '<div class="alert alert-warn">';
          result.validationErrors.forEach(ve => {
            vh += `<div style="margin:4px 0"><strong>${escapeHTML(ve.phase)}</strong>: ${escapeHTML(ve.message)}</div>`;
          });
          vh += '</div>';
        }

        vh += '<hr class="divider"><button class="btn" id="validationDownloadBtn">Download Validation Report</button>';
        el.innerHTML = vh;
        const valDlBtn = document.getElementById('validationDownloadBtn');
        if (valDlBtn) valDlBtn.addEventListener('click', () => downloadValidationReport(getState()));
      }
      setTabStatus('validate', 'done');
      updateProgressViz();
      unlockTab('value');
      const notReady = document.getElementById('valueNotReady');
      const ready = document.getElementById('valueReady');
      if (notReady) notReady.classList.add('hidden');
      if (ready) ready.classList.remove('hidden');
    } catch (e) {
      rollbackState(snapshot);
      handleError(new AppError('Validation failed: ' + e.message, { code: 'VALIDATE_FAILED', phase: 'validate', cause: e }));
      if (el) el.innerHTML = `<div class="alert alert-error">${escapeHTML('Validation failed: ' + e.message)}</div>`;
      setTabStatus('validate', 'ready');
    }
  };

  // ── STEP 8: Value Analysis (enhanced with 8.1-8.5) ──
  window.runValueAnalysis = async () => {
    const STATE = getState();
    if (!STATE.parsed || !STATE.parsed._nifi || !STATE.notebook) return;
    const snapshot = snapshotState();
    setTabStatus('value', 'processing');
    try {
      // escapeHTML is passed to runValueAnalysis which uses it internally to escape
      // all user-derived values (processor names, types, system names) in the built HTML.
      // The returned result.html is safe pre-built HTML and must NOT be double-escaped.
      const result = runValueAnalysis({
        nifi: STATE.parsed._nifi,
        notebook: STATE.notebook,
        escapeHTML,
      });
      setState({ valueAnalysis: typeof result === 'object' ? result : { html: result } });
      const el = document.getElementById('valueResults');
      if (el) {
        el.innerHTML = typeof result === 'string' ? result : (result?.html || '');
        // Attach event listener for download button rendered in the HTML
        const valDlBtn = document.getElementById('valueAnalysisDownloadBtn');
        if (valDlBtn) {
          valDlBtn.addEventListener('click', () => downloadValueAnalysis(getState()));
        }
      }
      setTabStatus('value', 'done');
      updateProgressViz();
    } catch (e) {
      rollbackState(snapshot);
      handleError(new AppError('Value analysis failed: ' + e.message, { code: 'VALUE_ANALYSIS_FAILED', phase: 'value', cause: e }));
      const el = document.getElementById('valueResults');
      if (el) el.innerHTML = `<div class="alert alert-error">${escapeHTML('Value analysis failed: ' + e.message)}</div>`;
      setTabStatus('value', 'ready');
    }
  };

  // ── Done ──
  console.info('[main] NiFi Flow Analyzer initialized');
  bus.emit('app:ready', { config });
});
