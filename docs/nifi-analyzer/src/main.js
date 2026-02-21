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

// ================================================================
// 2. Core Modules
// ================================================================
import { createStore, getState, setState, resetState, snapshotState, rollbackState } from './core/state.js';
import bus from './core/event-bus.js';
import { loadDbxConfig, saveDbxConfig, getDbxConfig, DBX_CONFIG_DEFAULTS } from './core/config.js';
import { handleError, AppError, clearErrorLog } from './core/errors.js';

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
import { NIFI_DATABRICKS_MAP } from './constants/nifi-databricks-map.js';
import { analyzeFlowGraph } from './analyzers/flow-graph-analyzer.js';

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

  // ── 6c. Wire file input change to trigger pipeline ──
  const fileInput = document.getElementById('fileInput');
  if (fileInput) {
    fileInput.addEventListener('change', async () => {
      resetState();
      clearErrorLog();
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
      loadSampleFile(path, filename, parseInput);
    });
  });

  // ── 6f. Wire step buttons (analyze, assess, convert, report) ──
  const analyzeBtn = document.getElementById('analyzeBtn');
  if (analyzeBtn) {
    analyzeBtn.addEventListener('click', async () => {
      analyzeBtn.disabled = true;
      try { await runAnalysis(); } finally { analyzeBtn.disabled = false; }
    });
  }

  const assessBtn = document.getElementById('assessBtn');
  if (assessBtn) {
    assessBtn.addEventListener('click', async () => {
      assessBtn.disabled = true;
      try { await runAssessment(); } finally { assessBtn.disabled = false; }
    });
  }

  const convertBtn = document.getElementById('convertBtn');
  if (convertBtn) {
    convertBtn.addEventListener('click', async () => {
      convertBtn.disabled = true;
      try { await generateNotebook(); } finally { convertBtn.disabled = false; }
    });
  }

  const reportBtn = document.getElementById('reportBtn');
  if (reportBtn) {
    reportBtn.addEventListener('click', async () => {
      reportBtn.disabled = true;
      try { await generateReport(); } finally { reportBtn.disabled = false; }
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
        cfgSparkVersion: defaults.sparkVersion,
        cfgNodeType: defaults.nodeType,
        cfgWorkers: defaults.numWorkers,
        cfgWorkspacePath: defaults.workspacePath,
      };
      Object.entries(fields).forEach(([id, value]) => {
        const el = document.getElementById(id);
        if (el) el.value = value ?? '';
      });
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

  // Steps 6-8: Final Report, Validation, Value Analysis
  // metricsHTML imported from ./utils/dom-helpers.js (XSS-safe version)

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

      // Attach event listener for download button rendered in the HTML
      const dlBtn = el && el.querySelector('.btn-primary, .btn');
      if (dlBtn && dlBtn.textContent.includes('Download Full Report')) {
        dlBtn.removeAttribute('onclick');
        dlBtn.addEventListener('click', () => downloadFinalReport(getState()));
      }

      setTabStatus('reportFinal', 'done');
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
        vh += metricsHTML([
          { label: 'Intent Match', value: Math.round(result.intentScore || 0) + '%' },
          { label: 'Line Coverage', value: Math.round(result.lineScore || 0) + '%' },
          { label: 'Reverse Eng.', value: Math.round(result.reScore || 0) + '%' },
          { label: 'Function Map', value: Math.round(result.funcScore || 0) + '%' },
        ]);
        if (result.allGaps && result.allGaps.length) {
          vh += '<hr class="divider"><h3>Gaps (' + result.allGaps.length + ')</h3>';
          const PAGE = 50;
          const pages = Math.ceil(result.allGaps.length / PAGE);
          for (let p = 0; p < pages; p++) {
            const s = p * PAGE, e = Math.min(s + PAGE, result.allGaps.length);
            const title = pages > 1 ? `Gaps ${s + 1}-${e}` : 'All Gaps';
            vh += `<div class="expander ${p === 0 ? 'open' : ''}"><div class="expander-header" data-expander-toggle><span>${title}</span><span class="expander-arrow">\u25B6</span></div><div class="expander-body">`;
            vh += '<ul style="margin:0;padding-left:20px;font-size:0.85rem">';
            result.allGaps.slice(s, e).forEach(g => {
              vh += '<li style="margin:4px 0"><strong>' + escapeHTML(g.proc || g.processor || g.name || '') + '</strong>: ' + escapeHTML(g.issue || g.gap || g.reason || g.message || '') + '</li>';
            });
            vh += '</ul></div></div>';
          }
        }
        if (result.missingImports && result.missingImports.length) {
          vh += '<hr class="divider"><h3>Missing Imports (' + result.missingImports.length + ')</h3>';
          vh += '<ul style="margin:0;padding-left:20px;font-size:0.85rem">';
          result.missingImports.forEach(mi => {
            vh += '<li style="margin:4px 0"><code>' + escapeHTML(mi.symbol || '') + '</code> needed by cell ' + escapeHTML(String(mi.usedIn || '')) + (mi.suggestion ? ' — add <code>' + escapeHTML(mi.suggestion) + '</code>' : '') + '</li>';
          });
          vh += '</ul>';
        }
        vh += '<hr class="divider"><button class="btn" id="validationDownloadBtn">Download Validation Report</button>';
        el.innerHTML = vh;
        const valDlBtn = document.getElementById('validationDownloadBtn');
        if (valDlBtn) valDlBtn.addEventListener('click', () => downloadValidationReport(getState()));
      }
      setTabStatus('validate', 'done');
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
        const valDlBtn = el.querySelector('.btn-primary, .btn');
        if (valDlBtn && valDlBtn.textContent.includes('Download Value Analysis')) {
          valDlBtn.removeAttribute('onclick');
          valDlBtn.addEventListener('click', () => downloadValueAnalysis(getState()));
        }
      }
      setTabStatus('value', 'done');
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
