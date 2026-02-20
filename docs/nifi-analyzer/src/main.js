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
import { createStore, getState, setState, resetState } from './core/state.js';
import bus from './core/event-bus.js';
import { loadDbxConfig, saveDbxConfig, getDbxConfig } from './core/config.js';
import { PipelineOrchestrator } from './core/pipeline.js';
import { handleError, AppError, wrapAsync, clearErrorLog } from './core/errors.js';

// ================================================================
// 3. UI Modules
// ================================================================
import { initTabs, switchTab, setTabStatus, unlockTab } from './ui/tabs.js';
import { escapeHTML } from './security/html-sanitizer.js';
import { initFileUpload, getUploadedContent, getUploadedName, getUploadedBytes } from './ui/file-upload.js';
import { loadSampleFlow, loadSampleFile } from './ui/sample-flows.js';
import { parseProgress, parseProgressHide, uiYield } from './ui/progress.js';
import { showPathToast, hidePathToast, flashNoPath } from './ui/toast.js';
import { showPanel, hidePanel } from './ui/panels.js';
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

// ================================================================
// 5. Initial State
// ================================================================

/** @type {object} Application initial state shape */
const INITIAL_STATE = {
  flowData: null,
  processors: [],
  connections: [],
  controllerServices: [],
  variables: {},
  blueprint: null,
  mappings: [],
  notebook: null,
  workflow: null,
  validationResults: null,
  reportData: null,
  currentStep: 0,
  isProcessing: false,
  errors: [],
};

// ================================================================
// 6. Application Bootstrap
// ================================================================

document.addEventListener('DOMContentLoaded', () => {
  // ── 6a. Initialize core infrastructure ──
  const store = createStore(INITIAL_STATE);
  const config = loadDbxConfig();
  const pipeline = new PipelineOrchestrator();

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
      setState(INITIAL_STATE);
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
      setState(INITIAL_STATE);
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
      setState(INITIAL_STATE);
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
      setState(INITIAL_STATE);
      loadSampleFile(path, filename, parseInput);
    });
  });

  // ── 6f. Wire step buttons (analyze, assess, convert, report) ──
  const analyzeBtn = document.getElementById('analyzeBtn');
  if (analyzeBtn) {
    analyzeBtn.addEventListener('click', () => {
      runAnalysis();
    });
  }

  const assessBtn = document.getElementById('assessBtn');
  if (assessBtn) {
    assessBtn.addEventListener('click', () => {
      runAssessment();
    });
  }

  const convertBtn = document.getElementById('convertBtn');
  if (convertBtn) {
    convertBtn.addEventListener('click', () => {
      generateNotebook();
    });
  }

  const reportBtn = document.getElementById('reportBtn');
  if (reportBtn) {
    reportBtn.addEventListener('click', () => {
      generateReport();
    });
  }

  // ── 6g. Wire download buttons ──
  const dlNotebookBtn = document.getElementById('downloadNotebookBtn');
  if (dlNotebookBtn) {
    dlNotebookBtn.addEventListener('click', () => {
      downloadNotebook(getState());
    });
  }

  const dlWorkflowBtn = document.getElementById('downloadWorkflowBtn');
  if (dlWorkflowBtn) {
    dlWorkflowBtn.addEventListener('click', () => {
      downloadWorkflow(getState());
    });
  }

  const dlReportBtn = document.getElementById('downloadReportBtn');
  if (dlReportBtn) {
    dlReportBtn.addEventListener('click', () => {
      downloadReport(getState());
    });
  }

  const dlFinalReportBtn = document.getElementById('downloadFinalReportBtn');
  if (dlFinalReportBtn) {
    dlFinalReportBtn.addEventListener('click', () => {
      downloadFinalReport(getState());
    });
  }

  const dlValidationBtn = document.getElementById('downloadValidationBtn');
  if (dlValidationBtn) {
    dlValidationBtn.addEventListener('click', () => {
      downloadValidationReport(getState());
    });
  }

  const dlValueBtn = document.getElementById('downloadValueBtn');
  if (dlValueBtn) {
    dlValueBtn.addEventListener('click', () => {
      downloadValueAnalysis(getState());
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
      const defaults = loadDbxConfig();
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

  // ── 6j. Register pipeline steps with PipelineOrchestrator ──
  pipeline.register([
    {
      name: 'parse',
      tab: 'load',
      run: wrapAsync(async () => {
        bus.emit('step:parse:start');
        parseProgress(0, 'Starting parse...');
        await parseInput();
        bus.emit('step:parse:done', getState());
      }, { phase: 'parse', code: 'PARSE_FAILED' }),
    },
    {
      name: 'analyze',
      tab: 'analyze',
      run: wrapAsync(async () => {
        bus.emit('step:analyze:start');
        switchTab('analyze');
        runAnalysis();
        bus.emit('step:analyze:done', getState());
      }, { phase: 'analyze', code: 'ANALYZE_FAILED' }),
    },
    {
      name: 'assess',
      tab: 'assess',
      run: wrapAsync(async () => {
        bus.emit('step:assess:start');
        switchTab('assess');
        runAssessment();
        bus.emit('step:assess:done', getState());
      }, { phase: 'assess', code: 'ASSESS_FAILED' }),
    },
    {
      name: 'generate',
      tab: 'convert',
      run: wrapAsync(async () => {
        bus.emit('step:generate:start');
        switchTab('convert');
        generateNotebook();
        bus.emit('step:generate:done', getState());
      }, { phase: 'generate', code: 'GENERATE_FAILED' }),
    },
    {
      name: 'report',
      tab: 'report',
      run: wrapAsync(async () => {
        bus.emit('step:report:start');
        switchTab('report');
        generateReport();
        bus.emit('step:report:done', getState());
      }, { phase: 'report', code: 'REPORT_FAILED' }),
    },
    {
      name: 'reportFinal',
      tab: 'reportFinal',
      run: wrapAsync(async () => {
        bus.emit('step:reportFinal:start');
        switchTab('reportFinal');
        if (typeof window.generateFinalReport === 'function') {
          await window.generateFinalReport();
        }
        bus.emit('step:reportFinal:done', getState());
      }, { phase: 'reportFinal', code: 'FINAL_REPORT_FAILED' }),
    },
    {
      name: 'validate',
      tab: 'validate',
      run: wrapAsync(async () => {
        bus.emit('step:validate:start');
        switchTab('validate');
        if (typeof window.runValidation === 'function') {
          await window.runValidation();
        }
        bus.emit('step:validate:done', getState());
      }, { phase: 'validate', code: 'VALIDATE_FAILED' }),
    },
    {
      name: 'value',
      tab: 'value',
      run: wrapAsync(async () => {
        bus.emit('step:value:start');
        switchTab('value');
        if (typeof window.runValueAnalysis === 'function') {
          window.runValueAnalysis();
        }
        bus.emit('step:value:done', getState());
      }, { phase: 'value', code: 'VALUE_ANALYSIS_FAILED' }),
    },
  ]);

  // ── 6k. Wire EventBus listeners for decoupled communication ──

  // When a flow is parsed, automatically kick off the full pipeline
  bus.on('flow:loaded', () => {
    resetState();
    clearErrorLog();
    setState(INITIAL_STATE);
    pipeline.execute();
  });

  // Progress events update the progress bar
  bus.on('pipeline:step:start', ({ step }) => {
    const stepLabels = {
      parse: 'Parsing flow...',
      analyze: 'Analyzing flow...',
      assess: 'Assessing migration readiness...',
      generate: 'Generating notebook...',
      report: 'Generating reports...',
      reportFinal: 'Building final report...',
      validate: 'Running validation...',
      value: 'Running value analysis...',
    };
    const stepIndex = [
      'parse', 'analyze', 'assess', 'generate',
      'report', 'reportFinal', 'validate', 'value',
    ].indexOf(step);
    const pct = stepIndex >= 0 ? Math.round(((stepIndex + 1) / 8) * 100) : 0;
    parseProgress(pct, stepLabels[step] || step);
  });

  bus.on('pipeline:done', () => {
    parseProgress(100, 'All steps complete!');
    setTimeout(parseProgressHide, 1500);
  });

  bus.on('pipeline:step:error', ({ step, error }) => {
    console.error(`[main] Pipeline step "${step}" failed:`, error);
  });

  // ── 6l. Wire dynamically-created download buttons in rendered HTML ──
  //
  // Because step-handlers.js renders HTML with onclick attributes for
  // download buttons (legacy pattern), we intercept those via event delegation
  // on the document body. This provides a safety net until the rendered HTML
  // is fully migrated away from inline handlers.
  document.body.addEventListener('click', (e) => {
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

  // Steps 6-8: Final Report, Validation, Value Analysis
  const metricsHTML = (items) => '<div class="metrics">' + items.map(item => {
    const l = Array.isArray(item) ? item[0] : item.label;
    const v = Array.isArray(item) ? item[1] : item.value;
    const d = Array.isArray(item) ? item[2] : item.delta;
    const c = Array.isArray(item) ? '' : (item.color || '');
    return `<div class="metric"><div class="label">${l}</div><div class="value"${c ? ' style="color:' + c + '"' : ''}>${v}</div>${d ? `<div class="delta">${d}</div>` : ''}</div>`;
  }).join('') + '</div>';

  window.generateFinalReport = async () => {
    const STATE = getState();
    if (!STATE.parsed) return;
    setTabStatus('reportFinal', 'processing');
    const { html, report } = generateFinalReport(STATE, metricsHTML, escapeHTML);
    setState({ finalReport: report });
    const el = document.getElementById('reportFinalResults');
    if (el) el.innerHTML = html;
    setTabStatus('reportFinal', 'done');
    unlockTab('validate');
    const notReady = document.getElementById('validateNotReady');
    const ready = document.getElementById('validateReady');
    if (notReady) notReady.classList.add('hidden');
    if (ready) ready.classList.remove('hidden');
  };

  window.runValidation = async () => {
    const STATE = getState();
    if (!STATE.parsed || !STATE.parsed._nifi || !STATE.notebook) return;
    setTabStatus('validate', 'processing');
    const el = document.getElementById('validateResults');
    const result = await runValidationEngine({
      nifi: STATE.parsed._nifi,
      mappings: STATE.notebook.mappings || STATE.assessment?.mappings || [],
      cells: STATE.notebook.cells || [],
      systems: STATE.assessment?.systems || {},
      nifiDatabricksMap: NIFI_DATABRICKS_MAP,
      onProgress: (pct, msg) => {
        if (el) el.innerHTML = `<div style="color:var(--text2);padding:16px">${msg} (${pct}%)</div>`;
      },
    });
    setState({ validation: result });
    if (el) {
      const score = result.overallScore || 0;
      const cls = score >= 80 ? 'green' : score >= 50 ? 'amber' : 'red';
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
            vh += '<li style="margin:4px 0"><strong>' + escapeHTML(g.processor || g.name || '') + '</strong>: ' + escapeHTML(g.gap || g.reason || g.message || '') + '</li>';
          });
          vh += '</ul></div></div>';
        }
      }
      if (result.missingImports && result.missingImports.length) {
        vh += '<hr class="divider"><h3>Missing Imports (' + result.missingImports.length + ')</h3>';
        vh += '<ul style="margin:0;padding-left:20px;font-size:0.85rem">';
        result.missingImports.forEach(mi => {
          vh += '<li style="margin:4px 0"><code>' + escapeHTML(mi.module || '') + '</code> needed by ' + escapeHTML(mi.cell || '') + '</li>';
        });
        vh += '</ul>';
      }
      vh += '<hr class="divider"><button class="btn" onclick="downloadValidationReport()">Download Validation Report</button>';
      el.innerHTML = vh;
    }
    setTabStatus('validate', 'done');
    unlockTab('value');
    const notReady = document.getElementById('valueNotReady');
    const ready = document.getElementById('valueReady');
    if (notReady) notReady.classList.add('hidden');
    if (ready) ready.classList.remove('hidden');
  };

  window.runValueAnalysis = () => {
    const STATE = getState();
    if (!STATE.parsed || !STATE.parsed._nifi || !STATE.notebook) return;
    setTabStatus('value', 'processing');
    const html = runValueAnalysis({
      nifi: STATE.parsed._nifi,
      notebook: STATE.notebook,
      escapeHTML,
    });
    setState({ valueAnalysis: html });
    const el = document.getElementById('valueResults');
    if (el) el.innerHTML = typeof html === 'string' ? html : (html?.html || '');
    setTabStatus('value', 'done');
  };

  // ── Done ──
  console.info('[main] NiFi Flow Analyzer initialized');
  bus.emit('app:ready', { config });
});
