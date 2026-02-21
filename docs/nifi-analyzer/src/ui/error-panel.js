/**
 * ui/error-panel.js — Collapsible error & warning panel
 *
 * Renders a fixed-bottom panel that displays structured errors from
 * the error log. Supports copy, download, clear, and per-error
 * expandable stack traces with suggested fixes.
 *
 * @module ui/error-panel
 */

import { getErrorLog, clearErrorLog, getErrorCodeInfo } from '../core/errors.js';

/**
 * Render the error panel and append it to the given container.
 * @param {HTMLElement} container — typically document.body
 */
export function renderErrorPanel(container) {
  const panel = document.createElement('div');
  panel.id = 'errorPanel';
  panel.className = 'error-panel collapsed';
  panel.innerHTML = `
    <div class="error-panel-header" id="errorPanelToggle">
      <span>Errors &amp; Warnings</span>
      <span id="errorBadge" class="error-badge hidden">0</span>
      <span class="error-panel-arrow">\u25B2</span>
    </div>
    <div class="error-panel-body">
      <div class="error-panel-toolbar">
        <button id="errorCopyBtn" class="btn btn-sm">Copy All</button>
        <button id="errorDownloadBtn" class="btn btn-sm">Download Log</button>
        <button id="errorClearBtn" class="btn btn-sm btn-secondary">Clear</button>
      </div>
      <div id="errorList" class="error-list"></div>
    </div>
  `;
  container.appendChild(panel);

  // Toggle collapse/expand
  const toggle = panel.querySelector('#errorPanelToggle');
  toggle.addEventListener('click', () => {
    panel.classList.toggle('collapsed');
  });

  // Copy All
  const copyBtn = panel.querySelector('#errorCopyBtn');
  copyBtn.addEventListener('click', () => {
    const log = getErrorLog();
    const text = log.map(e => {
      const ts = new Date(e.timestamp).toISOString();
      return `[${ts}] [${e.severity}] [${e.code}] ${e.message}${e.phase ? ' (phase: ' + e.phase + ')' : ''}`;
    }).join('\n');
    navigator.clipboard.writeText(text).then(() => {
      copyBtn.textContent = 'Copied!';
      setTimeout(() => { copyBtn.textContent = 'Copy All'; }, 1500);
    }).catch(() => {
      // Fallback for environments without clipboard API
      copyBtn.textContent = 'Failed';
      setTimeout(() => { copyBtn.textContent = 'Copy All'; }, 1500);
    });
  });

  // Download Log as JSON
  const downloadBtn = panel.querySelector('#errorDownloadBtn');
  downloadBtn.addEventListener('click', () => {
    const log = getErrorLog();
    const serializable = log.map(e => ({
      timestamp: new Date(e.timestamp).toISOString(),
      code: e.code,
      severity: e.severity,
      phase: e.phase,
      message: e.message,
      context: e.context,
      stack: e.stack,
    }));
    const blob = new Blob([JSON.stringify(serializable, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'nifi-analyzer-errors-' + new Date().toISOString().slice(0, 19).replace(/:/g, '-') + '.json';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  });

  // Clear
  const clearBtn = panel.querySelector('#errorClearBtn');
  clearBtn.addEventListener('click', () => {
    clearErrorLog();
    const list = panel.querySelector('#errorList');
    if (list) list.innerHTML = '';
    updateErrorBadge();
  });

  // Initialize badge from existing log
  updateErrorBadge();

  // Populate existing errors from the persisted log
  const existingErrors = getErrorLog();
  existingErrors.forEach(err => {
    addErrorToPanel(err, true);
  });
}

/**
 * Severity icon lookup.
 * @param {string} severity
 * @returns {string}
 */
function severityIcon(severity) {
  switch (severity) {
    case 'critical':
    case 'high':
      return '\uD83D\uDD34'; // red circle
    case 'medium':
      return '\uD83D\uDFE1'; // yellow circle
    case 'low':
      return '\uD83D\uDD35'; // blue circle
    default:
      return '\uD83D\uDD35'; // blue circle
  }
}

/**
 * Escape HTML entities to prevent XSS in error messages.
 * @param {string} str
 * @returns {string}
 */
function escapeStr(str) {
  const div = document.createElement('div');
  div.textContent = str;
  return div.innerHTML;
}

/**
 * Add an error entry to the error panel (prepended, newest first).
 * @param {import('../core/errors.js').AppError} error
 * @param {boolean} [skipBadgeUpdate=false] — skip badge update (for batch init)
 */
export function addErrorToPanel(error, skipBadgeUpdate = false) {
  const list = document.getElementById('errorList');
  if (!list) return;

  const ts = new Date(error.timestamp);
  const timeStr = ts.toLocaleTimeString();

  const codeInfo = error.code ? getErrorCodeInfo(error.code) : null;

  const item = document.createElement('div');
  item.className = 'error-item';
  item.setAttribute('role', 'listitem');

  let fixHtml = '';
  if (codeInfo && codeInfo.fix) {
    fixHtml = `<div class="error-fix">Fix: ${escapeStr(codeInfo.fix)}</div>`;
  }

  let stackHtml = '';
  if (error.stack) {
    stackHtml = `<div class="error-stack">${escapeStr(error.stack)}</div>`;
  }

  item.innerHTML = `
    <span class="error-severity">${severityIcon(error.severity)}</span>
    <span class="error-phase">${escapeStr(error.phase || error.code || 'unknown')}</span>
    <span class="error-message">
      ${escapeStr(error.message)}
      ${fixHtml}
      ${stackHtml}
    </span>
    <span class="error-timestamp">${escapeStr(timeStr)}</span>
  `;

  // Click to expand/collapse stack trace
  item.addEventListener('click', () => {
    item.classList.toggle('expanded');
  });

  // Prepend (newest first)
  list.insertBefore(item, list.firstChild);

  if (!skipBadgeUpdate) {
    updateErrorBadge();
  }
}

/**
 * Update the error badge count from the current error log.
 */
export function updateErrorBadge() {
  const badge = document.getElementById('errorBadge');
  if (!badge) return;

  const count = getErrorLog().length;
  badge.textContent = String(count);
  if (count > 0) {
    badge.classList.remove('hidden');
  } else {
    badge.classList.add('hidden');
  }
}
