/**
 * ui/progress.js — Parse progress bar helpers
 *
 * Extracted from index.html lines 7334-7352.
 * Controls the inline parse progress indicator.
 */

/**
 * Update the parse progress bar.
 * Extracted from index.html lines 7334-7346.
 *
 * @param {number} pct    — percentage (0-100)
 * @param {string} status — main status text
 * @param {string} [sub]  — optional sub-status
 */
export function parseProgress(pct, status, sub) {
  const el = document.getElementById('parseProgress');
  const bar = document.getElementById('parsePBar');
  const pctEl = document.getElementById('parsePPct');
  const statusEl = document.getElementById('parsePStatus');
  if (!el) return;
  el.style.display = 'flex';
  if (bar) {
    bar.style.width = pct + '%';
    bar.style.background = pct >= 100 ? 'var(--green)' : 'var(--primary)';
  }
  if (pctEl) pctEl.textContent = Math.round(pct) + '%';
  if (statusEl) statusEl.textContent = status + (sub ? ' — ' + sub : '');
}

/**
 * Hide the parse progress bar.
 * Extracted from index.html lines 7347-7350.
 */
export function parseProgressHide() {
  const el = document.getElementById('parseProgress');
  if (el) el.style.display = 'none';
}

/**
 * Yield to the UI event loop so progress bar updates render.
 * Extracted from index.html line 7352.
 *
 * @returns {Promise<void>}
 */
export function uiYield() {
  return new Promise(r => setTimeout(r, 0));
}
