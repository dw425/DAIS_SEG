/**
 * utils/score-helpers.js — Score display and data preview helpers
 *
 * Extracted from index.html lines 7312-7328.
 */

import { tableHTML } from './dom-helpers.js';

/**
 * Return an HTML badge for a confidence score (0-1).
 *
 * @param {number} score — value between 0 and 1
 * @returns {string} HTML string with coloured badge
 */
export function scoreBadge(score) {
  const pct = Math.round(score * 100);
  if (score >= 0.9) {
    return `<span class="badge badge-green">GREEN ${pct}%</span>`;
  }
  if (score >= 0.7) {
    return `<span class="badge badge-amber">AMBER ${pct}%</span>`;
  }
  return `<span class="badge badge-red">RED ${pct}%</span>`;
}

/**
 * Return an HTML progress bar with label and percentage.
 *
 * @param {number} score — value between 0 and 1
 * @param {string} label
 * @returns {string} HTML string
 */
export function progressHTML(score, label) {
  const pct = Math.round(score * 100);
  const cls = score >= 0.9 ? 'green' : score >= 0.7 ? 'amber' : 'red';
  return `<div style="margin:4px 0">`
    + `<div style="display:flex;justify-content:space-between;font-size:0.85rem">`
    + `<span>${label}</span><span>${pct}%</span></div>`
    + `<div class="progress-bar"><div class="progress-fill ${cls}" style="width:${pct}%"></div></div></div>`;
}

/**
 * Render a columnar data object as a preview table.
 *
 * @param {Object<string, Array>} data — column-name to array-of-values
 * @param {number} [maxRows=15]
 * @returns {string} HTML string
 */
export function dataPreviewHTML(data, maxRows = 15) {
  const cols = Object.keys(data);
  if (!cols.length) return '';

  const rc = data[cols[0]] ? data[cols[0]].length : 0;
  const rows = [];
  for (let i = 0; i < Math.min(rc, maxRows); i++) {
    rows.push(cols.map(c => {
      const v = data[c][i];
      return v == null
        ? '<span style="color:var(--text2)">null</span>'
        : String(v).substring(0, 40);
    }));
  }
  return tableHTML(cols, rows);
}
