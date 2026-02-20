/**
 * ui/charts/effort-bar.js — Effort bar chart
 *
 * Generates the stacked effort bar showing auto/manual/unsupported segments.
 * Uses the .effort-bar and .effort-seg CSS classes from the HTML.
 */

/**
 * Generate an HTML string for a stacked effort bar.
 *
 * @param {{ auto: number, manual: number, unsupported: number }} counts — processor counts
 * @param {number} total — total processor count
 * @returns {string} HTML markup
 */
export function effortBar(counts, total) {
  if (!total) return '';

  const autoPct = (counts.auto / total) * 100;
  const manPct = (counts.manual / total) * 100;
  const unsPct = (counts.unsupported / total) * 100;

  let html = '<div class="effort-bar">';
  if (autoPct > 0) {
    html += `<div class="effort-seg" style="width:${autoPct}%;background:var(--green)">${counts.auto} auto</div>`;
  }
  if (manPct > 0) {
    html += `<div class="effort-seg" style="width:${manPct}%;background:var(--amber)">${counts.manual} manual</div>`;
  }
  if (unsPct > 0) {
    html += `<div class="effort-seg" style="width:${unsPct}%;background:var(--red)">${counts.unsupported} unsupported</div>`;
  }
  html += '</div>';
  return html;
}
