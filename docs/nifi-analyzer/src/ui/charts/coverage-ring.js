/**
 * ui/charts/coverage-ring.js — Coverage ring chart
 *
 * Generates a CSS-based coverage ring (the .coverage-ring class from the HTML).
 * Uses conic-gradient for the ring fill.
 */

/**
 * Generate an HTML string for a coverage ring chart.
 *
 * @param {number} pct   — coverage percentage (0-100)
 * @param {string} label — text label below the ring
 * @returns {string} HTML markup
 */
export function coverageRing(pct, label) {
  const color = pct >= 85 ? 'var(--green)' : pct >= 60 ? 'var(--amber)' : 'var(--red)';
  const trackColor = 'rgba(128,132,149,0.15)';
  return `<div class="coverage-ring" style="background:conic-gradient(${color} ${pct * 3.6}deg, ${trackColor} ${pct * 3.6}deg)">
    <div class="ring-inner" style="background:var(--bg)">
      <span class="ring-text" style="color:${color}">${pct}%</span>
    </div>
  </div>
  <div class="ring-label">${label}</div>`;
}
