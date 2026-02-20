/**
 * ui/charts/donut-svg.js — SVG donut chart
 *
 * Extracted from index.html lines 6035-6051.
 */

/**
 * Generate an SVG donut chart as an HTML string.
 * Extracted from index.html lines 6035-6051.
 *
 * @param {number} pct      — percentage (0-100)
 * @param {string} label    — chart label below the donut
 * @param {string} sublabel — smaller text inside the donut ring
 * @returns {string} HTML markup
 */
export function donutSVG(pct, label, sublabel) {
  const r = 54;
  const circ = 2 * Math.PI * r;
  const filled = circ * pct / 100;
  const gap = circ - filled;
  const color = pct >= 85 ? '#21C354' : pct >= 60 ? '#EAB308' : '#EF4444';
  const track = 'rgba(128,132,149,0.15)';
  return `<div class="donut-chart">
    <svg width="140" height="140" viewBox="0 0 140 140">
      <circle cx="70" cy="70" r="${r}" fill="none" stroke="${track}" stroke-width="12"/>
      <circle cx="70" cy="70" r="${r}" fill="none" stroke="${color}" stroke-width="12"
        stroke-dasharray="${filled} ${gap}" stroke-dashoffset="${circ * 0.25}"
        stroke-linecap="round" style="transition:stroke-dasharray 0.6s ease"/>
      <text x="70" y="66" text-anchor="middle" fill="${color}" font-size="28" font-weight="800">${pct}%</text>
      <text x="70" y="84" text-anchor="middle" fill="#9ca3af" font-size="11">${sublabel}</text>
    </svg>
    <div class="donut-label">${label}</div>
  </div>`;
}
