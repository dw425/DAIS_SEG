/**
 * ui/display-security.js — Security findings display
 *
 * Extracted from index.html lines 9200-9222.
 */

import { escapeHTML } from '../security/html-sanitizer.js';

/**
 * Render security findings as an HTML string.
 * Extracted from index.html lines 9200-9222.
 *
 * @param {Array<{severity:string, finding:string, processor?:string, snippet?:string}>} findings
 * @returns {string} HTML markup
 */
export function displaySecurityFindings(findings) {
  if (!findings || findings.length === 0) {
    return '<div style="color:var(--green);padding:12px;">' + escapeHTML('\u2713 No security issues detected') + '</div>';
  }

  const byLevel = { CRITICAL: [], HIGH: [], MEDIUM: [] };
  findings.forEach(f => {
    (byLevel[f.severity] || byLevel.MEDIUM).push(f);
  });

  let html = '<div style="padding:12px;">';
  html += '<h3 style="margin:0 0 12px;">Security Scan: ' + findings.length + ' Finding' + (findings.length !== 1 ? 's' : '') + '</h3>';

  const colors = { CRITICAL: '#EF4444', HIGH: '#EAB308', MEDIUM: '#3B82F6' };

  for (const [level, items] of Object.entries(byLevel)) {
    if (items.length === 0) continue;
    html += '<div style="margin-bottom:12px;">';
    html += '<div style="color:' + colors[level] + ';font-weight:700;margin-bottom:4px;">' + escapeHTML(level) + ' (' + items.length + ')</div>';
    items.slice(0, 20).forEach(f => {
      html += '<div style="padding:4px 8px;margin:2px 0;background:rgba(0,0,0,0.2);border-radius:4px;font-size:12px;">';
      html += '<b>' + escapeHTML(f.finding) + '</b> in <code>' + escapeHTML((f.processor || '').substring(0, 40)) + '</code>';
      html += ' <span style="opacity:0.6;">' + escapeHTML((f.snippet || '').substring(0, 60)) + '</span>';
      html += '</div>';
    });
    if (items.length > 20) {
      html += '<div style="opacity:0.5;font-size:11px;">...and ' + (items.length - 20) + ' more</div>';
    }
    html += '</div>';
  }
  html += '</div>';
  return html;
}
