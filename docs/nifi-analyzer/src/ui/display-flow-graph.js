/**
 * ui/display-flow-graph.js — Flow graph analysis display
 *
 * Extracted from index.html lines 9224-9253.
 */

import { escapeHTML } from '../security/html-sanitizer.js';

/**
 * Render flow graph analysis results as an HTML string.
 * Extracted from index.html lines 9224-9253.
 *
 * @param {{ deadEnds?:Array, orphans?:Array, circularRefs?:Array, disconnected?:Array }} result
 * @returns {string} HTML markup
 */
export function displayFlowGraphAnalysis(result) {
  if (!result) return '';

  let html = '<div style="padding:12px;">';
  html += '<h3 style="margin:0 0 12px;">Flow Graph Analysis</h3>';

  const sections = [
    { key: 'deadEnds', label: 'Dead Ends', desc: 'Processors with no outgoing connections (not sinks)' },
    { key: 'orphans', label: 'Orphans', desc: 'Processors with no incoming connections (not sources)' },
    { key: 'circularRefs', label: 'Circular References', desc: 'Cycles detected in flow graph' },
    { key: 'disconnected', label: 'Disconnected', desc: 'Processors with no connections at all' },
  ];

  sections.forEach(s => {
    const items = result[s.key] || [];
    html += '<div style="margin-bottom:10px;">';
    html += '<div style="font-weight:600;">' + escapeHTML(s.label) + ': ' + items.length + '</div>';
    if (items.length > 0 && items.length <= 30) {
      items.forEach(item => {
        const name = item.name || (item.cycle ? item.cycle.join(' \u2192 ') : '?');
        html += '<div style="padding:2px 8px;font-size:11px;opacity:0.7;">' + escapeHTML(name) + '</div>';
      });
    } else if (items.length > 30) {
      items.slice(0, 10).forEach(item => {
        html += '<div style="padding:2px 8px;font-size:11px;opacity:0.7;">' + escapeHTML(item.name || '?') + '</div>';
      });
      html += '<div style="font-size:11px;opacity:0.5;">...and ' + (items.length - 10) + ' more</div>';
    }
    html += '</div>';
  });

  html += '</div>';
  return html;
}
