/**
 * utils/dom-helpers.js — DOM element creation and HTML template helpers
 *
 * Extracted from index.html lines 7242-7310.
 *
 * FIX CRIT: html() now uses textContent for string children instead of
 *           innerHTML, preventing XSS when rendering user-supplied data.
 */

import { escapeHTML } from '../security/html-sanitizer.js';

/**
 * Create a DOM element with attributes and children.
 *
 * String children are set via textContent (safe).  To insert trusted HTML
 * markup, pass a pre-built DOM node or use the raw template helpers below.
 *
 * @param {string} tag
 * @param {Object|null} attrs
 * @param  {...(string|Node)} children
 * @returns {HTMLElement}
 */
export function html(tag, attrs, ...children) {
  const el = document.createElement(tag);

  if (attrs) {
    Object.entries(attrs).forEach(([k, v]) => {
      if (k === 'className') {
        el.className = v;
      } else if (k === 'onclick') {
        el.onclick = v;
      } else if (k === 'textContent') {
        el.textContent = v;
      } else {
        el.setAttribute(k, v);
      }
    });
  }

  children.forEach(c => {
    if (typeof c === 'string') {
      // FIX CRIT: use textContent instead of innerHTML to prevent XSS
      el.appendChild(document.createTextNode(c));
    } else if (c instanceof Node) {
      el.appendChild(c);
    }
  });

  return el;
}

/**
 * Build a metrics bar from an array of metric items.
 *
 * Each item is either [label, value, delta?] or {label, value, delta?, color?}.
 *
 * @param {Array} items
 * @returns {string} HTML string (trusted template — values are escaped)
 */
export function metricsHTML(items) {
  return '<div class="metrics">' + items.map(item => {
    const l = escapeHTML(Array.isArray(item) ? item[0] : item.label);
    const v = escapeHTML(Array.isArray(item) ? item[1] : item.value);
    const d = Array.isArray(item) ? item[2] : item.delta;
    const c = Array.isArray(item) ? '' : (item.color || '');
    return `<div class="metric"><div class="label">${l}</div>`
      + `<div class="value"${c ? ' style="color:' + escapeHTML(c) + '"' : ''}>${v}</div>`
      + (d ? `<div class="delta">${escapeHTML(d)}</div>` : '')
      + '</div>';
  }).join('') + '</div>';
}

/**
 * Build a scrollable table from headers and row data.
 *
 * @param {string[]} headers
 * @param {Array<Array<string>>} rows
 * @returns {string} HTML string (trusted template — cell values are escaped)
 */
export function tableHTML(headers, rows) {
  const thead = headers.map(h => `<th>${escapeHTML(h)}</th>`).join('');
  const tbody = rows.map(r =>
    `<tr>${r.map(c => `<td>${escapeHTML(c ?? '')}</td>`).join('')}</tr>`
  ).join('');
  return `<div class="table-scroll"><table><thead><tr>${thead}</tr></thead><tbody>${tbody}</tbody></table></div>`;
}

/**
 * Build a collapsible expander section.
 *
 * @param {string} title
 * @param {string} content — trusted HTML content
 * @param {boolean} [open=false]
 * @returns {string} HTML string
 */
export function expanderHTML(title, content, open = false) {
  return `<div class="expander ${open ? 'open' : ''}">`
    + `<div class="expander-header" onclick="this.parentElement.classList.toggle('open')">`
    + `<span>${escapeHTML(title)}</span><span class="expander-arrow">&#9654;</span></div>`
    + `<div class="expander-body">${content}</div></div>`;
}
