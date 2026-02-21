/**
 * ui/json-explorer.js — Interactive JSON tree viewer
 *
 * Provides an expandable/collapsible tree view with search by key/value,
 * copy JSON path on click, and syntax highlighting.
 *
 * @module ui/json-explorer
 */

import { escapeHTML } from '../security/html-sanitizer.js';

/**
 * Render an interactive JSON explorer tree.
 *
 * @param {*} data - The JSON data to render
 * @param {string} [containerId] - Optional container element ID
 * @returns {string} HTML string for the explorer
 */
export function renderJSONExplorer(data, containerId = 'jsonExplorer') {
  let html = '';
  html += `<div class="json-explorer" id="${escapeHTML(containerId)}">`;
  html += '<div class="json-explorer-toolbar">';
  html += `<input type="text" class="json-search-input" placeholder="Search keys or values..." data-json-search="${escapeHTML(containerId)}" />`;
  html += `<span class="json-search-count" id="${escapeHTML(containerId)}-search-count"></span>`;
  html += `<button class="btn btn-secondary json-expand-all" data-json-expand-all="${escapeHTML(containerId)}" style="padding:4px 10px;font-size:0.75rem">Expand All</button>`;
  html += `<button class="btn btn-secondary json-collapse-all" data-json-collapse-all="${escapeHTML(containerId)}" style="padding:4px 10px;font-size:0.75rem">Collapse All</button>`;
  html += '</div>';
  html += '<div class="json-tree-container">';
  html += renderNode(data, '', 0, true);
  html += '</div>';
  html += `<div class="json-path-toast" id="${escapeHTML(containerId)}-path-toast"></div>`;
  html += '</div>';
  return html;
}

/**
 * Recursively render a JSON node.
 * @param {*} value
 * @param {string} path
 * @param {number} depth
 * @param {boolean} expanded
 * @returns {string}
 */
function renderNode(value, path, depth, expanded) {
  if (value === null) return `<span class="json-null" data-json-path="${escapeHTML(path)}">null</span>`;
  if (value === undefined) return `<span class="json-null" data-json-path="${escapeHTML(path)}">undefined</span>`;

  const type = typeof value;

  if (type === 'string') {
    const displayVal = value.length > 200 ? value.substring(0, 200) + '...' : value;
    return `<span class="json-string" data-json-path="${escapeHTML(path)}">"${escapeHTML(displayVal)}"</span>`;
  }
  if (type === 'number') return `<span class="json-number" data-json-path="${escapeHTML(path)}">${value}</span>`;
  if (type === 'boolean') return `<span class="json-boolean" data-json-path="${escapeHTML(path)}">${value}</span>`;

  if (Array.isArray(value)) {
    if (value.length === 0) return `<span class="json-bracket" data-json-path="${escapeHTML(path)}">[]</span>`;
    const isOpen = expanded && depth < 2;
    let h = `<span class="json-toggle ${isOpen ? 'open' : ''}" data-json-toggle data-json-path="${escapeHTML(path)}">`;
    h += `<span class="json-arrow">&#9654;</span>`;
    h += `<span class="json-bracket">[</span>`;
    h += `<span class="json-count">${value.length} items</span>`;
    h += '</span>';
    h += `<div class="json-children ${isOpen ? '' : 'hidden'}">`;
    value.forEach((item, i) => {
      const childPath = path + '[' + i + ']';
      h += `<div class="json-entry" style="padding-left:${(depth + 1) * 16}px">`;
      h += `<span class="json-index">${i}: </span>`;
      h += renderNode(item, childPath, depth + 1, expanded);
      if (i < value.length - 1) h += '<span class="json-comma">,</span>';
      h += '</div>';
    });
    h += '</div>';
    h += `<span class="json-bracket json-close-bracket ${isOpen ? '' : 'hidden'}">]</span>`;
    return h;
  }

  if (type === 'object') {
    const keys = Object.keys(value);
    if (keys.length === 0) return `<span class="json-bracket" data-json-path="${escapeHTML(path)}">{}</span>`;
    const isOpen = expanded && depth < 2;
    let h = `<span class="json-toggle ${isOpen ? 'open' : ''}" data-json-toggle data-json-path="${escapeHTML(path)}">`;
    h += `<span class="json-arrow">&#9654;</span>`;
    h += `<span class="json-bracket">{</span>`;
    h += `<span class="json-count">${keys.length} keys</span>`;
    h += '</span>';
    h += `<div class="json-children ${isOpen ? '' : 'hidden'}">`;
    keys.forEach((key, i) => {
      const childPath = path ? path + '.' + key : key;
      h += `<div class="json-entry" style="padding-left:${(depth + 1) * 16}px">`;
      h += `<span class="json-key" data-json-path="${escapeHTML(childPath)}">"${escapeHTML(key)}"</span>`;
      h += '<span class="json-colon">: </span>';
      h += renderNode(value[key], childPath, depth + 1, expanded);
      if (i < keys.length - 1) h += '<span class="json-comma">,</span>';
      h += '</div>';
    });
    h += '</div>';
    h += `<span class="json-bracket json-close-bracket ${isOpen ? '' : 'hidden'}">}</span>`;
    return h;
  }

  return `<span>${escapeHTML(String(value))}</span>`;
}

/**
 * Initialize JSON explorer event delegation.
 * Call once at app startup.
 */
export function initJSONExplorer() {
  if (typeof document === 'undefined') return;

  document.addEventListener('click', (e) => {
    // Toggle expand/collapse
    const toggle = e.target.closest('[data-json-toggle]');
    if (toggle) {
      e.preventDefault();
      toggle.classList.toggle('open');
      const children = toggle.nextElementSibling;
      if (children && children.classList.contains('json-children')) {
        children.classList.toggle('hidden');
        // Also toggle the closing bracket
        const closeBracket = children.nextElementSibling;
        if (closeBracket && closeBracket.classList.contains('json-close-bracket')) {
          closeBracket.classList.toggle('hidden');
        }
      }
      return;
    }

    // Copy path on click
    const pathEl = e.target.closest('[data-json-path]');
    if (pathEl && pathEl.dataset.jsonPath) {
      const path = pathEl.dataset.jsonPath;
      const explorer = pathEl.closest('.json-explorer');
      if (explorer) {
        const toast = explorer.querySelector('.json-path-toast');
        if (toast) {
          navigator.clipboard.writeText(path).then(() => {
            toast.textContent = 'Copied: ' + path;
            toast.classList.add('visible');
            setTimeout(() => toast.classList.remove('visible'), 2000);
          }).catch(() => {
            toast.textContent = path;
            toast.classList.add('visible');
            setTimeout(() => toast.classList.remove('visible'), 2000);
          });
        }
      }
      return;
    }

    // Expand all
    const expandAll = e.target.closest('[data-json-expand-all]');
    if (expandAll) {
      const id = expandAll.dataset.jsonExpandAll;
      const container = document.getElementById(id);
      if (container) {
        container.querySelectorAll('.json-toggle').forEach(t => t.classList.add('open'));
        container.querySelectorAll('.json-children').forEach(c => c.classList.remove('hidden'));
        container.querySelectorAll('.json-close-bracket').forEach(b => b.classList.remove('hidden'));
      }
      return;
    }

    // Collapse all
    const collapseAll = e.target.closest('[data-json-collapse-all]');
    if (collapseAll) {
      const id = collapseAll.dataset.jsonCollapseAll;
      const container = document.getElementById(id);
      if (container) {
        container.querySelectorAll('.json-toggle').forEach(t => t.classList.remove('open'));
        container.querySelectorAll('.json-children').forEach(c => c.classList.add('hidden'));
        container.querySelectorAll('.json-close-bracket').forEach(b => b.classList.add('hidden'));
      }
      return;
    }
  });

  // Search handler
  document.addEventListener('input', (e) => {
    if (!e.target.matches('[data-json-search]')) return;
    const query = e.target.value.toLowerCase().trim();
    const containerId = e.target.dataset.jsonSearch;
    const container = document.getElementById(containerId);
    if (!container) return;

    const countEl = document.getElementById(containerId + '-search-count');
    const entries = container.querySelectorAll('.json-entry');

    if (!query) {
      entries.forEach(entry => {
        entry.style.display = '';
        entry.querySelectorAll('.json-highlight').forEach(h => {
          h.classList.remove('json-highlight');
        });
      });
      if (countEl) countEl.textContent = '';
      return;
    }

    let matchCount = 0;
    entries.forEach(entry => {
      const text = entry.textContent.toLowerCase();
      if (text.includes(query)) {
        entry.style.display = '';
        matchCount++;
        // Expand parents
        let parent = entry.parentElement;
        while (parent && !parent.classList.contains('json-explorer')) {
          if (parent.classList.contains('json-children') && parent.classList.contains('hidden')) {
            parent.classList.remove('hidden');
            const toggle = parent.previousElementSibling;
            if (toggle) toggle.classList.add('open');
            const closeBracket = parent.nextElementSibling;
            if (closeBracket && closeBracket.classList.contains('json-close-bracket')) {
              closeBracket.classList.remove('hidden');
            }
          }
          parent = parent.parentElement;
        }
      } else {
        entry.style.display = 'none';
      }
    });
    if (countEl) countEl.textContent = matchCount + ' match' + (matchCount !== 1 ? 'es' : '');
  });
}
