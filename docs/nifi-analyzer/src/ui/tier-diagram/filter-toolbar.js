/**
 * ui/tier-diagram/filter-toolbar.js — Tier diagram filter toolbar
 *
 * Extracted from index.html lines 6248-6265.
 *
 * FIX MED: Applies debounced search to prevent excessive DOM updates.
 */

import { debounce } from '../../utils/debounce.js';

/**
 * Internal filter state.
 */
let _tierFilterState = { role: 'all', conf: 'all', search: '' };

/**
 * Apply tier filter to all nodes in the diagram.
 * Extracted from index.html lines 6248-6265.
 *
 * @param {HTMLElement} toolbar — the filter toolbar element
 * @param {string}      type   — filter type ('role', 'conf', 'search')
 * @param {string}      value  — filter value
 */
function _applyTierFilter(toolbar, type, value) {
  _tierFilterState[type] = value;
  const container = toolbar.parentElement;
  if (!container) return;

  container.querySelectorAll('[data-node-id]').forEach(el => {
    const role = el.dataset.role || '';
    const conf = parseFloat(el.dataset.conf || 0);
    const name = (el.dataset.name || '').toLowerCase();
    const tp = (el.dataset.type || '').toLowerCase();
    let show = true;
    if (_tierFilterState.role !== 'all' && role !== _tierFilterState.role) show = false;
    if (_tierFilterState.conf === 'high' && conf < 0.7) show = false;
    if (_tierFilterState.conf === 'med' && (conf < 0.3 || conf >= 0.7)) show = false;
    if (_tierFilterState.conf === 'low' && conf >= 0.3) show = false;
    if (_tierFilterState.search && !name.includes(_tierFilterState.search.toLowerCase()) && !tp.includes(_tierFilterState.search.toLowerCase())) show = false;
    el.style.opacity = show ? '1' : '0.15';
    el.style.pointerEvents = show ? '' : 'none';
  });
}

/**
 * Debounced search variant for the text input.
 */
const _debouncedSearchFilter = debounce((toolbar, value) => {
  _applyTierFilter(toolbar, 'search', value);
}, 150);

/**
 * Public tier filter function.
 * For search type, applies debouncing. For role/conf, applies immediately.
 *
 * @param {HTMLElement} toolbar — the filter toolbar element
 * @param {string}      type   — filter type ('role', 'conf', 'search')
 * @param {string}      value  — filter value
 */
export function tierFilter(toolbar, type, value) {
  if (type === 'search') {
    _debouncedSearchFilter(toolbar, value);
  } else {
    _applyTierFilter(toolbar, type, value);
  }
}

/**
 * Reset the filter state.
 */
export function resetTierFilterState() {
  _tierFilterState = { role: 'all', conf: 'all', search: '' };
}
