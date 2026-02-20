/**
 * security/html-sanitizer.js — XSS prevention utilities
 *
 * escapeHTML extracted from index.html line 7250-7252.
 *
 * FIX CRIT: Provides safe alternatives to raw innerHTML assignments
 * throughout the codebase.
 */

/**
 * Escape a value for safe insertion into HTML.
 *
 * Original (index.html line 7250):
 *   function escapeHTML(s) {
 *     if (s === null || s === undefined) return '';
 *     return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;')
 *       .replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
 *   }
 *
 * @param {*} s
 * @returns {string}
 */
export function escapeHTML(s) {
  if (s === null || s === undefined) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

/**
 * Set the inner HTML of an element using only escaped text content.
 * If `html` is already trusted (e.g. built entirely from escapeHTML
 * calls), pass `trusted: true` to skip double-escaping.
 *
 * @param {HTMLElement} el
 * @param {string}      html
 * @param {object}      [opts]
 * @param {boolean}     [opts.trusted=false]
 */
export function safeInnerHTML(el, html, { trusted = false } = {}) {
  if (!el) return;
  if (trusted) {
    el.innerHTML = html;
  } else {
    el.textContent = html; // textContent is inherently safe
  }
}

/**
 * Create a DOM element with safe attribute/text assignment.
 * No raw innerHTML — text children use textContent.
 *
 * @param {string}  tag
 * @param {object}  [attrs]     — key/value attribute pairs
 * @param {Array<string|HTMLElement>} children — strings become text nodes
 * @returns {HTMLElement}
 */
export function createSafeEl(tag, attrs = {}, ...children) {
  const el = document.createElement(tag);

  for (const [key, value] of Object.entries(attrs)) {
    if (key === 'className') {
      el.className = value;
    } else if (key === 'onclick' && typeof value === 'function') {
      el.addEventListener('click', value);
    } else if (key === 'textContent') {
      el.textContent = value;
    } else if (key.startsWith('data-')) {
      el.setAttribute(key, value);
    } else {
      // Only allow safe attribute names (letters, hyphens, underscores)
      if (/^[a-zA-Z][a-zA-Z0-9_-]*$/.test(key)) {
        el.setAttribute(key, value);
      }
    }
  }

  for (const child of children) {
    if (typeof child === 'string') {
      el.appendChild(document.createTextNode(child));
    } else if (child instanceof Node) {
      el.appendChild(child);
    }
  }

  return el;
}
