/**
 * ui/panels.js — Panel show/hide utilities
 *
 * Provides simple helpers to show or hide `.panel` elements by id.
 */

/**
 * Show a panel by id and hide all other `.panel` siblings.
 *
 * @param {string} id — the DOM id of the panel to show
 */
export function showPanel(id) {
  document.querySelectorAll('.panel').forEach(p => {
    p.classList.toggle('active', p.id === id);
  });
}

/**
 * Hide a specific panel by id.
 *
 * @param {string} id — the DOM id of the panel to hide
 */
export function hidePanel(id) {
  const el = document.getElementById(id);
  if (el) el.classList.remove('active');
}
