/**
 * ui/tabs.js — Tab navigation logic
 *
 * Extracted from index.html lines 543-568.
 *
 * FIX CRIT: Uses addEventListener instead of inline onclick handlers.
 */

/**
 * Initialize tab navigation by attaching click listeners to all `.tab` elements.
 * Replaces the inline `document.querySelectorAll('.tab').forEach(...)` from line 543.
 */
export function initTabs() {
  document.querySelectorAll('.tab').forEach(tab => {
    tab.addEventListener('click', () => {
      if (tab.classList.contains('locked')) return;
      document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
      document.querySelectorAll('.panel').forEach(p => p.classList.remove('active'));
      tab.classList.add('active');
      const panel = document.getElementById('panel-' + tab.dataset.tab);
      if (panel) panel.classList.add('active');
    });
  });
}

/**
 * Programmatically switch to a named tab.
 * Extracted from index.html line 553-556.
 *
 * @param {string} name — the `data-tab` value to activate
 */
export function switchTab(name) {
  document.querySelectorAll('.tab').forEach(t => {
    t.classList.toggle('active', t.dataset.tab === name);
  });
  document.querySelectorAll('.panel').forEach(p => {
    p.classList.toggle('active', p.id === 'panel-' + name);
  });
}

/**
 * Set the status of a tab (locked, processing, done, or ready).
 * Extracted from index.html lines 558-566.
 *
 * @param {string} name   — the `data-tab` value
 * @param {string} status — one of 'locked', 'processing', 'done', 'ready'
 */
export function setTabStatus(name, status) {
  const tab = document.querySelector(`.tab[data-tab="${name}"]`);
  if (!tab) return;
  tab.classList.remove('locked', 'processing', 'done');
  if (status === 'locked') tab.classList.add('locked');
  else if (status === 'processing') tab.classList.add('processing');
  else if (status === 'done') tab.classList.add('done');
  if (status !== 'locked') {
    tab.style.pointerEvents = '';
    tab.style.opacity = '';
  }
}

/**
 * Unlock a tab by setting its status to 'ready'.
 * Extracted from index.html line 568.
 *
 * @param {string} name — the `data-tab` value
 */
export function unlockTab(name) {
  setTabStatus(name, 'ready');
}
