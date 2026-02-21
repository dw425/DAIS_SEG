/**
 * ui/progress-viz.js — Step completion progress visualization
 *
 * Shows step completion status in the tab bar and overall progress percentage.
 *
 * @module ui/progress-viz
 */

import { escapeHTML } from '../security/html-sanitizer.js';

const STEPS = ['load', 'analyze', 'assess', 'convert', 'report', 'reportFinal', 'validate', 'value'];

/**
 * Initialize the progress visualization bar beneath the tab bar.
 * Inserts a progress track after the .tabs element.
 */
export function initProgressViz() {
  if (typeof document === 'undefined') return;
  const tabs = document.getElementById('tabs');
  if (!tabs) return;

  // Create progress bar element
  const bar = document.createElement('div');
  bar.className = 'step-progress-bar';
  bar.id = 'stepProgressBar';
  bar.innerHTML = `
    <span class="step-progress-label">Progress</span>
    <div class="step-progress-track">
      <div class="step-progress-fill" id="stepProgressFill" style="width:0%"></div>
    </div>
    <span class="step-progress-pct" id="stepProgressPct">0%</span>
  `;
  tabs.parentNode.insertBefore(bar, tabs.nextSibling);
}

/**
 * Update the progress visualization based on current tab statuses.
 * Called after each step completes or status changes.
 */
export function updateProgressViz() {
  if (typeof document === 'undefined') return;

  let completed = 0;
  STEPS.forEach(step => {
    const tab = document.querySelector(`.tab[data-tab="${step}"]`);
    if (tab && tab.classList.contains('done')) {
      completed++;
    }
  });

  const pct = Math.round((completed / STEPS.length) * 100);
  const fill = document.getElementById('stepProgressFill');
  const pctEl = document.getElementById('stepProgressPct');

  if (fill) fill.style.width = pct + '%';
  if (pctEl) pctEl.textContent = pct + '%';
}
