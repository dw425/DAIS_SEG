/**
 * ui/toast.js — Path trace toast notifications
 *
 * Extracted from index.html lines 7033-7066.
 */

/**
 * Show the path trace toast with current multi-select state.
 * Extracted from index.html lines 7033-7050.
 *
 * @param {{ selected: string[], pathNodes: Set<string> }} ms — multi-select state
 */
export function showPathToast(ms) {
  let toast = document.getElementById('pathTraceToast');
  if (!toast) {
    toast = document.createElement('div');
    toast.id = 'pathTraceToast';
    toast.className = 'path-trace-toast';
    document.body.appendChild(toast);
  }
  const count = ms.selected.length;
  const pathLen = ms.pathNodes.size;
  const msg = count === 1
    ? '1 node selected \u2014 click another to trace route'
    : `${count} nodes selected \u2014 ${pathLen} in path`;
  toast.innerHTML =
    `<span>${msg}</span>` +
    `<span class="toast-hint">Click nodes to build route</span>` +
    `<span class="toast-clear" id="pathTraceToastClear">\u2715 Clear</span>`;
  toast.style.display = 'flex';
  const clearBtn = document.getElementById('pathTraceToastClear');
  if (clearBtn) {
    clearBtn.addEventListener('click', () => { toast.style.display = 'none'; });
  }
}

/**
 * Hide the path trace toast.
 * Extracted from index.html lines 7052-7055.
 */
export function hidePathToast() {
  const toast = document.getElementById('pathTraceToast');
  if (toast) toast.style.display = 'none';
}

/**
 * Flash a "No direct path" message on the toast.
 * Extracted from index.html lines 7057-7066.
 */
export function flashNoPath() {
  const toast = document.getElementById('pathTraceToast');
  if (toast) {
    const noPath = document.createElement('span');
    noPath.style.cssText = 'color:var(--red);margin-left:8px';
    noPath.textContent = 'No direct path';
    toast.appendChild(noPath);
    setTimeout(() => { if (noPath.parentNode) noPath.remove(); }, 2500);
  }
}
