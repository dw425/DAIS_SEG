/**
 * ui/tier-diagram/attribute-trace.js — Attribute-level tracing visualization
 *
 * When an attribute is selected (from the detail panel), highlights all
 * processors that create, read, or modify that attribute and dims all others.
 *
 * @module ui/tier-diagram/attribute-trace
 */

let _activeTrace = null;

/**
 * Trace an attribute through the flow, highlighting creator/reader nodes.
 *
 * @param {string} attrName - attribute name to trace
 * @param {Object} attrFlowData - from analyzeAttributeFlow()
 * @param {Object} nodeEls - map of node DOM elements (keyed by node id)
 * @param {HTMLElement} container - diagram container
 */
export function traceAttribute(attrName, attrFlowData, nodeEls, container) {
  if (!attrFlowData || !attrFlowData.attributeMap) return;

  const entry = attrFlowData.attributeMap[attrName];
  if (!entry) return;

  // Clear any existing trace
  clearAttributeTrace(nodeEls, container);

  const creatorSet = new Set(entry.creators || []);
  const readerSet = new Set(entry.readers || []);
  const modifierSet = new Set(entry.modifiers || []);
  const involvedProcs = new Set([...creatorSet, ...readerSet, ...modifierSet]);

  // Build a set of group names that contain involved processors
  const involvedGroups = new Set();
  if (attrFlowData.processorAttributes) {
    Object.entries(attrFlowData.processorAttributes).forEach(([procName, pa]) => {
      if (involvedProcs.has(procName)) {
        // Find the group this processor belongs to by checking node elements
        Object.keys(nodeEls).forEach(nodeId => {
          if (nodeId.startsWith('pg_')) {
            const groupName = nodeId.slice(3);
            const el = nodeEls[nodeId];
            // Check if this group's expanded processors contain the processor
            const procEls = el.querySelectorAll ? el.querySelectorAll('[data-proc-name]') : [];
            procEls.forEach(pe => {
              if (pe.dataset.procName === procName) involvedGroups.add(groupName);
            });
          }
        });
      }
    });
  }

  // Apply highlighting
  Object.entries(nodeEls).forEach(([nodeId, el]) => {
    el.classList.remove('attr-creator', 'attr-reader', 'attr-modifier', 'attr-dimmed');

    if (nodeId.startsWith('pg_')) {
      const groupName = nodeId.slice(3);
      if (involvedGroups.has(groupName)) {
        el.classList.add('attr-involved');
      } else {
        el.classList.add('attr-dimmed');
      }
    }

    // Check individual processor elements within expanded groups
    const procEls = el.querySelectorAll ? el.querySelectorAll('[data-proc-name]') : [];
    procEls.forEach(pe => {
      const procName = pe.dataset.procName;
      pe.classList.remove('attr-creator', 'attr-reader', 'attr-modifier', 'attr-dimmed');
      if (creatorSet.has(procName)) {
        pe.classList.add('attr-creator');
      } else if (modifierSet.has(procName)) {
        pe.classList.add('attr-modifier');
      } else if (readerSet.has(procName)) {
        pe.classList.add('attr-reader');
      } else {
        pe.classList.add('attr-dimmed');
      }
    });
  });

  // Show trace info banner
  let banner = container.querySelector('.attr-trace-banner');
  if (!banner) {
    banner = document.createElement('div');
    banner.className = 'attr-trace-banner';
    container.insertBefore(banner, container.firstChild);
  }
  const creators = entry.creators.length;
  const readers = entry.readers.length;
  const modifiers = entry.modifiers.length;
  banner.innerHTML = `
    <span style="font-weight:600">Tracing: <code>${attrName}</code></span>
    <span class="attr-badge attr-create">${creators} creator${creators !== 1 ? 's' : ''}</span>
    <span class="attr-badge attr-read">${readers} reader${readers !== 1 ? 's' : ''}</span>
    ${modifiers > 0 ? `<span class="attr-badge attr-modify">${modifiers} modifier${modifiers !== 1 ? 's' : ''}</span>` : ''}
    <button class="btn btn-sm attr-trace-clear" style="margin-left:auto">Clear Trace (Esc)</button>
  `;
  banner.style.display = 'flex';

  const clearBtn = banner.querySelector('.attr-trace-clear');
  if (clearBtn) {
    clearBtn.addEventListener('click', () => clearAttributeTrace(nodeEls, container), { once: true });
  }

  _activeTrace = { attrName, nodeEls, container };
}

/**
 * Clear attribute trace highlighting.
 *
 * @param {Object} nodeEls - map of node DOM elements
 * @param {HTMLElement} container - diagram container
 */
export function clearAttributeTrace(nodeEls, container) {
  if (nodeEls) {
    Object.values(nodeEls).forEach(el => {
      el.classList.remove('attr-creator', 'attr-reader', 'attr-modifier', 'attr-dimmed', 'attr-involved');
      const procEls = el.querySelectorAll ? el.querySelectorAll('[data-proc-name]') : [];
      procEls.forEach(pe => {
        pe.classList.remove('attr-creator', 'attr-reader', 'attr-modifier', 'attr-dimmed');
      });
    });
  }

  if (container) {
    const banner = container.querySelector('.attr-trace-banner');
    if (banner) banner.style.display = 'none';
  }

  _activeTrace = null;
}

/**
 * Check if an attribute trace is currently active.
 * @returns {boolean}
 */
export function isTraceActive() {
  return _activeTrace !== null;
}

/**
 * Get the currently traced attribute name.
 * @returns {string|null}
 */
export function getActiveTrace() {
  return _activeTrace ? _activeTrace.attrName : null;
}

// Handle Escape key to clear trace
if (typeof document !== 'undefined') {
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && _activeTrace) {
      clearAttributeTrace(_activeTrace.nodeEls, _activeTrace.container);
    }
  });
}
