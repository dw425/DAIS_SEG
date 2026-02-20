/**
 * core/state.js — Reactive state store
 *
 * Replaces the global `let STATE = {...}` from index.html line 538.
 * Adds subscription support and resetState() to clear between flow loads.
 *
 * FIX HIGH: resetState() prevents stale data leaking between flow loads.
 */

const INITIAL_STATE = Object.freeze({
  parsed: null,
  analysis: null,
  assessment: null,
  notebook: null,
  migrationReport: null,
  finalReport: null,
  manifest: null,
  validation: null,
  valueAnalysis: null,
});

let _state = { ...INITIAL_STATE };
const _listeners = new Set();

/**
 * Create and return the store API.  Calling createStore() more than once
 * resets internal state (useful in tests).
 */
export function createStore(initial = {}) {
  _state = { ...INITIAL_STATE, ...initial };
  _listeners.clear();
  return { getState, setState, subscribe, resetState };
}

/**
 * Return a shallow copy of the current state (or a single key).
 * @param {string} [key] — optional top-level key
 */
export function getState(key) {
  if (key !== undefined) {
    return _state[key];
  }
  return { ..._state };
}

/**
 * Merge partial updates into the store and notify subscribers.
 * @param {object} partial — keys to merge
 */
export function setState(partial) {
  if (!partial || typeof partial !== 'object') return;
  const prev = { ..._state };
  Object.assign(_state, partial);
  _notify(prev);
}

/**
 * Subscribe to state changes.
 * @param {function} listener — called with (newState, prevState)
 * @returns {function} unsubscribe
 */
export function subscribe(listener) {
  if (typeof listener !== 'function') {
    throw new TypeError('subscribe() expects a function');
  }
  _listeners.add(listener);
  return () => _listeners.delete(listener);
}

/**
 * FIX HIGH — Reset all state keys back to null.
 * Must be called before loading a new flow to prevent stale data.
 */
export function resetState() {
  const prev = { ..._state };
  _state = { ...INITIAL_STATE };
  _notify(prev);
}

/**
 * Step prerequisites — keys required in state before a step can run.
 */
const STEP_PREREQUISITES = Object.freeze({
  analyze:     ['parsed'],
  assess:      ['parsed'],
  convert:     ['parsed', 'assessment'],
  report:      ['parsed', 'notebook'],
  reportFinal: ['parsed'],
  validate:    ['parsed', 'notebook'],
  value:       ['parsed', 'notebook'],
});

/**
 * Deep-clone the current state for snapshot/rollback.
 * @returns {object} Deep copy of state
 */
export function snapshotState() {
  return JSON.parse(JSON.stringify(_state));
}

/**
 * Restore state from a previous snapshot.
 * @param {object} snapshot - Previously captured state
 */
export function rollbackState(snapshot) {
  if (!snapshot || typeof snapshot !== 'object') return;
  const prev = { ..._state };
  _state = { ...snapshot };
  _notify(prev);
}

/**
 * Return a map of step name → boolean indicating completion.
 * @returns {object}
 */
export function getStepStatus() {
  return {
    parse:       _state.parsed != null,
    analyze:     _state.analysis != null,
    assess:      _state.assessment != null,
    convert:     _state.notebook != null,
    report:      _state.migrationReport != null,
    reportFinal: _state.finalReport != null,
    validate:    _state.validation != null,
    value:       _state.valueAnalysis != null,
  };
}

/**
 * Check whether prerequisites for a step are satisfied.
 * @param {string} stepName - Step key from STEP_PREREQUISITES
 * @returns {{ok:boolean, missing:string[]}}
 */
export function validatePrerequisites(stepName) {
  const required = STEP_PREREQUISITES[stepName];
  if (!required) return { ok: true, missing: [] };
  const missing = required.filter(key => _state[key] == null);
  return { ok: missing.length === 0, missing };
}

/* ---- internal ---- */

function _notify(prev) {
  const snapshot = { ..._state };
  for (const fn of _listeners) {
    try {
      fn(snapshot, prev);
    } catch (err) {
      console.error('[state] subscriber threw:', err);
    }
  }
}
