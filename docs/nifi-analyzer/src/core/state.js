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
