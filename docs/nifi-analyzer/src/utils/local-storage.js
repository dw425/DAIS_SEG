/**
 * utils/local-storage.js — Safe localStorage wrapper
 *
 * FIX MED: Feature-detects localStorage availability and falls back
 * to an in-memory Map when it is unavailable (e.g. sandboxed iframes,
 * private browsing in some browsers, or Storage quota exceeded).
 */

/** In-memory fallback store. */
const _memoryStore = new Map();

/**
 * Check whether localStorage is available and functional.
 * Caches the result so the probe runs only once.
 */
let _localStorageAvailable = null;
function isLocalStorageAvailable() {
  if (_localStorageAvailable !== null) return _localStorageAvailable;
  try {
    const testKey = '__ls_probe__';
    localStorage.setItem(testKey, '1');
    localStorage.removeItem(testKey);
    _localStorageAvailable = true;
  } catch {
    _localStorageAvailable = false;
  }
  return _localStorageAvailable;
}

/**
 * Safely read a value from localStorage (or memory fallback).
 * Returns `defaultValue` on any failure.
 *
 * @param {string} key
 * @param {*}      [defaultValue=null]
 * @returns {*}    — parsed JSON value, raw string, or defaultValue
 */
export function safeGetItem(key, defaultValue = null) {
  try {
    if (isLocalStorageAvailable()) {
      const raw = localStorage.getItem(key);
      if (raw === null) return defaultValue;
      try {
        return JSON.parse(raw);
      } catch {
        return raw; // not JSON, return as-is
      }
    }
    return _memoryStore.has(key) ? _memoryStore.get(key) : defaultValue;
  } catch {
    return defaultValue;
  }
}

/**
 * Safely write a value to localStorage (or memory fallback).
 * Objects are JSON-stringified; strings are stored as-is.
 *
 * @param {string} key
 * @param {*}      value
 * @returns {boolean} — true if the write succeeded
 */
export function safeSetItem(key, value) {
  const serialized = typeof value === 'string' ? value : JSON.stringify(value);
  try {
    if (isLocalStorageAvailable()) {
      localStorage.setItem(key, serialized);
      return true;
    }
    _memoryStore.set(key, value);
    return true;
  } catch {
    // Quota exceeded or other error — fall back to memory
    _memoryStore.set(key, value);
    return false;
  }
}

/**
 * Safely remove a key from localStorage and memory fallback.
 *
 * @param {string} key
 */
export function safeRemoveItem(key) {
  try {
    if (isLocalStorageAvailable()) {
      localStorage.removeItem(key);
    }
  } catch {
    // ignore
  }
  _memoryStore.delete(key);
}
