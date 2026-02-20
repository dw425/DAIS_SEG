/**
 * utils/debounce.js — debounce and throttle utilities
 *
 * FIX MED: Applied to search input and hover handlers to prevent
 * excessive DOM updates and layout thrashing.
 */

/**
 * Classic trailing-edge debounce.
 * The wrapped function is invoked after `wait` ms of inactivity.
 *
 * @param {Function} fn
 * @param {number}   wait — milliseconds
 * @returns {Function & { cancel: Function, flush: Function }}
 */
export function debounce(fn, wait = 250) {
  let timerId = null;
  let lastArgs = null;
  let lastThis = null;

  function debounced(...args) {
    lastArgs = args;
    lastThis = this;
    clearTimeout(timerId);
    timerId = setTimeout(() => {
      timerId = null;
      fn.apply(lastThis, lastArgs);
      lastArgs = lastThis = null;
    }, wait);
  }

  debounced.cancel = () => {
    clearTimeout(timerId);
    timerId = null;
    lastArgs = lastThis = null;
  };

  debounced.flush = () => {
    if (timerId !== null) {
      clearTimeout(timerId);
      timerId = null;
      fn.apply(lastThis, lastArgs);
      lastArgs = lastThis = null;
    }
  };

  return debounced;
}

/**
 * Leading-edge throttle — invokes immediately, then ignores calls for
 * `limit` ms.  Optionally fires a trailing call.
 *
 * @param {Function} fn
 * @param {number}   limit — milliseconds
 * @param {object}   [opts]
 * @param {boolean}  [opts.trailing=true] — fire once more after cooldown
 * @returns {Function & { cancel: Function }}
 */
export function throttle(fn, limit = 100, { trailing = true } = {}) {
  let waiting = false;
  let trailingArgs = null;
  let trailingThis = null;
  let timerId = null;

  function throttled(...args) {
    if (!waiting) {
      fn.apply(this, args);
      waiting = true;
      timerId = setTimeout(() => {
        waiting = false;
        if (trailing && trailingArgs !== null) {
          fn.apply(trailingThis, trailingArgs);
          trailingArgs = trailingThis = null;
        }
      }, limit);
    } else if (trailing) {
      trailingArgs = args;
      trailingThis = this;
    }
  }

  throttled.cancel = () => {
    clearTimeout(timerId);
    waiting = false;
    trailingArgs = trailingThis = null;
  };

  return throttled;
}
