/**
 * core/errors.js — Structured error handling
 *
 * FIX HIGH: Replaces silent catch blocks throughout the codebase.
 * All errors are logged to the console AND stored in a ring buffer
 * so the UI can surface them.
 */

/** Maximum number of errors retained in the ring buffer. */
const MAX_ERROR_LOG = 200;

/** @type {AppError[]} */
const _errorLog = [];

/**
 * Structured application error.
 */
export class AppError extends Error {
  /**
   * @param {string} message
   * @param {object} [opts]
   * @param {string} [opts.code]    — machine-readable code, e.g. 'PARSE_FAILED'
   * @param {string} [opts.phase]   — pipeline phase, e.g. 'parse', 'analyze'
   * @param {string} [opts.severity] — 'low' | 'medium' | 'high' | 'critical'
   * @param {Error}  [opts.cause]   — original error
   * @param {object} [opts.context] — arbitrary context data
   */
  constructor(message, { code, phase, severity, cause, context } = {}) {
    super(message);
    this.name = 'AppError';
    this.code = code || 'UNKNOWN';
    this.phase = phase || '';
    this.severity = severity || 'medium';
    this.cause = cause || null;
    this.context = context || {};
    this.timestamp = Date.now();
  }
}

/**
 * Central error handler — logs to console and stores in ring buffer.
 * @param {Error|AppError} err
 * @param {object} [meta] — extra metadata merged into the log entry
 */
export function handleError(err, meta = {}) {
  const appErr =
    err instanceof AppError
      ? err
      : new AppError(err.message || String(err), {
          code: 'UNHANDLED',
          cause: err,
          ...meta,
        });

  // Always log to console so errors are never silently swallowed
  console.error(`[${appErr.code}] ${appErr.message}`, appErr);

  _errorLog.push(appErr);
  if (_errorLog.length > MAX_ERROR_LOG) {
    _errorLog.shift();
  }
}

/**
 * Wrap an async function so that thrown errors are funneled through
 * handleError instead of being silently swallowed.
 *
 * @param {Function} fn        — async function to wrap
 * @param {object}  [meta]     — default metadata attached to any error
 * @returns {Function}         — wrapped async function
 */
export function wrapAsync(fn, meta = {}) {
  return async function wrappedAsync(...args) {
    try {
      return await fn.apply(this, args);
    } catch (err) {
      handleError(err, meta);
      throw err; // re-throw so callers can still react
    }
  };
}

/**
 * Return a shallow copy of the error log (newest last).
 * @returns {AppError[]}
 */
export function getErrorLog() {
  return [..._errorLog];
}

/**
 * Clear the error log.
 */
export function clearErrorLog() {
  _errorLog.length = 0;
}
