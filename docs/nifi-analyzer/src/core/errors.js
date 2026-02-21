/**
 * core/errors.js — Structured error handling
 *
 * FIX HIGH: Replaces silent catch blocks throughout the codebase.
 * All errors are logged to the console AND stored in a ring buffer
 * so the UI can surface them. Errors are also persisted to localStorage.
 */

/** Maximum number of errors retained in the ring buffer. */
const MAX_ERROR_LOG = 500;

/** Structured error codes with severity, messages, and suggested fixes. */
export const ERROR_CODES = {
  PARSE_XML_MALFORMED: { severity: 'high', message: 'XML is malformed or incomplete', fix: 'Check XML syntax — ensure all tags are properly closed', recoverable: false },
  PARSE_JSON_INVALID: { severity: 'high', message: 'JSON is not valid', fix: 'Validate JSON format at jsonlint.com', recoverable: false },
  PARSE_ARCHIVE_CORRUPT: { severity: 'high', message: 'Archive file is corrupt or unreadable', fix: 'Re-download or re-export the NiFi flow', recoverable: false },
  PARSE_EMPTY_FLOW: { severity: 'medium', message: 'Flow contains no processors', fix: 'Verify this is the correct NiFi flow file', recoverable: false },
  ANALYZE_CIRCULAR_DEP: { severity: 'low', message: 'Circular dependency detected in flow', fix: 'Review cycle groups in the tier diagram', recoverable: true },
  MAP_UNKNOWN_PROCESSOR: { severity: 'medium', message: 'Unknown processor type — no mapping template', fix: 'This processor requires manual Databricks implementation', recoverable: true },
  MAP_TEMPLATE_UNRESOLVED: { severity: 'high', message: 'Template placeholders could not be resolved', fix: 'Check processor properties match template expectations', recoverable: true },
  MAP_PROPERTY_MISSING: { severity: 'medium', message: 'Required processor property not found', fix: 'Verify NiFi processor configuration is complete', recoverable: true },
  GEN_UNRESOLVED_PLACEHOLDER: { severity: 'high', message: 'Generated code contains unresolved placeholders', fix: 'Configure Databricks settings in Step 4', recoverable: true },
  GEN_INVALID_PYTHON: { severity: 'high', message: 'Generated code has syntax errors', fix: 'Review flagged cells and fix Python syntax', recoverable: true },
  GEN_MISSING_IMPORT: { severity: 'medium', message: 'Generated code uses unimported modules', fix: 'Add missing imports to the requirements cell', recoverable: true },
  GEN_DEPRECATED_API: { severity: 'low', message: 'Generated code uses deprecated Databricks API', fix: 'Update to current API — see Databricks release notes', recoverable: true },
  VALIDATE_INTENT_MISMATCH: { severity: 'medium', message: 'Processor intent not preserved in notebook', fix: 'Review the generated cell and verify logic matches NiFi behavior', recoverable: true },
  VALIDATE_SCHEMA_VIOLATION: { severity: 'high', message: 'Output schema does not match expected schema', fix: 'Check column names and types in generated code', recoverable: true },
  CONFIG_INVALID_CLOUD: { severity: 'low', message: 'Invalid cloud provider selected', fix: 'Choose azure, aws, or gcp', recoverable: true },
  CONFIG_MISSING_REQUIRED: { severity: 'medium', message: 'Required configuration field is empty', fix: 'Fill in catalog, schema, and secret scope in Step 4', recoverable: true },
};

const STORAGE_KEY = 'nifi_analyzer_errors';

/**
 * Initialize the ring buffer from localStorage if available.
 * @returns {AppError[]}
 */
function loadPersistedErrors() {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored) {
      const parsed = JSON.parse(stored);
      if (Array.isArray(parsed)) {
        // Reconstruct AppError instances from plain objects
        return parsed.map(e => {
          const err = new AppError(e.message || '', {
            code: e.code,
            phase: e.phase,
            severity: e.severity,
            context: e.context,
          });
          err.timestamp = e.timestamp || Date.now();
          return err;
        }).slice(-MAX_ERROR_LOG);
      }
    }
  } catch (_) {
    // localStorage may be unavailable or corrupt — start fresh
  }
  return [];
}

/**
 * Persist the current error log to localStorage.
 */
function persistErrors() {
  try {
    const serializable = _errorLog.map(e => ({
      message: e.message,
      code: e.code,
      phase: e.phase,
      severity: e.severity,
      context: e.context,
      timestamp: e.timestamp,
      stack: e.stack,
    }));
    localStorage.setItem(STORAGE_KEY, JSON.stringify(serializable));
  } catch (_) {
    // localStorage may be full or unavailable — silently ignore
  }
}

/** @type {AppError[]} */
const _errorLog = loadPersistedErrors();

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

/** @type {Array<Function>} Listeners notified on each new error */
const _errorListeners = [];

/**
 * Subscribe to error events. The callback receives the AppError.
 * @param {Function} fn
 */
export function onError(fn) {
  if (typeof fn === 'function') _errorListeners.push(fn);
}

/**
 * Central error handler — logs to console, stores in ring buffer,
 * persists to localStorage, and notifies listeners.
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

  persistErrors();

  // Notify all listeners (e.g., the error panel UI)
  _errorListeners.forEach(fn => {
    try { fn(appErr); } catch (_) { /* prevent listener errors from cascading */ }
  });
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
 * Clear the error log (ring buffer and localStorage).
 */
export function clearErrorLog() {
  _errorLog.length = 0;
  try {
    localStorage.removeItem(STORAGE_KEY);
  } catch (_) {
    // localStorage may be unavailable
  }
}

/**
 * Look up the ERROR_CODES entry for a given code string.
 * @param {string} code — e.g. 'PARSE_XML_MALFORMED'
 * @returns {object|undefined} The error code info, or undefined if not found
 */
export function getErrorCodeInfo(code) {
  return ERROR_CODES[code];
}
