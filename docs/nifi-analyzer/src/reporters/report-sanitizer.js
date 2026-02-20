/**
 * reporters/report-sanitizer.js — JSON report sanitization
 *
 * Extracted from index.html line 8141.
 * Recursively strips control characters from report JSON to prevent
 * malformed output when serializing.
 *
 * @module reporters/report-sanitizer
 */

/**
 * Recursively sanitize a JSON-serializable object by stripping control characters.
 *
 * @param {*} obj - The value to sanitize (string, array, object, or primitive)
 * @returns {*}   - Sanitized copy
 */
export function sanitizeReportJSON(obj) {
  if (typeof obj === 'string') return obj.replace(/[\x00-\x1f]/g, '');
  if (Array.isArray(obj)) return obj.map(sanitizeReportJSON);
  if (obj && typeof obj === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(obj)) out[k] = sanitizeReportJSON(v);
    return out;
  }
  return obj;
}
