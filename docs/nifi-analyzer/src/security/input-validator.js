/**
 * security/input-validator.js — Input validation & sanitization
 *
 * FIX CRIT: Prevents eval/injection vectors by validating all
 * user-supplied input (XML uploads, JSON pastes, text fields) before
 * they enter the processing pipeline.
 */

/**
 * Validate that a string is well-formed XML.
 * Returns an object with `valid` (boolean) and `error` (string|null).
 *
 * @param {string} xmlString
 * @returns {{ valid: boolean, error: string|null, doc: Document|null }}
 */
export function validateXML(xmlString) {
  if (!xmlString || typeof xmlString !== 'string') {
    return { valid: false, error: 'Input is empty or not a string', doc: null };
  }

  // Reject excessively large input (10 MB)
  if (xmlString.length > 10 * 1024 * 1024) {
    return { valid: false, error: 'Input exceeds 10 MB size limit', doc: null };
  }

  // Reject DOCTYPE declarations that could enable XXE attacks
  if (/<!DOCTYPE\s/i.test(xmlString)) {
    return { valid: false, error: 'DOCTYPE declarations are not allowed', doc: null };
  }

  try {
    const parser = new DOMParser();
    const doc = parser.parseFromString(xmlString, 'application/xml');
    const parseError = doc.querySelector('parsererror');
    if (parseError) {
      return {
        valid: false,
        error: parseError.textContent || 'XML parse error',
        doc: null,
      };
    }
    return { valid: true, error: null, doc };
  } catch (err) {
    return { valid: false, error: err.message, doc: null };
  }
}

/**
 * Validate that a string is well-formed JSON.
 * Returns an object with `valid` (boolean), `error`, and `data`.
 *
 * @param {string} jsonString
 * @returns {{ valid: boolean, error: string|null, data: any }}
 */
export function validateJSON(jsonString) {
  if (!jsonString || typeof jsonString !== 'string') {
    return { valid: false, error: 'Input is empty or not a string', data: null };
  }

  // Reject excessively large input (10 MB)
  if (jsonString.length > 10 * 1024 * 1024) {
    return { valid: false, error: 'Input exceeds 10 MB size limit', data: null };
  }

  try {
    const data = JSON.parse(jsonString);
    return { valid: true, error: null, data };
  } catch (err) {
    return { valid: false, error: err.message, data: null };
  }
}

/**
 * Sanitize a general text input string.
 * - Strips null bytes
 * - Removes control characters (except \n, \r, \t)
 * - Trims whitespace
 * - Optionally limits length
 *
 * @param {string} input
 * @param {object} [opts]
 * @param {number} [opts.maxLength=10000]
 * @returns {string}
 */
export function sanitizeInput(input, { maxLength = 10000 } = {}) {
  if (!input || typeof input !== 'string') return '';

  let s = input;

  // Remove null bytes
  s = s.replace(/\0/g, '');

  // Remove control characters except \n \r \t
  // eslint-disable-next-line no-control-regex
  s = s.replace(/[\x01-\x08\x0B\x0C\x0E-\x1F\x7F]/g, '');

  // Trim
  s = s.trim();

  // Enforce max length
  if (s.length > maxLength) {
    s = s.substring(0, maxLength);
  }

  return s;
}
