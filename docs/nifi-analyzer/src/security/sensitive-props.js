/**
 * security/sensitive-props.js — Sensitive property detection & masking
 *
 * Extracted from index.html lines 7256-7261:
 *   const SENSITIVE_PROP_RE = /password|secret|token|key|auth|credential|cert|private|keytab|passphrase/i;
 *   function maskProperty(key, value) {
 *     if (SENSITIVE_PROP_RE.test(key)) return '********';
 *     return value;
 *   }
 *   function isSensitiveProp(key) { return SENSITIVE_PROP_RE.test(key); }
 */

/**
 * Regex matching NiFi property names that contain sensitive data.
 * @type {RegExp}
 */
export const SENSITIVE_PROP_RE =
  /password|secret|token|key|auth|credential|cert|private|keytab|passphrase/i;

/**
 * Return a masked placeholder if the property key looks sensitive,
 * otherwise return the original value.
 *
 * @param {string} key   — property name
 * @param {string} value — property value
 * @returns {string}
 */
export function maskProperty(key, value) {
  if (SENSITIVE_PROP_RE.test(key)) return '********';
  return value;
}

/**
 * Check whether a property name matches the sensitive pattern.
 *
 * @param {string} key
 * @returns {boolean}
 */
export function isSensitiveProp(key) {
  return SENSITIVE_PROP_RE.test(key);
}
