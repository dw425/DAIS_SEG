/**
 * utils/string-helpers.js — String utility functions
 *
 * sanitizeVarName extracted from index.html line 3030.
 */

/**
 * Convert an arbitrary name into a safe variable/column identifier.
 * - Replaces non-alphanumeric chars with underscores
 * - Prepends underscore if it starts with a digit
 * - Lowercases and truncates to 40 chars
 *
 * Original: index.html line 3030
 *   function sanitizeVarName(name) {
 *     return name.replace(/[^a-zA-Z0-9_]/g, '_').replace(/^(\d)/, '_$1').toLowerCase().substring(0, 40);
 *   }
 *
 * @param {string} name
 * @returns {string}
 */
export function sanitizeVarName(name) {
  if (!name) return '_empty';
  return name
    .replace(/[^a-zA-Z0-9_]/g, '_')
    .replace(/^(\d)/, '_$1')
    .toLowerCase()
    .substring(0, 40);
}

/**
 * Truncate a string to `max` characters, appending a suffix if truncated.
 *
 * @param {string} str
 * @param {number} [max=80]
 * @param {string} [suffix='...']
 * @returns {string}
 */
export function truncate(str, max = 80, suffix = '...') {
  if (!str) return '';
  const s = String(str);
  if (s.length <= max) return s;
  return s.substring(0, max - suffix.length) + suffix;
}

/**
 * Naive English pluralizer.
 * Handles common cases; not a full inflection library.
 *
 * @param {string} word
 * @param {number} count
 * @param {string} [plural] — explicit plural form override
 * @returns {string} — e.g. "3 processors"
 */
export function pluralize(word, count, plural) {
  const form =
    count === 1
      ? word
      : plural || (word.endsWith('s') ? word + 'es' : word + 's');
  return `${count} ${form}`;
}
