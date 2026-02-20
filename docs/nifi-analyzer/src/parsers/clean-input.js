// ================================================================
// clean-input.js — Sanitize raw file content before parsing
// Extracted from monolith lines 880-894
// ================================================================

/**
 * Sanitize raw file content: strip BOM, normalize line endings,
 * remove NULL bytes, replace non-breaking spaces and smart quotes.
 * @param {string} content - Raw file content
 * @returns {string} Cleaned content
 */
export function cleanInput(content) {
  if (!content) return '';
  let c = content;
  // Strip BOM
  if (c.charCodeAt(0) === 0xFEFF) c = c.substring(1);
  // Normalize line endings
  c = c.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
  // Remove NULL bytes
  c = c.replace(/\x00/g, '');
  // Replace non-breaking spaces
  c = c.replace(/\u00A0/g, ' ');
  // Replace smart quotes
  c = c.replace(/[\u201C\u201D]/g, '"').replace(/[\u2018\u2019]/g, "'");
  return c.trim();
}
