// ================================================================
// nel/tokenizer.js — Tokenize NEL chained expressions
// Extracted from monolith lines 3187-3210
// ================================================================

/**
 * Split a NiFi Expression Language chain on top-level colons,
 * respecting nested parentheses and quoted strings.
 *
 * Example: "filename:substringBefore('.'):toUpper()" -> ["filename", "substringBefore('.')", "toUpper()"]
 *
 * @param {string} expr - Raw NEL expression (inside ${...})
 * @returns {string[]} Array of chain segments
 */
export function tokenizeNELChain(expr) {
  var parts = []; var current = ''; var depth = 0; var inStr = false; var strChar = '';
  for (var i = 0; i < expr.length; i++) {
    var ch = expr[i];
    if (inStr) {
      current += ch;
      if (ch === strChar) inStr = false;
    } else if (ch === "'" || ch === '"') {
      inStr = true; strChar = ch; current += ch;
    } else if (ch === '(') {
      depth++; current += ch;
    } else if (ch === ')') {
      depth--; current += ch;
    } else if (ch === ':' && depth === 0) {
      // Always split on top-level colons — each segment is a chain step.
      // Namespace prefixes (math:floor, etc.) are handled at the parser level.
      parts.push(current); current = '';
    } else {
      current += ch;
    }
  }
  if (current) parts.push(current);
  return parts;
}
