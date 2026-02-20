// ================================================================
// nel/index.js — NiFi Expression Language (NEL) translator entry point
// Extracted from monolith lines 3086-3120
// ================================================================

import { parseNELExpression } from './parser.js';

// Re-export sub-modules for direct access
export { parseNELExpression } from './parser.js';
export { tokenizeNELChain } from './tokenizer.js';
export { resolveNELVariableContext } from './variable-context.js';
export { resolveNELArg, extractNELFuncArgs, unquoteArg } from './arg-resolver.js';
export { applyNELFunction } from './function-handlers.js';
export { javaDateToPython } from './java-date-format.js';
export { evaluateNiFiEL } from './el-evaluator.js';
export { parseVariableRegistry, resolveVariables } from './variable-registry.js';

/**
 * Translate a NiFi Expression Language string to PySpark column operations.
 * Replaces each ${...} block with its PySpark equivalent.
 *
 * @param {string} elExpr - NiFi EL expression string (may contain multiple ${...} blocks)
 * @param {string} [mode='col'] - 'col' for PySpark column ops, 'python' for string ops
 * @returns {string} Translated PySpark/Python code
 */
export function translateNELtoPySpark(elExpr, mode) {
  if (!elExpr || typeof elExpr !== 'string') return elExpr;
  mode = mode || 'col'; // 'col' for PySpark column ops, 'python' for string ops

  // If the expression doesn't contain ${, return as-is
  if (!elExpr.includes('${')) return elExpr;

  // Replace each ${...} block
  return elExpr.replace(/\$\{([^}]+)\}/g, function(fullMatch, inner) {
    return parseNELExpression(inner.trim(), mode);
  });
}

/**
 * Translate a NiFi Expression Language string to Python string operations.
 * Backward-compatible alias for translateNELtoPySpark(expr, 'python').
 *
 * @param {string} elExpr - NiFi EL expression string
 * @returns {string} Translated Python code
 */
export function translateNiFiELtoPython(elExpr) {
  return translateNELtoPySpark(elExpr, 'python');
}
