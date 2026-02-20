// ================================================================
// nel/arg-resolver.js — Resolve NEL function arguments
// Extracted from monolith lines 3177-3246
// ================================================================

import { resolveNELVariableContext } from './variable-context.js';
import { parseNELExpression } from './parser.js';

/**
 * Resolve a single NEL argument to PySpark or Python code.
 * Handles string literals, numeric literals, nested expressions,
 * and variable references.
 *
 * @param {string} arg - Raw argument string
 * @param {string} mode - 'col' for PySpark column ops, 'python' for string ops
 * @returns {string} Resolved code string
 */
export function resolveNELArg(arg, mode) {
  arg = arg.trim();
  if (/^'([^']*)'$/.test(arg)) return mode === 'col' ? 'lit("' + arg.slice(1,-1) + '")' : '"' + arg.slice(1,-1) + '"';
  if (/^\d+$/.test(arg)) return mode === 'col' ? 'lit(' + arg + ')' : arg;
  if (arg.includes(':')) return parseNELExpression(arg, mode);
  var ctx = resolveNELVariableContext(arg);
  return mode === 'col' ? (ctx.type === 'column' ? 'col("' + arg + '")' : ctx.code) :
    (ctx.type === 'column' ? '_attrs.get("' + arg + '", "")' : ctx.code);
}

/**
 * Extract function name and arguments from a NEL function call string.
 * Parses comma-separated args respecting nested parens and quotes.
 *
 * @param {string} call - Function call string, e.g. "replace('a', 'b')"
 * @returns {{ name: string, args: string[] }}
 */
export function extractNELFuncArgs(call) {
  var m = call.match(/^([\w:]+)\((.*)\)$/s);
  if (!m) return { name: call, args: [] };
  var name = m[1];
  var rawArgs = m[2].trim();
  if (!rawArgs) return { name: name, args: [] };
  // Parse comma-separated args, respecting quotes and parens
  var args = []; var current = ''; var depth = 0; var inStr = false; var strChar = '';
  for (var i = 0; i < rawArgs.length; i++) {
    var ch = rawArgs[i];
    if (inStr) {
      current += ch;
      if (ch === strChar) inStr = false;
    } else if (ch === "'" || ch === '"') {
      inStr = true; strChar = ch; current += ch;
    } else if (ch === '(') {
      depth++; current += ch;
    } else if (ch === ')') {
      depth--; current += ch;
    } else if (ch === ',' && depth === 0) {
      args.push(current.trim()); current = '';
    } else {
      current += ch;
    }
  }
  if (current.trim()) args.push(current.trim());
  return { name: name, args: args };
}

/**
 * Remove surrounding single or double quotes from an argument string.
 * @param {string} a - Possibly quoted string
 * @returns {string} Unquoted string
 */
export function unquoteArg(a) {
  if (/^'([^']*)'$/.test(a)) return a.slice(1, -1);
  if (/^"([^"]*)"$/.test(a)) return a.slice(1, -1);
  return a;
}
