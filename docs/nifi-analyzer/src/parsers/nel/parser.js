// ================================================================
// nel/parser.js — Recursive-descent NEL expression parser
// Extracted from monolith lines 3124-3175
// ================================================================

import { tokenizeNELChain } from './tokenizer.js';
import { resolveNELVariableContext } from './variable-context.js';
import { resolveNELArg } from './arg-resolver.js';
import { applyNELFunction } from './function-handlers.js';

/**
 * Parse a single NEL expression (the content inside ${...}) into
 * PySpark column operations or Python string operations.
 *
 * Handles:
 *   - System functions: now(), UUID(), hostname(), nextInt(), random(), literal()
 *   - Math namespace: math:abs(), math:ceil(), math:floor(), math:round()
 *   - Variable references with context classification (secret/config/column)
 *   - Chained function calls via colon-delimited syntax
 *
 * @param {string} expr - Expression content (without surrounding ${})
 * @param {string} mode - 'col' for PySpark column ops, 'python' for string ops
 * @returns {string} Translated PySpark or Python code
 */
export function parseNELExpression(expr, mode) {
  // Tokenize: split on colon-delimited function calls, respecting nested parens
  var parts = tokenizeNELChain(expr);
  if (parts.length === 0) return '""';

  var base = parts[0].trim();
  var chain = parts.slice(1);

  // Resolve the base reference
  var result;
  // System functions (no base variable)
  if (/^now\(\)$/i.test(base)) {
    result = mode === 'col' ? 'current_timestamp()' : 'datetime.now()';
  } else if (/^UUID\(\)$/i.test(base)) {
    result = mode === 'col' ? 'expr("uuid()")' : 'str(uuid.uuid4())';
  } else if (/^hostname\(\)$/i.test(base)) {
    result = mode === 'col' ? 'lit(spark.conf.get("spark.databricks.clusterUsageTags.clusterName", "unknown"))' : 'socket.gethostname()';
  } else if (/^nextInt\(\)$/i.test(base) || /^random\(\)$/i.test(base)) {
    result = mode === 'col' ? '(rand() * 2147483647).cast("int")' : 'random.randint(0, 2147483647)';
  } else if (/^literal\('((?:[^'\\]|\\.)*)'\)$/i.test(base)) {
    var litVal = base.match(/^literal\('((?:[^'\\]|\\.)*)'\)$/i)[1];
    var escaped = litVal.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n');
    result = mode === 'col' ? 'lit("' + escaped + '")' : '"' + escaped + '"';
  } else if (/^literal\((\d+)\)$/i.test(base)) {
    var litNum = base.match(/^literal\((\d+)\)$/i)[1];
    result = mode === 'col' ? 'lit(' + litNum + ')' : litNum;
  // NOTE: math:* regexes use /^math:func\((.+)\)$/ — the $ anchor forces the
  // literal \) to match the LAST ')' in the string, so .+ (greedy) correctly
  // captures everything between the first '(' and last ')'. This handles
  // single-level nesting like math:ceil(abs(-5)) -> captures "abs(-5)".
  } else if (/^math:abs\((.+)\)$/i.test(base)) {
    var absArg = base.match(/^math:abs\((.+)\)$/i)[1];
    result = mode === 'col' ? 'abs(' + resolveNELArg(absArg, mode) + ')' : 'abs(' + resolveNELArg(absArg, mode) + ')';
  } else if (/^math:ceil\((.+)\)$/i.test(base)) {
    result = mode === 'col' ? 'ceil(' + resolveNELArg(base.match(/^math:ceil\((.+)\)$/i)[1], mode) + ')' : 'math.ceil(' + resolveNELArg(base.match(/^math:ceil\((.+)\)$/i)[1], mode) + ')';
  } else if (/^math:floor\((.+)\)$/i.test(base)) {
    result = mode === 'col' ? 'floor(' + resolveNELArg(base.match(/^math:floor\((.+)\)$/i)[1], mode) + ')' : 'math.floor(' + resolveNELArg(base.match(/^math:floor\((.+)\)$/i)[1], mode) + ')';
  } else if (/^math:round\((.+)\)$/i.test(base)) {
    result = mode === 'col' ? 'round(' + resolveNELArg(base.match(/^math:round\((.+)\)$/i)[1], mode) + ')' : 'round(' + resolveNELArg(base.match(/^math:round\((.+)\)$/i)[1], mode) + ')';
  } else {
    // Variable reference
    var ctx = resolveNELVariableContext(base);
    if (mode === 'col') {
      result = ctx.type === 'column' ? 'col("' + base + '")' : ctx.code;
    } else {
      result = ctx.type === 'column' ? '_attrs.get("' + base + '", "")' :
               ctx.type === 'secret' ? ctx.code : ctx.code;
    }
  }

  // Apply chained function calls
  for (var i = 0; i < chain.length; i++) {
    result = applyNELFunction(result, chain[i].trim(), mode);
  }

  return result;
}
