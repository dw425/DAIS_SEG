/**
 * generators/cycle-loop-generator.js — Cycle/Loop Detection to Python Iteration
 *
 * Converts NiFi flow cycles (circular processor references) into
 * Python loop constructs: tenacity retries for API cycles, while-loops
 * for conditional cycles.
 *
 * Extracted from index.html lines 3654-3700.
 * GAP FIX: Loop bodies now include actual processor code from cycle members
 * instead of TODO placeholders.
 *
 * @module generators/cycle-loop-generator
 */

import { sanitizeVarName } from '../utils/string-helpers.js';

/**
 * Generate a Python loop from a detected NiFi flow cycle.
 *
 * @param {string[]} cycle — ordered list of processor names forming the cycle
 * @param {Array<Object>} mappings — all processor mappings
 * @param {Object} lineage — DataFrame lineage map
 * @returns {string|null} — generated loop code, or null if cycle is trivial
 */
export function generateLoopFromCycle(cycle, mappings, lineage) {
  if (!cycle || cycle.length < 2) return null;
  const loopProcs = cycle.map(name => mappings.find(m => m.name === name)).filter(Boolean);
  if (loopProcs.length === 0) return null;
  // Detect pattern: API retry cycles vs conditional loops
  const hasHTTP = loopProcs.some(m => /InvokeHTTP|PostHTTP|GetHTTP/i.test(m.type));
  const varName = sanitizeVarName(cycle[0]);

  // Build actual loop body from cycle member code
  const loopBodyLines = _buildLoopBody(loopProcs, lineage);

  if (hasHTTP) {
    // API retry cycle -> tenacity while-loop
    const bodyCode = loopBodyLines || '    result = df  # passthrough (no mapped code)';
    return `# Cycle detected: ${cycle.join(' \u2192 ')} \u2192 (loop back)\n` +
      `# Pattern: API retry loop \u2014 converted to tenacity retry\n` +
      `from tenacity import retry, stop_after_attempt, wait_exponential, RetryError\n\n` +
      `@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=60))\n` +
      `def _retry_loop_${varName}(df):\n` +
      `    """Retry loop for: ${cycle.join(' \u2192 ')}"""\n` +
      `    result = df\n` +
      `${bodyCode}\n` +
      `    return result\n\n` +
      `try:\n` +
      `    df_${varName} = _retry_loop_${varName}(df_${varName})\n` +
      `except RetryError:\n` +
      `    print(f"[LOOP] Retry cycle exhausted for ${cycle[0]}")`;
  }

  // Conditional loop
  const whileBody = loopBodyLines || '    _loop_result = _loop_df_' + varName + '  # passthrough (no mapped code)';
  const indentedBody = whileBody.split('\n').map(l => l ? '    ' + l : l).join('\n');
  return `# Cycle detected: ${cycle.join(' \u2192 ')} \u2192 (loop back)\n` +
    `# Pattern: Conditional loop \u2014 converted to while iteration\n` +
    `_max_iterations = 100\n` +
    `_iteration = 0\n` +
    `_loop_df_${varName} = df_${varName}\n` +
    `_loop_result = _loop_df_${varName}\n` +
    `while _iteration < _max_iterations:\n` +
    `    _iteration += 1\n` +
    `${indentedBody}\n` +
    `    if _loop_result.count() == 0:\n` +
    `        break  # No more records to process\n` +
    `    _loop_df_${varName} = _loop_result\n` +
    `df_${varName} = _loop_df_${varName}\n` +
    `print(f"[LOOP] Completed {_iteration} iterations for ${cycle[0]}")`;
}

/**
 * Build the loop body from actual processor code of cycle members.
 *
 * @param {Array<Object>} loopProcs — processor mappings in the cycle
 * @param {Object} lineage — DataFrame lineage map
 * @returns {string|null} — indented body code or null if none available
 * @private
 */
function _buildLoopBody(loopProcs, lineage) {
  const codeLines = [];
  for (const proc of loopProcs) {
    if (!proc.mapped || !proc.code || proc.code.startsWith('# TODO')) continue;
    const li = lineage[proc.name] || {};
    const outputVar = li.outputVar || 'df_' + sanitizeVarName(proc.name);
    codeLines.push(`    # --- ${proc.name} (${proc.type}) ---`);
    // Indent each line of the processor code for the loop body
    const indented = proc.code.split('\n').map(l => '    ' + l).join('\n');
    codeLines.push(indented);
    codeLines.push(`    _loop_result = ${outputVar}`);
  }
  return codeLines.length > 0 ? codeLines.join('\n') : null;
}
