/**
 * mappers/post-process-el.js -- Post-process NiFi Expression Language in generated code
 *
 * Extracted from index.html lines 3830-3848.
 * Any ${var} that survives template substitution gets converted to
 * Python _attrs dict lookups. Also flags unresolvable expressions.
 */

/**
 * Post-process ALL generated code to resolve NiFi EL literals.
 * Delegates remaining ${...} expressions to the NEL parser,
 * and injects _attrs dict setup when _attrs.get() references appear.
 *
 * @param {string} code - Generated PySpark code that may contain ${...}
 * @param {string} procName - Processor name for variable naming
 * @param {function} translateNELtoPySpark - NEL translation function
 * @returns {string} - Code with EL expressions resolved
 */
export function postProcessELInCode(code, procName, translateNELtoPySpark) {
  if (!code || !code.includes('${')) return code;

  var lines = code.split('\n');
  var hasColOps = false;
  var processed = lines.map(function(line) {
    if (line.trim().charAt(0) === '#') return line;
    if (!line.includes('${')) return line;
    // Delegate to the NEL parser in python mode for remaining EL
    var fixed = translateNELtoPySpark(line, 'python');
    if (fixed !== line) hasColOps = true;
    return fixed;
  });

  var result = processed.join('\n');

  // If we resolved any _attrs references, ensure _attrs is defined
  if (result.indexOf('_attrs.get(') >= 0 && result.indexOf('_attrs =') < 0 && result.indexOf('_attrs=') < 0) {
    var safeName = procName.replace(/[^a-zA-Z0-9_]/g, '_');
    result = '# Resolve NiFi FlowFile attributes from upstream DataFrame\n' +
      '_attrs = {}\n' +
      'try:\n' +
      '    _first_row = df_' + safeName + '_input.first()\n' +
      '    if _first_row: _attrs = _first_row.asDict()\n' +
      'except: pass\n\n' + result;
  }

  return result;
}
