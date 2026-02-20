/**
 * mappers/template-resolver.js -- Resolve NiFi processor type to Databricks template
 *
 * Extracted from index.html lines 4036-4057.
 * Applies property substitution, NEL translation, and EL post-processing.
 */

import { sanitizeVarName } from '../utils/string-helpers.js';

/**
 * Resolve a NIFI_DATABRICKS_MAP template entry by substituting variable names
 * and processor properties into the template string.
 *
 * @param {object} mapEntry - The NIFI_DATABRICKS_MAP entry with .tpl, .conf, etc.
 * @param {string} varName - Sanitized variable name for this processor
 * @param {string} inputVar - Sanitized variable name for the input processor
 * @param {string[]} inputProcs - Array of input processor names
 * @param {object} props - Processor properties (already variable-resolved)
 * @param {function} translateNELtoPySpark - NEL translation function
 * @param {function} postProcessELInCode - EL post-processing function
 * @param {string} procName - Original processor name (for EL post-processing)
 * @returns {{ code: string, conf: number }}
 */
export function resolveTemplate(mapEntry, varName, inputVar, inputProcs, props, translateNELtoPySpark, postProcessELInCode, procName) {
  let code = mapEntry.tpl
    .replace(/\{v\}/g, varName)
    .replace(/\{in\}/g, inputVar)
    .replace(/\{in1\}/g, inputVar)
    .replace(/\{in2\}/g, inputProcs[1] ? sanitizeVarName(inputProcs[1]) : 'input2');

  // Generic property substitution
  Object.entries(props).forEach(([k, val]) => {
    const key = k.replace(/\s+/g, '_').toLowerCase();
    code = code.replace(new RegExp('\\{' + key + '\\}', 'gi'), val);
  });

  // REC #3: Translate remaining NiFi EL expressions to PySpark
  if (code.includes('${')) {
    code = translateNELtoPySpark(code, 'python');
  }

  // FIX #1: Post-process to resolve ALL remaining NiFi EL literals
  if (code.includes('${')) {
    code = postProcessELInCode(code, procName);
  }

  return { code, conf: mapEntry.conf };
}
