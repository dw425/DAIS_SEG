// ================================================================
// nel/variable-context.js — NEL variable context classification
// Extracted from monolith lines 3093-3108 (Gap #14)
// ================================================================

/**
 * Classify a NiFi Expression Language variable by context:
 *   - secret: password/token/credential references -> dbutils.secrets
 *   - config: infrastructure config (s3, kafka, jdbc) -> dbutils.widgets
 *   - column: row-level attributes -> col()
 *
 * @param {string} varName - NiFi variable name
 * @returns {{ type: string, code: string }} Context classification and PySpark code
 */
export function resolveNELVariableContext(varName) {
  const lower = varName.toLowerCase();
  // Secrets: password, token, key, credential, secret
  if (/password|token|secret|credential|api[_.]?key|private[_.]?key|passphrase/i.test(lower)) {
    const scope = lower.includes('kafka') ? 'kafka' : lower.includes('jdbc') ? 'jdbc' :
      lower.includes('s3') || lower.includes('aws') ? 'aws' : lower.includes('azure') ? 'azure' :
      lower.includes('es') ? 'es' : 'app';
    return { type: 'secret', code: `dbutils.secrets.get(scope="${scope}", key="${varName}")` };
  }
  // Config: s3.*, kafka.*, jdbc.*, nifi.*, aws.*
  if (/^(s3|kafka|jdbc|nifi|aws|azure|gcp|http|ftp|smtp|ssl|sasl)\./i.test(varName) ||
      /\.url$|\.host$|\.port$|\.path$|\.bucket$|\.region$/i.test(lower)) {
    return { type: 'config', code: `dbutils.widgets.get("${varName}")` };
  }
  // Row-level attributes -> col()
  return { type: 'column', code: `col("${varName}")` };
}
