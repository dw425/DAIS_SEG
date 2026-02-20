/**
 * generators/retry-wrapper.js — Retry / Error Handling Code Wrapper
 *
 * Wraps network-dependent processor code in tenacity retry logic
 * with exponential backoff.
 *
 * Extracted from index.html lines 3701-3711.
 *
 * @module generators/retry-wrapper
 */

import { sanitizeVarName } from '../utils/string-helpers.js';

/**
 * Wrap processor code in tenacity retry logic if it is a network operation.
 *
 * Only wraps processors whose type matches known network-dependent types
 * (InvokeHTTP, PutElasticsearch, ExecuteSQL, ConsumeKafka, etc.).
 * Non-network processors return code unchanged.
 *
 * @param {string} procName — processor name
 * @param {string} procType — NiFi processor type
 * @param {string} code — generated PySpark code
 * @param {string} [penaltyDuration] — unused (reserved for NiFi parity)
 * @param {string} [yieldDuration] — unused (reserved for NiFi parity)
 * @param {number} [maxRetries=3] — maximum retry attempts
 * @returns {string} — code with retry wrapper (or unchanged)
 */
export function generateRetryWrapper(procName, procType, code, penaltyDuration, yieldDuration, maxRetries) {
  const retries = maxRetries || 3;
  const isNetworkOp = /^(InvokeHTTP|PutElasticsearch|PutMongo|PutS3|PutDynamoDB|ExecuteSQL|PutSQL|PutDatabaseRecord|ConsumeKafka|PublishKafka|Fetch|Get(HTTP|SQS|DynamoDB))/.test(procType);
  if (!isNetworkOp) return code;

  const safeFuncName = procType.toLowerCase().replace(/[^a-z0-9]/g, '_');
  // Use sanitizeVarName so the df_ variable matches what the rest of codegen produces
  // (lowercased, non-alphanum replaced with _, truncated to 40 chars).
  const safeVarName = sanitizeVarName(procName);
  const indented = code.split('\n').map(l => '    ' + l).join('\n');

  return `# ${procName} — with retry logic\n` +
    `from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type\n` +
    `import logging\n` +
    `_logger = logging.getLogger("nifi_migration")\n\n` +
    `@retry(\n` +
    `    stop=stop_after_attempt(${retries}),\n` +
    `    wait=wait_exponential(multiplier=1, min=2, max=30),\n` +
    `    retry=retry_if_exception_type((ConnectionError, TimeoutError, IOError)),\n` +
    `    before_sleep=lambda rs: _logger.warning(f"Retry {rs.attempt_number}/${retries} for ${procName}")\n` +
    `)\n` +
    `def _exec_${safeFuncName}():\n` +
    `${indented}\n` +
    `    return df_${safeVarName}\n\n` +
    `try:\n` +
    `    df_${safeVarName} = _exec_${safeFuncName}()\n` +
    `except Exception as e:\n` +
    `    _logger.error(f"[FAILED] ${procName}: {e}")\n` +
    `    raise`;
}
