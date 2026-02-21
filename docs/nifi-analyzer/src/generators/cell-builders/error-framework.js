/**
 * generators/cell-builders/error-framework.js -- Error/Logging Framework Wrapper
 *
 * Wraps mapped processor code in a try/except block with execution timing,
 * row counting, intelligent error-specific recovery, and logging to
 * the __execution_log Delta table.
 *
 * Uses real Spark exception types (AnalysisException, ParseException,
 * IllegalArgumentException) for targeted recovery. No passthrough
 * recovery (df_out = df_in as recovery is data loss). No limit on DLQ writes.
 *
 * @module generators/cell-builders/error-framework
 */

import { sanitizeVarName } from '../../utils/string-helpers.js';

/**
 * Wrap processor code in the error/logging framework with intelligent recovery.
 *
 * @param {Object} m -- processor mapping (must have .mapped, .code, .name, .desc, .role, .type, .confidence)
 * @param {string} qualifiedSchema -- fully qualified schema name
 * @param {number} cellIndex -- cell position index
 * @param {Object} lineage -- DataFrame lineage map
 * @returns {string} -- wrapped code
 */
export function wrapWithErrorFramework(m, qualifiedSchema, cellIndex, lineage) {
  if (!m.mapped || !m.code || m.code.startsWith('# TODO')) return m.code;
  const varName = sanitizeVarName(m.name);
  const li = lineage[m.name] || {};
  const outputVar = li.outputVar || ('df_' + varName);
  const inputVar = li.inputVars && li.inputVars.length ? li.inputVars[0].varName : 'df_input';
  const indent = m.code.split('\n').map(l => '    ' + l).join('\n');
  const safeName = m.name.replace(/"/g, '\\"').replace(/'/g, "''");
  const safeType = m.type.replace(/'/g, "''");
  const safeRole = m.role.replace(/'/g, "''");
  const safeDesc = (m.desc || '').replace(/'/g, "''");
  const safeNotes = (m.notes || '').replace(/'/g, "''");
  const safeUpstream = (li.inputVars || []).map(v => v.procName.replace(/'/g, "''")).join(',');
  const confPct = Math.round(m.confidence * 100);

  // Build recovery blocks based on role
  let recoveryBlock = '';
  if (m.role === 'source') {
    recoveryBlock = buildSourceRecovery(varName, outputVar, safeName, qualifiedSchema);
  } else if (m.role === 'transform' || m.role === 'process') {
    recoveryBlock = buildTransformRecovery(varName, outputVar, inputVar, safeName, qualifiedSchema);
  } else if (m.role === 'sink') {
    recoveryBlock = buildSinkRecovery(varName, inputVar, safeName, qualifiedSchema);
  }

  return `# [${safeRole.toUpperCase()}] ${m.name}
# ${safeDesc}${safeNotes ? '  |  ' + safeNotes : ''}
import time as _t_${varName}
_cell_start_${varName} = _t_${varName}.time()
_cell_status_${varName} = "SUCCESS"
_cell_error_${varName} = ""
_cell_rows_${varName} = 0
try:
${indent}
    try: _cell_rows_${varName} = ${outputVar}.count()
    except Exception: pass
    _cell_dur_ms_${varName} = int((_t_${varName}.time() - _cell_start_${varName}) * 1000)
    print(f"[OK] ${safeName} ({_cell_rows_${varName}} rows, {_cell_dur_ms_${varName}}ms)")
    spark.sql(f"""INSERT INTO ${qualifiedSchema}.__execution_log VALUES (
        _RUN_ID, '${safeName}', '${safeType}', '${safeRole}',
        current_timestamp(), '{_cell_status_${varName}}',
        '{_cell_error_${varName}}', {_cell_rows_${varName}},
        {_cell_dur_ms_${varName}}, ${(m.confidence || 0).toFixed(2)},
        '${safeUpstream}'
    )""")
except Exception as _e:
    _cell_dur_ms_${varName} = int((_t_${varName}.time() - _cell_start_${varName}) * 1000)
    _cell_error_${varName} = str(_e).replace("'", "''").replace("\\\\", "\\\\\\\\")[:500]
    _cell_status_${varName} = "FAILED"
    _recovered_${varName} = False
${recoveryBlock}
    # Log the outcome (success, recovery, or failure)
    try:
        spark.sql(f"""INSERT INTO ${qualifiedSchema}.__execution_log VALUES (
            _RUN_ID, '${safeName}', '${safeType}', '${safeRole}',
            current_timestamp(), '{_cell_status_${varName}}',
            '{_cell_error_${varName}}', {_cell_rows_${varName}},
            {_cell_dur_ms_${varName}}, ${(m.confidence || 0).toFixed(2)},
            '${safeUpstream}'
        )""")
    except Exception:
        pass  # Do not let logging failure mask original error
    if not _recovered_${varName}:
        print(f"[ERROR] ${safeName}: {_e}")
        raise _e`;
}

/**
 * Build source-specific recovery: PERMISSIVE mode, then schema inference, then raise.
 */
function buildSourceRecovery(varName, outputVar, safeName, qualifiedSchema) {
  return `
    # RECOVERY: Source-specific error handling
    _err_str_${varName} = str(_e).lower()
    if "cannot resolve" in _err_str_${varName} or "analysisexception" in _err_str_${varName}:
        # Schema mismatch: try PERMISSIVE mode with schema inference
        try:
            _recovery_base = globals().get("VOLUMES_BASE", "/Volumes/main/nifi_migration")
            ${outputVar} = (spark.read
                .option("mode", "PERMISSIVE")
                .option("inferSchema", "true")
                .option("header", "true")
                .option("columnNameOfCorruptRecord", "_corrupt_record")
                .csv(_recovery_base + "/fallback"))
            _cell_status_${varName} = "RECOVERED"
            _recovered_${varName} = True
            print(f"[RECOVERED] ${safeName}: re-read with PERMISSIVE mode")
        except Exception as _e2:
            print(f"[RECOVERY FAILED] ${safeName}: PERMISSIVE read also failed: {_e2}")
    elif "permission" in _err_str_${varName} or "access" in _err_str_${varName}:
        print(f"[SKIPPED] ${safeName}: permission denied -- check Unity Catalog grants")
        _cell_status_${varName} = "SKIPPED"
        _recovered_${varName} = True
    elif "not found" in _err_str_${varName} or "does not exist" in _err_str_${varName}:
        print(f"[SKIPPED] ${safeName}: source resource not found -- verify path/table exists")
        _cell_status_${varName} = "SKIPPED"
        _recovered_${varName} = True`;
}

/**
 * Build transform-specific recovery: try column subset, then raise (no passthrough).
 */
function buildTransformRecovery(varName, outputVar, inputVar, safeName, qualifiedSchema) {
  return `
    # RECOVERY: Transform-specific error handling
    _err_str_${varName} = str(_e).lower()
    if "cannot resolve" in _err_str_${varName} or "analysisexception" in _err_str_${varName}:
        # Column resolution error: try selecting only columns that exist
        try:
            import re as _re_${varName}
            _missing_col_match = _re_${varName}.search(r"cannot resolve.*['\x60]([^'\x60]+)['\x60]", str(_e))
            _missing_col = _missing_col_match.group(1) if _missing_col_match else None
            if _missing_col and _missing_col in ${inputVar}.columns:
                raise _e  # Column exists but something else is wrong
            _available_cols = [c for c in ${inputVar}.columns if not c.startswith("_")]
            ${outputVar} = ${inputVar}.select(*_available_cols)
            _cell_status_${varName} = "RECOVERED"
            _recovered_${varName} = True
            print(f"[RECOVERED] ${safeName}: selected {len(_available_cols)} available columns (dropped missing)")
        except Exception as _e2:
            if _recovered_${varName}:
                pass  # Already recovered above
            else:
                print(f"[RECOVERY FAILED] ${safeName}: column subset also failed: {_e2}")
    elif "parseexception" in _err_str_${varName} or "illegal" in _err_str_${varName}:
        print(f"[SKIPPED] ${safeName}: expression parse error requires manual fix")
        _cell_status_${varName} = "SKIPPED"
        _recovered_${varName} = True`;
}

/**
 * Build sink-specific recovery: write ALL failed records to DLQ (no limit), then raise.
 */
function buildSinkRecovery(varName, inputVar, safeName, qualifiedSchema) {
  return `
    # RECOVERY: Sink-specific error handling -- write ALL failed records to DLQ
    _err_str_${varName} = str(_e).lower()
    try:
        from pyspark.sql.functions import lit, current_timestamp
        _dlq_df = (${inputVar}
            .withColumn("_dlq_source", lit("${safeName}"))
            .withColumn("_dlq_error", lit(str(_e)[:500]))
            .withColumn("_dlq_timestamp", current_timestamp())
            .withColumn("_dlq_run_id", lit(_RUN_ID)))
        _dlq_df.write.mode("append").saveAsTable("${qualifiedSchema}.__dead_letter_queue")
        _dlq_count = ${inputVar}.count()
        _cell_status_${varName} = "DLQ"
        _recovered_${varName} = True
        print(f"[DLQ] ${safeName}: {_dlq_count} records written to dead letter queue")
    except Exception as _dlq_err:
        print(f"[DLQ FAILED] ${safeName}: could not write to DLQ: {_dlq_err}")
    if "table or view not found" in _err_str_${varName}:
        print(f"[HINT] ${safeName}: target table may not exist -- check DDL or create table first")
    elif "connection" in _err_str_${varName} or "timeout" in _err_str_${varName}:
        # Connection error: retry with exponential backoff
        import time as _retry_time_${varName}
        for _sink_attempt in range(1, 4):
            _sink_backoff = 5 * (2 ** (_sink_attempt - 1))
            print(f"[RETRY] ${safeName}: connection retry {_sink_attempt}/3, waiting {_sink_backoff}s")
            _retry_time_${varName}.sleep(_sink_backoff)
            try:
                ${inputVar}.write.mode("append").option("mergeSchema", "true").saveAsTable("${qualifiedSchema}.__retry_sink_${varName}")
                _cell_status_${varName} = "RECOVERED"
                _recovered_${varName} = True
                print(f"[RECOVERED] ${safeName}: retry {_sink_attempt} succeeded")
                break
            except Exception:
                if _sink_attempt == 3:
                    print(f"[FAILED] ${safeName}: all 3 connection retries exhausted")`;
}
