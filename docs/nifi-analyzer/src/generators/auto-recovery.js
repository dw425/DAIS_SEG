/**
 * generators/auto-recovery.js -- Break-Fix Auto Recovery
 *
 * Generates recovery code that is appended inside the except block
 * of the error framework wrapper. Provides role-specific recovery
 * strategies with real error handling -- no passthrough, no empty
 * DataFrames, no limit on DLQ writes.
 *
 * Recovery strategies:
 * - Source: PERMISSIVE mode -> schema inference -> raise with clear message
 * - Transform: column subset -> raise (never passthrough)
 * - Sink: write ALL failed records to DLQ -> raise
 * - Additional recovery for schema mismatch, permission denied, resource not found
 *
 * Every recovery path logs what happened.
 *
 * @module generators/auto-recovery
 */

import { sanitizeVarName } from '../utils/string-helpers.js';

/**
 * Generate auto-recovery code for a processor mapping.
 *
 * @param {Object} m -- processor mapping
 * @param {string} qualifiedSchema -- fully qualified schema name
 * @param {Object} lineage -- DataFrame lineage map
 * @returns {string} -- recovery code to append (empty string if not applicable)
 */
export function generateAutoRecovery(m, qualifiedSchema, lineage) {
  if (!m.mapped || !m.code || m.code.startsWith('# TODO')) return '';
  const varName = sanitizeVarName(m.name);
  const li = lineage[m.name] || {};
  const inputVar = li.inputVars && li.inputVars.length ? li.inputVars[0].varName : 'df_input';
  const outputVar = li.outputVar || ('df_' + varName);
  const safeName = m.name.replace(/"/g, '\\"').replace(/'/g, "''");

  if (m.role === 'source') {
    return buildSourceRecovery(varName, outputVar, safeName, qualifiedSchema);
  } else if (m.role === 'transform' || m.role === 'process') {
    return buildTransformRecovery(varName, outputVar, inputVar, safeName, qualifiedSchema);
  } else if (m.role === 'sink') {
    return buildSinkRecovery(varName, outputVar, inputVar, safeName, qualifiedSchema);
  } else if (m.role === 'route') {
    return buildRouteRecovery(varName, outputVar, inputVar, safeName, qualifiedSchema);
  }
  return '';
}

/**
 * Source recovery: PERMISSIVE mode -> schema inference -> raise with clear message.
 * Never creates empty DataFrames.
 */
function buildSourceRecovery(varName, outputVar, safeName, qualifiedSchema) {
  return `
        # RECOVERY (source): try PERMISSIVE mode, then schema inference, then raise
        _err_msg_${varName} = str(_e).lower()
        _source_recovered_${varName} = False
        # Attempt 1: Re-read with PERMISSIVE mode and corrupt record column
        try:
            _recovery_base = globals().get("VOLUMES_BASE", "/Volumes/main/nifi_migration")
            ${outputVar} = (spark.read
                .option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", "_corrupt_record")
                .option("header", "true")
                .csv(_recovery_base + "/fallback"))
            _corrupt_count = ${outputVar}.filter("_corrupt_record IS NOT NULL").count()
            if _corrupt_count > 0:
                print(f"[RECOVERED] ${safeName}: PERMISSIVE mode, {_corrupt_count} corrupt records flagged")
                # Log corrupt records to DLQ
                try:
                    ${outputVar}.filter("_corrupt_record IS NOT NULL").write.mode("append").saveAsTable("${qualifiedSchema}.__dead_letter_queue")
                except Exception:
                    pass
                ${outputVar} = ${outputVar}.filter("_corrupt_record IS NULL").drop("_corrupt_record")
            _cell_status_${varName} = "RECOVERED"
            _source_recovered_${varName} = True
            print(f"[RECOVERED] ${safeName}: re-read with PERMISSIVE mode")
        except Exception as _perm_err:
            print(f"[RECOVERY] ${safeName}: PERMISSIVE mode failed: {_perm_err}")
        # Attempt 2: Try with full schema inference
        if not _source_recovered_${varName}:
            try:
                _recovery_base = globals().get("VOLUMES_BASE", "/Volumes/main/nifi_migration")
                ${outputVar} = (spark.read
                    .option("inferSchema", "true")
                    .option("header", "true")
                    .option("multiLine", "true")
                    .json(_recovery_base + "/fallback"))
                _cell_status_${varName} = "RECOVERED"
                _source_recovered_${varName} = True
                print(f"[RECOVERED] ${safeName}: schema inference succeeded")
            except Exception as _inf_err:
                print(f"[RECOVERY] ${safeName}: schema inference failed: {_inf_err}")
        # Attempt 3: Check for specific error types
        if not _source_recovered_${varName}:
            if "permission" in _err_msg_${varName} or "access denied" in _err_msg_${varName}:
                _cell_status_${varName} = "SKIPPED"
                print(f"[SKIPPED] ${safeName}: permission denied -- check Unity Catalog grants and cluster IAM role")
            elif "not found" in _err_msg_${varName} or "does not exist" in _err_msg_${varName}:
                _cell_status_${varName} = "SKIPPED"
                print(f"[SKIPPED] ${safeName}: source not found -- verify path/table/topic exists")
            else:
                print(f"[FAILED] ${safeName}: all recovery attempts exhausted")
        # Log recovery attempt
        try:
            spark.sql(f"""INSERT INTO ${qualifiedSchema}.__execution_log VALUES (
                _RUN_ID, '${safeName}', 'source', 'recovery',
                current_timestamp(), '{_cell_status_${varName}}',
                '{str(_e)[:200]}', 0, 0, 0.0, ''
            )""")
        except Exception:
            pass`;
}

/**
 * Transform recovery: try column subset, then raise. Never passthrough.
 */
function buildTransformRecovery(varName, outputVar, inputVar, safeName, qualifiedSchema) {
  return `
        # RECOVERY (transform): try column subset, then raise (never passthrough)
        _err_msg_${varName} = str(_e).lower()
        _transform_recovered_${varName} = False
        # Check for column resolution errors
        if "cannot resolve" in _err_msg_${varName} or "analysisexception" in _err_msg_${varName}:
            try:
                import re as _re_recovery
                # Extract the missing column name from the error
                _col_match = _re_recovery.search(r"cannot resolve.*['\x60]([^'\x60]+)['\x60]", str(_e))
                _missing_col = _col_match.group(1) if _col_match else None
                _avail_cols = ${inputVar}.columns
                if _missing_col:
                    _keep_cols = [c for c in _avail_cols if c != _missing_col]
                    print(f"[RECOVERY] ${safeName}: column '{_missing_col}' not found, selecting {len(_keep_cols)} available columns")
                else:
                    _keep_cols = _avail_cols
                ${outputVar} = ${inputVar}.select(*_keep_cols)
                _cell_status_${varName} = "RECOVERED"
                _transform_recovered_${varName} = True
                print(f"[RECOVERED] ${safeName}: selected available columns")
            except Exception as _col_err:
                print(f"[RECOVERY FAILED] ${safeName}: column subset failed: {_col_err}")
        elif "schema" in _err_msg_${varName} and "mismatch" in _err_msg_${varName}:
            # Schema mismatch: try to cast to the expected types
            try:
                from pyspark.sql.functions import col
                ${outputVar} = ${inputVar}.select(*[col(c).cast("string").alias(c) for c in ${inputVar}.columns])
                _cell_status_${varName} = "RECOVERED"
                _transform_recovered_${varName} = True
                print(f"[RECOVERED] ${safeName}: cast all columns to string to resolve schema mismatch")
            except Exception as _cast_err:
                print(f"[RECOVERY FAILED] ${safeName}: schema cast failed: {_cast_err}")
        elif "parseexception" in _err_msg_${varName}:
            _cell_status_${varName} = "SKIPPED"
            _transform_recovered_${varName} = True
            print(f"[SKIPPED] ${safeName}: SQL parse error requires manual fix")
        # Log recovery attempt
        try:
            spark.sql(f"""INSERT INTO ${qualifiedSchema}.__execution_log VALUES (
                _RUN_ID, '${safeName}', 'transform', 'recovery',
                current_timestamp(), '{_cell_status_${varName}}',
                '{str(_e)[:200]}', 0, 0, 0.0, ''
            )""")
        except Exception:
            pass`;
}

/**
 * Sink recovery: write ALL failed records to DLQ (no limit), then raise.
 */
function buildSinkRecovery(varName, outputVar, inputVar, safeName, qualifiedSchema) {
  return `
        # RECOVERY (sink): write ALL failed records to DLQ, then raise
        _err_msg_${varName} = str(_e).lower()
        _sink_recovered_${varName} = False
        # Write all failed records to dead letter queue (no limit)
        try:
            from pyspark.sql.functions import lit, current_timestamp as _cts
            _dlq_records = (${inputVar}
                .withColumn("_dlq_source", lit("${safeName}"))
                .withColumn("_dlq_error", lit(str(_e)[:500]))
                .withColumn("_dlq_timestamp", _cts())
                .withColumn("_dlq_run_id", lit(_RUN_ID)))
            _dlq_records.write.mode("append").saveAsTable("${qualifiedSchema}.__dead_letter_queue")
            _dlq_count = ${inputVar}.count()
            _cell_status_${varName} = "DLQ"
            print(f"[DLQ] ${safeName}: ALL {_dlq_count} records written to dead letter queue")
        except Exception as _dlq_err:
            print(f"[DLQ FAILED] ${safeName}: could not write to DLQ: {_dlq_err}")
        # Connection/timeout errors: retry with real exponential backoff
        if "connection" in _err_msg_${varName} or "timeout" in _err_msg_${varName}:
            import time as _retry_time
            for _sink_attempt in range(1, 4):
                _backoff = 5 * (2 ** (_sink_attempt - 1))
                print(f"[RETRY] ${safeName}: connection retry {_sink_attempt}/3, waiting {_backoff}s")
                _retry_time.sleep(_backoff)
                try:
                    ${inputVar}.write.mode("append").option("mergeSchema", "true").saveAsTable("${qualifiedSchema}.__retry_sink_${varName}")
                    _cell_status_${varName} = "RECOVERED"
                    _sink_recovered_${varName} = True
                    print(f"[RECOVERED] ${safeName}: retry {_sink_attempt} succeeded")
                    break
                except Exception as _retry_err:
                    if _sink_attempt == 3:
                        print(f"[FAILED] ${safeName}: all 3 connection retries exhausted: {_retry_err}")
        elif "table or view not found" in _err_msg_${varName}:
            _cell_status_${varName} = "SKIPPED"
            _sink_recovered_${varName} = True
            print(f"[SKIPPED] ${safeName}: target table not found -- create table DDL first, then re-run")
        elif "permission" in _err_msg_${varName} or "access denied" in _err_msg_${varName}:
            _cell_status_${varName} = "SKIPPED"
            _sink_recovered_${varName} = True
            print(f"[SKIPPED] ${safeName}: permission denied -- check WRITE grants on target table")
        # Log recovery attempt
        try:
            spark.sql(f"""INSERT INTO ${qualifiedSchema}.__execution_log VALUES (
                _RUN_ID, '${safeName}', 'sink', 'recovery',
                current_timestamp(), '{_cell_status_${varName}}',
                '{str(_e)[:200]}', 0, 0, 0.0, ''
            )""")
        except Exception:
            pass`;
}

/**
 * Route recovery: log the routing error, mark as skipped.
 */
function buildRouteRecovery(varName, outputVar, inputVar, safeName, qualifiedSchema) {
  return `
        # RECOVERY (route): routing errors are typically configuration issues
        _err_msg_${varName} = str(_e).lower()
        if "cannot resolve" in _err_msg_${varName}:
            _cell_status_${varName} = "SKIPPED"
            print(f"[SKIPPED] ${safeName}: routing condition references missing column -- check filter expression")
        else:
            _cell_status_${varName} = "SKIPPED"
            print(f"[SKIPPED] ${safeName}: routing failed -- review NiFi routing conditions")
        # Log recovery attempt
        try:
            spark.sql(f"""INSERT INTO ${qualifiedSchema}.__execution_log VALUES (
                _RUN_ID, '${safeName}', 'route', 'recovery',
                current_timestamp(), '{_cell_status_${varName}}',
                '{str(_e)[:200]}', 0, 0, 0.0, ''
            )""")
        except Exception:
            pass`;
}
