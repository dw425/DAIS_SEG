/**
 * generators/auto-recovery.js — Break-Fix Auto Recovery
 *
 * Generates recovery code that is appended inside the except block
 * of the error framework wrapper. Provides role-specific recovery
 * strategies: source fallback, transform passthrough, sink DLQ.
 *
 * Extracted from index.html lines 5570-5627.
 *
 * @module generators/auto-recovery
 */

import { sanitizeVarName } from '../utils/string-helpers.js';

/**
 * Generate auto-recovery code for a processor mapping.
 *
 * Recovery strategies by role:
 * - **source**: Try relaxed schema (PERMISSIVE mode), fallback to empty DataFrame
 * - **transform/process**: Pass through the input DataFrame unchanged
 * - **sink**: Write failed records to the Dead Letter Queue
 *
 * @param {Object} m — processor mapping
 * @param {string} qualifiedSchema — fully qualified schema name
 * @param {Object} lineage — DataFrame lineage map
 * @returns {string} — recovery code to append (empty string if not applicable)
 */
export function generateAutoRecovery(m, qualifiedSchema, lineage) {
  if (!m.mapped || !m.code || m.code.startsWith('# TODO')) return '';
  const varName = sanitizeVarName(m.name);
  const li = lineage[m.name] || {};
  const inputVar = li.inputVars && li.inputVars.length ? li.inputVars[0].varName : 'df_input';
  const outputVar = li.outputVar || ('df_' + varName);
  const safeName = m.name.replace(/"/g, '\\"');
  if (m.role === 'source') {
    return '\n        # RECOVERY: Try relaxed schema\n' +
      '        try:\n' +
      '            _recovery_base = globals().get("VOLUMES_BASE", "/Volumes/main/nifi_migration")\n' +
      '            ' + outputVar + ' = spark.read.option("mode","PERMISSIVE").option("inferSchema","true").option("header","true").csv(_recovery_base + "/fallback")\n' +
      '            _cell_status_' + varName + ' = "RECOVERED"\n' +
      '            print(f"[RECOVERED] ' + safeName + '")\n' +
      '        except Exception as _e2:\n' +
      '            ' + outputVar + ' = spark.createDataFrame([], "col1 STRING")\n' +
      '            print(f"[FALLBACK] ' + safeName + ' \u2014 empty df: {_e2}")';
  } else if (m.role === 'transform' || m.role === 'process') {
    return '\n        # RECOVERY: Pass through input\n' +
      '        try:\n' +
      '            ' + outputVar + ' = ' + inputVar + '\n' +
      '            _cell_status_' + varName + ' = "PASSTHROUGH"\n' +
      '            print(f"[PASSTHROUGH] ' + safeName + '")\n' +
      '        except: print(f"[SKIP] ' + safeName + '")';
  } else if (m.role === 'sink') {
    return '\n        # RECOVERY: Write to DLQ\n' +
      '        try:\n' +
      '            ' + inputVar + '.limit(1000).write.mode("append").saveAsTable("' + qualifiedSchema + '.__dead_letter_queue")\n' +
      '            _cell_status_' + varName + ' = "DLQ"\n' +
      '            print(f"[DLQ] ' + safeName + '")\n' +
      '        except: print(f"[LOST] ' + safeName + '")';
  }
  return '';
}
