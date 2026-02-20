/**
 * generators/cell-builders/error-framework.js — Error/Logging Framework Wrapper
 *
 * Wraps mapped processor code in a try/except block with execution timing,
 * row counting, and logging to the __execution_log Delta table.
 *
 * Extracted from index.html lines 5535-5569.
 *
 * @module generators/cell-builders/error-framework
 */

import { sanitizeVarName } from '../../utils/string-helpers.js';

/**
 * Wrap processor code in the error/logging framework.
 *
 * Generates a try/except block that:
 * - Times execution (start/end)
 * - Counts output rows
 * - Logs success/failure to __execution_log
 * - Records error messages and upstream processor names
 *
 * @param {Object} m — processor mapping (must have .mapped, .code, .name, .desc, .role, .type, .confidence)
 * @param {string} qualifiedSchema — fully qualified schema name
 * @param {number} cellIndex — cell position index
 * @param {Object} lineage — DataFrame lineage map
 * @returns {string} — wrapped code
 */
export function wrapWithErrorFramework(m, qualifiedSchema, cellIndex, lineage) {
  if (!m.mapped || !m.code || m.code.startsWith('# TODO')) return m.code;
  const varName = sanitizeVarName(m.name);
  const li = lineage[m.name] || {};
  const outputVar = li.outputVar || ('df_' + varName);
  const indent = m.code.split('\n').map(l => '        ' + l).join('\n');
  const safeName = m.name.replace(/"/g, '\\"').replace(/'/g, "''");
  const safeType = m.type.replace(/'/g, "''");
  const safeRole = m.role.replace(/'/g, "''");
  const safeDesc = (m.desc || '').replace(/'/g, "''");
  const safeNotes = (m.notes || '').replace(/'/g, "''");
  const safeUpstream = (li.inputVars || []).map(v => v.procName.replace(/'/g, "''")).join(',');
  return '# [' + safeRole.toUpperCase() + '] ' + m.name + '\n' +
    '# ' + safeDesc + (safeNotes ? '  |  ' + safeNotes : '') + '\n' +
    '_cell_start_' + varName + ' = datetime.now()\n' +
    '_cell_status_' + varName + ' = "SUCCESS"\n' +
    '_cell_error_' + varName + ' = ""\n' +
    '_cell_rows_' + varName + ' = 0\n' +
    'try:\n' +
    indent + '\n' +
    '        try: _cell_rows_' + varName + ' = ' + outputVar + '.count()\n' +
    '        except: pass\n' +
    '        print(f"[OK] ' + safeName + ' ({_cell_rows_' + varName + '} rows)")\n' +
    '        spark.sql(f"""INSERT INTO ' + qualifiedSchema + '.__execution_log VALUES (\n' +
    "            '" + safeName + "', '" + safeType + "', '" + safeRole + "',\n" +
    "            current_timestamp(), '{_cell_status_" + varName + "}',\n" +
    "            '{_cell_error_" + varName + "}', {_cell_rows_" + varName + "},\n" +
    "            '{str(datetime.now() - _cell_start_" + varName + ")}',\n" +
    '            ' + Math.round(m.confidence * 100) + ",\n" +
    "            '" + safeUpstream + "'\n" +
    '        )""")\n' +
    'except Exception as _e:\n' +
    '        _cell_status_' + varName + ' = "FAILED"\n' +
    '        _cell_error_' + varName + " = str(_e).replace(\"'\", \"''\")\n" +
    '        print(f"[ERROR] ' + safeName + ': {_e}")\n' +
    '        spark.sql(f"""INSERT INTO ' + qualifiedSchema + '.__execution_log VALUES (\n' +
    "            '" + safeName + "', '" + safeType + "', '" + safeRole + "',\n" +
    "            current_timestamp(), '{_cell_status_" + varName + "}',\n" +
    "            '{_cell_error_" + varName + "}', {_cell_rows_" + varName + "},\n" +
    "            '{str(datetime.now() - _cell_start_" + varName + ")}',\n" +
    '            ' + Math.round(m.confidence * 100) + ",\n" +
    "            '" + safeUpstream + "'\n" +
    '        )""")';
}
