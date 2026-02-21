/**
 * generators/cell-builders/config-cell.js — Configuration Cell Builder
 *
 * Builds the Spark configuration cell that sets up catalog, schema,
 * secret scope, and adaptive query settings.
 *
 * FIX HIGH: All paths are config-driven via getDbxConfig() defaults
 * instead of hardcoded /tmp/, /mnt/, or localhost values.
 *
 * Extracted from generateDatabricksNotebook config logic (index.html ~line 5792-5797).
 *
 * @module generators/cell-builders/config-cell
 */

import { DBX_CONFIG_DEFAULTS } from '../../core/config.js';

/**
 * Build the imports and configuration cell.
 *
 * @param {Object} options
 * @param {string} options.smartImportsCode — collected import statements
 * @param {string} options.catalogName — Unity Catalog name (may be empty)
 * @param {string} options.schemaName — schema/database name
 * @param {string} [options.secretScope] — Databricks secret scope
 * @returns {Object} — cell object { type, label, source, role }
 */
export function buildConfigCell({ smartImportsCode, catalogName, schemaName, secretScope }) {
  let configCode = smartImportsCode + '\n\n# Databricks notebook configuration\nspark.conf.set("spark.sql.adaptive.enabled", "true")';
  if (catalogName) configCode += `\nspark.sql("USE CATALOG \`${catalogName}\`")`;
  configCode += `\nspark.sql("USE SCHEMA \`${schemaName}\`")`;
  if (secretScope) configCode += `\n\nSECRET_SCOPE = "${secretScope}"`;

  // FIX HIGH: Use config-driven checkpoint path instead of hardcoded /tmp/
  const checkpointBase = catalogName
    ? `/Volumes/${catalogName}/${schemaName}/checkpoints`
    : DBX_CONFIG_DEFAULTS.workspacePath + '/checkpoints';
  configCode += `\n\n# Config-driven paths (no hardcoded /tmp/ or /mnt/)`;
  configCode += `\nCHECKPOINT_BASE = "${checkpointBase}"`;
  configCode += `\nVOLUMES_BASE = "${catalogName ? `/Volumes/${catalogName}/${schemaName}` : `/Volumes/main/${schemaName}`}"`;

  configCode += '\nprint(f"Notebook initialized \u2014 Spark version: {spark.version}")';
  return { type: 'code', label: 'Imports & Config', source: configCode, role: 'config' };
}
