/**
 * core/config.js — Databricks configuration management
 *
 * Extracted from index.html lines 7263-7292.
 *
 * FIX MED:  localStorage access wrapped in safeGetItem / safeSetItem to
 *           handle SecurityError in restricted contexts (iframes, etc.).
 * FIX HIGH: Hardcoded paths replaced with config-driven defaults that
 *           can be overridden via DBX_CONFIG_DEFAULTS.
 */

import { safeGetItem, safeSetItem } from '../utils/local-storage.js';

/** Storage key for persisted Databricks configuration. */
const STORAGE_KEY = 'dbx_config';

/**
 * Default Databricks configuration values.
 *
 * FIX HIGH: All paths and infrastructure defaults are centralised here
 * instead of being hardcoded throughout the codebase.
 */
export const DBX_CONFIG_DEFAULTS = Object.freeze({
  catalog: '',
  schema: '',
  secretScope: '',
  cloudProvider: 'azure',
  sparkVersion: '14.3.x-scala2.12',
  nodeType: 'Standard_DS3_v2',
  numWorkers: 2,
  workspacePath: '/Workspace/Migrations/NiFi',
});

/**
 * Load the Databricks config from localStorage, merged with defaults.
 *
 * @returns {Object}
 */
export function loadDbxConfig() {
  try {
    const s = safeGetItem(STORAGE_KEY);
    return s
      ? { ...DBX_CONFIG_DEFAULTS, ...JSON.parse(s) }
      : { ...DBX_CONFIG_DEFAULTS };
  } catch (e) {
    return { ...DBX_CONFIG_DEFAULTS };
  }
}

/**
 * Persist the Databricks config to localStorage.
 *
 * @param {Object} cfg
 */
export function saveDbxConfig(cfg) {
  try {
    safeSetItem(STORAGE_KEY, JSON.stringify(cfg));
  } catch (e) {
    // Silently ignore — storage may be unavailable
  }
}

/**
 * Read current config values from the UI form elements.
 *
 * @returns {Object}
 */
export function getDbxConfig() {
  return {
    catalog: document.getElementById('cfgCatalog')?.value || '',
    schema: document.getElementById('cfgSchema')?.value || '',
    secretScope: document.getElementById('cfgScope')?.value || '',
    cloudProvider: document.getElementById('cfgCloud')?.value || DBX_CONFIG_DEFAULTS.cloudProvider,
    sparkVersion: document.getElementById('cfgSparkVersion')?.value || DBX_CONFIG_DEFAULTS.sparkVersion,
    nodeType: document.getElementById('cfgNodeType')?.value || DBX_CONFIG_DEFAULTS.nodeType,
    numWorkers: parseInt(document.getElementById('cfgWorkers')?.value) || DBX_CONFIG_DEFAULTS.numWorkers,
    workspacePath: document.getElementById('cfgWorkspacePath')?.value || DBX_CONFIG_DEFAULTS.workspacePath,
  };
}

/**
 * Replace placeholder tokens in generated notebook code with actual config values.
 *
 * @param {string} code
 * @param {Object} cfg — config object (typically from getDbxConfig())
 * @returns {string}
 */
export function resolveNotebookPlaceholders(code, cfg) {
  if (!cfg || !cfg.catalog) return code;
  return code
    .replace(/<catalog>|\{catalog\}/g, cfg.catalog)
    .replace(/<schema>|\{schema\}/g, cfg.schema || 'default')
    .replace(/<scope>|\{scope\}/g, cfg.secretScope || 'migration_secrets')
    .replace(/<workspace_path>/g, cfg.workspacePath || DBX_CONFIG_DEFAULTS.workspacePath)
    .replace(/<spark_version>/g, cfg.sparkVersion || DBX_CONFIG_DEFAULTS.sparkVersion)
    .replace(/<node_type>/g, cfg.nodeType || DBX_CONFIG_DEFAULTS.nodeType);
}
