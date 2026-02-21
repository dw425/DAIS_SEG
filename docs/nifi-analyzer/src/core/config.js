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
 * Compute type options for Databricks clusters.
 */
export const COMPUTE_TYPES = Object.freeze([
  { value: 'cluster', label: 'All-Purpose Cluster' },
  { value: 'serverless', label: 'Serverless Compute' },
  { value: 'sql-warehouse', label: 'SQL Warehouse' },
]);

/**
 * Supported Databricks Runtime versions.
 */
export const RUNTIME_VERSIONS = Object.freeze([
  { value: '14.3', label: '14.3 LTS', sparkVersion: '14.3.x-scala2.12' },
  { value: '15.4', label: '15.4 LTS', sparkVersion: '15.4.x-scala2.12' },
  { value: '16.0', label: '16.0', sparkVersion: '16.0.x-scala2.12' },
]);

/**
 * Cloud-specific node type maps.
 * Each cloud provider has a set of recommended instance types grouped by size.
 */
export const CLOUD_NODE_TYPES = Object.freeze({
  azure: [
    { value: 'Standard_DS3_v2', label: 'Standard_DS3_v2 (4 vCPU, 14 GB)', tier: 'small' },
    { value: 'Standard_DS4_v2', label: 'Standard_DS4_v2 (8 vCPU, 28 GB)', tier: 'medium' },
    { value: 'Standard_DS5_v2', label: 'Standard_DS5_v2 (16 vCPU, 56 GB)', tier: 'large' },
    { value: 'Standard_E4ds_v5', label: 'Standard_E4ds_v5 (4 vCPU, 32 GB, memory-opt)', tier: 'memory' },
    { value: 'Standard_NC6s_v3', label: 'Standard_NC6s_v3 (6 vCPU, 112 GB, GPU)', tier: 'gpu' },
  ],
  aws: [
    { value: 'i3.xlarge', label: 'i3.xlarge (4 vCPU, 30.5 GB)', tier: 'small' },
    { value: 'i3.2xlarge', label: 'i3.2xlarge (8 vCPU, 61 GB)', tier: 'medium' },
    { value: 'i3.4xlarge', label: 'i3.4xlarge (16 vCPU, 122 GB)', tier: 'large' },
    { value: 'r5.xlarge', label: 'r5.xlarge (4 vCPU, 32 GB, memory-opt)', tier: 'memory' },
    { value: 'p3.2xlarge', label: 'p3.2xlarge (8 vCPU, 61 GB, GPU)', tier: 'gpu' },
  ],
  gcp: [
    { value: 'n2-standard-4', label: 'n2-standard-4 (4 vCPU, 16 GB)', tier: 'small' },
    { value: 'n2-standard-8', label: 'n2-standard-8 (8 vCPU, 32 GB)', tier: 'medium' },
    { value: 'n2-standard-16', label: 'n2-standard-16 (16 vCPU, 64 GB)', tier: 'large' },
    { value: 'n2-highmem-4', label: 'n2-highmem-4 (4 vCPU, 32 GB, memory-opt)', tier: 'memory' },
    { value: 'a2-highgpu-1g', label: 'a2-highgpu-1g (12 vCPU, 85 GB, GPU)', tier: 'gpu' },
  ],
});

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
  computeType: 'cluster',
  runtimeVersion: '14.3',
});

/**
 * Load the Databricks config from localStorage, merged with defaults.
 *
 * @returns {Object}
 */
export function loadDbxConfig() {
  try {
    const s = safeGetItem(STORAGE_KEY);
    // safeGetItem already returns a parsed object (not a raw string),
    // so spread directly without a redundant JSON.parse() call.
    return s && typeof s === 'object'
      ? { ...DBX_CONFIG_DEFAULTS, ...s }
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
    safeSetItem(STORAGE_KEY, cfg);
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
  const runtimeVal = document.getElementById('cfgRuntimeVersion')?.value || DBX_CONFIG_DEFAULTS.runtimeVersion;
  const runtimeEntry = RUNTIME_VERSIONS.find(r => r.value === runtimeVal);
  return {
    catalog: document.getElementById('cfgCatalog')?.value || '',
    schema: document.getElementById('cfgSchema')?.value || '',
    secretScope: document.getElementById('cfgScope')?.value || '',
    cloudProvider: document.getElementById('cfgCloud')?.value || DBX_CONFIG_DEFAULTS.cloudProvider,
    sparkVersion: runtimeEntry ? runtimeEntry.sparkVersion : DBX_CONFIG_DEFAULTS.sparkVersion,
    nodeType: document.getElementById('cfgNodeType')?.value || DBX_CONFIG_DEFAULTS.nodeType,
    numWorkers: parseInt(document.getElementById('cfgWorkers')?.value) || DBX_CONFIG_DEFAULTS.numWorkers,
    workspacePath: document.getElementById('cfgWorkspacePath')?.value || DBX_CONFIG_DEFAULTS.workspacePath,
    computeType: document.getElementById('cfgComputeType')?.value || DBX_CONFIG_DEFAULTS.computeType,
    runtimeVersion: runtimeVal,
  };
}

/**
 * Get the node types for a given cloud provider.
 * @param {string} cloud — cloud provider key (azure, aws, gcp)
 * @returns {Array<{value: string, label: string, tier: string}>}
 */
export function getNodeTypesForCloud(cloud) {
  return CLOUD_NODE_TYPES[cloud] || CLOUD_NODE_TYPES.azure;
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
