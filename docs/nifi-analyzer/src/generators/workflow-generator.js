/**
 * generators/workflow-generator.js — Databricks Workflow JSON Generator
 *
 * Generates a multi-task Databricks Workflow JSON with shared job clusters
 * (not per-task new_cluster) to avoid cost explosion.
 *
 * Extracted from index.html lines 5920-5965.
 *
 * @module generators/workflow-generator
 */

import { sanitizeVarName } from '../utils/string-helpers.js';

/**
 * Generate a Databricks Workflow JSON definition.
 *
 * Creates one task per process group, with inter-group dependencies
 * derived from NiFi connections. All tasks share a single job cluster.
 *
 * @param {Array<Object>} mappings — processor mappings
 * @param {Object} nifi — parsed NiFi flow
 * @param {Object} [cfg] — Databricks config overrides
 * @param {string} [cfg.workspacePath='/Workspace/Migrations/NiFi']
 * @param {string} [cfg.sparkVersion='14.3.x-scala2.12']
 * @param {string} [cfg.nodeType='Standard_DS3_v2']
 * @param {number} [cfg.numWorkers=2]
 * @returns {Object} — Databricks Workflow JSON
 */
export function generateWorkflowJSON(mappings, nifi, cfg) {
  cfg = cfg || {};
  const wsPath = cfg.workspacePath || '/Workspace/Migrations/NiFi';
  const sparkVer = cfg.sparkVersion || '14.3.x-scala2.12';
  const nodeType = cfg.nodeType || 'Standard_DS3_v2';
  const numWorkers = cfg.numWorkers || 2;
  const conns = nifi.connections || [];
  const procToGroup = {};
  (nifi.processors || []).forEach(p => { procToGroup[p.name] = p.group || '(root)'; });
  const groups = [...new Set(Object.values(procToGroup))];
  const groupDeps = {};
  groups.forEach(g => { groupDeps[g] = new Set(); });
  conns.forEach(c => {
    const sg = procToGroup[c.sourceName], dg = procToGroup[c.destinationName];
    if (sg && dg && sg !== dg) groupDeps[dg].add(sg);
  });
  // Shared job cluster definition — avoids cost explosion from per-task clusters
  const sharedClusterKey = 'nifi_migration_cluster';
  const jobClusters = [{
    job_cluster_key: sharedClusterKey,
    new_cluster: {
      spark_version: sparkVer,
      node_type_id: nodeType,
      num_workers: numWorkers,
      spark_conf: {
        'spark.databricks.delta.optimizeWrite.enabled': 'true',
        'spark.databricks.delta.autoCompact.enabled': 'true',
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.shuffle.partitions': 'auto'
      },
      custom_tags: { source: 'nifi_migration' }
    }
  }];

  const tasks = groups.map(g => ({
    task_key: sanitizeVarName(g),
    description: `Process group: ${g}`,
    notebook_task: { notebook_path: `${wsPath}/${sanitizeVarName(g)}_notebook`, source: 'WORKSPACE' },
    depends_on: [...groupDeps[g]].map(d => ({ task_key: sanitizeVarName(d) })),
    job_cluster_key: sharedClusterKey
  }));
  return {
    name: `NiFi_Migration_${sanitizeVarName(groups[0] || 'flow')}`,
    job_clusters: jobClusters,
    tasks,
    format: 'MULTI_TASK',
    tags: { source: 'nifi_migration', generated_by: 'seg_demo' }
  };
}
