/**
 * analyzers/stage-classifier.js — Infer pipeline stages from processor sequence
 *
 * Stages are higher-level groupings than roles. A stage is a contiguous
 * block of processors performing a related function in the data pipeline.
 * Used by the tier diagram to show stage swimlane bands.
 *
 * @module analyzers/stage-classifier
 */

import { classifyNiFiProcessorFull } from '../mappers/processor-classifier.js';

/**
 * Pipeline stage definitions with detection heuristics.
 * Order matters — first match wins.
 */
const STAGE_DEFS = [
  { id: 'ingestion',      label: 'INGESTION',      color: '#3B82F6',
    match: (role, sub, action) => role === 'source' },
  { id: 'extraction',     label: 'EXTRACTION',      color: '#8B5CF6',
    match: (role, sub) => sub === 'extract' },
  { id: 'routing',        label: 'ROUTING',          color: '#EAB308',
    match: (role) => role === 'route' },
  { id: 'enrichment',     label: 'ENRICHMENT',       color: '#06B6D4',
    match: (role, sub, action) => action === 'enrich' || sub === 'enrichment' || sub === 'api-call' },
  { id: 'transformation', label: 'TRANSFORMATION',   color: '#A855F7',
    match: (role) => role === 'transform' },
  { id: 'loading',        label: 'LOADING',          color: '#21C354',
    match: (role) => role === 'sink' },
  { id: 'monitoring',     label: 'MONITORING',       color: '#808495',
    match: (role, sub, action) => role === 'utility' || action === 'monitor' },
  { id: 'processing',     label: 'PROCESSING',       color: '#6366F1',
    match: () => true }, // fallback
];

/**
 * Classify individual processors into pipeline stages.
 *
 * @param {Array} processors - parsed processor array with .type
 * @returns {{ stages: Array<{id, label, color, processors: string[]}>,
 *             processorStage: Object<string, string> }}
 */
export function classifyStages(processors) {
  const processorStage = {};
  const stageBuckets = {};

  (processors || []).forEach(p => {
    if (!p || !p.type) return;
    const meta = classifyNiFiProcessorFull(p.type);
    const stage = STAGE_DEFS.find(s => s.match(meta.role, meta.subcategory, meta.action));
    processorStage[p.name] = stage.id;
    if (!stageBuckets[stage.id]) {
      stageBuckets[stage.id] = { id: stage.id, label: stage.label, color: stage.color, processors: [] };
    }
    stageBuckets[stage.id].processors.push(p.name);
  });

  // Order stages by STAGE_DEFS order (not alphabetical)
  const stages = STAGE_DEFS
    .filter(s => stageBuckets[s.id])
    .map(s => stageBuckets[s.id]);

  return { stages, processorStage };
}

/**
 * Classify process groups into stages based on dominant processor stage.
 *
 * @param {Object} groupStats - { groupName: { processors: [{type}], ... } }
 * @returns {Object<string, string>} - { groupName: stageId }
 */
export function classifyGroupStages(groupStats) {
  const groupStage = {};

  Object.entries(groupStats || {}).forEach(([groupName, stats]) => {
    const stageCounts = {};
    (stats.processors || []).forEach(p => {
      if (!p || !p.type) return;
      const meta = classifyNiFiProcessorFull(p.type);
      const stage = STAGE_DEFS.find(s => s.match(meta.role, meta.subcategory, meta.action));
      stageCounts[stage.id] = (stageCounts[stage.id] || 0) + 1;
    });

    const sorted = Object.entries(stageCounts).sort((a, b) => b[1] - a[1]);
    groupStage[groupName] = sorted.length > 0 ? sorted[0][0] : 'processing';
  });

  return groupStage;
}

/**
 * Get the stage definition by ID.
 * @param {string} stageId
 * @returns {Object|undefined}
 */
export function getStageDef(stageId) {
  return STAGE_DEFS.find(s => s.id === stageId);
}

export { STAGE_DEFS };
