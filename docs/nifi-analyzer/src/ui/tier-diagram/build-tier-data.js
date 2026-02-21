/**
 * ui/tier-diagram/build-tier-data.js — Build tier layout data for visualization
 *
 * Extracted from index.html lines 6105-6244.
 * Builds node/connection data structures for the tier diagram renderer.
 */

import { detectCyclesSCC } from '../../analyzers/cycle-detection.js';
import { classifyNiFiProcessor, classifyNiFiProcessorFull } from '../../mappers/processor-classifier.js';
import { classifyGroupStages, getStageDef } from '../../analyzers/stage-classifier.js';
import { analyzeAttributeFlow, getGroupAttrCreates, getGroupAttrReads } from '../../analyzers/attribute-flow.js';

/**
 * Role-based tier ordering (source to utility).
 * @type {string[]}
 */
const ROLE_TIER_ORDER = ['source', 'route', 'transform', 'process', 'sink', 'utility'];

/**
 * Colors for each role tier.
 */
const ROLE_TIER_COLORS = {
  source: '#3B82F6', route: '#EAB308', transform: '#A855F7',
  process: '#6366F1', sink: '#21C354', utility: '#808495'
};

/**
 * Labels for each role tier.
 */
const ROLE_TIER_LABELS = {
  source: 'SOURCES', route: 'ROUTING', transform: 'TRANSFORMS',
  process: 'PROCESSING', sink: 'SINKS', utility: 'UTILITY'
};

/**
 * Classify the dominant role of a process group based on processor stats.
 *
 * @param {{ sources:number, sinks:number, routes:number, transforms:number, processes:number, utilities:number }} stats
 * @returns {string} dominant role name
 */
function classifyGroupDominantRole(stats) {
  const counts = [
    ['source', stats.sources], ['route', stats.routes], ['transform', stats.transforms],
    ['process', stats.processes], ['sink', stats.sinks], ['utility', stats.utilities]
  ];
  counts.sort((a, b) =>
    b[1] !== a[1] ? b[1] - a[1] : ROLE_TIER_ORDER.indexOf(a[0]) - ROLE_TIER_ORDER.indexOf(b[0])
  );
  return counts[0][1] > 0 ? counts[0][0] : 'process';
}

/**
 * Build tier data from a blueprint and parsed result.
 * Dispatches to buildNiFiTierData for NiFi flows.
 * Extracted from index.html lines 6105-6108.
 *
 * @param {object} blueprint
 * @param {object} parsed
 * @returns {object} tier data with nodes, connections, tierLabels, etc.
 */
export function buildTierData(blueprint, parsed) {
  if (parsed && parsed._nifi) return buildNiFiTierData(parsed._nifi, blueprint);
  return { nodes: [], connections: [], tierLabels: {}, tierColors: {}, cycles: [] };
}

/**
 * Build NiFi-specific tier data from parsed NiFi flow.
 * Extracted from index.html lines 6110-6244.
 *
 * @param {object} nifi      — parsed NiFi flow data
 * @param {object} blueprint — assembled blueprint
 * @returns {object} tier data
 */
export function buildNiFiTierData(nifi, blueprint) {
  const nodes = [];
  const connections = [];
  const tierLabels = {};
  const processors = nifi.processors || [];
  const conns = nifi.connections || [];
  const processGroups = nifi.processGroups || [];

  // Step 1: Build per-group stats
  const groupStats = {};
  processors.forEach(p => {
    const g = p.group || '(root)';
    if (!groupStats[g]) {
      groupStats[g] = {
        sources: 0, sinks: 0, routes: 0, transforms: 0, processes: 0, utilities: 0,
        total: 0, processors: [], typeCount: {}
      };
    }
    const role = classifyNiFiProcessor(p.type);
    groupStats[g][role + 's'] = (groupStats[g][role + 's'] || 0) + 1;
    groupStats[g].total++;
    groupStats[g].processors.push(p);
    groupStats[g].typeCount[p.type] = (groupStats[g].typeCount[p.type] || 0) + 1;
  });

  // Step 1b: Stage classification and attribute flow analysis
  const groupStages = classifyGroupStages(groupStats);
  const attrFlowData = analyzeAttributeFlow(processors, conns);

  // Build per-processor full metadata for subcategory/action enrichment
  const procFullMeta = {};
  processors.forEach(p => {
    procFullMeta[p.name] = classifyNiFiProcessorFull(p.type);
  });

  // Collect unique subcategories and action types per group
  const groupSubcategories = {};
  const groupActionTypes = {};
  Object.entries(groupStats).forEach(([gn, stats]) => {
    const subs = new Set();
    const actions = new Set();
    stats.processors.forEach(p => {
      const meta = procFullMeta[p.name];
      if (meta) {
        if (meta.subcategory) subs.add(meta.subcategory);
        if (meta.action) actions.add(meta.action);
      }
    });
    groupSubcategories[gn] = [...subs];
    groupActionTypes[gn] = [...actions];
  });

  // Step 2: Build inter-group connections
  const procToGroup = {};
  processors.forEach(p => { procToGroup[p.name] = p.group || '(root)'; });

  const interGroupConns = {};
  const intraGroupConns = {};
  conns.forEach(c => {
    const srcGroup = procToGroup[c.sourceName] || '(root)';
    const dstGroup = procToGroup[c.destinationName] || '(root)';
    if (srcGroup !== dstGroup) {
      const key = srcGroup + '|' + dstGroup;
      interGroupConns[key] = (interGroupConns[key] || 0) + 1;
    } else {
      intraGroupConns[srcGroup] = (intraGroupConns[srcGroup] || 0) + 1;
    }
  });

  // Step 3: Detect cycles + assign role-based tiers
  const groupNames = Object.keys(groupStats);
  const groupDownstream = {};
  Object.keys(interGroupConns).forEach(key => {
    const [from, to] = key.split('|');
    if (!groupDownstream[from]) groupDownstream[from] = new Set();
    groupDownstream[from].add(to);
  });
  groupNames.forEach(gn => { if (!groupDownstream[gn]) groupDownstream[gn] = new Set(); });

  const sccs = detectCyclesSCC(groupDownstream);
  const cycleGroups = new Set();
  const groupToSCC = {};
  sccs.forEach((scc, i) => { scc.forEach(gn => { cycleGroups.add(gn); groupToSCC[gn] = i; }); });

  // Assign dominant role per group
  const groupDominantRole = {};
  groupNames.forEach(gn => { groupDominantRole[gn] = classifyGroupDominantRole(groupStats[gn]); });

  // Group groups by dominant role
  const roleGroups = {};
  ROLE_TIER_ORDER.forEach(r => { roleGroups[r] = []; });
  groupNames.forEach(gn => { roleGroups[groupDominantRole[gn]].push(gn); });

  // Sort within each role: purity desc, then total desc
  ROLE_TIER_ORDER.forEach(role => {
    const key = role + 's';
    roleGroups[role].sort((a, b) => {
      const aFrac = (groupStats[a][key] || 0) / (groupStats[a].total || 1);
      const bFrac = (groupStats[b][key] || 0) / (groupStats[b].total || 1);
      if (bFrac !== aFrac) return bFrac - aFrac;
      return groupStats[b].total - groupStats[a].total;
    });
  });

  // Step 4: Build role-based tier layout
  let tierNum = 0;
  ROLE_TIER_ORDER.forEach(role => {
    const groups = roleGroups[role];
    if (!groups.length) return;
    tierNum++;
    const color = ROLE_TIER_COLORS[role];
    const rgb = color.replace('#', '').match(/.{2}/g).map(h => parseInt(h, 16));
    tierLabels[tierNum] = {
      label: ROLE_TIER_LABELS[role], color,
      bg: `rgba(${rgb[0]},${rgb[1]},${rgb[2]},0.06)`, role
    };

    groups.forEach(gn => {
      const stats = groupStats[gn];
      const topTypes = Object.entries(stats.typeCount)
        .sort((a, b) => b[1] - a[1]).slice(0, 3)
        .map(([t, c]) => `${t}(${c})`).join(', ');
      const inCycle = cycleGroups.has(gn);
      const sccIdx = groupToSCC[gn];
      const sccMembers = inCycle ? sccs[sccIdx] : [];
      const cycleEdges = inCycle
        ? Object.entries(interGroupConns)
            .filter(([k]) => { const [f, t] = k.split('|'); return sccs[sccIdx].includes(f) && sccs[sccIdx].includes(t); })
            .map(([k, v]) => { const [f, t] = k.split('|'); return { from: f, to: t, count: v }; })
        : [];

      const stageDef = getStageDef(groupStages[gn]);
      const attrCreates = getGroupAttrCreates(stats.processors, attrFlowData.processorAttributes);
      const attrReads = getGroupAttrReads(stats.processors, attrFlowData.processorAttributes);

      nodes.push({
        id: 'pg_' + gn, name: gn, tier: tierNum,
        type: 'process_group', dominantRole: groupDominantRole[gn],
        subtype: groupDominantRole[gn] + 's',
        procCount: stats.total,
        srcCount: stats.sources, sinkCount: stats.sinks,
        routeCount: stats.routes, transformCount: stats.transforms,
        processCount: stats.processes, utilityCount: stats.utilities,
        intraConns: intraGroupConns[gn] || 0,
        topTypes, inCycle, sccMembers, cycleEdges, expandable: true,
        // New: stage, subcategory, action, and attribute data
        stage: groupStages[gn],
        stageLabel: stageDef ? stageDef.label : '',
        stageColor: stageDef ? stageDef.color : '#6366F1',
        subcategories: groupSubcategories[gn] || [],
        actionTypes: groupActionTypes[gn] || [],
        attrCreates, attrReads,
        detail: {
          processors: stats.processors, typeCount: stats.typeCount,
          intraConns: intraGroupConns[gn] || 0,
          procFullMeta, // expose per-processor metadata for expanded view
        }
      });
    });
  });

  // Step 5: Build inter-group connections (red for cycle edges)
  Object.entries(interGroupConns).forEach(([key, count]) => {
    const [from, to] = key.split('|');
    const bothInCycle = cycleGroups.has(from) && cycleGroups.has(to) && groupToSCC[from] === groupToSCC[to];
    connections.push({
      from: 'pg_' + from, to: 'pg_' + to,
      label: count > 1 ? count + ' flows' : '1 flow',
      type: 'flow', color: bothInCycle ? '#EF4444' : '#4B5563',
      width: Math.min(1 + count * 0.3, 4), inCycle: bothInCycle
    });
  });

  // Step 6: Connection density sidebar data
  const densityData = [];
  const globalTypeCount = {};
  processors.forEach(p => { globalTypeCount[p.type] = (globalTypeCount[p.type] || 0) + 1; });
  Object.entries(globalTypeCount).sort((a, b) => b[1] - a[1]).forEach(([type, count]) => {
    const role = classifyNiFiProcessor(type);
    densityData.push({ name: type, writers: count, readers: 0, lookups: 0, total: count, role });
  });

  // Step 7: Cycle summary
  const cycleData = sccs.map((scc, i) => ({
    id: i, groups: scc,
    edgeCount: Object.keys(interGroupConns).filter(k => {
      const [f, t] = k.split('|');
      return scc.includes(f) && scc.includes(t);
    }).length
  }));

  return {
    nodes, connections, tierLabels, diagramType: 'nifi_flow', densityData, cycleData,
    stageData: groupStages, attrFlowData,
  };
}
