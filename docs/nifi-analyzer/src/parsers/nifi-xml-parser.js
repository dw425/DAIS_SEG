// ================================================================
// nifi-xml-parser.js — Parse NiFi XML templates / flow definitions
// Extracted from monolith lines 895-1070
// ================================================================

import { getChildText, extractProperties } from './nifi-xml-helpers.js';

/**
 * Parse a NiFi XML document (template, flowController, or registry export)
 * into a structured representation of processors, connections,
 * controller services, and process groups.
 *
 * @param {Document} doc - Parsed XML DOM document
 * @param {string} sourceName - Display name for the source file
 * @returns {{ processors: Array, connections: Array, controllerServices: Array, processGroups: Array, idToName: Object }}
 */
export function parseNiFiXML(doc, sourceName) {
  const tables = [], processors = [], connections = [], controllerServices = [], processGroups = [];
  const idToName = {};

  // NiFi templates nest everything: template > snippet > processGroups > contents > processors
  // Recursively extract all processors, connections, etc. from nested processGroups
  function extractFromGroup(groupEl, groupName) {
    const contents = groupEl.querySelector(':scope > contents') || groupEl;
    // Processors — direct children of contents (handle both plural <processors> and singular <processor>)
    const procEls = contents.querySelectorAll(':scope > processors');
    const procElsSingular = procEls.length === 0 ? contents.querySelectorAll(':scope > processor') : [];
    const allProcEls = procEls.length > 0 ? procEls : procElsSingular;
    allProcEls.forEach(proc => {
      const name = getChildText(proc, 'name');
      const type = getChildText(proc, 'type') || getChildText(proc, 'class');
      const shortType = type.split('.').pop();
      const state = getChildText(proc, 'state');
      const props = extractProperties(proc);
      const schedPeriod = proc.querySelector('config > schedulingPeriod')?.textContent || getChildText(proc, 'schedulingPeriod') || '';
      const schedStrategy = proc.querySelector('config > schedulingStrategy')?.textContent || getChildText(proc, 'schedulingStrategy') || '';
      const id = getChildText(proc, 'id');
      if (id) idToName[id] = name || shortType;
      processors.push({name, type:shortType, fullType:type, state, properties:props, group:groupName, schedulingPeriod:schedPeriod, schedulingStrategy:schedStrategy, _id:id||('gen_'+processors.length)});
    });
    // Connections — direct children of contents (handle both plural and singular tags)
    const connEls = contents.querySelectorAll(':scope > connections');
    const connElsSingular = connEls.length === 0 ? contents.querySelectorAll(':scope > connection') : [];
    const allConnEls = connEls.length > 0 ? connEls : connElsSingular;
    allConnEls.forEach(conn => {
      const srcId = conn.querySelector('source > id')?.textContent || getChildText(conn, 'sourceId') || '';
      const dstId = conn.querySelector('destination > id')?.textContent || getChildText(conn, 'destinationId') || '';
      const srcType = conn.querySelector('source > type')?.textContent || '';
      const dstType = conn.querySelector('destination > type')?.textContent || '';
      const rels = [];
      conn.querySelectorAll(':scope > selectedRelationships').forEach(r => { if(r.textContent) rels.push(r.textContent); });
      // Also check singular <relationship> tag
      if (!rels.length) conn.querySelectorAll(':scope > relationship').forEach(r => { if(r.textContent) rels.push(r.textContent); });
      const bp = getChildText(conn, 'backPressureObjectThreshold');
      connections.push({sourceId:srcId, destinationId:dstId, sourceType:srcType, destinationType:dstType, relationships:rels, backPressure:bp});
    });
    // Extract input ports as pseudo-processors
    contents.querySelectorAll(':scope > inputPorts > inputPort, :scope > inputPort, :scope > inputPorts').forEach(p => {
      // Skip wrapper elements that don't have an id directly
      const id = getChildText(p, 'id'), name = getChildText(p, 'name');
      if (id) {
        idToName[id] = name || 'InputPort';
        processors.push({ name: name || 'InputPort', type: 'InputPort', id: id, group: groupName, properties: {} });
      }
    });
    // Extract output ports as pseudo-processors
    contents.querySelectorAll(':scope > outputPorts > outputPort, :scope > outputPort, :scope > outputPorts').forEach(p => {
      const id = getChildText(p, 'id'), name = getChildText(p, 'name');
      if (id) {
        idToName[id] = name || 'OutputPort';
        processors.push({ name: name || 'OutputPort', type: 'OutputPort', id: id, group: groupName, properties: {} });
      }
    });
    // Extract funnels as pseudo-processors
    contents.querySelectorAll(':scope > funnels > funnel, :scope > funnel, :scope > funnels').forEach(f => {
      const id = getChildText(f, 'id');
      if (id) {
        idToName[id] = 'Funnel';
        processors.push({ name: 'Funnel', type: 'Funnel', id: id, group: groupName, properties: {} });
      }
    });
    // Nested processGroups (handle both plural and singular tags)
    const pgEls = contents.querySelectorAll(':scope > processGroups');
    const pgElsSingular = pgEls.length === 0 ? contents.querySelectorAll(':scope > processGroup') : [];
    const allPgEls = pgEls.length > 0 ? pgEls : pgElsSingular;
    allPgEls.forEach(pg => {
      const pgName = getChildText(pg, 'name');
      const pgId = getChildText(pg, 'id');
      if(pgId) idToName[pgId] = pgName;
      processGroups.push({name:pgName, parentGroup:groupName});
      extractFromGroup(pg, pgName);
    });
  }

  // Start from snippet (template format), flowController (NiFi registry), or root
  const snippet = doc.querySelector('template > snippet') || doc.querySelector('snippet') ||
    doc.querySelector('flowController > rootGroup') || doc.querySelector('rootGroup') ||
    doc.querySelector('processGroupFlow > flow') || doc.documentElement;

  // Top-level controllerServices (handle both plural and singular, also nested in <controllerServices> container)
  const csContainer = doc.querySelector('controllerServices');
  const csEls = snippet.querySelectorAll(':scope > controllerServices');
  const csElsSingular = csContainer ? csContainer.querySelectorAll(':scope > controllerService') : [];
  const allCsEls = csEls.length > 0 ? csEls : csElsSingular;
  allCsEls.forEach(cs => {
    const name = getChildText(cs, 'name');
    const type = getChildText(cs, 'type') || getChildText(cs, 'class');
    const state = getChildText(cs, 'state');
    const props = {};
    cs.querySelectorAll(':scope > properties > entry').forEach(entry => {
      const key = entry.querySelector(':scope > key')?.textContent || '';
      const valEl = entry.querySelector(':scope > value');
      if (key && valEl) props[key] = valEl.textContent || '';
    });
    // Also handle direct <property> children (flowController format)
    cs.querySelectorAll(':scope > property').forEach(prop => {
      const key = prop.querySelector(':scope > name')?.textContent || '';
      const val = prop.querySelector(':scope > value')?.textContent || '';
      if (key) props[key] = val;
    });
    controllerServices.push({name, type:type.split('.').pop(), fullType:type, state, properties:props});
  });

  // Top-level processGroups (handle both plural and singular tags)
  const topPgEls = snippet.querySelectorAll(':scope > processGroups');
  const topPgElsSingular = topPgEls.length === 0 ? snippet.querySelectorAll(':scope > processGroup') : [];
  const allTopPgEls = topPgEls.length > 0 ? topPgEls : topPgElsSingular;
  allTopPgEls.forEach(pg => {
    const pgName = getChildText(pg, 'name');
    const pgId = getChildText(pg, 'id');
    if(pgId) idToName[pgId] = pgName;
    processGroups.push({name:pgName, parentGroup:'(root)'});
    extractFromGroup(pg, pgName);
  });

  // Also check for top-level processors directly in snippet (handle both plural and singular)
  const topProcEls = snippet.querySelectorAll(':scope > processors');
  const topProcElsSingular = topProcEls.length === 0 ? snippet.querySelectorAll(':scope > processor') : [];
  const allTopProcEls = topProcEls.length > 0 ? topProcEls : topProcElsSingular;
  allTopProcEls.forEach(proc => {
    const name = getChildText(proc, 'name');
    const type = getChildText(proc, 'type') || getChildText(proc, 'class');
    const id = getChildText(proc, 'id');
    if(id) idToName[id] = name || type.split('.').pop();
    const props = extractProperties(proc);
    const schedPeriod = proc.querySelector('config > schedulingPeriod')?.textContent || getChildText(proc, 'schedulingPeriod') || '';
    const schedStrategy = proc.querySelector('config > schedulingStrategy')?.textContent || getChildText(proc, 'schedulingStrategy') || '';
    processors.push({name, type:type.split('.').pop(), fullType:type, state:getChildText(proc,'state'), properties:props, group:'(root)', schedulingPeriod:schedPeriod, schedulingStrategy:schedStrategy, _id:id||('gen_'+processors.length)});
  });

  // Also check for top-level connections directly in snippet (handle both plural and singular)
  const topConnEls = snippet.querySelectorAll(':scope > connections');
  const topConnElsSingular = topConnEls.length === 0 ? snippet.querySelectorAll(':scope > connection') : [];
  const allTopConnEls = topConnEls.length > 0 ? topConnEls : topConnElsSingular;
  // Use Set for O(1) deduplication instead of .some() O(n) scan
  const connKeys = new Set();
  connections.forEach(c => {
    connKeys.add(`${c.sourceId}|${c.destinationId}|${[...c.relationships].sort().join(',')}`);
  });
  allTopConnEls.forEach(conn => {
    const srcId = conn.querySelector('source > id')?.textContent || getChildText(conn, 'sourceId') || '';
    const dstId = conn.querySelector('destination > id')?.textContent || getChildText(conn, 'destinationId') || '';
    const srcType = conn.querySelector('source > type')?.textContent || '';
    const dstType = conn.querySelector('destination > type')?.textContent || '';
    const rels = [];
    conn.querySelectorAll(':scope > selectedRelationships').forEach(r => { if(r.textContent) rels.push(r.textContent); });
    if (!rels.length) conn.querySelectorAll(':scope > relationship').forEach(r => { if(r.textContent) rels.push(r.textContent); });
    const bp = getChildText(conn, 'backPressureObjectThreshold');
    // Avoid duplicate connections using Set for O(1) lookup
    const connKey = `${srcId}|${dstId}|${[...rels].sort().join(',')}`;
    if (!connKeys.has(connKey)) {
      connKeys.add(connKey);
      connections.push({sourceId:srcId, destinationId:dstId, sourceType:srcType, destinationType:dstType, relationships:rels, backPressure:bp});
    }
  });

  // Resolve connection IDs to processor names
  connections.forEach(c => {
    c.sourceName = idToName[c.sourceId] || c.sourceId.substring(0,12)+'...';
    c.destinationName = idToName[c.destinationId] || c.destinationId.substring(0,12)+'...';
  });

  return { tables, processors, connections, controllerServices, processGroups, idToName };
}
