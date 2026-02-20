// ================================================================
// nifi-registry-json.js — Parse NiFi Registry JSON flow exports
// Extracted from monolith lines 3015-3028
// ================================================================

/**
 * Parse a NiFi Registry JSON export into a structured representation.
 * Recursively walks process groups to extract processors, connections,
 * controller services, and process groups.
 *
 * @param {Object} flowData - Parsed JSON flow data (root process group)
 * @param {string} sourceName - Display name for the source file
 * @returns {{ source_name: string, source_type: string, _nifi: Object, parse_warnings: Array, _deferredProcessorWork: null }}
 */
export function parseNiFiRegistryJSON(flowData, sourceName) {
  const processors = [], connections = [], processGroups = [], controllerServices = [];

  function walk(group, parentName) {
    const gName = group.name || parentName || 'root';
    if (group.name) processGroups.push({ name: gName, id: group.identifier || gName });

    (group.processors || []).forEach(p => {
      const props = {};
      if (Array.isArray(p.properties)) {
        p.properties.forEach(pr => { if (pr.name && pr.value) props[pr.name] = pr.value; });
      } else if (p.properties) {
        Object.entries(p.properties).forEach(([k, v]) => { if (v !== null) props[k] = String(v); });
      }
      processors.push({
        id: p.identifier || p.id || ('p_' + processors.length),
        name: p.name || p.type || 'Unknown',
        type: (p.type || '').replace(/^org\.apache\.nifi\.processors?\.\w+\./, ''),
        class: p.type || '',
        group: gName,
        state: p.scheduledState || p.state || 'STOPPED',
        schedulingStrategy: p.schedulingStrategy || 'TIMER_DRIVEN',
        schedulingPeriod: p.schedulingPeriod || '0 sec',
        properties: props,
        relationships: p.autoTerminatedRelationships || []
      });
    });

    (group.connections || []).forEach(c => {
      connections.push({
        sourceId: c.source?.id || c.sourceId || '',
        destinationId: c.destination?.id || c.destinationId || '',
        sourceName: c.source?.name || c.sourceName || '',
        destinationName: c.destination?.name || c.destinationName || '',
        relationships: c.selectedRelationships || [],
        backPressure: c.backPressureObjectThreshold || ''
      });
    });

    (group.controllerServices || []).forEach(cs => {
      controllerServices.push({
        name: cs.name || cs.type || 'Unknown',
        type: (cs.type || '').replace(/^org\.apache\.nifi\.\w+\./, ''),
        state: cs.scheduledState || 'ENABLED',
        properties: cs.properties || {}
      });
    });

    (group.processGroups || []).forEach(pg => walk(pg, pg.name || gName));
  }

  walk(flowData, 'root');

  // Resolve connection IDs to processor names
  const idToName = {};
  processors.forEach(p => { idToName[p.id] = p.name; });
  connections.forEach(c => {
    if (!c.sourceName && c.sourceId) c.sourceName = idToName[c.sourceId] || c.sourceId;
    if (!c.destinationName && c.destinationId) c.destinationName = idToName[c.destinationId] || c.destinationId;
  });

  const _RC = /password|secret|token|key|auth|credential|cert|private|keytab|passphrase/i;

  const nifi = {
    processors,
    connections,
    processGroups,
    controllerServices,
    clouderaTools: [],
    deepPropertyInventory: {
      filePaths: {}, urls: {}, jdbcUrls: {}, nifiEL: {},
      cronExprs: {}, credentialRefs: {}, hostPorts: {},
      dataFormats: new Set(), encodings: new Set()
    },
    sqlTables: [],
    sqlTableMeta: {}
  };

  // Scan properties for deep inventory items
  processors.forEach(p => {
    Object.entries(p.properties).forEach(([k, v]) => {
      if (!v) return;
      if (/\$\{/.test(v)) {
        if (!nifi.deepPropertyInventory.nifiEL[v]) nifi.deepPropertyInventory.nifiEL[v] = [];
        nifi.deepPropertyInventory.nifiEL[v].push(p.name);
      }
      if (/jdbc:/i.test(v)) {
        if (!nifi.deepPropertyInventory.jdbcUrls[v]) nifi.deepPropertyInventory.jdbcUrls[v] = [];
        nifi.deepPropertyInventory.jdbcUrls[v].push(p.name);
      }
      if (_RC.test(k)) {
        if (!nifi.deepPropertyInventory.credentialRefs[k]) nifi.deepPropertyInventory.credentialRefs[k] = [];
        nifi.deepPropertyInventory.credentialRefs[k].push(p.name);
      }
    });
  });

  return {
    source_name: sourceName,
    source_type: 'nifi_registry_json',
    _nifi: nifi,
    parse_warnings: [],
    _deferredProcessorWork: null
  };
}
