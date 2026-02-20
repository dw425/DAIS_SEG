/**
 * validators/reverse-engineering.js — Reverse engineering readiness checks
 *
 * Extracted from index.html lines 8441-8529.
 * Checks whether the generated notebook retains enough information
 * (topology, types, scheduling, controller services, external systems, error handling)
 * to recreate the original NiFi flow.
 *
 * @module validators/reverse-engineering
 */

/**
 * Check reverse engineering readiness across 6 dimensions.
 *
 * @param {object} opts
 * @param {object} opts.nifi             - Parsed NiFi flow object
 * @param {object} opts.systems          - Detected external systems map
 * @param {string} opts.allCellTextLower - All notebook cell source joined and lowercased
 * @param {Function} [opts.onProgress]
 * @returns {Promise<{reScore:number, reChecks:Array}>}
 */
export async function checkReverseEngineering({
  nifi,
  systems,
  allCellTextLower,
  onProgress,
}) {
  let reScore = 0;
  const reChecks = [];

  // Check 1: Flow topology
  const connCount = nifi.connections.length;
  const procCount = nifi.processors.length;
  let topologyInCode = 0;
  nifi.connections.forEach(c => {
    const srcVar = (c.sourceName || '').replace(/[^a-zA-Z0-9]/g, '_').toLowerCase();
    const dstVar = (c.destinationName || '').replace(/[^a-zA-Z0-9]/g, '_').toLowerCase();
    if (srcVar && dstVar && allCellTextLower.includes(srcVar) && allCellTextLower.includes(dstVar)) topologyInCode++;
  });
  const topologyPct = connCount ? Math.round((topologyInCode / connCount) * 100) : 100;
  reChecks.push({ label: 'Flow Topology Preserved', score: topologyPct, detail: topologyInCode + '/' + connCount + ' connections reflected in variable chaining' });

  if (onProgress) {
    onProgress(65, 'Checking processor type identifiability...');
  }
  await new Promise(r => setTimeout(r, 0));

  // Check 2: Processor types identifiable
  let typeIdentifiable = 0;
  nifi.processors.forEach(p => {
    const shortType = p.type.split('.').pop().toLowerCase();
    if (allCellTextLower.includes(shortType) || allCellTextLower.includes('nifi: ' + shortType) || allCellTextLower.includes(p.type.toLowerCase())) typeIdentifiable++;
  });
  const typePct = procCount ? Math.round((typeIdentifiable / procCount) * 100) : 100;
  reChecks.push({ label: 'Processor Types Identifiable', score: typePct, detail: typeIdentifiable + '/' + procCount + ' NiFi types referenced in notebook comments/code' });

  // Check 3: Scheduling parameters
  let schedPreserved = 0;
  nifi.processors.forEach(p => {
    if (p.schedulingPeriod && p.schedulingPeriod !== '0 sec') {
      if (allCellTextLower.includes(p.schedulingPeriod.toLowerCase()) || allCellTextLower.includes('schedule') || allCellTextLower.includes('trigger')) schedPreserved++;
    } else {
      schedPreserved++;
    }
  });
  const schedPct = procCount ? Math.round((schedPreserved / procCount) * 100) : 100;
  reChecks.push({ label: 'Scheduling Parameters Preserved', score: schedPct, detail: schedPreserved + '/' + procCount + ' scheduling configs captured' });

  if (onProgress) {
    onProgress(72, 'Checking controller services and external systems...');
  }
  await new Promise(r => setTimeout(r, 0));

  // Check 4: Controller services
  const csCount = nifi.controllerServices.length;
  let csInCode = 0;
  nifi.controllerServices.forEach(cs => {
    const csName = cs.name.replace(/[^a-zA-Z0-9]/g, '_').toLowerCase();
    if (allCellTextLower.includes(csName) || allCellTextLower.includes(cs.name.toLowerCase())) csInCode++;
  });
  const csPct = csCount ? Math.round((csInCode / csCount) * 100) : 100;
  reChecks.push({ label: 'Controller Services Referenced', score: csPct, detail: csInCode + '/' + csCount + ' controller services found in notebook' });

  // Check 5: External systems
  const sysKeys = Object.keys(systems);
  let sysInCode = 0;
  sysKeys.forEach(sys => {
    if (allCellTextLower.includes(sys.toLowerCase())) sysInCode++;
  });
  const sysPct = sysKeys.length ? Math.round((sysInCode / sysKeys.length) * 100) : 100;
  reChecks.push({ label: 'External Systems Connected', score: sysPct, detail: sysInCode + '/' + sysKeys.length + ' external systems referenced in code' });

  // Check 6: Error handling
  const autoTerminated = [];
  nifi.processors.forEach(p => {
    if (p.autoTerminatedRelationships && p.autoTerminatedRelationships.length > 0) {
      autoTerminated.push({ name: p.name, rels: p.autoTerminatedRelationships });
    }
  });
  const errorHandled = allCellTextLower.includes('try:') || allCellTextLower.includes('except') || allCellTextLower.includes('.option("failonerror"');
  const errorPct = errorHandled ? 70 : (autoTerminated.length > 0 ? 30 : 50);
  reChecks.push({ label: 'Error Handling Coverage', score: errorPct, detail: errorHandled ? 'Try/except or error options found in notebook' : 'No explicit error handling — ' + autoTerminated.length + ' processors have auto-terminated failure relationships' });

  reScore = Math.round(reChecks.reduce((s, c) => s + c.score, 0) / reChecks.length);

  return { reScore, reChecks };
}
