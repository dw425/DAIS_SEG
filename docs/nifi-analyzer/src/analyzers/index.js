/**
 * analyzers/index.js — Analysis engine orchestrator
 *
 * Imports all analysis sub-modules and runs them against a parsed NiFi flow.
 * This is the single entry point for Phase 4 (Analysis).
 *
 * @module analyzers
 */

import { buildDependencyGraph } from './dependency-graph.js';
import { detectExternalSystems } from './external-systems.js';
import { detectCyclesSCC } from './cycle-detection.js';
import { buildResourceManifest } from './resource-manifest.js';
import { renderManifestHTML } from './manifest-renderer.js';
import { assembleBlueprint, genStats } from './blueprint-assembler.js';
import { analyzeFlowGraph } from './flow-graph-analyzer.js';
import { scanSecurity } from './security-scanner.js';
import { detectPHIFields, _PHI_PATTERNS } from './phi-detector.js';
import { analyzeCRON, analyzeTimers } from './scheduling-analyzer.js';
import { scanProperties } from './property-scanner.js';

/**
 * Run the full analysis engine against a parsed NiFi flow.
 *
 * Executes all analysis sub-modules and returns a consolidated result object.
 *
 * @param {object} parsed — parsed NiFi flow object with:
 *   - processors {Array}
 *   - connections {Array}
 *   - controllerServices {Array}
 *   - processGroups {Array}
 *   - deepPropertyInventory {object} (optional, will be built if missing)
 *   - tables {Array} (optional, for blueprint assembly)
 * @returns {object} — consolidated analysis results
 */
export function runAnalysisEngine(parsed) {
  // Build deep property inventory if not already present
  if (!parsed.deepPropertyInventory) {
    parsed.deepPropertyInventory = scanProperties(parsed.processors);
  }

  // 1. Dependency graph
  const depGraph = buildDependencyGraph(parsed);

  // 2. External systems
  const externalSystems = detectExternalSystems(parsed);

  // 3. Cycle detection via Tarjan's SCC on the downstream adjacency map
  const cycles = detectCyclesSCC(depGraph.downstream);

  // 4. Resource manifest
  const manifest = buildResourceManifest(parsed);

  // 5. Flow graph structural analysis
  const flowGraph = analyzeFlowGraph(parsed.processors, parsed.connections);

  // 6. Security scan
  const securityFindings = scanSecurity(parsed.processors || []);

  // 7. PHI detection across all processors
  const phiResults = [];
  (parsed.processors || []).forEach(p => {
    const fields = detectPHIFields(p.properties);
    if (fields.length > 0) {
      phiResults.push({ processor: p.name, type: p.type, group: p.group, fields });
    }
  });

  // 8. Scheduling analysis
  const scheduling = analyzeTimers(parsed.processors);

  // 9. Blueprint assembly (if DDL tables are available)
  let blueprint = null;
  if (parsed.tables && parsed.tables.length > 0) {
    blueprint = assembleBlueprint(parsed);
  }

  return {
    dependencyGraph: depGraph,
    externalSystems,
    cycles,
    manifest,
    flowGraph,
    securityFindings,
    phiResults,
    scheduling,
    blueprint,
    deepPropertyInventory: parsed.deepPropertyInventory,
  };
}

// Re-export all sub-modules for direct access
export {
  buildDependencyGraph,
  detectExternalSystems,
  detectCyclesSCC,
  buildResourceManifest,
  renderManifestHTML,
  assembleBlueprint,
  genStats,
  analyzeFlowGraph,
  scanSecurity,
  detectPHIFields,
  _PHI_PATTERNS,
  analyzeCRON,
  analyzeTimers,
  scanProperties,
};
