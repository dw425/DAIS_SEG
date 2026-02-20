/**
 * reporters/migration-report.js — Migration report generator
 *
 * Extracted from index.html lines 5966-6001.
 * Produces a structured migration report with coverage by role,
 * by process group, gaps, recommendations, and effort estimate.
 *
 * @module reporters/migration-report
 */

import { ROLE_TIER_ORDER } from '../constants/nifi-role-map.js';

/**
 * Generate a structured migration report from assessment mappings and NiFi flow.
 *
 * @param {Array}  mappings - Assessment mappings array
 * @param {object} nifi     - Parsed NiFi flow object
 * @returns {object} Report with summary, byRole, byGroup, gaps, recommendations, effort
 */
export function generateMigrationReport(mappings, nifi) {
  const total = mappings.length, mapped = mappings.filter(m => m.mapped).length;
  const byRole = {};
  ROLE_TIER_ORDER.forEach(r => { byRole[r] = { total: 0, mapped: 0, unmapped: 0, procs: [] }; });
  mappings.forEach(m => {
    const r = byRole[m.role] || byRole.process;
    r.total++; if (m.mapped) r.mapped++; else r.unmapped++;
    r.procs.push(m);
  });
  const byGroup = {};
  mappings.forEach(m => {
    if (!byGroup[m.group]) byGroup[m.group] = { total: 0, mapped: 0, unmapped: 0, procs: [] };
    byGroup[m.group].total++; if (m.mapped) byGroup[m.group].mapped++; else byGroup[m.group].unmapped++;
    byGroup[m.group].procs.push(m);
  });
  const gaps = mappings.filter(m => !m.mapped || m.confidence < 0.3).map(m => ({
    name: m.name, type: m.type, group: m.group, role: m.role,
    reason: m.gapReason || `Low confidence mapping (${Math.round(m.confidence * 100)}%)`,
    recommendation: m.type.match(/^(Listen|Handle)/) ? 'Consider Databricks Model Serving or external API gateway'
      : m.type.match(/^Execute(Script|Stream)/) ? 'Manual translation required — review original script logic'
      : m.type.match(/^(Put|Send)(Email|TCP|Syslog)/) ? 'Use webhook notification service or Databricks workflow alerts'
      : 'Review processor documentation and implement custom PySpark logic'
  }));
  const recs = [];
  if (gaps.length > total * 0.2) recs.push('High gap rate — consider custom UDFs for unsupported processor types');
  if (mappings.some(m => m.type.match(/Listen|Handle/))) recs.push('HTTP endpoints detected — evaluate Databricks Model Serving for REST API replacement');
  if (mappings.some(m => m.type.match(/Consume.*Kafka|Subscribe/))) recs.push('Streaming sources present — use Structured Streaming with Auto Loader trigger intervals');
  if ((nifi.controllerServices || []).length) recs.push(`${nifi.controllerServices.length} controller service(s) detected — map credentials to Databricks secret scopes`);
  if (mapped > total * 0.8) recs.push('High coverage — prioritize testing the mapped processors before addressing gaps');
  recs.push('Run the generated notebook in a Databricks workspace to validate each cell');
  const coveragePct = total ? Math.round(mapped / total * 100) : 0;

  // Weighted effort: accounts for processor type complexity
  // Note: 'script' and 'custom' roles are not currently assigned by the classifier;
  // these weights exist as placeholders for future role expansion
  const EFFORT_WEIGHTS = {
    source: 2, sink: 2, transform: 1, route: 1.5, process: 3, script: 4, custom: 5
  };
  const totalWeight = mappings.reduce((sum, m) => sum + (EFFORT_WEIGHTS[m.role] || 2), 0);
  const mappedWeight = mappings.filter(m => m.mapped).reduce((sum, m) => sum + (EFFORT_WEIGHTS[m.role] || 2), 0);
  const weightedCoverage = totalWeight > 0 ? Math.round(mappedWeight / totalWeight * 100) : 0;
  const effort = weightedCoverage >= 85 ? 'Low' : weightedCoverage >= 60 ? 'Medium' : 'High';
  return {
    summary: {
      totalProcessors: total, mappedProcessors: mapped, unmappedProcessors: total - mapped, coveragePercent: coveragePct,
      totalProcessGroups: Object.keys(byGroup).length, totalConnections: (nifi.connections || []).length, controllerServices: (nifi.controllerServices || []).length
    },
    byRole, byGroup, gaps, recommendations: recs, effort
  };
}
