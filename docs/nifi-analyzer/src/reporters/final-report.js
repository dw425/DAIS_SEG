/**
 * reporters/final-report.js — Final analysis report builder
 *
 * Extracted from index.html lines 8082-8205.
 * Builds a comprehensive JSON report from all STATE data, and provides
 * a rendering function that shows a summary and JSON preview.
 *
 * @module reporters/final-report
 */

import { classifyNiFiProcessor } from '../mappers/processor-classifier.js';

/**
 * Build the final analysis report JSON from application state.
 *
 * @param {object} STATE - The application state object
 * @returns {object} Full report JSON
 */
export function buildFinalReportJSON(STATE) {
  const nifi = STATE.parsed ? STATE.parsed._nifi : null;
  const report = {
    meta: { generated: new Date().toISOString(), tool: 'NiFi Flow Analyzer', version: '1.0' },
    flow_summary: {
      source_name: STATE.parsed ? STATE.parsed.source_name : 'Unknown',
      processor_count: nifi ? nifi.processors.length : 0,
      connection_count: nifi ? nifi.connections.length : 0,
      process_group_count: nifi ? nifi.processGroups.length : 0,
      controller_service_count: nifi ? nifi.controllerServices.length : 0,
      external_system_count: nifi ? nifi.clouderaTools.length : 0
    },
    processors: nifi ? nifi.processors.map(p => ({
      name: p.name, type: p.type, group: p.group, state: p.state,
      role: classifyNiFiProcessor(p.type),
      scheduling: { strategy: p.schedulingStrategy, period: p.schedulingPeriod },
      properties: Object.fromEntries(Object.entries(p.properties).map(([k, v]) => [k, /password|secret|token/i.test(k) ? '***' : v]))
    })) : [],
    connections: nifi ? nifi.connections.map(c => ({
      source: c.sourceName, destination: c.destinationName,
      relationships: c.relationships, backPressure: c.backPressure
    })) : [],
    controller_services: nifi ? nifi.controllerServices.map(cs => ({
      name: cs.name, type: cs.type,
      properties: Object.fromEntries(Object.entries(cs.properties).map(([k, v]) => [k, /password|secret|token/i.test(k) ? '***' : v]))
    })) : [],
    assessment: STATE.assessment ? {
      readiness_score: STATE.assessment.readinessScore,
      auto_convertible: STATE.assessment.autoCount,
      manual_conversion: STATE.assessment.manualCount,
      unsupported: STATE.assessment.unsupportedCount,
      mappings: STATE.assessment.mappings ? STATE.assessment.mappings.map(m => ({
        name: m.name, nifi_type: m.nifiType || m.type, role: m.role,
        mapped: m.mapped, databricks: m.desc, confidence: m.confidence
      })) : []
    } : null,
    notebook: STATE.notebook ? { cell_count: STATE.notebook.cells.length, config: STATE.notebook.config } : null,
    migration_report: STATE.migrationReport || null,
    manifest: STATE.manifest ? {
      directories: Object.keys(STATE.manifest.directories).length,
      sql_tables: Object.keys(STATE.manifest.sqlTables).length,
      http_endpoints: STATE.manifest.httpEndpoints.length,
      kafka_topics: STATE.manifest.kafkaTopics.length,
      scripts: STATE.manifest.scripts.length,
      db_connections: STATE.manifest.dbConnections.length,
      external_systems: (STATE.manifest.clouderaTools || []).length
    } : null,
    deep_property_inventory: nifi ? {
      file_paths: Object.keys(nifi.deepPropertyInventory.filePaths || {}).length,
      urls: Object.keys(nifi.deepPropertyInventory.urls || {}).length,
      jdbc_urls: Object.keys(nifi.deepPropertyInventory.jdbcUrls || {}).length,
      nifi_el_expressions: Object.keys(nifi.deepPropertyInventory.nifiEL || {}).length,
      cron_expressions: Object.keys(nifi.deepPropertyInventory.cronExprs || {}).length,
      credential_references: Object.keys(nifi.deepPropertyInventory.credentialRefs || {}).length
    } : null
  };
  return report;
}

/**
 * Generate the final report and return HTML + the report object.
 * This is the pure-logic portion; the caller handles DOM updates.
 *
 * @param {object} STATE - Application state
 * @param {Function} metricsHTML - Helper to render metric cards
 * @param {Function} escapeHTML  - HTML escaper
 * @returns {{html:string, report:object}}
 */
export function generateFinalReport(STATE, metricsHTML, escapeHTML) {
  const report = buildFinalReportJSON(STATE);
  const r = report;

  let h = '<hr class="divider">';
  h += '<h3>Report Summary</h3>';
  h += metricsHTML([
    { label: 'Processors', value: r.flow_summary.processor_count },
    { label: 'Connections', value: r.flow_summary.connection_count },
    { label: 'External Systems', value: r.flow_summary.external_system_count },
    { label: 'Readiness', value: r.assessment ? r.assessment.readiness_score + '%' : 'N/A' }
  ]);

  if (r.assessment) {
    h += '<h3>Assessment Overview</h3>';
    h += metricsHTML([
      { label: 'Auto-Convert', value: r.assessment.auto_convertible, color: 'var(--green)' },
      { label: 'Manual', value: r.assessment.manual_conversion, color: 'var(--amber)' },
      { label: 'Unsupported', value: r.assessment.unsupported, color: 'var(--red)' }
    ]);
  }

  // JSON Preview
  const preview = JSON.stringify(r, null, 2).substring(0, 5000);
  h += '<hr class="divider"><h3>Report Preview</h3>';
  h += '<pre style="max-height:400px;overflow:auto;font-size:0.75rem">' + escapeHTML(preview) + (JSON.stringify(r).length > 5000 ? '\n... (truncated)' : '') + '</pre>';

  h += '<hr class="divider"><div style="display:flex;gap:8px">';
  h += '<button class="btn btn-primary" onclick="downloadFinalReport()">Download Full Report (JSON)</button>';
  h += '</div>';

  return { html: h, report };
}
