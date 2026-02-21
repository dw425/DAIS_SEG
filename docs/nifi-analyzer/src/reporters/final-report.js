/**
 * reporters/final-report.js — Final analysis report builder
 *
 * Extracted from index.html lines 8082-8205.
 * Builds a comprehensive JSON report from all STATE data, and provides
 * a rendering function that shows a summary and JSON preview.
 *
 * Enhanced with:
 * - 6.1 Interactive JSON Explorer
 * - 6.2 Multi-Format Export (HTML, Markdown, CSV)
 * - 6.3 Executive Summary Section
 * - 6.4 Statistical Dashboard
 * - 6.5 Advanced Property Masking
 *
 * @module reporters/final-report
 */

import { classifyNiFiProcessor } from '../mappers/processor-classifier.js';
import { renderJSONExplorer } from '../ui/json-explorer.js';

// ── 6.5 Advanced Property Masking ──

/** Patterns for sensitive property detection */
const SENSITIVE_KEY_PATTERNS = [
  /password/i, /secret/i, /token/i, /\bkey\b/i, /credential/i,
  /api[_.]?key/i, /private[_.]?key/i, /passphrase/i, /auth/i,
  /\bpin\b/i, /certificate/i, /signing/i,
];

const SENSITIVE_VALUE_PATTERNS = [
  { pattern: /(jdbc:\w+:\/\/)([^:]+):([^@]+)@/g, replace: '$1$2:***@' },
  { pattern: /AKIA[0-9A-Z]{16}/g, replace: '***AWS_KEY***' },
  { pattern: /(password|pwd|pass)\s*=\s*[^;}&\s]+/gi, replace: '$1=***' },
  { pattern: /Bearer\s+[A-Za-z0-9\-._~+\/]+=*/g, replace: 'Bearer ***' },
  { pattern: /-----BEGIN\s+[\w\s]+-----[\s\S]*?-----END\s+[\w\s]+-----/g, replace: '***CERTIFICATE***' },
  { pattern: /[A-Za-z0-9+/]{40,}={0,2}/g, replace: '***BASE64***' },
];

/** URL masking: show host only */
const URL_PATTERN = /^(https?:\/\/|s3[an]?:\/\/|abfss?:\/\/|gs:\/\/)([^\/\s?#]+)(.*)/i;

/** Counter for masked properties */
let _maskedCount = 0;

/**
 * Reset masked property counter. Called before building report.
 */
function resetMaskedCount() { _maskedCount = 0; }

/**
 * Get the current masked property count.
 * @returns {number}
 */
export function getMaskedCount() { return _maskedCount; }

/**
 * Mask sensitive property values with advanced pattern-based detection.
 * @param {string} key - Property name
 * @param {string} value - Property value
 * @returns {string} Masked or original value
 */
function maskSensitiveValue(key, value) {
  // Check key-based patterns
  for (const pat of SENSITIVE_KEY_PATTERNS) {
    if (pat.test(key)) {
      _maskedCount++;
      return '***';
    }
  }

  if (typeof value === 'string') {
    let v = value;
    let wasMasked = false;

    // Apply value-based patterns
    for (const { pattern, replace } of SENSITIVE_VALUE_PATTERNS) {
      const before = v;
      // Reset regex lastIndex for global patterns
      pattern.lastIndex = 0;
      v = v.replace(pattern, replace);
      if (v !== before) wasMasked = true;
    }

    // URL masking: show host only
    const urlMatch = v.match(URL_PATTERN);
    if (urlMatch && !wasMasked) {
      v = urlMatch[1] + urlMatch[2] + '/***';
      wasMasked = true;
    }

    if (wasMasked) _maskedCount++;
    return v;
  }
  return value;
}

/**
 * Build the final analysis report JSON from application state.
 *
 * @param {object} STATE - The application state object
 * @returns {object} Full report JSON
 */
export function buildFinalReportJSON(STATE) {
  resetMaskedCount();
  const nifi = STATE.parsed ? STATE.parsed._nifi : null;
  const report = {
    meta: { generated: new Date().toISOString(), tool: 'NiFi Flow Analyzer', version: '2.0.1', flow_name: STATE.parsed ? STATE.parsed.source_name : 'unknown' },
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
      properties: Object.fromEntries(Object.entries(p.properties).map(([k, v]) => [k, maskSensitiveValue(k, v)]))
    })) : [],
    connections: nifi ? nifi.connections.map(c => ({
      source: c.sourceName, destination: c.destinationName,
      relationships: c.relationships, backPressure: c.backPressure
    })) : [],
    controller_services: nifi ? nifi.controllerServices.map(cs => ({
      name: cs.name, type: cs.type,
      properties: Object.fromEntries(Object.entries(cs.properties).map(([k, v]) => [k, maskSensitiveValue(k, v)]))
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
    } : null,
    sensitive_properties_masked: 0, // placeholder, updated below
  };
  report.sensitive_properties_masked = _maskedCount;
  return report;
}

/**
 * Compute flow complexity metrics for the statistical dashboard (6.4).
 * @param {object} nifi - Parsed NiFi flow object
 * @returns {object}
 */
function computeFlowStats(nifi) {
  if (!nifi) return { processorCount: 0, connectionCount: 0, connectionDensity: 0, maxDepth: 0, roleDistribution: {}, externalSystems: 0 };
  const procs = nifi.processors || [];
  const conns = nifi.connections || [];
  const connectionDensity = procs.length > 0 ? (conns.length / procs.length).toFixed(2) : 0;

  // Max nesting depth
  const maxDepth = (nifi.processGroups || []).reduce((max, pg) => {
    const depth = (pg.path || pg.name || '').split('/').length;
    return Math.max(max, depth);
  }, 1);

  // Role distribution
  const roleDistribution = {};
  procs.forEach(p => {
    const role = classifyNiFiProcessor(p.type) || 'unknown';
    roleDistribution[role] = (roleDistribution[role] || 0) + 1;
  });

  // External system count
  const externalSystems = (nifi.clouderaTools || []).length;

  return { processorCount: procs.length, connectionCount: conns.length, connectionDensity, maxDepth, roleDistribution, externalSystems };
}

/**
 * Render the role distribution bar chart as HTML.
 * @param {object} roleDistribution
 * @param {number} total
 * @param {Function} escapeHTML
 * @returns {string}
 */
function renderRoleDistributionChart(roleDistribution, total, escapeHTML) {
  const ROLE_COLORS = {
    source: '#3B82F6',
    transform: '#A855F7',
    route: '#EAB308',
    process: '#6366F1',
    sink: '#21C354',
    unknown: '#8b949e',
  };
  const entries = Object.entries(roleDistribution).sort((a, b) => b[1] - a[1]);
  let h = '<div style="margin:12px 0">';
  entries.forEach(([role, count]) => {
    const pct = total > 0 ? Math.round((count / total) * 100) : 0;
    const color = ROLE_COLORS[role] || '#8b949e';
    h += '<div style="display:flex;align-items:center;gap:8px;margin:4px 0">';
    h += `<span style="min-width:80px;font-size:0.82rem;text-transform:capitalize;color:var(--text2)">${escapeHTML(role)}</span>`;
    h += `<div style="flex:1;height:20px;background:var(--surface2);border-radius:4px;overflow:hidden">`;
    h += `<div style="height:100%;width:${pct}%;background:${color};border-radius:4px;transition:width 0.4s"></div></div>`;
    h += `<span style="min-width:60px;font-size:0.82rem;font-weight:600;text-align:right">${count} (${pct}%)</span>`;
    h += '</div>';
  });
  h += '</div>';
  return h;
}

/**
 * Generate the executive summary section (6.3).
 * @param {object} report
 * @param {object} stats
 * @param {Function} escapeHTML
 * @returns {string}
 */
function renderExecutiveSummary(report, stats, escapeHTML) {
  const r = report;
  const readiness = r.assessment?.readiness_score || 0;
  const procCount = r.flow_summary.processor_count;
  const connCount = r.flow_summary.connection_count;
  const extCount = r.flow_summary.external_system_count;

  // Determine complexity level
  let complexity = 'Low';
  let complexityColor = 'var(--green)';
  if (procCount > 30 || extCount > 5 || parseFloat(stats.connectionDensity) > 3) {
    complexity = 'High';
    complexityColor = 'var(--red)';
  } else if (procCount > 10 || extCount > 2 || parseFloat(stats.connectionDensity) > 2) {
    complexity = 'Medium';
    complexityColor = 'var(--amber)';
  }

  // Effort estimate
  const autoCount = r.assessment?.auto_convertible || 0;
  const manualCount = r.assessment?.manual_conversion || 0;
  const unsupported = r.assessment?.unsupported || 0;
  const effortDays = Math.max(1, Math.ceil(autoCount * 0.25 + manualCount * 2 + unsupported * 4));
  const effortWeeks = (effortDays / 5).toFixed(1);

  // Recommendation
  let recommendation, recClass;
  if (readiness >= 80 && unsupported === 0) {
    recommendation = 'Proceed';
    recClass = 'exec-proceed';
  } else if (readiness >= 50 || unsupported <= 2) {
    recommendation = 'Caution';
    recClass = 'exec-caution';
  } else {
    recommendation = 'Needs Planning';
    recClass = 'exec-planning';
  }

  // Key risks
  const risks = [];
  if (unsupported > 0) risks.push(`${unsupported} unsupported processor(s) require custom implementation`);
  if (extCount > 3) risks.push(`${extCount} external system integrations increase migration complexity`);
  if (stats.maxDepth > 3) risks.push(`Deeply nested process groups (depth ${stats.maxDepth}) may need refactoring`);
  if (manualCount > autoCount) risks.push('More manual than auto-convertible processors');
  if (risks.length === 0) risks.push('No significant risks identified');

  let h = '<div class="exec-summary">';
  h += '<h4>Executive Summary</h4>';
  h += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-bottom:16px">';

  const summaryCards = [
    { label: 'Flow Complexity', value: complexity, color: complexityColor },
    { label: 'Readiness Score', value: readiness + '%', color: readiness >= 80 ? 'var(--green)' : readiness >= 50 ? 'var(--amber)' : 'var(--red)' },
    { label: 'Effort Estimate', value: effortWeeks + ' wks', color: 'var(--primary)' },
    { label: 'Recommendation', value: recommendation, color: recClass === 'exec-proceed' ? 'var(--green)' : recClass === 'exec-caution' ? 'var(--amber)' : 'var(--red)' },
  ];
  summaryCards.forEach(c => {
    h += `<div style="text-align:center;padding:12px;background:var(--surface);border:1px solid var(--border);border-radius:8px">`;
    h += `<div style="font-size:1.4rem;font-weight:800;color:${c.color}">${escapeHTML(String(c.value))}</div>`;
    h += `<div style="font-size:0.78rem;color:var(--text2)">${escapeHTML(c.label)}</div></div>`;
  });
  h += '</div>';

  // Recommendation badge
  h += `<div style="text-align:center;margin:12px 0"><span class="exec-recommendation ${recClass}">${escapeHTML(recommendation)}</span></div>`;

  // Key risks
  h += '<h4 style="margin:16px 0 8px;font-size:0.9rem">Key Risks</h4>';
  h += '<ul style="margin:0;padding-left:20px;font-size:0.85rem">';
  risks.forEach(r => { h += `<li style="margin:4px 0;color:var(--text2)">${escapeHTML(r)}</li>`; });
  h += '</ul>';

  // One-liner summary
  h += `<div style="margin-top:12px;padding:10px;background:var(--surface2);border-radius:6px;font-size:0.85rem;color:var(--text)">`;
  h += `This NiFi flow has <strong>${procCount}</strong> processors, <strong>${connCount}</strong> connections, `;
  h += `and <strong>${extCount}</strong> external system dependencies. `;
  h += `With a readiness score of <strong>${readiness}%</strong>, estimated migration effort is <strong>${effortWeeks} weeks</strong> `;
  h += `(${effortDays} person-days).`;
  h += '</div>';

  h += '</div>';
  return h;
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
  const nifi = STATE.parsed ? STATE.parsed._nifi : null;
  const stats = computeFlowStats(nifi);

  let h = '';

  // ── 6.3 Executive Summary Section ──
  h += renderExecutiveSummary(report, stats, escapeHTML);

  // ── 6.5 Sensitive property masking notice ──
  if (report.sensitive_properties_masked > 0) {
    h += `<div class="alert alert-warn" style="margin:12px 0"><strong>${report.sensitive_properties_masked}</strong> sensitive properties masked in this report.</div>`;
  }

  h += '<hr class="divider">';
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

  // ── 6.4 Statistical Dashboard ──
  h += '<hr class="divider"><h3>Statistical Dashboard</h3>';
  h += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin-bottom:16px">';
  const dashMetrics = [
    { label: 'Processor Count', value: stats.processorCount, color: 'var(--primary)' },
    { label: 'Conn. Density', value: stats.connectionDensity, color: 'var(--text)' },
    { label: 'Max Depth', value: stats.maxDepth, color: 'var(--amber)' },
    { label: 'Ext. Systems', value: stats.externalSystems, color: 'var(--red)' },
  ];
  dashMetrics.forEach(m => {
    h += `<div style="text-align:center;padding:12px;background:var(--surface);border:1px solid var(--border);border-radius:8px">`;
    h += `<div style="font-size:1.6rem;font-weight:800;color:${m.color}">${m.value}</div>`;
    h += `<div style="font-size:0.78rem;color:var(--text2)">${escapeHTML(m.label)}</div></div>`;
  });
  h += '</div>';

  // Role distribution bar chart
  h += '<h4 style="margin:12px 0 4px">Processor Role Distribution</h4>';
  h += renderRoleDistributionChart(stats.roleDistribution, stats.processorCount, escapeHTML);

  // ── 6.1 Interactive JSON Explorer ──
  h += '<hr class="divider"><h3>Report Data Explorer</h3>';
  h += '<p style="font-size:0.8rem;color:var(--text2);margin:0 0 8px">Click any key or value to copy its JSON path. Use search to filter by key/value.</p>';
  h += renderJSONExplorer(r, 'finalReportExplorer');

  // ── 6.2 Multi-Format Export ──
  h += '<hr class="divider"><div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center">';
  h += '<button class="btn btn-primary" id="finalReportDownloadBtn">Download Full Report (JSON)</button>';
  h += '<div class="format-selector">';
  h += '<select id="finalReportFormatSelect">';
  h += '<option value="">More formats...</option>';
  h += '<option value="html">HTML Report</option>';
  h += '<option value="markdown">Markdown Report</option>';
  h += '<option value="csv-processors">CSV (Processor List)</option>';
  h += '<option value="csv-gaps">CSV (Gap List)</option>';
  h += '</select>';
  h += '</div>';
  h += '<button class="btn btn-secondary" id="finalReportExportBtn">Export</button>';
  h += '</div>';

  return { html: h, report };
}
