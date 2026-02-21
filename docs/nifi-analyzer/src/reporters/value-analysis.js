/**
 * reporters/value-analysis.js — Migration value analysis
 *
 * Extracted from index.html lines 8775-9092.
 * Produces a comprehensive value analysis covering:
 *   1. What the workflow does
 *   2. How to build it better in Databricks
 *   3. Steps that aren't needed (with reasoning — 8.1)
 *   4. Quantified complexity reduction (with formula viz — 8.2)
 *   5. Migration ROI summary (with ROI calculator — 8.3)
 *   6. Implementation Roadmap (8.4)
 *   7. Capability Discovery Guide (8.5)
 *
 * @module reporters/value-analysis
 */

import { DROPPABLE_PROCESSORS } from '../constants/droppable-processors.js';
import { DBX_BETTER_APPROACH } from '../constants/dbx-better-approach.js';
import { classifyNiFiProcessor } from '../mappers/processor-classifier.js';
import { detectExternalSystems } from '../analyzers/external-systems.js';

/** 8.1 — Detailed reasons why each droppable processor is not needed */
const DROPPABLE_REPLACEMENTS = {
  MergeContent: 'Delta Lake Auto Optimize + Auto Compaction handle small file merging automatically.',
  MergeRecord: 'Spark DataFrames handle records as partitions -- no need for manual record merging.',
  CompressContent: 'Delta Lake uses zstd/snappy compression by default on all writes.',
  UnpackContent: 'Spark reads compressed files (gzip, snappy, zstd) natively.',
  SplitText: 'spark.read.text() reads entire files; use .split() or regex for row splitting.',
  SplitJson: 'spark.read.json() reads entire JSON files as DataFrames.',
  SplitXml: 'spark-xml library reads XML documents directly into DataFrames.',
  SplitContent: 'Spark processes data as partitions -- no manual content splitting needed.',
  SplitAvro: 'spark.read.format("avro") reads Avro files natively as DataFrames.',
  SplitRecord: 'Spark operates on DataFrames at partition level -- individual record splitting is unnecessary.',
  UpdateAttribute: '.withColumn() and .withColumnRenamed() handle all attribute/column transformations.',
  RouteOnAttribute: '.filter() or .when()/.otherwise() handle attribute-based routing.',
  LogAttribute: 'Use display() in notebooks, or logging module; Spark UI shows all execution details.',
  LogMessage: 'Use print() or Python logging module in notebooks.',
  Wait: 'Databricks Workflows task dependencies replace in-flow waits with proper DAG orchestration.',
  Notify: 'Databricks Workflows task dependencies handle cross-task notifications.',
  DetectDuplicate: 'dropDuplicates() is built into DataFrame API; Delta MERGE handles upserts.',
  ControlRate: 'Structured Streaming handles backpressure natively; no manual rate control needed.',
  DistributeLoad: 'Spark distributes data across executors via partitioning -- automatic load balancing.',
  ValidateRecord: 'DLT Expectations (expect, expect_or_drop, expect_or_fail) provide declarative quality rules.',
  RetryFlowFile: 'Databricks Workflows retry policies handle job-level and task-level retries.',
  MonitorActivity: 'Spark UI, Databricks Workflows monitoring, and Ganglia provide comprehensive monitoring.',
  DebugFlow: 'Notebook debugging, display(), and Spark UI replace dedicated debug processors.',
  CountText: 'df.count() provides native row counting in Spark.',
  AttributesToJSON: 'to_json() function in PySpark handles struct-to-JSON conversion.',
  GenerateFlowFile: 'spark.range() or spark.createDataFrame() generate test data.',
  EnforceOrder: 'orderBy() provides native sorting in Spark.',
  Funnel: 'DataFrame union() merges multiple DataFrames -- NiFi funnels have no equivalent needed.',
  InputPort: 'Databricks Jobs pass parameters between tasks -- NiFi ports are not needed.',
  OutputPort: 'Databricks Jobs pass parameters between tasks -- NiFi ports are not needed.',
};

/**
 * Run the full value analysis and return structured results + HTML.
 *
 * @param {object} opts
 * @param {object} opts.nifi        - Parsed NiFi flow object
 * @param {object} opts.notebook    - Generated notebook
 * @param {Function} opts.escapeHTML - HTML escaper
 * @returns {{html:string, valueAnalysis:object}}
 */
export function runValueAnalysis({ nifi, notebook, escapeHTML }) {
  const procs = nifi.processors || [];
  const conns = nifi.connections || [];

  let h = '';

  // ════════════════════════════════════════════
  // 1. WHAT THIS WORKFLOW DOES
  // ════════════════════════════════════════════
  h += '<h3 style="margin:0 0 12px;color:var(--primary)">1. What This Workflow Does</h3>';

  const sources = procs.filter(p => classifyNiFiProcessor(p.type) === 'source');
  const sinks = procs.filter(p => classifyNiFiProcessor(p.type) === 'sink');
  const transforms = procs.filter(p => classifyNiFiProcessor(p.type) === 'transform');
  const routes = procs.filter(p => classifyNiFiProcessor(p.type) === 'route');
  const processes = procs.filter(p => classifyNiFiProcessor(p.type) === 'process');

  const srcTypes = [...new Set(sources.map(s => s.type))];
  const sinkTypes = [...new Set(sinks.map(s => s.type))];
  const extSystems = detectExternalSystems(nifi);
  const extNames = Object.values(extSystems).map(s => s.name);

  let summary = '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:16px;margin-bottom:16px">';
  summary += '<p style="font-size:0.95rem;line-height:1.6;margin:0">';
  summary += `This NiFi flow consists of <strong>${procs.length} processors</strong> organized into <strong>${nifi.processGroups.length} process group(s)</strong>. `;
  summary += `It ingests data from <strong>${sources.length} source(s)</strong> (${srcTypes.join(', ') || 'none identified'}), `;
  summary += `applies <strong>${transforms.length} transformation(s)</strong> and <strong>${routes.length} routing decision(s)</strong>, `;
  summary += `then delivers to <strong>${sinks.length} destination(s)</strong> (${sinkTypes.join(', ') || 'none identified'}).`;
  if (extNames.length) summary += ` External systems involved: <strong>${extNames.join(', ')}</strong>.`;
  summary += '</p></div>';

  // Flow path summary
  summary += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px;margin-bottom:16px">';
  const roleCounts = [
    { role: 'Sources', count: sources.length, color: '#3B82F6', icon: '&#9654;' },
    { role: 'Transforms', count: transforms.length, color: '#A855F7', icon: '&#9881;' },
    { role: 'Routing', count: routes.length, color: '#EAB308', icon: '&#8644;' },
    { role: 'Processing', count: processes.length, color: '#6366F1', icon: '&#9881;' },
    { role: 'Sinks', count: sinks.length, color: '#21C354', icon: '&#9632;' }
  ];
  roleCounts.forEach(r => {
    summary += `<div style="background:${r.color}11;border:1px solid ${r.color}44;border-radius:8px;padding:12px;text-align:center">`;
    summary += `<div style="font-size:1.5rem">${r.icon}</div>`;
    summary += `<div style="font-size:1.8rem;font-weight:700;color:${r.color}">${r.count}</div>`;
    summary += `<div style="font-size:0.8rem;color:var(--text2)">${r.role}</div></div>`;
  });
  summary += '</div>';

  // Integration points
  if (Object.keys(extSystems).length) {
    summary += '<h4 style="margin:12px 0 6px">Integration Points</h4>';
    summary += '<div style="display:flex;gap:8px;flex-wrap:wrap">';
    Object.values(extSystems).forEach(sys => {
      const dir = sys.processors.map(p => p.direction).includes('WRITE') && sys.processors.map(p => p.direction).includes('READ') ? '&#8644;' : sys.processors.map(p => p.direction).includes('WRITE') ? '&#8594;' : '&#8592;';
      summary += `<span style="display:inline-flex;align-items:center;gap:4px;padding:4px 10px;background:var(--surface);border:1px solid var(--border);border-radius:6px;font-size:0.82rem">${dir} <strong>${escapeHTML(sys.name)}</strong> <span style="color:var(--text2)">(${sys.processors.length})</span></span>`;
    });
    summary += '</div>';
  }

  // Data formats
  const formats = new Set();
  procs.forEach(p => {
    if (/JSON/i.test(p.type)) formats.add('JSON');
    if (/XML/i.test(p.type)) formats.add('XML');
    if (/Avro/i.test(p.type)) formats.add('Avro');
    if (/CSV|Delimited/i.test(p.type)) formats.add('CSV');
    if (/Parquet/i.test(p.type)) formats.add('Parquet');
    if (/ORC/i.test(p.type)) formats.add('ORC');
    const props = Object.values(p.properties || {}).join(' ');
    if (/json/i.test(props)) formats.add('JSON');
    if (/xml/i.test(props)) formats.add('XML');
    if (/csv|delimited/i.test(props)) formats.add('CSV');
    if (/avro/i.test(props)) formats.add('Avro');
    if (/parquet/i.test(props)) formats.add('Parquet');
  });
  if (formats.size) {
    summary += `<p style="margin:12px 0 0;font-size:0.85rem;color:var(--text2)">Data formats detected: <strong>${[...formats].join(', ')}</strong></p>`;
  }
  h += summary;

  // ════════════════════════════════════════════
  // 2. HOW TO BUILD IT BETTER IN DATABRICKS
  // ════════════════════════════════════════════
  h += '<h3 style="margin:24px 0 12px;color:var(--primary)">2. How to Build It Better in Databricks</h3>';

  const recommendations = [];

  if (procs.some(p => /^(GetFile|ListFile|TailFile|FetchFile)$/i.test(p.type))) {
    recommendations.push({ ...DBX_BETTER_APPROACH.file_polling, category: 'Ingestion', priority: 1 });
  }
  if (procs.some(p => p.schedulingStrategy === 'CRON_DRIVEN' || /TIMER_DRIVEN/i.test(p.schedulingStrategy))) {
    recommendations.push({ ...DBX_BETTER_APPROACH.batch_loop, category: 'Processing', priority: 2 });
  }
  if (procs.some(p => /Schema|Avro|Record/i.test(p.type))) {
    recommendations.push({ ...DBX_BETTER_APPROACH.schema_mgmt, category: 'Governance', priority: 2 });
  }
  if (procs.some(p => /Validate|RouteOn/i.test(p.type))) {
    recommendations.push({ ...DBX_BETTER_APPROACH.data_quality, category: 'Quality', priority: 1 });
  }
  if (procs.some(p => /DetectDuplicate/i.test(p.type))) {
    recommendations.push({ ...DBX_BETTER_APPROACH.dedup, category: 'Deduplication', priority: 2 });
  }
  if (procs.some(p => /MergeContent|MergeRecord/i.test(p.type))) {
    recommendations.push({ ...DBX_BETTER_APPROACH.merge_small, category: 'Optimization', priority: 2 });
  }
  if (procs.length > 5) {
    recommendations.push({ ...DBX_BETTER_APPROACH.scheduling, category: 'Orchestration', priority: 3 });
  }
  if (procs.some(p => /Cache|Lookup/i.test(p.type))) {
    recommendations.push({ ...DBX_BETTER_APPROACH.caching, category: 'Enrichment', priority: 3 });
  }
  if (nifi.controllerServices.some(cs => /SSL|Kerberos|LDAP|Credential/i.test(cs.type))) {
    recommendations.push({ ...DBX_BETTER_APPROACH.security, category: 'Security', priority: 1 });
  }
  recommendations.push({ ...DBX_BETTER_APPROACH.monitoring, category: 'Monitoring', priority: 3 });

  recommendations.sort((a, b) => a.priority - b.priority);

  h += '<div style="display:grid;gap:12px">';
  recommendations.forEach((rec) => {
    const prioColor = rec.priority === 1 ? 'var(--green)' : rec.priority === 2 ? 'var(--primary)' : 'var(--text2)';
    const prioLabel = rec.priority === 1 ? 'HIGH' : rec.priority === 2 ? 'MEDIUM' : 'LOW';
    h += `<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:14px">`;
    h += `<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">`;
    h += `<strong style="font-size:0.95rem">${escapeHTML(rec.category)}</strong>`;
    h += `<span style="font-size:0.72rem;padding:2px 8px;border-radius:4px;background:${prioColor}22;color:${prioColor};font-weight:700">${prioLabel} PRIORITY</span>`;
    h += '</div>';
    h += `<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;font-size:0.85rem">`;
    h += `<div><div style="color:var(--text2);font-size:0.75rem;margin-bottom:4px">CURRENT (NiFi)</div>${escapeHTML(rec.nifi)}</div>`;
    h += `<div><div style="color:var(--green);font-size:0.75rem;margin-bottom:4px">RECOMMENDED (Databricks)</div><strong>${escapeHTML(rec.dbx)}</strong></div>`;
    h += '</div>';
    h += `<div style="margin-top:8px;font-size:0.82rem;color:var(--text2)">&#9889; ${escapeHTML(rec.benefit)}</div>`;
    h += '</div>';
  });
  h += '</div>';

  // ════════════════════════════════════════════
  // 3. STEPS THAT AREN'T NEEDED (8.1 — with reasoning)
  // ════════════════════════════════════════════
  h += '<h3 style="margin:24px 0 12px;color:var(--primary)">3. Steps That Aren\'t Needed in Databricks</h3>';

  const droppable = [];
  procs.forEach(p => {
    const drop = DROPPABLE_PROCESSORS[p.type];
    if (drop) {
      droppable.push({
        name: p.name, type: p.type, group: p.group, ...drop,
        replacement: DROPPABLE_REPLACEMENTS[p.type] || 'Handled natively by Databricks/Spark.',
      });
    }
  });

  if (droppable.length === 0) {
    h += '<div class="val-ok">All processors in this flow serve essential functions in the Databricks migration.</div>';
  } else {
    const savingsMap = { high: 3, medium: 2, low: 1 };
    const droppableSorted = droppable.sort((a, b) => (savingsMap[b.savings] || 0) - (savingsMap[a.savings] || 0));

    h += `<div class="alert alert-success" style="margin-bottom:12px"><strong>${droppable.length}</strong> of ${procs.length} processors (${Math.round(droppable.length / procs.length * 100)}%) can be eliminated in Databricks</div>`;

    // 8.1 — Enhanced table with WHY column and replacement feature
    h += '<div class="table-scroll"><table style="font-size:0.82rem"><thead><tr><th>Processor</th><th>Type</th><th>Why Not Needed</th><th>Databricks Replacement</th><th>Savings</th><th>Risk</th></tr></thead><tbody>';
    droppableSorted.forEach(d => {
      const savColor = d.savings === 'high' ? 'var(--green)' : d.savings === 'medium' ? 'var(--primary)' : 'var(--text2)';
      const riskColor = d.risk === 'high' ? 'var(--red)' : d.risk === 'medium' ? 'var(--amber)' : d.risk === 'low' ? 'var(--text2)' : 'var(--green)';
      h += `<tr><td><strong>${escapeHTML(d.name)}</strong></td>`;
      h += `<td><code style="font-size:0.75rem">${escapeHTML(d.type)}</code></td>`;
      h += `<td style="font-size:0.8rem">${escapeHTML(d.reason)}</td>`;
      h += `<td style="font-size:0.8rem;color:var(--green)">${escapeHTML(d.replacement)}</td>`;
      h += `<td><span style="color:${savColor};font-weight:600">${d.savings.toUpperCase()}</span></td>`;
      h += `<td><span style="color:${riskColor};font-weight:600">${(d.risk || 'none').toUpperCase()}</span></td></tr>`;
    });
    h += '</tbody></table></div>';
  }

  // ════════════════════════════════════════════
  // 4. QUANTIFIED COMPLEXITY REDUCTION (8.2 — formula viz)
  // ════════════════════════════════════════════
  h += '<h3 style="margin:24px 0 12px;color:var(--primary)">4. Quantified Complexity Reduction</h3>';

  const totalProcs = procs.length;
  const droppableCount = droppable.length;
  const essentialCount = totalProcs - droppableCount;
  const reductionPct = totalProcs > 0 ? Math.round(droppableCount / totalProcs * 100) : 0;

  const notebookCells = notebook ? (notebook.cells || []).length : essentialCount;
  const projectedCells = Math.max(3, notebookCells - droppableCount);

  h += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin-bottom:16px">';
  const qMetrics = [
    { label: 'NiFi Processors', value: totalProcs, color: 'var(--text2)', sub: 'current flow' },
    { label: 'Can Be Dropped', value: droppableCount, color: 'var(--amber)', sub: `${reductionPct}% reduction` },
    { label: 'Essential Steps', value: essentialCount, color: 'var(--green)', sub: 'remain in Databricks' },
    { label: 'Notebook Cells', value: projectedCells, color: 'var(--primary)', sub: 'projected output' }
  ];
  qMetrics.forEach(m => {
    h += `<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:14px;text-align:center">`;
    h += `<div style="font-size:2rem;font-weight:800;color:${m.color}">${m.value}</div>`;
    h += `<div style="font-size:0.85rem;font-weight:600">${m.label}</div>`;
    h += `<div style="font-size:0.75rem;color:var(--text2)">${m.sub}</div></div>`;
  });
  h += '</div>';

  // 8.2 — Scoring formula breakdown table
  const nestedGroupDepth = nifi.processGroups.reduce((max, pg) => {
    const depth = (pg.path || pg.name || '').split('/').length;
    return Math.max(max, depth);
  }, 1);
  const routingMultiplier = Math.min(routes.length > 0 ? 1 + (routes.length * 0.15) : 1, 3.0);
  const controllerChainWeight = (nifi.controllerServices || []).length * 4;
  const nifiComplexity = Math.round(
    (procs.length * 2 + conns.length + controllerChainWeight + Object.keys(extSystems).length * 5 + nestedGroupDepth * 3) * routingMultiplier
  );
  const dbxComplexity = essentialCount * 2 + Object.keys(extSystems).length * 3;
  const complexityReduction = nifiComplexity > 0 ? Math.round((1 - dbxComplexity / nifiComplexity) * 100) : 0;

  // Formula breakdown
  h += '<h4 style="margin:12px 0 6px">Complexity Scoring Formula</h4>';
  h += '<div class="table-scroll"><table style="font-size:0.82rem"><thead><tr><th>Factor</th><th>Count</th><th>Weight</th><th>Score</th><th>Rationale</th></tr></thead><tbody>';
  const formulaRows = [
    { factor: 'Processors', count: procs.length, weight: 'x2', score: procs.length * 2, rationale: 'Each processor = discrete transformation step' },
    { factor: 'Connections', count: conns.length, weight: 'x1', score: conns.length, rationale: 'Data flow links between processors' },
    { factor: 'Controller Services', count: (nifi.controllerServices || []).length, weight: 'x4', score: controllerChainWeight, rationale: 'Shared services add cross-cutting complexity' },
    { factor: 'External Systems', count: Object.keys(extSystems).length, weight: 'x5', score: Object.keys(extSystems).length * 5, rationale: 'Each integration adds deployment complexity' },
    { factor: 'Nesting Depth', count: nestedGroupDepth, weight: 'x3', score: nestedGroupDepth * 3, rationale: 'Deeper nesting increases maintenance burden' },
    { factor: 'Routing Multiplier', count: routes.length + ' routes', weight: 'x' + routingMultiplier.toFixed(2), score: '(applied to total)', rationale: 'Conditional routing adds branching complexity' },
  ];
  formulaRows.forEach(row => {
    h += `<tr><td><strong>${escapeHTML(String(row.factor))}</strong></td><td>${escapeHTML(String(row.count))}</td><td>${escapeHTML(String(row.weight))}</td><td>${escapeHTML(String(row.score))}</td><td style="font-size:0.78rem;color:var(--text2)">${escapeHTML(row.rationale)}</td></tr>`;
  });
  h += `<tr style="font-weight:700;border-top:2px solid var(--border)"><td>NiFi Total</td><td></td><td></td><td style="color:var(--red)">${nifiComplexity} pts</td><td></td></tr>`;
  h += `<tr style="font-weight:700"><td>Databricks Projected</td><td></td><td></td><td style="color:var(--green)">${dbxComplexity} pts</td><td>Essential x2 + ExtSys x3</td></tr>`;
  h += '</tbody></table></div>';

  // 8.2 — Stacked bar visualization
  h += '<div class="stacked-bar-container" style="margin:16px 0">';
  h += '<div style="display:flex;justify-content:space-between;font-size:0.82rem;margin-bottom:4px"><span>NiFi Complexity</span><strong style="color:var(--red)">' + nifiComplexity + ' pts</strong></div>';
  h += '<div class="stacked-bar">';
  const procPct = nifiComplexity > 0 ? Math.round((procs.length * 2 * routingMultiplier) / nifiComplexity * 100) : 0;
  const connPct = nifiComplexity > 0 ? Math.round((conns.length * routingMultiplier) / nifiComplexity * 100) : 0;
  const csPct = nifiComplexity > 0 ? Math.round((controllerChainWeight * routingMultiplier) / nifiComplexity * 100) : 0;
  const extPct = nifiComplexity > 0 ? Math.round((Object.keys(extSystems).length * 5 * routingMultiplier) / nifiComplexity * 100) : 0;
  const restPct = Math.max(0, 100 - procPct - connPct - csPct - extPct);
  h += `<div class="stacked-bar-seg" style="width:${procPct}%;background:#3B82F6">${procPct > 8 ? procPct + '%' : ''}</div>`;
  h += `<div class="stacked-bar-seg" style="width:${connPct}%;background:#A855F7">${connPct > 8 ? connPct + '%' : ''}</div>`;
  h += `<div class="stacked-bar-seg" style="width:${csPct}%;background:#EAB308">${csPct > 8 ? csPct + '%' : ''}</div>`;
  h += `<div class="stacked-bar-seg" style="width:${extPct}%;background:#EF4444">${extPct > 8 ? extPct + '%' : ''}</div>`;
  h += `<div class="stacked-bar-seg" style="width:${restPct}%;background:#8b949e">${restPct > 8 ? restPct + '%' : ''}</div>`;
  h += '</div>';
  h += '<div class="stacked-bar-legend">';
  h += '<span class="stacked-bar-legend-item"><span class="stacked-bar-legend-dot" style="background:#3B82F6"></span>Processors</span>';
  h += '<span class="stacked-bar-legend-item"><span class="stacked-bar-legend-dot" style="background:#A855F7"></span>Connections</span>';
  h += '<span class="stacked-bar-legend-item"><span class="stacked-bar-legend-dot" style="background:#EAB308"></span>Controller Svc</span>';
  h += '<span class="stacked-bar-legend-item"><span class="stacked-bar-legend-dot" style="background:#EF4444"></span>Ext Systems</span>';
  h += '<span class="stacked-bar-legend-item"><span class="stacked-bar-legend-dot" style="background:#8b949e"></span>Nesting/Routing</span>';
  h += '</div>';

  // Databricks projected bar
  h += '<div style="display:flex;justify-content:space-between;font-size:0.82rem;margin:12px 0 4px"><span>Databricks Projected</span><strong style="color:var(--green)">' + dbxComplexity + ' pts</strong></div>';
  const dbxBarW = nifiComplexity > 0 ? Math.round(dbxComplexity / nifiComplexity * 100) : 50;
  h += `<div style="height:24px;background:var(--green)22;border-radius:6px;overflow:hidden"><div style="height:100%;width:${dbxBarW}%;background:var(--green);border-radius:6px"></div></div>`;
  h += `<div style="text-align:center;margin-top:8px;font-size:1.1rem;font-weight:700;color:var(--green)">${complexityReduction}% complexity reduction</div>`;
  h += '</div>';

  // Breakdown by category
  if (droppable.length) {
    const byCat = {};
    droppable.forEach(d => {
      const cat = d.type.match(/Merge|Split/) ? 'Merge/Split' : d.type.match(/Log|Debug|Count|Monitor/) ? 'Logging/Monitoring' : d.type.match(/Route|Distribute|Control|Detect/) ? 'Routing/Control' : d.type.match(/Update|Attribute|Enforce/) ? 'Attribute Management' : d.type.match(/Wait|Notify|Retry/) ? 'Flow Control' : d.type.match(/Validate/) ? 'Validation' : d.type.match(/Compress|Unpack/) ? 'Compression' : d.type.match(/Generate|Funnel|Port/) ? 'NiFi Internal' : 'Other';
      if (!byCat[cat]) byCat[cat] = [];
      byCat[cat].push(d);
    });

    h += '<h4 style="margin:12px 0 6px">Dropped Processors by Category</h4>';
    h += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:8px">';
    Object.entries(byCat).sort((a, b) => b[1].length - a[1].length).forEach(([cat, items]) => {
      h += `<div style="background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:10px">`;
      h += `<strong style="font-size:0.85rem">${escapeHTML(cat)}</strong> <span style="color:var(--text2);font-size:0.8rem">(${items.length})</span>`;
      h += `<div style="font-size:0.78rem;color:var(--text2);margin-top:4px">${items.map(i => escapeHTML(i.type)).join(', ')}</div></div>`;
    });
    h += '</div>';
  }

  // ════════════════════════════════════════════
  // 5. MIGRATION ROI SUMMARY (8.3 — ROI Calculator)
  // ════════════════════════════════════════════
  h += '<h3 style="margin:24px 0 12px;color:var(--primary)">5. Migration ROI Summary</h3>';

  // Context-aware capabilities — only show relevant ones
  const allCaps = [
    { name: 'ACID Transactions', desc: 'Delta Lake provides full ACID guarantees on all data operations', icon: '&#128274;', when: () => true, useCases: ['Reliable upserts (MERGE INTO)', 'Consistent reads during writes', 'Transaction log for auditability'] },
    { name: 'Time Travel', desc: 'Query and restore previous versions of data with VERSION AS OF', icon: '&#9200;', when: () => true, useCases: ['Roll back bad data loads', 'Audit historical states', 'Reproduce pipeline results'] },
    { name: 'Unity Catalog Governance', desc: 'Centralized access control, lineage, and audit logging', icon: '&#128737;', when: () => (nifi.controllerServices || []).length > 0 || Object.keys(extSystems).length > 0, useCases: ['Fine-grained access control', 'Data lineage tracking', 'Cross-workspace governance'] },
    { name: 'ML Integration', desc: 'Seamless integration with MLflow, Feature Store, and model serving', icon: '&#129302;', when: () => procs.some(p => /Execute(Script|Stream)|Predict|ML|Model/i.test(p.type)), useCases: ['Model training on pipeline data', 'Feature engineering', 'Real-time model serving'] },
    { name: 'Auto Scaling', desc: 'Automatic cluster scaling based on workload demand', icon: '&#128200;', when: () => procs.length > 10, useCases: ['Handle variable data volumes', 'Cost optimization', 'Peak load management'] },
    { name: 'Photon Engine', desc: 'Vectorized query engine for 2-8x performance improvement', icon: '&#9889;', when: () => transforms.length > 3 || procs.some(p => /SQL|Query|Convert/i.test(p.type)), useCases: ['Faster SQL transformations', 'Reduced processing time', 'Lower compute costs'] },
    { name: 'Delta Live Tables', desc: 'Declarative data pipelines with built-in quality management', icon: '&#128736;', when: () => true, useCases: ['Declarative pipeline definitions', 'Automatic error handling', 'Built-in quality dashboards'] },
    { name: 'Liquid Clustering', desc: 'Automatic data layout optimization replacing manual Z-ordering', icon: '&#128204;', when: () => procs.some(p => /Merge|Partition|Sort/i.test(p.type)), useCases: ['Automatic data layout', 'Faster query performance', 'No manual optimization'] },
    { name: 'Structured Streaming', desc: 'Unified batch and streaming with exactly-once processing', icon: '&#127919;', when: () => procs.some(p => /Consume|Subscribe|Listen|Kafka|JMS/i.test(p.type)), useCases: ['Real-time data processing', 'Exactly-once semantics', 'Unified batch/stream code'] },
    { name: 'Secret Scopes', desc: 'Secure credential management replacing NiFi controller services', icon: '&#128272;', when: () => (nifi.controllerServices || []).some(cs => /SSL|Password|Credential|DBCP/i.test(cs.type)), useCases: ['Centralized secrets', 'Key rotation', 'Audit access'] },
  ];
  const newCaps = allCaps.filter(c => c.when());

  // 8.3 — ROI Calculator
  h += '<div class="roi-grid">';

  // Cost delta
  const nifiHoursPerWeek = Math.max(2, Math.ceil(totalProcs * 0.5 + Object.keys(extSystems).length * 1));
  const dbxHoursPerWeek = Math.max(1, Math.ceil(essentialCount * 0.2 + Object.keys(extSystems).length * 0.5));
  const hoursSavedPerWeek = nifiHoursPerWeek - dbxHoursPerWeek;
  const annualHoursSaved = hoursSavedPerWeek * 52;
  const costPerHour = 85; // average engineer cost
  const annualSavings = annualHoursSaved * costPerHour;

  h += `<div class="roi-card"><div class="roi-value" style="color:var(--green)">$${annualSavings.toLocaleString()}</div><div class="roi-label">Est. Annual Savings</div><div class="roi-detail">${annualHoursSaved} hours/year at $${costPerHour}/hr</div></div>`;
  h += `<div class="roi-card"><div class="roi-value" style="color:var(--primary)">${hoursSavedPerWeek}h/wk</div><div class="roi-label">Weekly Time Savings</div><div class="roi-detail">${nifiHoursPerWeek}h (NiFi) vs ${dbxHoursPerWeek}h (Databricks)</div></div>`;
  h += `<div class="roi-card"><div class="roi-value" style="color:var(--amber)">${complexityReduction}%</div><div class="roi-label">Complexity Reduction</div><div class="roi-detail">${nifiComplexity} pts to ${dbxComplexity} pts</div></div>`;
  h += `<div class="roi-card"><div class="roi-value" style="color:var(--green)">${reductionPct}%</div><div class="roi-label">Component Reduction</div><div class="roi-detail">${droppableCount} of ${totalProcs} processors eliminated</div></div>`;
  h += '</div>';

  // Productivity gains
  h += '<h4 style="margin:16px 0 8px">Productivity Gains</h4>';
  h += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px">';

  h += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:16px">';
  h += '<h4 style="margin:0 0 12px;font-size:0.95rem">Complexity Comparison</h4>';
  const nifiBarW = 100;
  const dbxBarWFull = nifiComplexity > 0 ? Math.round(dbxComplexity / nifiComplexity * 100) : 50;
  h += `<div style="margin-bottom:12px"><div style="display:flex;justify-content:space-between;font-size:0.82rem;margin-bottom:4px"><span>NiFi (current)</span><strong>${nifiComplexity} pts</strong></div>`;
  h += `<div style="height:20px;background:var(--red)33;border-radius:4px;overflow:hidden"><div style="height:100%;width:${nifiBarW}%;background:var(--red);border-radius:4px"></div></div></div>`;
  h += `<div><div style="display:flex;justify-content:space-between;font-size:0.82rem;margin-bottom:4px"><span>Databricks (projected)</span><strong>${dbxComplexity} pts</strong></div>`;
  h += `<div style="height:20px;background:var(--green)33;border-radius:4px;overflow:hidden"><div style="height:100%;width:${dbxBarWFull}%;background:var(--green);border-radius:4px"></div></div></div>`;
  h += `<div style="text-align:center;margin-top:12px;font-size:1.1rem;font-weight:700;color:var(--green)">${complexityReduction}% complexity reduction</div>`;
  h += '</div>';

  // Right: Key metrics
  h += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:16px">';
  h += '<h4 style="margin:0 0 12px;font-size:0.95rem">Migration Summary</h4>';
  const summaryItems = [
    { label: 'Processors eliminated', value: `${droppableCount} of ${totalProcs} (${reductionPct}%)` },
    { label: 'External systems', value: `${Object.keys(extSystems).length} integration(s)` },
    { label: 'Controller services', value: `${(nifi.controllerServices || []).length} to migrate` },
    { label: 'Process groups', value: `${nifi.processGroups.length} → Databricks jobs` },
    { label: 'Notebook cells', value: `${projectedCells} (vs ${totalProcs} NiFi processors)` },
    { label: 'New capabilities', value: `${newCaps.length} Databricks features gained` }
  ];
  summaryItems.forEach(item => {
    h += `<div style="display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px solid var(--border);font-size:0.85rem"><span style="color:var(--text2)">${item.label}</span><strong>${item.value}</strong></div>`;
  });
  h += '</div></div>';

  // Risk reduction
  h += '<h4 style="margin:16px 0 8px">Risk Reduction</h4>';
  h += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:8px;margin-bottom:16px">';
  const riskItems = [
    { label: 'Data Loss Risk', nifi: 'Medium (manual checkpoints)', dbx: 'Low (ACID + exactly-once)', color: 'var(--green)' },
    { label: 'Credential Exposure', nifi: 'Medium (per-processor secrets)', dbx: 'Low (Secret Scopes + IAM)', color: 'var(--green)' },
    { label: 'Downtime Impact', nifi: 'High (single cluster)', dbx: 'Low (auto-scaling + HA)', color: 'var(--green)' },
    { label: 'Compliance', nifi: 'Manual audit trails', dbx: 'Unity Catalog lineage + audit', color: 'var(--primary)' },
  ];
  riskItems.forEach(ri => {
    h += `<div style="background:var(--surface);border:1px solid var(--border);border-radius:6px;padding:10px">`;
    h += `<div style="font-weight:600;font-size:0.85rem;margin-bottom:6px">${escapeHTML(ri.label)}</div>`;
    h += `<div style="font-size:0.78rem"><span style="color:var(--red)">Before:</span> ${escapeHTML(ri.nifi)}</div>`;
    h += `<div style="font-size:0.78rem"><span style="color:${ri.color}">After:</span> ${escapeHTML(ri.dbx)}</div></div>`;
  });
  h += '</div>';

  // ════════════════════════════════════════════
  // 6. IMPLEMENTATION ROADMAP (8.4)
  // ════════════════════════════════════════════
  h += '<h3 style="margin:24px 0 12px;color:var(--primary)">6. Implementation Roadmap</h3>';

  const roadmapItems = [];
  // Phase 1: Foundation
  roadmapItems.push({
    phase: 'Phase 1: Foundation',
    title: 'Environment Setup & Secret Migration',
    effort: `${Math.max(1, Math.ceil((nifi.controllerServices || []).length * 0.5))} days`,
    tasks: ['Set up Unity Catalog schema', 'Migrate secrets to Databricks Secret Scopes', 'Configure cluster policies'],
    dependencies: 'None',
  });

  // Phase 2: Core pipeline
  roadmapItems.push({
    phase: 'Phase 2: Core Pipeline',
    title: 'Source + Transform + Sink Migration',
    effort: `${Math.max(2, Math.ceil(essentialCount * 0.5))} days`,
    tasks: [`Migrate ${sources.length} source processor(s)`, `Migrate ${transforms.length} transform processor(s)`, `Migrate ${sinks.length} sink processor(s)`],
    dependencies: 'Phase 1',
  });

  // Phase 3: External systems
  if (Object.keys(extSystems).length > 0) {
    roadmapItems.push({
      phase: 'Phase 3: Integrations',
      title: 'External System Connectivity',
      effort: `${Math.max(1, Object.keys(extSystems).length * 2)} days`,
      tasks: Object.values(extSystems).map(s => `Connect to ${s.name} (${s.processors.length} processor(s))`),
      dependencies: 'Phase 2',
    });
  }

  // Phase 4: Testing
  roadmapItems.push({
    phase: `Phase ${Object.keys(extSystems).length > 0 ? '4' : '3'}: Testing`,
    title: 'Validation & Data Quality',
    effort: `${Math.max(2, Math.ceil(totalProcs * 0.3))} days`,
    tasks: ['Run data comparison between NiFi and Databricks outputs', 'Validate all processor mappings', 'Set up DLT Expectations for quality rules'],
    dependencies: `Phase ${Object.keys(extSystems).length > 0 ? '3' : '2'}`,
  });

  // Phase 5: Cutover
  roadmapItems.push({
    phase: `Phase ${Object.keys(extSystems).length > 0 ? '5' : '4'}: Cutover`,
    title: 'Production Deployment',
    effort: '2-3 days',
    tasks: ['Configure Databricks Workflows scheduling', 'Set up monitoring and alerting', 'Parallel run and cutover'],
    dependencies: 'Previous phase',
  });

  h += '<div class="roadmap-timeline">';
  roadmapItems.forEach(item => {
    h += '<div class="roadmap-item">';
    h += `<div class="roadmap-phase">${escapeHTML(item.phase)}</div>`;
    h += `<div class="roadmap-title">${escapeHTML(item.title)}</div>`;
    h += `<div class="roadmap-effort">Effort: <strong>${escapeHTML(item.effort)}</strong> | Depends on: ${escapeHTML(item.dependencies)}</div>`;
    h += '<ul style="margin:6px 0 0;padding-left:18px;font-size:0.8rem;color:var(--text2)">';
    item.tasks.forEach(t => { h += `<li style="margin:2px 0">${escapeHTML(t)}</li>`; });
    h += '</ul></div>';
  });
  h += '</div>';

  // ════════════════════════════════════════════
  // 7. CAPABILITY DISCOVERY GUIDE (8.5)
  // ════════════════════════════════════════════
  h += '<h3 style="margin:24px 0 12px;color:var(--primary)">7. Capability Discovery Guide</h3>';
  h += '<p style="font-size:0.85rem;color:var(--text2);margin-bottom:12px">New capabilities gained with Databricks, with use cases relevant to <strong>this flow</strong>.</p>';

  h += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:12px">';
  newCaps.forEach(cap => {
    h += `<div style="background:var(--green)08;border:1px solid var(--green)33;border-radius:8px;padding:14px">`;
    h += `<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">`;
    h += `<span style="font-size:1.2rem">${cap.icon}</span>`;
    h += `<strong style="font-size:0.92rem">${escapeHTML(cap.name)}</strong>`;
    h += '</div>';
    h += `<div style="font-size:0.82rem;color:var(--text2);margin-bottom:8px">${escapeHTML(cap.desc)}</div>`;
    if (cap.useCases && cap.useCases.length) {
      h += '<div style="font-size:0.78rem;font-weight:600;color:var(--green);margin-bottom:4px">Relevant Use Cases:</div>';
      h += '<ul style="margin:0;padding-left:16px;font-size:0.78rem;color:var(--text2)">';
      cap.useCases.forEach(uc => { h += `<li style="margin:2px 0">${escapeHTML(uc)}</li>`; });
      h += '</ul>';
    }
    h += '</div>';
  });
  h += '</div>';

  // Download button
  h += `<div style="margin-top:24px;text-align:center">`;
  h += `<button class="btn btn-primary" id="valueAnalysisDownloadBtn">Download Value Analysis (JSON)</button>`;
  h += '</div>';

  // Build structured result
  const totalEffortDays = roadmapItems.reduce((sum, item) => {
    const match = item.effort.match(/(\d+)/);
    return sum + (match ? parseInt(match[1]) : 0);
  }, 0);

  const valueAnalysis = {
    summary: { totalProcessors: totalProcs, droppable: droppableCount, essential: essentialCount, reductionPct, nifiComplexity, dbxComplexity, complexityReduction },
    droppableProcessors: droppable.map(d => ({ ...d, replacement: d.replacement })),
    recommendations: recommendations.map(r => ({ category: r.category, nifi: r.nifi, dbx: r.dbx, benefit: r.benefit, priority: r.priority })),
    externalSystems: Object.keys(extSystems),
    newCapabilities: newCaps.map(c => ({ name: c.name, useCases: c.useCases || [] })),
    roi: { annualSavings, hoursSavedPerWeek, nifiHoursPerWeek, dbxHoursPerWeek, complexityReduction },
    roadmap: roadmapItems.map(r => ({ phase: r.phase, title: r.title, effort: r.effort, dependencies: r.dependencies })),
    totalEstimatedEffort: totalEffortDays + ' days',
    timestamp: new Date().toISOString()
  };

  return { html: h, valueAnalysis };
}
