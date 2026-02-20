/**
 * reporters/value-analysis.js — Migration value analysis
 *
 * Extracted from index.html lines 8775-9092.
 * Produces a comprehensive value analysis covering:
 *   1. What the workflow does
 *   2. How to build it better in Databricks
 *   3. Steps that aren't needed
 *   4. Quantified complexity reduction
 *   5. Migration ROI summary
 *
 * @module reporters/value-analysis
 */

import { DROPPABLE_PROCESSORS } from '../constants/droppable-processors.js';
import { DBX_BETTER_APPROACH } from '../constants/dbx-better-approach.js';
import { classifyNiFiProcessor } from '../mappers/processor-classifier.js';
import { detectExternalSystems } from '../analyzers/external-systems.js';

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
  // 3. STEPS THAT AREN'T NEEDED
  // ════════════════════════════════════════════
  h += '<h3 style="margin:24px 0 12px;color:var(--primary)">3. Steps That Aren\'t Needed in Databricks</h3>';

  const droppable = [];
  procs.forEach(p => {
    const drop = DROPPABLE_PROCESSORS[p.type];
    if (drop) {
      droppable.push({ name: p.name, type: p.type, group: p.group, ...drop });
    }
  });

  if (droppable.length === 0) {
    h += '<div class="val-ok">All processors in this flow serve essential functions in the Databricks migration.</div>';
  } else {
    const savingsMap = { high: 3, medium: 2, low: 1 };
    const droppableSorted = droppable.sort((a, b) => (savingsMap[b.savings] || 0) - (savingsMap[a.savings] || 0));

    h += `<div class="alert alert-success" style="margin-bottom:12px"><strong>${droppable.length}</strong> of ${procs.length} processors (${Math.round(droppable.length / procs.length * 100)}%) can be eliminated in Databricks</div>`;

    h += '<div class="table-scroll"><table style="font-size:0.82rem"><thead><tr><th>Processor</th><th>Type</th><th>Reason to Drop</th><th>Savings</th><th>Risk</th></tr></thead><tbody>';
    droppableSorted.forEach(d => {
      const savColor = d.savings === 'high' ? 'var(--green)' : d.savings === 'medium' ? 'var(--primary)' : 'var(--text2)';
      const riskColor = d.risk === 'high' ? 'var(--red)' : d.risk === 'medium' ? 'var(--amber)' : d.risk === 'low' ? 'var(--text2)' : 'var(--green)';
      h += `<tr><td><strong>${escapeHTML(d.name)}</strong></td>`;
      h += `<td><code style="font-size:0.75rem">${escapeHTML(d.type)}</code></td>`;
      h += `<td style="font-size:0.8rem">${escapeHTML(d.reason)}</td>`;
      h += `<td><span style="color:${savColor};font-weight:600">${d.savings.toUpperCase()}</span></td>`;
      h += `<td><span style="color:${riskColor};font-weight:600">${(d.risk || 'none').toUpperCase()}</span></td></tr>`;
    });
    h += '</tbody></table></div>';
  }

  // ════════════════════════════════════════════
  // 4. QUANTIFIED COMPLEXITY REDUCTION
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
  // 5. MIGRATION ROI SUMMARY
  // ════════════════════════════════════════════
  h += '<h3 style="margin:24px 0 12px;color:var(--primary)">5. Migration ROI Summary</h3>';

  // Complexity scoring formula:
  // - Processors (x2): Each processor represents a discrete transformation step
  // - Connections (x1): Data flow links between processors
  // - Controller services (x4): Shared services add cross-cutting complexity
  // - External systems (x5): Each external integration adds deployment/config complexity
  // - Nested group depth (x3): Deeper nesting increases maintenance burden
  // - Routing multiplier (1 + routes x 0.15): Conditional routing adds branching complexity
  //   Capped at 3.0 to prevent unrealistic inflation
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

  // Context-aware capabilities — only show relevant ones
  const allCaps = [
    { name: 'ACID Transactions', desc: 'Delta Lake provides full ACID guarantees on all data operations', icon: '&#128274;', when: () => true },
    { name: 'Time Travel', desc: 'Query and restore previous versions of data with VERSION AS OF', icon: '&#9200;', when: () => true },
    { name: 'Unity Catalog Governance', desc: 'Centralized access control, lineage, and audit logging', icon: '&#128737;', when: () => (nifi.controllerServices || []).length > 0 || Object.keys(extSystems).length > 0 },
    { name: 'ML Integration', desc: 'Seamless integration with MLflow, Feature Store, and model serving', icon: '&#129302;', when: () => procs.some(p => /Execute(Script|Stream)|Predict|ML|Model/i.test(p.type)) },
    { name: 'Auto Scaling', desc: 'Automatic cluster scaling based on workload demand', icon: '&#128200;', when: () => procs.length > 10 },
    { name: 'Photon Engine', desc: 'Vectorized query engine for 2-8x performance improvement', icon: '&#9889;', when: () => transforms.length > 3 || procs.some(p => /SQL|Query|Convert/i.test(p.type)) },
    { name: 'Delta Live Tables', desc: 'Declarative data pipelines with built-in quality management', icon: '&#128736;', when: () => true },
    { name: 'Liquid Clustering', desc: 'Automatic data layout optimization replacing manual Z-ordering', icon: '&#128204;', when: () => procs.some(p => /Merge|Partition|Sort/i.test(p.type)) },
    { name: 'Structured Streaming', desc: 'Unified batch and streaming with exactly-once processing', icon: '&#127919;', when: () => procs.some(p => /Consume|Subscribe|Listen|Kafka|JMS/i.test(p.type)) },
    { name: 'Secret Scopes', desc: 'Secure credential management replacing NiFi controller services', icon: '&#128272;', when: () => (nifi.controllerServices || []).some(cs => /SSL|Password|Credential|DBCP/i.test(cs.type)) },
  ];
  const newCaps = allCaps.filter(c => c.when());

  h += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px">';

  // Left: Complexity comparison
  h += '<div style="background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:16px">';
  h += '<h4 style="margin:0 0 12px;font-size:0.95rem">Complexity Comparison</h4>';
  const nifiBarW = 100;
  const dbxBarW = nifiComplexity > 0 ? Math.round(dbxComplexity / nifiComplexity * 100) : 50;
  h += `<div style="margin-bottom:12px"><div style="display:flex;justify-content:space-between;font-size:0.82rem;margin-bottom:4px"><span>NiFi (current)</span><strong>${nifiComplexity} pts</strong></div>`;
  h += `<div style="height:20px;background:var(--red)33;border-radius:4px;overflow:hidden"><div style="height:100%;width:${nifiBarW}%;background:var(--red);border-radius:4px"></div></div></div>`;
  h += `<div><div style="display:flex;justify-content:space-between;font-size:0.82rem;margin-bottom:4px"><span>Databricks (projected)</span><strong>${dbxComplexity} pts</strong></div>`;
  h += `<div style="height:20px;background:var(--green)33;border-radius:4px;overflow:hidden"><div style="height:100%;width:${dbxBarW}%;background:var(--green);border-radius:4px"></div></div></div>`;
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

  // New capabilities gained
  h += '<h4 style="margin:16px 0 8px">New Capabilities Gained with Databricks</h4>';
  h += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:8px">';
  newCaps.forEach(cap => {
    h += `<div style="background:var(--green)08;border:1px solid var(--green)33;border-radius:8px;padding:10px">`;
    h += `<div style="font-size:1.1rem;margin-bottom:4px">${cap.icon}</div>`;
    h += `<div style="font-weight:600;font-size:0.88rem">${escapeHTML(cap.name)}</div>`;
    h += `<div style="font-size:0.78rem;color:var(--text2);margin-top:2px">${escapeHTML(cap.desc)}</div></div>`;
  });
  h += '</div>';

  // Download button
  h += `<div style="margin-top:24px;text-align:center">`;
  h += `<button class="btn btn-primary" id="valueAnalysisDownloadBtn">Download Value Analysis (JSON)</button>`;
  h += '</div>';

  // Build structured result
  const valueAnalysis = {
    summary: { totalProcessors: totalProcs, droppable: droppableCount, essential: essentialCount, reductionPct, nifiComplexity, dbxComplexity, complexityReduction },
    droppableProcessors: droppable,
    recommendations: recommendations.map(r => ({ category: r.category, nifi: r.nifi, dbx: r.dbx, benefit: r.benefit, priority: r.priority })),
    externalSystems: Object.keys(extSystems),
    newCapabilities: newCaps.map(c => c.name),
    timestamp: new Date().toISOString()
  };

  return { html: h, valueAnalysis };
}
