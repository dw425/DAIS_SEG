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
import { CONFIDENCE_THRESHOLDS } from '../constants/confidence-thresholds.js';

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
  const gaps = mappings.filter(m => !m.mapped || m.confidence < CONFIDENCE_THRESHOLDS.PARTIAL).map(m => ({
    name: m.name, type: m.type, group: m.group, role: m.role,
    reason: m.gapReason || `Low confidence mapping (${Math.round(m.confidence * 100)}%)`,
    recommendation: m.type.match(/^(Listen|Handle)/) ? 'Consider Databricks Model Serving or external API gateway'
      : m.type.match(/^Execute(Script|Stream)/) ? 'Manual translation required — review original script logic'
      : m.type.match(/^(Put|Send)(Email|TCP|Syslog)/) ? 'Use webhook notification service or Databricks workflow alerts'
      : 'Review processor documentation and implement custom PySpark logic'
  }));
  // 5.1: Gap Resolution Playbook — alternatives ranked by feasibility with code snippets
  const GAP_PLAYBOOK = {
    'Listen': [
      { alternative: 'Databricks Model Serving Endpoint', feasibility: 'High', effort: 'Medium',
        snippet: '# Deploy as Model Serving endpoint\nimport mlflow\nmlflow.pyfunc.log_model(...)\n# Configure REST endpoint in Databricks Serving' },
      { alternative: 'Azure Function / AWS Lambda webhook', feasibility: 'Medium', effort: 'Low',
        snippet: '# Serverless function forwards HTTP requests to Databricks\n# Use Jobs API: POST /api/2.1/jobs/run-now' },
      { alternative: 'Structured Streaming + Kafka bridge', feasibility: 'Medium', effort: 'High',
        snippet: 'df = spark.readStream.format("kafka")\\\n  .option("subscribe", "nifi-http-topic")\\\n  .load()' },
    ],
    'Handle': [
      { alternative: 'Databricks Model Serving Endpoint', feasibility: 'High', effort: 'Medium',
        snippet: '# Create serving endpoint for HTTP request handling\nfrom databricks.sdk import WorkspaceClient\nw = WorkspaceClient()\nw.serving_endpoints.create(...)' },
      { alternative: 'API Gateway + Jobs API', feasibility: 'Medium', effort: 'Low',
        snippet: '# Route HTTP through API Gateway to Databricks Jobs\n# POST /api/2.1/jobs/run-now with request payload' },
    ],
    'ExecuteScript': [
      { alternative: 'PySpark UDF (inline translation)', feasibility: 'High', effort: 'High',
        snippet: 'from pyspark.sql.functions import udf\n@udf(returnType=StringType())\ndef custom_logic(value):\n    # Translate NiFi script logic here\n    return transformed_value\ndf = df.withColumn("result", custom_logic(col("input")))' },
      { alternative: 'Databricks notebook job with parameters', feasibility: 'Medium', effort: 'Medium',
        snippet: 'dbutils.notebook.run("./custom_script_notebook", timeout_seconds=300,\n  arguments={"input_table": "...", "output_table": "..."})' },
      { alternative: 'Delta Live Tables expectations', feasibility: 'Low', effort: 'High',
        snippet: '@dlt.expect_or_drop("valid_data", "value IS NOT NULL")\n@dlt.table\ndef processed():\n    return spark.read.table("input_table")' },
    ],
    'ExecuteStream': [
      { alternative: 'Subprocess via %sh magic', feasibility: 'Medium', effort: 'Medium',
        snippet: '# MAGIC %sh\n# MAGIC /path/to/external_command --input /tmp/data --output /tmp/result' },
      { alternative: 'PySpark subprocess module', feasibility: 'Medium', effort: 'Low',
        snippet: 'import subprocess\nresult = subprocess.run(["command", "--arg"], capture_output=True)\nprint(result.stdout.decode())' },
    ],
    'PutEmail': [
      { alternative: 'Databricks Workflow notification', feasibility: 'High', effort: 'Low',
        snippet: '# Configure email notification in workflow JSON:\n# "email_notifications": {"on_success": ["team@company.com"]}' },
      { alternative: 'SendGrid / SES API call', feasibility: 'Medium', effort: 'Low',
        snippet: 'import requests\napi_key = dbutils.secrets.get("scope", "sendgrid_key")\nrequests.post("https://api.sendgrid.com/v3/mail/send",\n  headers={"Authorization": f"Bearer {api_key}"},\n  json={"personalizations": [...], "from": {...}, "content": [...]})' },
    ],
    'PutTCP': [
      { alternative: 'Webhook via requests library', feasibility: 'High', effort: 'Low',
        snippet: 'import requests\nresponse = requests.post("http://target:port/endpoint",\n  json={"data": row.asDict()}, timeout=30)' },
      { alternative: 'Kafka producer', feasibility: 'Medium', effort: 'Medium',
        snippet: 'df.selectExpr("CAST(key AS STRING)", "to_json(struct(*)) AS value")\\\n  .write.format("kafka")\\\n  .option("kafka.bootstrap.servers", "host:9092")\\\n  .option("topic", "output-topic").save()' },
    ],
  };

  const gapPlaybook = gaps.map(g => {
    const matchedKey = Object.keys(GAP_PLAYBOOK).find(k => g.type.includes(k));
    return {
      ...g,
      alternatives: matchedKey ? GAP_PLAYBOOK[matchedKey] : [
        { alternative: 'Custom PySpark implementation', feasibility: 'Medium', effort: 'High',
          snippet: `# Manual implementation for ${g.type}\n# Review NiFi processor docs and translate logic to PySpark\n# df = spark.read.table("input")\n# ... custom transformation ...\n# df.write.saveAsTable("output")` },
        { alternative: 'Databricks Partner Connect integration', feasibility: 'Low', effort: 'Medium',
          snippet: '# Check Databricks Partner Connect for third-party tools\n# that can replace this NiFi processor functionality' },
      ],
    };
  });

  // 5.2: Per-Processor Migration Guide — NiFi vs Databricks comparison
  const processorGuides = gaps.map(g => {
    const nifiDesc = getNiFiProcessorDescription(g.type);
    const dbxEquiv = getDatabricksEquivalent(g.type);
    return {
      name: g.name,
      type: g.type,
      role: g.role,
      nifiDescription: nifiDesc,
      databricksApproach: dbxEquiv.approach,
      keyDifferences: dbxEquiv.differences,
      migrationSteps: dbxEquiv.steps,
      estimatedEffort: dbxEquiv.effort,
    };
  });

  // 5.3: Risk & Impact Matrix
  const CRITICALITY_WEIGHTS = { source: 5, sink: 4, transform: 3, route: 3, process: 4, utility: 1 };
  const riskMatrix = gaps.map(g => {
    const critWeight = CRITICALITY_WEIGHTS[g.role] || 3;
    const hasDownstream = (nifi.connections || []).some(c => {
      const srcName = (nifi.processors || []).find(p => p.id === c.source?.id)?.name;
      return srcName === g.name;
    });
    const downstreamImpact = hasDownstream ? 2 : 0;
    const complexityScore = g.type.match(/ExecuteScript|ExecuteStream|Custom/) ? 5
      : g.type.match(/Listen|Handle/) ? 4
      : g.type.match(/Put|Send/) ? 3
      : 2;
    const totalScore = critWeight + downstreamImpact + complexityScore;
    const maxScore = 12; // 5 + 2 + 5
    const normalizedScore = Math.round(totalScore / maxScore * 100);
    const rating = normalizedScore >= 70 ? 'red' : normalizedScore >= 40 ? 'amber' : 'green';
    return {
      name: g.name,
      type: g.type,
      role: g.role,
      criticalityScore: critWeight,
      downstreamImpact: hasDownstream ? 'High' : 'Low',
      complexityScore,
      totalRiskScore: normalizedScore,
      rating,
      rationale: `Role weight: ${critWeight}/5, Downstream: ${hasDownstream ? 'Yes (+2)' : 'No'}, Complexity: ${complexityScore}/5`,
    };
  });

  // 5.4: Timeline & Resource Estimation
  const EFFORT_HOURS = { source: 4, sink: 4, transform: 2, route: 3, process: 6, utility: 1 };
  const GAP_EFFORT_HOURS = { source: 16, sink: 12, transform: 8, route: 8, process: 24, utility: 4 };
  const mappedHours = mappings.filter(m => m.mapped).reduce((sum, m) => sum + (EFFORT_HOURS[m.role] || 3), 0);
  const gapHours = gaps.reduce((sum, g) => sum + (GAP_EFFORT_HOURS[g.role] || 12), 0);
  const testingHours = Math.round((mappedHours + gapHours) * 0.4);
  const totalHours = mappedHours + gapHours + testingHours;
  const weeksAt40h = Math.ceil(totalHours / 40);
  const teamSizeRec = totalHours <= 80 ? 1 : totalHours <= 200 ? 2 : totalHours <= 500 ? 3 : 4;
  const weeksWithTeam = Math.ceil(totalHours / (teamSizeRec * 32)); // 32 productive hours/week

  const timeline = {
    phases: [
      { name: 'Phase 1: Environment Setup & Review', weeks: Math.max(1, Math.ceil(weeksWithTeam * 0.15)), tasks: ['Set up Databricks workspace', 'Review NiFi flow architecture', 'Configure Unity Catalog', 'Set up CI/CD pipeline'] },
      { name: 'Phase 2: Mapped Processor Migration', weeks: Math.max(1, Math.ceil(weeksWithTeam * 0.30)), tasks: [`Migrate ${mapped} auto-mapped processors`, 'Validate PySpark equivalents', 'Unit test each processor cell'] },
      { name: 'Phase 3: Gap Resolution', weeks: Math.max(1, Math.ceil(weeksWithTeam * 0.30)), tasks: [`Implement ${gaps.length} unmapped processors`, 'Build custom UDFs where needed', 'Integration testing'] },
      { name: 'Phase 4: Testing & Validation', weeks: Math.max(1, Math.ceil(weeksWithTeam * 0.15)), tasks: ['End-to-end pipeline testing', 'Data comparison validation', 'Performance benchmarking'] },
      { name: 'Phase 5: Cutover & Monitoring', weeks: Math.max(1, Math.ceil(weeksWithTeam * 0.10)), tasks: ['Parallel run with NiFi', 'Production cutover', 'Post-migration monitoring'] },
    ],
    totalWeeks: weeksWithTeam,
    totalHours,
    mappedHours,
    gapHours,
    testingHours,
    recommendedTeamSize: teamSizeRec,
    teamComposition: teamSizeRec === 1
      ? ['1 Senior Data Engineer (PySpark + NiFi experience)']
      : teamSizeRec === 2
        ? ['1 Senior Data Engineer (PySpark lead)', '1 Data Engineer (testing & validation)']
        : teamSizeRec === 3
          ? ['1 Senior Data Engineer (PySpark lead)', '1 Data Engineer (processor migration)', '1 QA Engineer (testing & validation)']
          : ['1 Tech Lead (architecture)', '1 Senior Data Engineer (PySpark)', '1 Data Engineer (processor migration)', '1 QA Engineer (testing)'],
  };

  // 5.5: Migration Success Criteria
  const successCriteria = {
    processorCriteria: mappings.map(m => ({
      name: m.name,
      type: m.type,
      mapped: m.mapped,
      acceptanceTests: m.mapped
        ? [
            `${m.name}: Output schema matches expected structure`,
            `${m.name}: Row count within 1% tolerance of NiFi output`,
            `${m.name}: No unexpected null values in required columns`,
            m.role === 'transform' ? `${m.name}: Transformation logic produces equivalent results` : null,
            m.role === 'source' ? `${m.name}: Data ingestion completes within SLA window` : null,
            m.role === 'sink' ? `${m.name}: Target system receives all records` : null,
          ].filter(Boolean)
        : [
            `${m.name}: Manual implementation reviewed and approved`,
            `${m.name}: Custom code passes unit tests`,
            `${m.name}: Output validated against NiFi reference data`,
          ],
    })),
    pipelineCriteria: [
      { metric: 'Data Completeness', target: '>=99.5%', description: 'Total output rows / total input rows' },
      { metric: 'Schema Fidelity', target: '100%', description: 'All column names, types, and constraints match' },
      { metric: 'Processing Latency', target: 'Within 2x NiFi baseline', description: 'End-to-end pipeline execution time' },
      { metric: 'Data Quality Score', target: '>=95%', description: 'Pass rate across all quality rules' },
      { metric: 'Error Rate', target: '<0.1%', description: 'Records routed to dead letter queue' },
    ],
    comparisonMetrics: [
      { metric: 'Row Count Delta', description: 'abs(nifi_rows - databricks_rows) / nifi_rows * 100', threshold: '<1%' },
      { metric: 'Column Hash Match', description: 'MD5 hash comparison of sorted column values', threshold: '100% match' },
      { metric: 'Null Rate Delta', description: 'Per-column null percentage difference', threshold: '<0.5%' },
      { metric: 'Value Distribution', description: 'KS-test p-value for numeric columns', threshold: 'p > 0.05' },
    ],
  };

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
    byRole, byGroup, gaps, recommendations: recs, effort,
    gapPlaybook,
    processorGuides,
    riskMatrix,
    timeline,
    successCriteria,
  };
}

// ================================================================
// Helper functions for processor migration guides (5.2)
// ================================================================

/**
 * Get a human-readable description of a NiFi processor type.
 * @param {string} type — NiFi processor type
 * @returns {string}
 */
function getNiFiProcessorDescription(type) {
  const descriptions = {
    'ListenHTTP': 'Starts an HTTP(S) server and listens for incoming requests on a configurable port. Routes FlowFiles based on request method and path.',
    'HandleHTTPRequest': 'Receives HTTP requests as part of a request/response cycle. Works with HandleHTTPResponse to create REST endpoints.',
    'HandleHTTPResponse': 'Sends HTTP responses back to clients. Completes request/response cycles started by HandleHTTPRequest.',
    'ExecuteScript': 'Executes a user-provided script (Groovy, Python, Ruby, etc.) to process FlowFiles. Supports full scripting flexibility.',
    'ExecuteStreamCommand': 'Executes an external command/process, feeding FlowFile content as stdin and capturing stdout as the output FlowFile.',
    'PutEmail': 'Sends an email using SMTP. Supports HTML/text content, attachments from FlowFile content, and dynamic recipients.',
    'PutTCP': 'Sends FlowFile content over a TCP connection to a configured endpoint.',
    'PutSyslog': 'Sends FlowFile content as syslog messages to a remote syslog server.',
    'ConsumeKafka': 'Consumes messages from Apache Kafka topics. Supports consumer groups, offset management, and SSL.',
    'PublishKafka': 'Publishes FlowFile content as messages to Apache Kafka topics.',
    'GetFile': 'Reads files from a local or network file system directory.',
    'PutFile': 'Writes FlowFile content to a file on the local or network file system.',
    'GetSFTP': 'Fetches files from a remote SFTP server.',
    'PutSFTP': 'Uploads files to a remote SFTP server.',
    'GetHTTP': 'Fetches content from a configurable HTTP/HTTPS URL.',
    'InvokeHTTP': 'Sends HTTP requests and routes responses. Supports all HTTP methods, headers, and authentication.',
    'QueryDatabaseTable': 'Executes a SQL query against a database and outputs the results as FlowFiles.',
    'PutDatabaseRecord': 'Inserts, updates, or upserts records into a database table.',
  };
  return descriptions[type] || `NiFi processor that handles ${type.replace(/([A-Z])/g, ' $1').trim().toLowerCase()} operations.`;
}

/**
 * Get the Databricks equivalent approach for a NiFi processor type.
 * @param {string} type — NiFi processor type
 * @returns {{ approach: string, differences: string[], steps: string[], effort: string }}
 */
function getDatabricksEquivalent(type) {
  if (type.match(/^(Listen|Handle)HTTP/)) {
    return {
      approach: 'Databricks Model Serving or external API Gateway triggering Jobs API',
      differences: [
        'NiFi provides built-in HTTP server; Databricks uses Model Serving or external gateways',
        'Request/response cycle requires async pattern in Databricks',
        'Authentication handled differently (NiFi SSL vs Databricks tokens)',
      ],
      steps: [
        'Create a Model Serving endpoint or set up API Gateway',
        'Implement request processing as a Databricks job',
        'Configure authentication and rate limiting',
        'Set up monitoring and alerting',
      ],
      effort: 'High (8-16 hours)',
    };
  }
  if (type.match(/^Execute(Script|Stream)/)) {
    return {
      approach: 'PySpark UDFs, notebook jobs, or %sh magic commands',
      differences: [
        'NiFi scripts run per-FlowFile; PySpark UDFs run per-row in parallel',
        'NiFi supports Groovy/Ruby/Python; Databricks primarily uses Python/Scala/SQL',
        'State management differs significantly',
      ],
      steps: [
        'Analyze the original script logic and identify pure functions',
        'Translate to PySpark UDF or DataFrame operations',
        'Handle state management through Delta tables if needed',
        'Test with representative data samples',
      ],
      effort: 'High (12-24 hours per script)',
    };
  }
  if (type.match(/^(Put|Send)(Email|TCP|Syslog)/)) {
    return {
      approach: 'Databricks workflow notifications, external API calls, or webhook integrations',
      differences: [
        'NiFi has native protocol support; Databricks uses REST APIs or libraries',
        'Notification is typically at job level, not per-record',
        'Batching and retry logic must be explicitly implemented',
      ],
      steps: [
        'Identify notification requirements (per-record vs batch)',
        'Set up external service (SendGrid, SNS, etc.) if per-record notification needed',
        'Configure Databricks workflow notifications for job-level alerts',
        'Implement retry and dead-letter handling',
      ],
      effort: 'Low-Medium (4-8 hours)',
    };
  }
  return {
    approach: 'Custom PySpark implementation with equivalent business logic',
    differences: [
      'NiFi processor handles data record-by-record; PySpark processes in batches',
      'Error handling patterns differ (NiFi relationships vs try/except)',
      'Configuration management through widgets instead of NiFi properties',
    ],
    steps: [
      'Review NiFi processor documentation and properties',
      'Design equivalent PySpark logic (DataFrame operations preferred)',
      'Implement error handling and dead-letter routing',
      'Validate output against NiFi reference data',
    ],
    effort: 'Medium (6-12 hours)',
  };
}
