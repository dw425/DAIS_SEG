/**
 * generators/notebook-generator.js — Databricks Notebook Generator
 *
 * Orchestrates all cell builders to produce a complete Databricks notebook
 * from NiFi processor mappings. Includes 10 improvements: lineage, smart
 * imports, topo-sort, adaptive code, error framework, auto recovery,
 * full properties, relationship routing, execution report, and E2E validation.
 *
 * Extracted from index.html lines 5764-5918.
 *
 * @module generators/notebook-generator
 */

import { sanitizeVarName } from '../utils/string-helpers.js';
import { buildDataFrameLineage } from './lineage-builder.js';
import { collectSmartImports } from './cell-builders/imports-cell.js';
import { topologicalSortMappings } from './topo-sort.js';
import { extractFullProperties } from './full-properties.js';
import { buildHeaderCell } from './cell-builders/header-cell.js';
import { buildConfigCell } from './cell-builders/config-cell.js';
import { buildSchemaCell } from './cell-builders/schema-cell.js';
import { buildProcessorCell } from './cell-builders/processor-cell.js';
import { generateExecutionReportCell } from './cell-builders/execution-report.js';
import { generateValidationCell } from './cell-builders/validation-cell.js';
import { generateLoopFromCycle } from './cycle-loop-generator.js';
import { resolveNotebookPlaceholders } from './placeholder-resolver.js';
import { validateGeneratedCode } from './code-validator.js';
import { validateNotebookCode } from './databricks-validator.js';
import { scrubGeneratedCode } from './code-scrubber.js';
import { checkRuntimeCompat, generateRuntimeCheck } from './runtime-compat.js';
import { buildPreflightCell } from './cell-builders/preflight-cell.js';

/**
 * Generate a complete Databricks notebook from NiFi processor mappings.
 *
 * @param {Array<Object>} mappings — processor mapping objects
 * @param {Object} nifi — parsed NiFi flow (processors[], connections[], processGroups[])
 * @param {Object} [blueprint] — optional blueprint with .tables[]
 * @param {Object} [cfg] — Databricks config overrides
 * @returns {{ cells: Array, flowName: string, lineage: Object, metadata: Object }}
 */
export function generateDatabricksNotebook(mappings, nifi, blueprint, cfg) {
  cfg = cfg || {};
  const catalogName = cfg.catalog || '';
  const schemaName = cfg.schema || 'nifi_migration';
  const qualifiedSchema = catalogName ? `${catalogName}.${schemaName}` : schemaName;
  const cells = [];

  // IMPROVEMENT #1: Build DataFrame lineage
  const lineage = buildDataFrameLineage(mappings, nifi);

  // IMPROVEMENT #2: Smart Import Manager
  const smartImports = collectSmartImports(mappings, nifi);

  // IMPROVEMENT #3: Topological sort mappings by connection graph
  const sortedMappings = topologicalSortMappings(mappings, nifi);

  // IMPROVEMENT #7: Extract full properties
  const fullProps = {};
  sortedMappings.forEach(m => { fullProps[m.name] = extractFullProperties(m, nifi); });

  const flowName = (nifi.processGroups && nifi.processGroups[0] ? nifi.processGroups[0].name : 'NiFi Flow');
  const mapCount = sortedMappings.filter(m => m.mapped).length;
  const covPct = Math.round(mapCount / Math.max(sortedMappings.length, 1) * 100);

  // Header
  cells.push(buildHeaderCell({
    flowName,
    totalProcessors: sortedMappings.length,
    mappedCount: mapCount,
    coveragePct: covPct,
    qualifiedSchema
  }));

  // IMPROVEMENT #11: Notebook Structure Overview Cell (4.2)
  const overviewLines = [
    `# Notebook Overview: ${flowName}`,
    `# Generated: ${new Date().toISOString().split('T')[0]}`,
    `# Processors: ${sortedMappings.length} total | ${mapCount} mapped | ${sortedMappings.length - mapCount} manual`,
    `# Coverage: ${covPct}% | Target: ${qualifiedSchema}`,
    '#',
    '# Cell Structure:',
    '#   1. Header & Overview          - Migration metadata and notebook map',
    '#   2. Environment Parameters     - Widget-based dev/staging/prod config',
    '#   3. Package Installation        - pip install for third-party libraries',
    '#   4. Imports & Configuration     - PySpark imports, catalog/schema setup',
    '#   5. Pre-flight Validation       - Verify cluster, packages, secrets',
    '#   6. Execution Framework         - Tracking tables, dead letter queue',
    '#   7. Schema DDL                  - Unity Catalog table definitions',
    '#   8. DataFrame Lineage Map       - Variable dependency graph',
  ];
  let overviewCellIdx = 9;
  const groupEntries = Object.entries(
    (() => { const g = {}; sortedMappings.forEach(m => { const gn = m.group || '(root)'; if (!g[gn]) g[gn] = []; g[gn].push(m); }); return g; })()
  );
  groupEntries.forEach(([gName, procs]) => {
    overviewLines.push(`#   ${overviewCellIdx}. Process Group: ${gName} (${procs.length} processors)`);
    overviewCellIdx++;
    procs.forEach(m => {
      const status = m.mapped ? (m.confidence >= 0.8 ? 'AUTO' : 'PARTIAL') : 'MANUAL';
      const deps = (lineage[m.name]?.inputVars || []).map(v => v.procName).join(', ') || 'none';
      overviewLines.push(`#       ${overviewCellIdx}. [${status}] ${m.name} (${m.type}) -> deps: ${deps}`);
      overviewCellIdx++;
    });
  });
  overviewLines.push(`#   ${overviewCellIdx}. Execution Report`);
  overviewLines.push(`#   ${overviewCellIdx + 1}. End-to-End Validation`);
  overviewLines.push(`#   ${overviewCellIdx + 2}. Pipeline Complete`);
  overviewLines.push('#');
  overviewLines.push(`# Compute: ${cfg.computeType || 'cluster'} | Runtime: DBR ${cfg.runtimeVersion || '14.3'}`);
  overviewLines.push(`# Cloud: ${cfg.cloudProvider || 'azure'} | Node: ${cfg.nodeType || 'Standard_DS3_v2'}`);
  cells.push({
    type: 'code', label: 'Notebook Overview',
    source: overviewLines.join('\n') + '\nprint("[OVERVIEW] Notebook structure loaded")',
    role: 'config'
  });

  // IMPROVEMENT #12: Environment Parameter Cell (4.3)
  const envParamCode = [
    '# Environment Parameters - Widget-based configuration',
    '# Toggle between dev/staging/prod environments',
    'dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Target Environment")',
    'dbutils.widgets.text("catalog_override", "", "Catalog Override (blank=use default)")',
    'dbutils.widgets.text("schema_override", "", "Schema Override (blank=use default)")',
    'dbutils.widgets.dropdown("dry_run", "false", ["true", "false"], "Dry Run Mode")',
    'dbutils.widgets.dropdown("log_level", "INFO", ["DEBUG", "INFO", "WARN", "ERROR"], "Log Level")',
    '',
    '_ENV = dbutils.widgets.get("environment")',
    '_DRY_RUN = dbutils.widgets.get("dry_run") == "true"',
    '_LOG_LEVEL = dbutils.widgets.get("log_level")',
    '',
    '# Environment-specific configuration',
    '_ENV_CONFIG = {',
    `    "dev":     {"catalog": "${catalogName || 'dev_catalog'}",     "schema": "${schemaName}_dev",     "parallel": 2},`,
    `    "staging": {"catalog": "${catalogName || 'staging_catalog'}", "schema": "${schemaName}_staging", "parallel": 4},`,
    `    "prod":    {"catalog": "${catalogName || catalogName}",       "schema": "${schemaName}",         "parallel": 8},`,
    '}',
    '',
    '# Apply overrides',
    '_active_cfg = _ENV_CONFIG[_ENV]',
    '_cat_override = dbutils.widgets.get("catalog_override")',
    '_sch_override = dbutils.widgets.get("schema_override")',
    'if _cat_override: _active_cfg["catalog"] = _cat_override',
    'if _sch_override: _active_cfg["schema"] = _sch_override',
    '',
    'ACTIVE_CATALOG = _active_cfg["catalog"]',
    'ACTIVE_SCHEMA = _active_cfg["schema"]',
    '',
    'if _DRY_RUN:',
    '    print(f"[DRY RUN] Environment: {_ENV} | Catalog: {ACTIVE_CATALOG} | Schema: {ACTIVE_SCHEMA}")',
    'else:',
    '    print(f"[LIVE] Environment: {_ENV} | Catalog: {ACTIVE_CATALOG} | Schema: {ACTIVE_SCHEMA}")',
  ].join('\n');
  cells.push({
    type: 'code', label: 'Environment Parameters',
    source: envParamCode,
    role: 'config'
  });

  // Pip install cell (if third-party packages are needed)
  if (smartImports.pipCell) {
    cells.push(smartImports.pipCell);
  }

  // Smart Imports & Config cell (IMPROVEMENT #2)
  cells.push(buildConfigCell({
    smartImportsCode: smartImports.code,
    catalogName,
    schemaName,
    secretScope: cfg.secretScope
  }));

  // Pre-flight validation cell
  const preflightPkgs = (smartImports.packages || []).filter(p => p !== 'pyspark');
  const preflightSecrets = [];
  // Scan mappings for dbutils.secrets.get references
  sortedMappings.forEach(m => {
    if (!m.code) return;
    const secretMatches = m.code.matchAll(/dbutils\.secrets\.get\s*\(\s*scope\s*=\s*["']([^"']+)["']\s*,\s*key\s*=\s*["']([^"']+)["']/g);
    for (const sm of secretMatches) {
      preflightSecrets.push(sm[2]);
    }
  });
  cells.push(buildPreflightCell(
    { catalog: catalogName, schema: schemaName, secretScope: cfg.secretScope },
    preflightPkgs,
    [...new Set(preflightSecrets)]
  ));

  // Execution tracking tables (IMPROVEMENT #5)
  cells.push({
    type: 'code', label: 'Execution Framework Setup',
    source: `# Execution Tracking Framework\nfrom datetime import datetime\n\nspark.sql(f"""\nCREATE TABLE IF NOT EXISTS ${qualifiedSchema}.__execution_log (\n  processor_name STRING, processor_type STRING, role STRING,\n  timestamp TIMESTAMP DEFAULT current_timestamp(), status STRING,\n  error_message STRING, rows_processed LONG, duration STRING,\n  confidence INT, upstream_procs STRING\n) USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')\n""")\n\nspark.sql(f"""\nCREATE TABLE IF NOT EXISTS ${qualifiedSchema}.__dead_letter_queue (\n  source_processor STRING, error STRING, record_data STRING,\n  timestamp STRING, _ingested_at TIMESTAMP DEFAULT current_timestamp()\n) USING DELTA TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')\n""")\n\nspark.sql(f"""\nCREATE TABLE IF NOT EXISTS ${qualifiedSchema}.__execution_reports (\n  report_json STRING, generated_at STRING, success_rate DOUBLE, total_procs INT\n) USING DELTA\n""")\n\nspark.sql(f"""\nCREATE TABLE IF NOT EXISTS ${qualifiedSchema}.__validation_reports (\n  report_json STRING, validated_at STRING, source_rows LONG, sink_rows LONG\n) USING DELTA\n""")\n\nprint(f"[FRAMEWORK] Execution tracking ready: ${qualifiedSchema}")`,
    role: 'utility', processor: 'Framework Setup', procType: 'Internal', confidence: 1.0, mapped: true
  });

  // Unity Catalog DDL
  const tables = (blueprint && blueprint.tables) || [];
  const schemaCell = buildSchemaCell({ catalogName, schemaName, qualifiedSchema, tables });
  if (schemaCell) cells.push(schemaCell);

  // DataFrame Lineage Map (IMPROVEMENT #1)
  const lineageSummary = Object.entries(lineage).slice(0, 30).map(([name, li]) => {
    const inputs = (li.inputVars || []).map(v => v.procName).join(', ') || '(source)';
    return `# ${li.outputVar} <- ${inputs}`;
  }).join('\n');
  cells.push({
    type: 'code', label: 'DataFrame Lineage Map',
    source: `# DataFrame Lineage Map\n${lineageSummary}\n${Object.keys(lineage).length > 30 ? '# ... and ' + (Object.keys(lineage).length - 30) + ' more' : ''}\nprint("[LINEAGE] DataFrame lineage map loaded \u2014 ${Object.keys(lineage).length} variables tracked")`,
    role: 'config'
  });

  // IMPROVEMENT #3: Process in topological order, grouped
  const groups = {};
  sortedMappings.forEach(m => { const g = m.group || '(root)'; if (!groups[g]) groups[g] = []; groups[g].push(m); });

  let cellIndex = cells.length;
  Object.entries(groups).forEach(([gName, procs]) => {
    const mapped = procs.filter(m => m.mapped).length;
    cells.push({
      type: 'md', label: gName,
      source: `## Process Group: ${gName}\n**${procs.length} processors** | ${mapped} mapped | ${procs.length - mapped} manual\n\n*Cells ordered by connection topology*`,
      role: 'config'
    });

    procs.forEach(m => {
      cellIndex++;
      cells.push(buildProcessorCell(m, {
        lineage,
        qualifiedSchema,
        nifi,
        fullProps: fullProps[m.name],
        cellIndex
      }));
    });
  });

  // IMPROVEMENT #13: Data Validation Cells (4.4) — input/output schema checks, null flags, checkpoints
  const sourceProcsForValidation = sortedMappings.filter(m => m.role === 'source' && m.mapped);
  const sinkProcsForValidation = sortedMappings.filter(m => m.role === 'sink' && m.mapped);

  if (sourceProcsForValidation.length > 0) {
    const inputValLines = [
      '# Input Data Validation',
      '# Verify schema, record counts, and null rates for source DataFrames',
      'from pyspark.sql.functions import col, count, when, lit',
      '',
      '_input_validation_results = []',
    ];
    sourceProcsForValidation.slice(0, 15).forEach(m => {
      const vn = 'df_' + sanitizeVarName(m.name);
      inputValLines.push(
        `try:`,
        `    _df = ${vn}`,
        `    _row_count = _df.count()`,
        `    _col_count = len(_df.columns)`,
        `    _null_counts = {c: _df.where(col(c).isNull()).count() for c in _df.columns[:20]}`,
        `    _null_pct = {c: round(n / max(_row_count, 1) * 100, 1) for c, n in _null_counts.items()}`,
        `    _high_nulls = [c for c, p in _null_pct.items() if p > 50]`,
        `    _result = {"processor": "${m.name}", "rows": _row_count, "columns": _col_count, "high_null_cols": _high_nulls, "schema": _df.dtypes[:10]}`,
        `    _input_validation_results.append(_result)`,
        `    _status = "WARN" if _high_nulls else "OK"`,
        `    print(f"[{_status}] ${m.name}: {_row_count} rows, {_col_count} cols" + (f", HIGH NULLS: {_high_nulls}" if _high_nulls else ""))`,
        `except NameError:`,
        `    print(f"[SKIP] ${m.name}: DataFrame not yet defined")`,
        `except Exception as _e:`,
        `    print(f"[FAIL] ${m.name}: {_e}")`,
        '',
      );
    });
    inputValLines.push('print(f"[INPUT VALIDATION] {len(_input_validation_results)} source(s) checked")');
    cells.push({
      type: 'code', label: 'Input Data Validation',
      source: inputValLines.join('\n'),
      role: 'utility', processor: 'InputValidation', procType: 'Internal', confidence: 1.0, mapped: true
    });
  }

  if (sinkProcsForValidation.length > 0) {
    const outputValLines = [
      '# Output Data Validation',
      '# Verify schema consistency, record counts, and checkpoint for sink DataFrames',
      'from pyspark.sql.functions import col, count, when, lit',
      '',
      '_output_validation_results = []',
    ];
    sinkProcsForValidation.slice(0, 15).forEach(m => {
      const vn = 'df_' + sanitizeVarName(m.name);
      outputValLines.push(
        `try:`,
        `    _df = ${vn}`,
        `    _row_count = _df.count()`,
        `    _col_count = len(_df.columns)`,
        `    _null_counts = {c: _df.where(col(c).isNull()).count() for c in _df.columns[:20]}`,
        `    _null_pct = {c: round(n / max(_row_count, 1) * 100, 1) for c, n in _null_counts.items()}`,
        `    _high_nulls = [c for c, p in _null_pct.items() if p > 50]`,
        `    _result = {"processor": "${m.name}", "rows": _row_count, "columns": _col_count, "high_null_cols": _high_nulls}`,
        `    _output_validation_results.append(_result)`,
        `    _status = "WARN" if _high_nulls else "OK"`,
        `    print(f"[{_status}] ${m.name}: {_row_count} rows, {_col_count} cols" + (f", HIGH NULLS: {_high_nulls}" if _high_nulls else ""))`,
        `except NameError:`,
        `    print(f"[SKIP] ${m.name}: DataFrame not yet defined")`,
        `except Exception as _e:`,
        `    print(f"[FAIL] ${m.name}: {_e}")`,
        '',
      );
    });
    outputValLines.push('print(f"[OUTPUT VALIDATION] {len(_output_validation_results)} sink(s) checked")');
    cells.push({
      type: 'code', label: 'Output Data Validation',
      source: outputValLines.join('\n'),
      role: 'utility', processor: 'OutputValidation', procType: 'Internal', confidence: 1.0, mapped: true
    });
  }

  // Checkpoint cell
  cells.push({
    type: 'code', label: 'Pipeline Checkpoint',
    source: [
      '# Pipeline Checkpoint',
      '# Persist intermediate state for recovery and auditing',
      'import json',
      'from datetime import datetime',
      '',
      '_checkpoint_data = {',
      '    "pipeline": "' + qualifiedSchema + '",',
      '    "checkpoint_time": datetime.now().isoformat(),',
      '    "environment": _ENV if "_ENV" in dir() else "unknown",',
      '    "dry_run": _DRY_RUN if "_DRY_RUN" in dir() else False,',
      '    "source_validations": len(_input_validation_results) if "_input_validation_results" in dir() else 0,',
      '    "sink_validations": len(_output_validation_results) if "_output_validation_results" in dir() else 0,',
      '}',
      '',
      'try:',
      '    _ckpt_df = spark.createDataFrame([{',
      '        "checkpoint_json": json.dumps(_checkpoint_data),',
      '        "checkpoint_time": datetime.now().isoformat(),',
      '        "pipeline": "' + qualifiedSchema + '"',
      '    }])',
      '    _ckpt_df.write.mode("append").saveAsTable("' + qualifiedSchema + '.__pipeline_checkpoints")',
      '    print(f"[CHECKPOINT] State saved at {_checkpoint_data[\'checkpoint_time\']}")',
      'except Exception as _e:',
      '    print(f"[CHECKPOINT] Save failed (non-fatal): {_e}")',
    ].join('\n'),
    role: 'utility', processor: 'Checkpoint', procType: 'Internal', confidence: 1.0, mapped: true
  });

  // IMPROVEMENT #9: Execution Report cell
  cells.push({
    type: 'code', label: 'Execution Report',
    source: generateExecutionReportCell(sortedMappings, qualifiedSchema),
    role: 'utility', processor: 'ExecutionReport', procType: 'Internal', confidence: 1.0, mapped: true
  });

  // IMPROVEMENT #10: E2E Validation cell
  cells.push({
    type: 'code', label: 'End-to-End Validation',
    source: generateValidationCell(sortedMappings, qualifiedSchema, lineage),
    role: 'utility', processor: 'E2EValidation', procType: 'Internal', confidence: 1.0, mapped: true
  });

  // Migration error table
  cells.push({
    type: 'sql', label: 'Migration Error Table',
    source: `CREATE TABLE IF NOT EXISTS ${qualifiedSchema}.__migration_errors (\n  processor_name STRING, processor_type STRING,\n  error_time TIMESTAMP, error_message STRING\n) USING DELTA;`,
    role: 'config'
  });

  // GAP #12: Detect cycles -> generate loop cells (before footer so they run before dbutils.notebook.exit)
  try {
    // analyzeFlowGraph may not be available as a module yet; guard import
    let analyzeFlowGraph;
    try {
      // Dynamic reference — will be wired when the analyzers module is extracted
      analyzeFlowGraph = window.analyzeFlowGraph || null;
    } catch (e) { analyzeFlowGraph = null; }

    if (analyzeFlowGraph) {
      const _graphResult = analyzeFlowGraph(nifi.processors || [], nifi.connections || []);
      if (_graphResult.circularRefs && _graphResult.circularRefs.length > 0) {
        _graphResult.circularRefs.forEach(ref => {
          const loopCode = generateLoopFromCycle(ref.cycle, sortedMappings, lineage);
          if (loopCode) {
            cells.push({
              type: 'code', label: 'Loop: ' + ref.cycle[0],
              source: loopCode, role: 'transform',
              processor: ref.cycle[0], procType: 'CycleLoop',
              confidence: 0.75, mapped: true
            });
          }
        });
      }
    }
  } catch (e) { console.warn('Cycle-to-loop generation:', e); }

  // Footer with comprehensive status (after cycle cells so dbutils.notebook.exit runs last)
  cells.push({
    type: 'code', label: 'Pipeline Complete',
    source: `# Final status\nimport json\ntry:\n    _exec_log = spark.sql("SELECT status, count(*) as cnt FROM ${qualifiedSchema}.__execution_log GROUP BY status").collect()\n    _counts = {r.status: r.cnt for r in _exec_log}\n    _ok = _counts.get("SUCCESS", 0)\n    _fail = _counts.get("FAILED", 0)\n    _recov = sum(v for k,v in _counts.items() if k in ("RECOVERED","PASSTHROUGH","DLQ"))\n    print("=" * 60)\n    print(f"PIPELINE COMPLETE: {_ok} success, {_fail} failed, {_recov} recovered")\n    print(f"Success rate: {round(_ok/max(_ok+_fail+_recov,1)*100,1)}%")\n    print("=" * 60)\n    if _fail > 0:\n        display(spark.sql("SELECT * FROM ${qualifiedSchema}.__execution_log WHERE status='FAILED'"))\n    dbutils.notebook.exit(json.dumps({"status":"COMPLETE","success":_ok,"failed":_fail,"recovered":_recov}))\nexcept Exception as _e:\n    print(f"[WARN] Status check failed: {_e}")\n    dbutils.notebook.exit("COMPLETE")`,
    role: 'utility'
  });

  // Apply placeholder resolution — always run so <catalog>.<schema> placeholders in
  // handler-generated code are resolved even when catalog is empty.
  cells.forEach(c => {
    if (!c.source) return;
    if (catalogName) {
      c.source = resolveNotebookPlaceholders(c.source, cfg);
    } else {
      // No-catalog mode: strip "<catalog>." prefix and resolve <schema> to schemaName.
      c.source = c.source
        .replace(/<catalog>\./g, '')
        .replace(/\/Volumes\/<catalog>\//g, '/Volumes/')
        .replace(/\/<catalog>\//g, '/')
        .replace(/<catalog>/g, '')
        .replace(/<schema>/g, schemaName)
        .replace(/\{schema\}/g, schemaName);
    }
  });

  // Code scrubbing: remove stubs, fix bare except, replace deprecated paths
  scrubGeneratedCode(cells);

  // Runtime compatibility check
  const targetDBR = cfg.runtimeVersion || '14.3';
  const compatResult = checkRuntimeCompat(cells, targetDBR);
  if (parseFloat(compatResult.minRequired) > 0) {
    // Insert runtime check cell near the top (after config cells)
    const runtimeCheckCode = generateRuntimeCheck(compatResult.minRequired);
    const configEndIdx = cells.findIndex(c => c.role !== 'config' && c.type !== 'md') || 3;
    cells.splice(configEndIdx, 0, {
      type: 'code', label: 'Runtime Version Check',
      source: runtimeCheckCode,
      role: 'config', processor: '__runtime_check', procType: 'RuntimeCheck',
      confidence: 1.0, mapped: true
    });
  }
  if (compatResult.issues.length > 0) {
    cells.push({
      type: 'code', label: 'Runtime Compatibility Warnings',
      source: '# Runtime Compatibility Warnings\n' +
        compatResult.issues.map(i =>
          `# Cell ${i.cell}: "${i.feature}" requires DBR ${i.minDBR}+ (target: ${targetDBR})`
        ).join('\n') +
        `\nprint("[COMPAT] ${compatResult.issues.length} feature(s) may require DBR ${compatResult.minRequired}+")`,
      role: 'utility', processor: 'CompatCheck', procType: 'Internal', confidence: 1.0, mapped: true
    });
  }

  // Databricks-specific validation (comprehensive)
  const dbxValidation = validateNotebookCode(cells);
  if (dbxValidation.issues.length > 0) {
    const issuesByGroup = {};
    dbxValidation.issues.forEach(i => {
      if (!issuesByGroup[i.severity]) issuesByGroup[i.severity] = [];
      issuesByGroup[i.severity].push(i);
    });
    const reportLines = ['# Databricks Code Validation Report',
      `# Score: ${dbxValidation.score}/100`,
      `# Issues: ${dbxValidation.issues.length} (${Object.keys(issuesByGroup).map(s => `${s}: ${issuesByGroup[s].length}`).join(', ')})`];
    dbxValidation.issues.forEach(i => {
      reportLines.push(`# [${i.severity.toUpperCase()}] Cell ${i.cell}: ${i.code} - ${i.message}`);
      reportLines.push(`#   Fix: ${i.fix}`);
    });
    cells.push({
      type: 'code', label: 'Databricks Validation Report',
      source: reportLines.join('\n') + `\nprint("[VALIDATION] Score: ${dbxValidation.score}/100, ${dbxValidation.issues.length} issues found")`,
      role: 'utility', processor: 'DbxValidator', procType: 'Internal', confidence: 1.0, mapped: true
    });
  }

  // Legacy validation (original code validator)
  const _codeValidation = validateGeneratedCode(cells.map(c => c.source || ''));
  const _validationIssues = _codeValidation.filter(v => !v.valid);
  if (_validationIssues.length > 0) {
    cells.push({
      type: 'code', label: 'Code Validation Report',
      source: '# Code Validation Report\n# ' + _validationIssues.length + ' cells with potential issues:\n' +
        _validationIssues.map(v => '# Cell ' + v.cellIndex + ': ' + v.issues.join('; ')).join('\n'),
      role: 'utility', processor: 'CodeValidator', procType: 'Internal', confidence: 1.0, mapped: true
    });
  }

  // Preserve for backward compat with monolith references
  if (typeof window !== 'undefined') {
    window._lastNotebookCells = cells;
    window._lastLineage = lineage;
  }

  return {
    cells, flowName, lineage,
    metadata: {
      processorCount: sortedMappings.length,
      mappedCount: mapCount,
      generatedAt: new Date().toISOString(),
      config: { catalog: catalogName, schema: schemaName },
      improvements: [
        'lineage', 'smartImports', 'topoSort', 'adaptiveCode', 'errorFramework',
        'autoRecovery', 'fullProperties', 'relationshipRouting', 'executionReport',
        'e2eValidation', 'nelParser', 'phiDetection', 'sharedClusters',
        'cycleDetection', 'streamingGuard'
      ]
    }
  };
}
