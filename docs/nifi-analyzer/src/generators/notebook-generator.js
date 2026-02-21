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

  // Validate generated code
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
