/**
 * mappers/index.js -- Main NiFi-to-Databricks mapping orchestrator
 *
 * Extracted from index.html lines 3990-5396.
 * This is the core conversion engine that takes parsed NiFi flow data
 * and produces Databricks PySpark code for each processor.
 *
 * Orchestrates:
 *  1. Connection graph building
 *  2. Streaming context propagation
 *  3. Template resolution from NIFI_DATABRICKS_MAP
 *  4. Smart code generation via role-specific handlers
 *  5. Controller service resolution
 *  6. Post-processing (EL, streaming, retry, DLQ, PHI)
 */

import { sanitizeVarName } from '../utils/string-helpers.js';
import { classifyNiFiProcessor } from './processor-classifier.js';
import { resolveTemplate } from './template-resolver.js';
import { propagateStreamingContext, wrapBatchSinkForStreaming } from './streaming-propagator.js';
import { resolveControllerService } from './controller-service.js';
import { postProcessELInCode } from './post-process-el.js';

// Handler imports
import { handleSourceProcessor } from './handlers/source-handlers.js';
import { handleTransformProcessor } from './handlers/transform-handlers.js';
import { handleRouteProcessor } from './handlers/route-handlers.js';
import { handleSinkProcessor } from './handlers/sink-handlers.js';
import { handleProcessProcessor } from './handlers/process-handlers.js';
import { handleUtilityProcessor } from './handlers/utility-handlers.js';
import { handleAWSProcessor } from './handlers/cloud-aws-handlers.js';
import { handleAzureProcessor } from './handlers/cloud-azure-handlers.js';
import { handleGCPProcessor } from './handlers/cloud-gcp-handlers.js';
import { handleDatabaseProcessor } from './handlers/database-handlers.js';
import { handleMessagingProcessor } from './handlers/messaging-handlers.js';
import { handleMonitoringProcessor } from './handlers/monitoring-handlers.js';
import { handleNoSQLProcessor } from './handlers/nosql-handlers.js';
import { handleHadoopProcessor } from './handlers/hadoop-handlers.js';

/**
 * Map an entire NiFi flow to Databricks PySpark code.
 *
 * NOTE: This function depends on several external functions that must be
 * provided by the caller or imported from their respective modules once
 * they are extracted:
 *  - NIFI_DATABRICKS_MAP (constants)
 *  - ROLE_FALLBACK_TEMPLATES (constants)
 *  - parseVariableRegistry, resolveVariables (parsers)
 *  - translateNELtoPySpark (parsers/nel)
 *  - generateRetryWrapper, wrapSubprocessAsPandasUDF, detectPHIFields (analyzers)
 *  - generateDLQWrapper (analyzers)
 *
 * @param {object} nifi - Parsed NiFi flow data
 * @param {object} deps - External dependencies
 * @param {object} deps.NIFI_DATABRICKS_MAP - Processor type to Databricks template map
 * @param {object} deps.ROLE_FALLBACK_TEMPLATES - Fallback templates by role
 * @param {function} deps.parseVariableRegistry - Variable registry parser
 * @param {function} deps.resolveVariables - Variable resolver
 * @param {function} deps.translateNELtoPySpark - NEL expression translator
 * @param {function} deps.generateRetryWrapper - Retry wrapper generator
 * @param {function} deps.wrapSubprocessAsPandasUDF - Subprocess to Pandas UDF converter
 * @param {function} deps.detectPHIFields - PHI field detector
 * @returns {Array<object>} - Array of mapping results, one per processor
 */
export function mapNiFiToDatabricks(nifi, deps) {
  const {
    NIFI_DATABRICKS_MAP,
    ROLE_FALLBACK_TEMPLATES,
    parseVariableRegistry,
    resolveVariables,
    translateNELtoPySpark,
    generateRetryWrapper,
    wrapSubprocessAsPandasUDF,
    detectPHIFields
  } = deps;

  window._lastParsedNiFi = nifi;

  const controllerServices = nifi.controllerServices || [];

  // REC #4: Parse variable registry & parameter contexts
  const _variableRegistry = parseVariableRegistry(nifi);
  const _hasVars = Object.keys(_variableRegistry).length > 0;

  const processors = nifi.processors || [];
  const conns = nifi.connections || [];

  // Build connection graph
  const connGraph = {};
  conns.forEach(c => {
    if (!connGraph[c.destinationName]) connGraph[c.destinationName] = {inputs:[],outputs:[]};
    connGraph[c.destinationName].inputs.push(c.sourceName);
    if (!connGraph[c.sourceName]) connGraph[c.sourceName] = {inputs:[],outputs:[]};
    connGraph[c.sourceName].outputs.push(c.destinationName);
  });

  // FIX #2: Track which processors are in streaming context
  const _streamingProcs = propagateStreamingContext(processors, conns);

  // Controller service cache
  const _csCache = {};
  function _resolveCS(name) {
    if (_csCache[name]) return _csCache[name];
    const resolved = resolveControllerService(name, controllerServices);
    if (resolved) _csCache[name] = resolved;
    return resolved;
  }

  return processors.map(p => {
    const role = classifyNiFiProcessor(p.type);
    const mapEntry = NIFI_DATABRICKS_MAP[p.type];
    const varName = sanitizeVarName(p.name);
    const inputProcs = (connGraph[p.name] && connGraph[p.name].inputs) || [];
    const inputVar = inputProcs.length ? sanitizeVarName(inputProcs[0]) : 'input';

    if (mapEntry) {
      // Step 1: Resolve template
      const props = p.properties || {};

      // REC #4: Resolve variables in property values
      if (_hasVars) {
        for (const [k, v] of Object.entries(props)) {
          if (typeof v === 'string') props[k] = resolveVariables(v, _variableRegistry);
        }
      }

      let { code, conf } = resolveTemplate(
        mapEntry, varName, inputVar, inputProcs, props,
        translateNELtoPySpark,
        (c, n) => postProcessELInCode(c, n, translateNELtoPySpark),
        p.name
      );

      // Replace remaining unresolved template placeholders
      code = code.replace(/\{(\w+)\}/g, '<$1>');

      // Step 2: Smart code generation via handlers (in priority order)
      // Each handler returns { code, conf } or null if not handled
      const handlerArgs = [p, props, varName, inputVar, code, conf];

      const result =
        handleSourceProcessor(...handlerArgs) ||
        handleSinkProcessor(...handlerArgs) ||
        handleRouteProcessor(p, props, varName, inputVar, code, conf, translateNELtoPySpark) ||
        handleTransformProcessor(p, props, varName, inputVar, code, conf, translateNELtoPySpark) ||
        handleProcessProcessor(...handlerArgs) ||
        handleUtilityProcessor(...handlerArgs) ||
        handleAWSProcessor(...handlerArgs) ||
        handleAzureProcessor(...handlerArgs) ||
        handleGCPProcessor(...handlerArgs) ||
        handleDatabaseProcessor(...handlerArgs) ||
        handleMessagingProcessor(...handlerArgs) ||
        handleMonitoringProcessor(...handlerArgs) ||
        handleNoSQLProcessor(...handlerArgs) ||
        handleHadoopProcessor(...handlerArgs);

      if (result) {
        code = result.code;
        conf = result.conf;
      }

      // Step 3: Controller service resolution enhancements
      // Enhance DB processors with resolved JDBC URLs
      if (/^(ExecuteSQL|QueryDatabase|GenerateTableFetch|PutDatabaseRecord|PutSQL|SelectHiveQL)/.test(p.type)) {
        const poolName = props['Database Connection Pooling Service'] || props['JDBC Connection Pool'] || '';
        const csInfo = _resolveCS(poolName);
        if (csInfo && csInfo.jdbcUrl) {
          const realUrl = csInfo.jdbcUrl;
          const realDriver = csInfo.driver || '';
          const realUser = csInfo.user || '';
          const query = props['SQL select query'] || props['SQL Statement'] || '';
          const table = props['Table Name'] || '';
          if (/ExecuteSQL|QueryDatabase|GenerateTableFetch|SelectHiveQL/.test(p.type)) {
            const sqlOrTable = query ? `"(${query.replace(/"/g, '\\"').substring(0, 300)}) AS subq"` : `"${table}"`;
            code = `# SQL: ${p.name} [CS: ${csInfo.name}]\n# JDBC: ${realUrl}\n# Driver: ${realDriver}\ndf_${varName} = (spark.read\n  .format("jdbc")\n  .option("url", "${realUrl}")\n  .option("dbtable", ${sqlOrTable})\n  .option("driver", "${realDriver}")\n  .option("user", dbutils.secrets.get(scope="db", key="${realUser || 'user'}"))\n  .option("password", dbutils.secrets.get(scope="db", key="pass"))\n  .load()\n)\nprint(f"[SQL] Read from ${table || 'query'} via ${csInfo.name}")`;
            conf = 0.95;
          } else {
            const fullTable = (props['Schema Name'] || '') ? `${props['Schema Name']}.${table}` : table;
            code = `# DB Write: ${p.name} [CS: ${csInfo.name}]\n# JDBC: ${realUrl}\n(df_${inputVar}.write\n  .format("jdbc")\n  .option("url", "${realUrl}")\n  .option("dbtable", "${fullTable}")\n  .option("driver", "${realDriver}")\n  .option("user", dbutils.secrets.get(scope="db", key="${realUser || 'user'}"))\n  .option("password", dbutils.secrets.get(scope="db", key="pass"))\n  .option("batchsize", 1000)\n  .mode("append")\n  .save()\n)\nprint(f"[DB] Wrote to ${fullTable} via ${csInfo.name}")`;
            conf = 0.95;
          }
        }
      }

      // Resolve Kafka controller services
      if (/Kafka/.test(p.type)) {
        const csName = props['Kafka Client Service'] || '';
        const csInfo = _resolveCS(csName);
        if (csInfo && csInfo.props) {
          const resolvedBrokers = csInfo.props['bootstrap.servers'] || csInfo.props['Kafka Brokers'] || '';
          if (resolvedBrokers && !code.includes(resolvedBrokers)) {
            code = code.replace(/kafka[_.]?[a-z]*:9092/gi, resolvedBrokers);
          }
        }
      }

      // Resolve Record Reader/Writer format from controller service
      if (/ConvertRecord|ValidateRecord|LookupRecord|PutRecord|QueryRecord|PartitionRecord|SplitRecord|ForkRecord|SampleRecord|UpdateRecord/.test(p.type)) {
        const readerName = props['Record Reader'] || '';
        const writerName = props['Record Writer'] || '';
        const readerCS = _resolveCS(readerName);
        const writerCS = _resolveCS(writerName);
        const inFmt = readerCS ? readerCS.format || 'json' : (/CSV/i.test(readerName) ? 'csv' : /Avro/i.test(readerName) ? 'avro' : 'json');
        const outFmt = writerCS ? writerCS.format || 'json' : (/CSV/i.test(writerName) ? 'csv' : /Avro/i.test(writerName) ? 'avro' : 'json');
        if (!code || code.includes('{')) {
          code = `# ${p.type}: ${p.name}\n# Reader: ${readerName} (${inFmt}) | Writer: ${writerName} (${outFmt})\ndf_${varName} = df_${inputVar}\n# Input format: ${inFmt}, Output format: ${outFmt}\n# Write: df_${varName}.write.format("${outFmt}").save("/path/output")\nprint(f"[RECORD] ${p.type} — ${inFmt} -> ${outFmt}")`;
          conf = 0.93;
        }
      }

      // Step 4: Post-processing pipeline
      // FIX #2: Wrap batch sinks in foreachBatch if upstream is streaming
      const _isStreaming = _streamingProcs.has(p.name);
      code = wrapBatchSinkForStreaming(code, p.name, varName, inputVar, _isStreaming);
      if (_isStreaming && !code.includes('readStream') && !code.includes('writeStream') && !code.includes('foreachBatch')) {
        code = '# \u26a0 STREAMING CONTEXT: upstream is a streaming source\n# Ensure all operations are streaming-compatible\n' + code;
      }

      // REC #8: Wrap network operations with retry logic
      const _penalty = props['Penalty Duration'] || '30 sec';
      const _yield = props['Yield Duration'] || '1 sec';
      const _retries = props['Max Retries'] || props['Retry Count'] || '3';
      code = generateRetryWrapper(p.name, p.type, code, _penalty, _yield, parseInt(_retries) || 3);

      // GAP #10: Convert per-row subprocess.run() to Pandas UDF
      code = wrapSubprocessAsPandasUDF(code, p.name, varName, inputVar, props);

      // GAP #13: PHI/HIPAA detection
      const _phiFields = detectPHIFields(props);
      if (_phiFields.length > 0) {
        code = '# \u26a0 PHI/HIPAA WARNING: Protected health information detected\n' +
          '# Fields: ' + _phiFields.map(f => f.key + ' (' + f.category + ')').join(', ') + '\n' +
          '# All PHI fields are hashed (SHA-256) in DLQ writes\n' + code;
      }

      return {
        name: p.name, type: p.type, group: p.group, role, mapped: true,
        confidence: conf, category: mapEntry.cat, code, desc: mapEntry.desc,
        notes: mapEntry.notes, imports: mapEntry.imp || [], state: p.state
      };
    }

    // Fallback: no direct mapping
    const fb = ROLE_FALLBACK_TEMPLATES[role] || ROLE_FALLBACK_TEMPLATES.process;
    const fbCode = `${fb.tpl}\n# Original: ${p.name} (${p.type}) in ${p.group || 'root'}`;
    return {
      name: p.name, type: p.type, group: p.group, role, mapped: false,
      confidence: fb.conf, category: 'Manual Migration', code: fbCode,
      desc: fb.desc, notes: 'Role-based template. Manual implementation required.',
      imports: [], state: p.state,
      gapReason: `No direct mapping - ${role}-based template provided`,
      fallbackUsed: true
    };
  });
}
