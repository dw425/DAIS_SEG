// ================================================================
// parsers/index.js — Format-detection dispatcher for NiFi flow files
// Dispatches to the correct parser based on XML vs JSON detection
// ================================================================

import { parseNiFiXML } from './nifi-xml-parser.js';
import { parseNiFiRegistryJSON } from './nifi-registry-json.js';
import { cleanInput } from './clean-input.js';

// Re-export sub-modules
export { parseNiFiXML } from './nifi-xml-parser.js';
export { parseNiFiRegistryJSON } from './nifi-registry-json.js';
export { cleanInput } from './clean-input.js';
export { getChildText, extractProperties } from './nifi-xml-helpers.js';
export { addSqlTableMeta, extractColumnsFromSQL, convertImpalaSqlToSpark } from './sql-extractor.js';
export { parseDDL } from './ddl-parser.js';
export { buildSqlTableBlueprints } from './sql-table-builder.js';
export {
  translateNELtoPySpark,
  translateNiFiELtoPython,
  parseNELExpression,
  tokenizeNELChain,
  resolveNELVariableContext,
  resolveNELArg,
  extractNELFuncArgs,
  unquoteArg,
  applyNELFunction,
  javaDateToPython,
  evaluateNiFiEL,
  parseVariableRegistry,
  resolveVariables
} from './nel/index.js';

/**
 * Detect input format (XML vs JSON) and dispatch to the appropriate parser.
 *
 * Detection logic:
 *   1. Clean the raw content (BOM, line endings, smart quotes, etc.)
 *   2. If content starts with '<' or '<?xml', treat as XML
 *   3. If content starts with '{' or '[', treat as JSON
 *   4. Otherwise, attempt XML parse first, fall back to JSON
 *
 * @param {string} raw - Raw file content
 * @param {string} filename - Original filename (used for display and format hints)
 * @returns {{ source_name: string, source_type: string, _nifi: Object, parse_warnings: string[] }}
 * @throws {Error} If content cannot be parsed as either XML or JSON
 */
export function parseFlow(raw, filename) {
  const content = cleanInput(raw);
  if (!content) {
    throw new Error('Empty or invalid file content');
  }

  const trimmed = content.trimStart();

  // XML detection: starts with < or <?xml
  if (trimmed.startsWith('<') || trimmed.startsWith('<?xml')) {
    const parser = new DOMParser();
    const doc = parser.parseFromString(content, 'text/xml');
    const parseError = doc.querySelector('parsererror');
    if (parseError) {
      throw new Error('XML parse error: ' + parseError.textContent.substring(0, 200));
    }
    const result = parseNiFiXML(doc, filename);
    return {
      source_name: filename,
      source_type: 'nifi_xml',
      _nifi: {
        processors: result.processors,
        connections: result.connections,
        controllerServices: result.controllerServices,
        processGroups: result.processGroups,
        idToName: result.idToName,
        clouderaTools: [],
        deepPropertyInventory: {
          filePaths: {}, urls: {}, jdbcUrls: {}, nifiEL: {},
          cronExprs: {}, credentialRefs: {}, hostPorts: {},
          dataFormats: new Set(), encodings: new Set()
        },
        sqlTables: [],
        sqlTableMeta: {}
      },
      tables: result.tables,
      parse_warnings: [],
      _deferredProcessorWork: null,
      _rawXml: content
    };
  }

  // JSON detection: starts with { or [
  if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
    let json;
    try {
      json = JSON.parse(content);
    } catch (e) {
      throw new Error('JSON parse error: ' + e.message);
    }

    // Determine the root flow data object
    // NiFi Registry exports may wrap in { "flowContents": {...} } or { "snapshotMetadata": ..., "flowContents": {...} }
    const flowData = json.flowContents || json.flow?.flowContents || json.processGroupFlow?.flow || json;

    return parseNiFiRegistryJSON(flowData, filename);
  }

  // Fallback: try XML first, then JSON
  try {
    const parser = new DOMParser();
    const doc = parser.parseFromString(content, 'text/xml');
    if (!doc.querySelector('parsererror')) {
      const result = parseNiFiXML(doc, filename);
      return {
        source_name: filename,
        source_type: 'nifi_xml',
        _nifi: {
          processors: result.processors,
          connections: result.connections,
          controllerServices: result.controllerServices,
          processGroups: result.processGroups,
          idToName: result.idToName,
          clouderaTools: [],
          deepPropertyInventory: {
            filePaths: {}, urls: {}, jdbcUrls: {}, nifiEL: {},
            cronExprs: {}, credentialRefs: {}, hostPorts: {},
            dataFormats: new Set(), encodings: new Set()
          },
          sqlTables: [],
          sqlTableMeta: {}
        },
        tables: result.tables,
        parse_warnings: [],
        _deferredProcessorWork: null,
        _rawXml: content
      };
    }
  } catch (e) { /* fall through to JSON attempt */ }

  try {
    const json = JSON.parse(content);
    const flowData = json.flowContents || json.flow?.flowContents || json.processGroupFlow?.flow || json;
    return parseNiFiRegistryJSON(flowData, filename);
  } catch (e) {
    throw new Error('Unable to parse file as XML or JSON. Ensure the file is a valid NiFi template, flow definition, or registry export.');
  }
}
