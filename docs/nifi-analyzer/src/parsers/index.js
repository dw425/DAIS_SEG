// ================================================================
// parsers/index.js — Format-detection dispatcher for NiFi flow files
// Dispatches to the correct parser based on file extension, then XML vs JSON detection
// ================================================================

import { parseNiFiXML } from './nifi-xml-parser.js';
import { parseNiFiRegistryJSON } from './nifi-registry-json.js';
import { cleanInput } from './clean-input.js';
import {
  decompressGzip,
  extractZipContents,
  extractTarGz,
  parseSqlFile,
  extractDocxText,
  extractXlsxData,
  buildDocumentFlow
} from './format-handlers.js';

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

// Re-export format handlers
export {
  decompressGzip,
  extractZipContents,
  extractTarGz,
  parseSqlFile,
  extractDocxText,
  extractXlsxData,
  buildDocumentFlow
} from './format-handlers.js';

/**
 * Detect input format and dispatch to the appropriate parser.
 *
 * Detection logic:
 *   1. Check filename extension for binary/special formats (.gz, .zip, .sql, etc.)
 *   2. For binary formats, use options.bytes (Uint8Array) to decompress/extract
 *   3. For text formats, clean the raw content (BOM, line endings, smart quotes, etc.)
 *   4. If content starts with '<' or '<?xml', treat as XML
 *   5. If content starts with '{' or '[', treat as JSON
 *   6. Otherwise, attempt XML parse first, fall back to JSON
 *
 * @param {string} raw - Raw file content (may be empty for binary-only files)
 * @param {string} filename - Original filename (used for display and format hints)
 * @param {Object} [options={}] - Additional options
 * @param {Uint8Array} [options.bytes] - Raw bytes for binary file formats
 * @returns {Promise<{ source_name: string, source_type: string, _nifi: Object, parse_warnings: string[] }>}
 * @throws {Error} If content cannot be parsed
 */
export async function parseFlow(raw, filename, options = {}) {
  const lower = (filename || '').toLowerCase();
  const bytes = options.bytes || null;

  // ---- 1. TAR.GZ / TGZ (check before .gz since .tar.gz ends with .gz) ----
  if (lower.endsWith('.tar.gz') || lower.endsWith('.tgz')) {
    if (!bytes) throw new Error('Binary data required for .tar.gz/.tgz files');
    const entries = await extractTarGz(bytes);
    if (entries.length === 0) throw new Error('No parseable text files found in tar.gz archive');
    // Parse the highest-priority file
    const best = entries[0];
    return parseFlow(best.content, best.filename);
  }

  // ---- 2. GZIP (.gz, .xml.gz, .json.gz) ----
  if (lower.endsWith('.gz')) {
    if (!bytes) throw new Error('Binary data required for .gz files');
    const decompressed = await decompressGzip(bytes);
    // Derive inner filename by removing .gz
    const innerName = filename.replace(/\.gz$/i, '') || filename;
    return parseFlow(decompressed, innerName);
  }

  // ---- 3. ZIP / NAR / JAR ----
  if (lower.endsWith('.zip') || lower.endsWith('.nar') || lower.endsWith('.jar')) {
    if (!bytes) throw new Error('Binary data required for .zip/.nar/.jar files');
    const entries = await extractZipContents(bytes);
    if (entries.length === 0) throw new Error('No parseable text files found in archive');
    // Parse the highest-priority file (sorted by NiFi relevance)
    const best = entries[0];
    return parseFlow(best.content, best.filename);
  }

  // ---- 4. DOCX ----
  if (lower.endsWith('.docx')) {
    if (!bytes) throw new Error('Binary data required for .docx files');
    const { text, tables } = await extractDocxText(bytes);
    if (!text && tables.length === 0) throw new Error('No content found in DOCX file');
    return buildDocumentFlow(text, tables, filename);
  }

  // ---- 5. XLSX ----
  if (lower.endsWith('.xlsx')) {
    if (!bytes) throw new Error('Binary data required for .xlsx files');
    const { sheets, sharedStrings } = await extractXlsxData(bytes);
    // Convert sheet data to a table format compatible with buildDocumentFlow
    const text = sheets.map(row => row.join('\t')).join('\n');
    const tables = sheets.length > 0 ? [sheets] : [];
    return buildDocumentFlow(text, tables, filename);
  }

  // ---- 6. SQL files ----
  if (lower.endsWith('.sql')) {
    const content = raw || '';
    if (!content.trim()) throw new Error('Empty SQL file');
    return parseSqlFile(content, filename);
  }

  // ---- 7. Standard text-based formats (XML, JSON, CSV, TXT) ----
  const content = cleanInput(raw || '');
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
