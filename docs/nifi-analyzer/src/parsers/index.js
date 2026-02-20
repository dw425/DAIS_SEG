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
  parseTar,
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
  parseTar,
  parseSqlFile,
  extractDocxText,
  extractXlsxData,
  buildDocumentFlow
} from './format-handlers.js';

const MAX_PARSE_SIZE = 50 * 1024 * 1024; // 50 MB

/**
 * Detect input format and dispatch to the appropriate parser.
 *
 * Detection logic:
 *   1. Check file size limit
 *   2. Check for empty file
 *   3. Magic-byte sniffing (gzip, ZIP)
 *   4. Check filename extension for binary/special formats (.gz, .zip, .sql, etc.)
 *   5. For binary formats, use options.bytes (Uint8Array) to decompress/extract
 *   6. For text formats, clean the raw content (BOM, line endings, smart quotes, etc.)
 *   7. If content starts with '<' or '<?xml', treat as XML
 *   8. If content starts with '{' or '[', treat as JSON
 *   9. Otherwise, attempt XML parse first, fall back to JSON
 *
 * @param {string|ArrayBuffer} raw - Raw file content (may be empty for binary-only files)
 * @param {string} filename - Original filename (used for display and format hints)
 * @param {Object} [options={}] - Additional options
 * @param {Uint8Array} [options.bytes] - Raw bytes for binary file formats
 * @returns {Promise<{ source_name: string, source_type: string, _nifi: Object, parse_warnings: string[] }>}
 * @throws {Error} If content cannot be parsed
 */
export async function parseFlow(raw, filename, options = {}) {
  // ---- File size limit ----
  const rawSize = typeof raw === 'string' ? raw.length : (raw?.byteLength ?? 0);
  if (rawSize > MAX_PARSE_SIZE) {
    return { processors: [], _nifi: {}, parse_warnings: ['File exceeds 50MB parse limit'] };
  }

  // ---- 0-byte / empty guard ----
  if (!raw || (raw instanceof ArrayBuffer && raw.byteLength === 0) || (typeof raw === 'string' && raw.trim().length === 0)) {
    return { processors: [], _nifi: {}, parse_warnings: ['Empty file'] };
  }

  let lower = (filename || '').toLowerCase();
  const bytes = options.bytes || null;

  // ---- Magic-byte sniffing (before extension-based dispatch) ----
  {
    const probe = new Uint8Array(
      typeof raw === 'string' ? new TextEncoder().encode(raw.slice(0, 4)) :
      raw instanceof ArrayBuffer ? new Uint8Array(raw, 0, Math.min(4, raw.byteLength)) :
      (bytes ? bytes.slice(0, 4) : new Uint8Array(0))
    );
    if (probe.length >= 2) {
      // gzip magic: 1f 8b
      if (probe[0] === 0x1f && probe[1] === 0x8b) {
        const gzBytes = bytes || new Uint8Array(typeof raw === 'string' ? new TextEncoder().encode(raw) : raw);
        const inner = await decompressGzip(gzBytes);
        if (inner && inner.error) return { processors: [], _nifi: {}, parse_warnings: [inner.error] };
        return parseFlow(inner, (filename || '').replace(/\.gz$/i, '') || 'flow');
      }
      // ZIP/PK magic: 50 4b
      if (probe[0] === 0x50 && probe[1] === 0x4b) {
        // Refine extension for proper dispatch if not already set
        let ext = 'zip';
        if (lower.endsWith('.docx')) ext = 'docx';
        else if (lower.endsWith('.xlsx')) ext = 'xlsx';
        // Let the extension-based dispatch below handle it; just ensure ext is recognized
        if (ext === 'zip' && !lower.endsWith('.zip') && !lower.endsWith('.nar') && !lower.endsWith('.jar')) {
          // Unknown extension but ZIP magic — treat as ZIP
          lower = filename.toLowerCase().replace(/\.[^.]+$/, '') + '.zip';
        }
      }
    }
  }

  // ---- 1. TAR.GZ / TGZ (check before .gz since .tar.gz ends with .gz) ----
  if (lower.endsWith('.tar.gz') || lower.endsWith('.tgz')) {
    if (!bytes) throw new Error('Binary data required for .tar.gz/.tgz files');
    const entries = await extractTarGz(bytes);
    if (entries.error) return { processors: [], _nifi: {}, parse_warnings: [entries.error] };
    if (entries.length === 0) throw new Error('No parseable text files found in tar.gz archive');
    // Parse the highest-priority file
    const best = entries[0];
    return parseFlow(best.content, best.filename);
  }

  // ---- 1b. Standalone TAR ----
  if (lower.endsWith('.tar')) {
    const bytes2 = raw instanceof ArrayBuffer ? new Uint8Array(raw) : new TextEncoder().encode(raw);
    const entries = parseTar(bytes2);
    if (entries.error) return { processors: [], _nifi: {}, parse_warnings: [entries.error] };
    if (!entries.length) return { processors: [], _nifi: {}, parse_warnings: ['No parseable files found in tar archive'] };
    // Parse best entry
    const best = entries.sort((a,b) => {
      const pri = n => /flow\.xml/i.test(n) ? 0 : /flow\.json/i.test(n) ? 1 : /template/i.test(n) ? 2 : /\.xml$/i.test(n) ? 3 : /\.json$/i.test(n) ? 4 : 5;
      return pri(a.filename) - pri(b.filename);
    })[0];
    return parseFlow(best.content, best.filename);
  }

  // ---- 2. GZIP (.gz, .xml.gz, .json.gz) ----
  if (lower.endsWith('.gz')) {
    if (!bytes) throw new Error('Binary data required for .gz files');
    const decompressed = await decompressGzip(bytes);
    if (decompressed && decompressed.error) return { processors: [], _nifi: {}, parse_warnings: [decompressed.error] };
    // Derive inner filename by removing .gz
    const innerName = (filename || '').replace(/\.gz$/i, '') || filename || 'flow';
    return parseFlow(decompressed, innerName);
  }

  // ---- 3. ZIP / NAR / JAR ----
  if (lower.endsWith('.zip') || lower.endsWith('.nar') || lower.endsWith('.jar')) {
    if (!bytes) throw new Error('Binary data required for .zip/.nar/.jar files');
    const entries = await extractZipContents(bytes);
    if (entries.error) return { processors: [], _nifi: {}, parse_warnings: [entries.error] };
    if (entries.length === 0) throw new Error('No parseable text files found in archive');
    // Parse the highest-priority file (sorted by NiFi relevance)
    const best = entries[0];
    return parseFlow(best.content, best.filename);
  }

  // ---- 4. DOCX ----
  if (lower.endsWith('.docx')) {
    if (!bytes) throw new Error('Binary data required for .docx files');
    const result = await extractDocxText(bytes);
    if (result.error) return { processors: [], _nifi: {}, parse_warnings: [result.error] };
    const { text, tables } = result;
    if (!text && tables.length === 0) throw new Error('No content found in DOCX file');
    return buildDocumentFlow(text, tables, filename);
  }

  // ---- 5. XLSX ----
  if (lower.endsWith('.xlsx')) {
    if (!bytes) throw new Error('Binary data required for .xlsx files');
    const result = await extractXlsxData(bytes);
    if (result.error) return { processors: [], _nifi: {}, parse_warnings: [result.error] };
    const { sheets } = result;
    // Flatten all sheets into rows for document flow
    const allRows = sheets.flat();
    const text = allRows.map(row => row.join('\t')).join('\n');
    const tables = allRows.length > 0 ? [allRows] : [];
    return buildDocumentFlow(text, tables, filename);
  }

  // ---- 6. SQL files ----
  if (lower.endsWith('.sql')) {
    const content = raw || '';
    if (!content.trim()) throw new Error('Empty SQL file');
    return parseSqlFile(content, filename);
  }

  // ---- 7. Unsupported ancillary formats (graceful rejection) ----
  if (/\.(csv|tsv)$/i.test(lower)) {
    return { processors: [], _nifi: {}, parse_warnings: ['CSV/TSV files are not NiFi flow definitions. Please upload a NiFi template (.xml), flow definition (.json), or flow.xml.gz archive.'] };
  }
  if (/\.(ya?ml)$/i.test(lower)) {
    return { processors: [], _nifi: {}, parse_warnings: ['YAML files are not directly supported as NiFi flow definitions. If this is a NiFi Registry export, try converting to JSON first.'] };
  }
  if (/\.(avro)$/i.test(lower)) {
    return { processors: [], _nifi: {}, parse_warnings: ['Avro files are data files, not NiFi flow definitions. Please upload a NiFi template (.xml) or flow definition (.json).'] };
  }
  if (/\.(parquet)$/i.test(lower)) {
    return { processors: [], _nifi: {}, parse_warnings: ['Parquet files are data files, not NiFi flow definitions. Please upload a NiFi template (.xml) or flow definition (.json).'] };
  }

  // ---- 8. Standard text-based formats (XML, JSON, TXT) ----
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

    // Unwrap versionedFlowSnapshot wrapper if present
    if (json.versionedFlowSnapshot) json = json.versionedFlowSnapshot;

    // Determine the root flow data object
    // NiFi Registry exports may wrap in { "flowContents": {...} } or { "snapshotMetadata": ..., "flowContents": {...} }
    const flowData = json.flowContents || json.flow?.flowContents || json.processGroupFlow?.flow || json;

    const result = parseNiFiRegistryJSON(flowData, filename);
    // Attach raw content for downstream use
    result._rawContent = typeof raw === 'string' ? raw : new TextDecoder().decode(raw);
    return result;
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
    let json = JSON.parse(content);
    // Unwrap versionedFlowSnapshot wrapper if present
    if (json.versionedFlowSnapshot) json = json.versionedFlowSnapshot;
    const flowData = json.flowContents || json.flow?.flowContents || json.processGroupFlow?.flow || json;
    const result = parseNiFiRegistryJSON(flowData, filename);
    result._rawContent = typeof raw === 'string' ? raw : new TextDecoder().decode(raw);
    return result;
  } catch (e) {
    throw new Error('Unable to parse file as XML or JSON. Ensure the file is a valid NiFi template, flow definition, or registry export.');
  }
}
