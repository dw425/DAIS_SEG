/**
 * parsers/format-handlers.js — Multi-format file handlers
 *
 * Handles decompression, archive extraction, and document parsing
 * for formats beyond plain XML/JSON.
 */

const MAX_FILE_SIZE = 50 * 1024 * 1024; // 50 MB

// ---- GZ Decompression ----
// Using pako for gzip decompression (loaded as ES module)

export async function decompressGzip(bytes) {
  try {
    if (!bytes || bytes.length === 0) return [];
    if (bytes.length > MAX_FILE_SIZE) return { error: 'File exceeds 50MB limit' };
    // bytes is Uint8Array
    // Use pako.inflate (bundled via Vite)
    const { default: pako } = await import('pako');
    const decompressed = pako.inflate(bytes, { to: 'string' });
    return decompressed;
  } catch (e) {
    return { error: 'Gzip decompression failed: ' + e.message };
  }
}

// ---- ZIP/NAR/JAR extraction ----
export async function extractZipContents(bytes) {
  try {
    if (bytes && bytes.length > MAX_FILE_SIZE) return { error: 'File exceeds 50MB limit' };
    const { default: JSZip } = await import('jszip');
    const zip = await JSZip.loadAsync(bytes);
    const results = [];

    // Look for NiFi-relevant files in the archive
    const priorities = [
      /flow\.xml$/i,
      /flow\.json$/i,
      /template.*\.xml$/i,
      /\.xml$/i,
      /\.json$/i,
      /\.sql$/i,
      /META-INF\/MANIFEST\.MF$/i,
    ];

    const files = Object.keys(zip.files).filter(name => !zip.files[name].dir);

    for (const filename of files) {
      const ext = filename.split('.').pop().toLowerCase();
      if (['xml', 'json', 'sql', 'txt', 'properties', 'cfg', 'yaml', 'yml', 'mf'].includes(ext)) {
        const content = await zip.files[filename].async('string');
        results.push({ filename, content, type: ext });
      }
    }

    // Sort by priority — flow files first
    results.sort((a, b) => {
      const aPri = priorities.findIndex(p => p.test(a.filename));
      const bPri = priorities.findIndex(p => p.test(b.filename));
      return (aPri === -1 ? 999 : aPri) - (bPri === -1 ? 999 : bPri);
    });

    return results;
  } catch (e) {
    return { error: 'ZIP extraction failed: ' + e.message };
  }
}

// ---- TAR.GZ extraction ----
export async function extractTarGz(bytes) {
  try {
    if (bytes && bytes.length > MAX_FILE_SIZE) return { error: 'File exceeds 50MB limit' };
    const { default: pako } = await import('pako');
    const decompressed = pako.inflate(bytes);
    return parseTar(decompressed);
  } catch (e) {
    return { error: 'TAR.GZ extraction failed: ' + e.message };
  }
}

// Simple tar parser (512-byte header blocks)
export function parseTar(buffer) {
  try {
    if (!buffer || buffer.length === 0) return [];
    const results = [];
    let offset = 0;
    const decoder = new TextDecoder();

    while (offset < buffer.length - 512) {
      const header = buffer.slice(offset, offset + 512);
      let name = decoder.decode(header.slice(0, 100)).replace(/\0/g, '').trim();
      if (!name) break;

      const sizeOctal = decoder.decode(header.slice(124, 136)).replace(/\0/g, '').trim();
      const size = parseInt(sizeOctal, 8) || 0;
      const typeFlag = decoder.decode(header.slice(156, 157));

      offset += 512; // skip header

      // GNU LongLink: typeFlag 'L' means this entry's data is the real filename
      if (typeFlag === 'L') {
        const longName = decoder.decode(buffer.slice(offset, offset + size)).replace(/\0/g, '').trim();
        // Advance past the long-name data (padded to 512-byte blocks)
        offset += Math.ceil(size / 512) * 512;

        // Now read the actual file header that follows
        if (offset >= buffer.length - 512) break;
        const realHeader = buffer.slice(offset, offset + 512);
        const realSizeOctal = decoder.decode(realHeader.slice(124, 136)).replace(/\0/g, '').trim();
        const realSize = parseInt(realSizeOctal, 8) || 0;
        const realTypeFlag = decoder.decode(realHeader.slice(156, 157));
        offset += 512; // skip real header

        if (realTypeFlag === '0' || realTypeFlag === '' || realTypeFlag === '\0') {
          const ext = longName.split('.').pop().toLowerCase();
          if (['xml', 'json', 'sql', 'txt', 'properties', 'cfg', 'yaml', 'yml'].includes(ext)) {
            const content = decoder.decode(buffer.slice(offset, offset + realSize));
            results.push({ filename: longName, content, type: ext });
          }
        }
        offset += Math.ceil(realSize / 512) * 512;
        continue;
      }

      // PAX extended header — skip the data block and continue to next entry
      if (typeFlag === 'x' || typeFlag === 'g') {
        offset += Math.ceil(size / 512) * 512;
        continue;
      }

      if (typeFlag === '0' || typeFlag === '' || typeFlag === '\0') {
        // Regular file
        const ext = name.split('.').pop().toLowerCase();
        if (['xml', 'json', 'sql', 'txt', 'properties', 'cfg', 'yaml', 'yml'].includes(ext)) {
          const content = decoder.decode(buffer.slice(offset, offset + size));
          results.push({ filename: name, content, type: ext });
        }
      }

      // Advance past file data (padded to 512-byte blocks)
      offset += Math.ceil(size / 512) * 512;
    }

    return results;
  } catch (e) {
    return { error: 'Tar parsing failed: ' + e.message };
  }
}

/**
 * Smart SQL splitter that respects quotes and comments.
 * Splits on `;` only when outside single-quoted strings,
 * double-quoted identifiers, line comments (--), and block comments.
 */
function splitSqlStatements(text) {
  const statements = [];
  let current = '';
  let i = 0;
  while (i < text.length) {
    const ch = text[i];

    // -- line comment
    if (ch === '-' && text[i + 1] === '-') {
      const end = text.indexOf('\n', i);
      if (end === -1) {
        current += text.slice(i);
        i = text.length;
      } else {
        current += text.slice(i, end + 1);
        i = end + 1;
      }
      continue;
    }

    // /* block comment */
    if (ch === '/' && text[i + 1] === '*') {
      const end = text.indexOf('*/', i + 2);
      if (end === -1) {
        current += text.slice(i);
        i = text.length;
      } else {
        current += text.slice(i, end + 2);
        i = end + 2;
      }
      continue;
    }

    // single-quoted string (with '' escape)
    if (ch === "'") {
      let j = i + 1;
      current += ch;
      while (j < text.length) {
        if (text[j] === "'" && text[j + 1] === "'") {
          current += "''";
          j += 2;
        } else if (text[j] === "'") {
          current += "'";
          j += 1;
          break;
        } else {
          current += text[j];
          j += 1;
        }
      }
      i = j;
      continue;
    }

    // double-quoted identifier
    if (ch === '"') {
      let j = i + 1;
      current += ch;
      while (j < text.length) {
        if (text[j] === '"' && text[j + 1] === '"') {
          current += '""';
          j += 2;
        } else if (text[j] === '"') {
          current += '"';
          j += 1;
          break;
        } else {
          current += text[j];
          j += 1;
        }
      }
      i = j;
      continue;
    }

    // semicolon delimiter
    if (ch === ';') {
      const trimmed = current.trim();
      if (trimmed.length > 0) statements.push(trimmed);
      current = '';
      i += 1;
      continue;
    }

    current += ch;
    i += 1;
  }

  const trimmed = current.trim();
  if (trimmed.length > 0) statements.push(trimmed);
  return statements;
}

// ---- SQL file handling ----
export function parseSqlFile(content, filename) {
  try {
    // Extract DDL statements and build a pseudo-flow representation
    const statements = splitSqlStatements(content);
    const processors = [];
    const connections = [];
    let idx = 0;

    for (const stmt of statements) {
      const upper = stmt.toUpperCase().trimStart();
      let type = 'unknown';
      let name = 'SQL_Statement_' + (++idx);

      if (upper.startsWith('CREATE TABLE') || upper.startsWith('CREATE EXTERNAL TABLE')) {
        type = 'CreateTable';
        const match = stmt.match(/CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:`?(\w+)`?\.)?`?(\w+)`?/i);
        if (match) name = match[2] || name;
      } else if (upper.startsWith('CREATE VIEW') || upper.startsWith('CREATE OR REPLACE VIEW')) {
        type = 'CreateView';
        const match = stmt.match(/CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(?:`?(\w+)`?\.)?`?(\w+)`?/i);
        if (match) name = match[2] || name;
      } else if (upper.startsWith('INSERT')) {
        type = 'InsertData';
        const match = stmt.match(/INSERT\s+(?:INTO|OVERWRITE)\s+(?:`?(\w+)`?\.)?`?(\w+)`?/i);
        if (match) name = 'Insert_' + (match[2] || name);
      } else if (upper.startsWith('SELECT')) {
        type = 'SelectQuery';
        name = 'Query_' + idx;
      } else if (upper.startsWith('ALTER')) {
        type = 'AlterTable';
      } else if (upper.startsWith('DROP')) {
        type = 'DropObject';
      } else {
        continue; // skip unknown statements
      }

      processors.push({
        name,
        type: 'SQL.' + type,
        state: 'RUNNING',
        group: 'SQL Script',
        schedulingStrategy: 'SEQUENTIAL',
        schedulingPeriod: 'N/A',
        properties: { 'SQL Statement': stmt.substring(0, 2000) }
      });
    }

    // Create sequential connections
    for (let i = 0; i < processors.length - 1; i++) {
      connections.push({
        sourceId: processors[i].name,
        sourceName: processors[i].name,
        destinationId: processors[i + 1].name,
        destinationName: processors[i + 1].name,
        relationships: ['success']
      });
    }

    return {
      source_name: filename,
      source_type: 'sql_script',
      _nifi: {
        processors,
        connections,
        controllerServices: [],
        processGroups: [{ name: 'SQL Script', parentGroup: '' }],
        idToName: {},
        clouderaTools: [],
        deepPropertyInventory: {
          filePaths: {}, urls: {}, jdbcUrls: {}, nifiEL: {},
          cronExprs: {}, credentialRefs: {}, hostPorts: {},
          dataFormats: new Set(), encodings: new Set()
        },
        sqlTables: processors.filter(p => p.type.includes('Create')).map(p => p.name),
        sqlTableMeta: {}
      },
      tables: [],
      parse_warnings: ['Parsed as SQL script — processors represent SQL statements, not NiFi processors.'],
      _deferredProcessorWork: null
    };
  } catch (e) {
    return { error: 'SQL parsing failed: ' + e.message };
  }
}

// ---- DOCX text extraction ----
export async function extractDocxText(bytes) {
  try {
    if (bytes && bytes.length > MAX_FILE_SIZE) return { error: 'File exceeds 50MB limit' };
    // DOCX is a ZIP containing XML files
    // Extract document.xml and parse the text content
    const { default: JSZip } = await import('jszip');
    const zip = await JSZip.loadAsync(bytes);

    const docXml = zip.files['word/document.xml'];
    if (!docXml) return { text: '', tables: [] };

    const xmlContent = await docXml.async('string');
    const parser = new DOMParser();
    const doc = parser.parseFromString(xmlContent, 'text/xml');

    // Extract all text content
    const textNodes = doc.getElementsByTagNameNS('http://schemas.openxmlformats.org/wordprocessingml/2006/main', 't');

    // Collect unique parent paragraphs
    const paragraphs = [];
    const seen = new Set();
    for (let i = 0; i < textNodes.length; i++) {
      const t = textNodes[i];
      let p = t.parentNode;
      while (p && p.nodeName !== 'w:p' && p.nodeName !== 'body') p = p.parentNode;
      if (p && p.nodeName === 'w:p' && !seen.has(p)) { seen.add(p); paragraphs.push(p); }
    }
    const textLines = paragraphs.map(p => {
      const texts = p.querySelectorAll('w\\:t, t');
      return Array.from(texts).map(t => t.textContent).join('');
    }).filter(Boolean);
    const text = textLines.join('\n');

    // Extract tables
    const tables = [];
    const tblNodes = doc.getElementsByTagNameNS('http://schemas.openxmlformats.org/wordprocessingml/2006/main', 'tbl');
    for (let t = 0; t < tblNodes.length; t++) {
      const rows = tblNodes[t].getElementsByTagNameNS('http://schemas.openxmlformats.org/wordprocessingml/2006/main', 'tr');
      const tableData = [];
      for (let r = 0; r < rows.length; r++) {
        const cells = rows[r].getElementsByTagNameNS('http://schemas.openxmlformats.org/wordprocessingml/2006/main', 'tc');
        const rowData = [];
        for (let c = 0; c < cells.length; c++) {
          const cellTexts = cells[c].getElementsByTagNameNS('http://schemas.openxmlformats.org/wordprocessingml/2006/main', 't');
          let cellText = '';
          for (let ct = 0; ct < cellTexts.length; ct++) cellText += cellTexts[ct].textContent + ' ';
          rowData.push(cellText.trim());
        }
        tableData.push(rowData);
      }
      tables.push(tableData);
    }

    return { text, tables };
  } catch (e) {
    return { error: 'DOCX extraction failed: ' + e.message };
  }
}

/**
 * Convert a cell reference like "C5" or "AA12" to a 0-based column index.
 */
function cellRefToColIndex(ref) {
  const match = ref.match(/^([A-Z]+)/);
  if (!match) return 0;
  const letters = match[1];
  let col = 0;
  for (let i = 0; i < letters.length; i++) {
    col = col * 26 + (letters.charCodeAt(i) - 64);
  }
  return col - 1; // 0-based
}

// ---- XLSX extraction ----
export async function extractXlsxData(bytes) {
  try {
    if (bytes && bytes.length > MAX_FILE_SIZE) return { error: 'File exceeds 50MB limit' };
    // XLSX is also a ZIP — extract shared strings and sheet data
    const { default: JSZip } = await import('jszip');
    const zip = await JSZip.loadAsync(bytes);

    // Parse shared strings
    const sharedStrings = [];
    const ssFile = zip.files['xl/sharedStrings.xml'];
    if (ssFile) {
      const ssXml = await ssFile.async('string');
      const parser = new DOMParser();
      const doc = parser.parseFromString(ssXml, 'text/xml');
      const siNodes = doc.getElementsByTagName('si');
      for (let i = 0; i < siNodes.length; i++) {
        const tNodes = siNodes[i].getElementsByTagName('t');
        let text = '';
        for (let t = 0; t < tNodes.length; t++) text += tNodes[t].textContent;
        sharedStrings.push(text);
      }
    }

    // Parse ALL sheets (not just sheet1)
    const sheets = [];
    const sheetFiles = Object.keys(zip.files)
      .filter(name => /^xl\/worksheets\/sheet\d+\.xml$/.test(name))
      .sort((a, b) => {
        const aNum = parseInt(a.match(/sheet(\d+)/)[1]);
        const bNum = parseInt(b.match(/sheet(\d+)/)[1]);
        return aNum - bNum;
      });

    for (const sheetPath of sheetFiles) {
      const sheetFile = zip.files[sheetPath];
      const sheetXml = await sheetFile.async('string');
      const parser = new DOMParser();
      const doc = parser.parseFromString(sheetXml, 'text/xml');
      const rows = doc.getElementsByTagName('row');
      const sheetData = [];
      for (let r = 0; r < rows.length; r++) {
        const cells = rows[r].getElementsByTagName('c');
        const rowData = [];
        for (let c = 0; c < cells.length; c++) {
          const cellRef = cells[c].getAttribute('r');
          const colIdx = cellRef ? cellRefToColIndex(cellRef) : c;
          const type = cells[c].getAttribute('t');
          const vEl = cells[c].getElementsByTagName('v')[0];
          let val = vEl ? vEl.textContent : '';
          if (type === 's' && vEl) {
            const idx = parseInt(vEl.textContent);
            if (!isNaN(idx) && idx < sharedStrings.length) {
              val = sharedStrings[idx];
            }
          }
          // Ensure array is large enough for sparse rows
          while (rowData.length <= colIdx) rowData.push('');
          rowData[colIdx] = val;
        }
        sheetData.push(rowData);
      }
      sheets.push(sheetData);
    }

    return { sheets, sharedStrings };
  } catch (e) {
    return { error: 'XLSX extraction failed: ' + e.message };
  }
}

// ---- Build pseudo-flow from document content ----
export function buildDocumentFlow(text, tables, filename) {
  try {
    // Extract any identifiable NiFi/data pipeline information from document text
    const processors = [];
    const connections = [];
    const warnings = [];

    // Look for processor-like mentions
    const processorPattern = /(?:processor|component|step|task|job)\s*[:\-]?\s*([A-Z]\w+(?:\s+\w+)*)/gi;
    let match;
    const seen = new Set();
    while ((match = processorPattern.exec(text)) !== null) {
      const name = match[1].trim();
      if (!seen.has(name) && name.length > 2 && name.length < 60) {
        seen.add(name);
        processors.push({
          name,
          type: 'Document.Reference',
          state: 'DOCUMENTED',
          group: 'Document Extract',
          properties: { 'Source': filename, 'Context': text.substring(Math.max(0, match.index - 50), match.index + 100).trim() }
        });
      }
    }

    // Process tables — look for processor/connection definitions
    for (const table of tables) {
      if (table.length < 2) continue;
      const headers = table[0].map(h => h.toLowerCase());
      const nameCol = headers.findIndex(h => h.includes('name') || h.includes('processor') || h.includes('component'));
      const typeCol = headers.findIndex(h => h.includes('type') || h.includes('class'));

      if (nameCol >= 0) {
        for (let r = 1; r < table.length; r++) {
          const name = table[r][nameCol];
          if (name && !seen.has(name)) {
            seen.add(name);
            const props = {};
            headers.forEach((h, i) => { if (table[r][i]) props[h] = table[r][i]; });
            processors.push({
              name,
              type: typeCol >= 0 ? (table[r][typeCol] || 'Document.TableEntry') : 'Document.TableEntry',
              state: 'DOCUMENTED',
              group: 'Document Table',
              properties: props
            });
          }
        }
      }
    }

    if (processors.length === 0) {
      warnings.push('No processor/component references found in document. Content extracted for analysis only.');
      processors.push({
        name: 'Document_Content',
        type: 'Document.FullText',
        state: 'DOCUMENTED',
        group: 'Document',
        properties: { 'Content Preview': text.substring(0, 2000), 'Total Length': String(text.length) }
      });
    }

    return {
      source_name: filename,
      source_type: 'document',
      _nifi: {
        processors,
        connections,
        controllerServices: [],
        processGroups: [{ name: 'Document Extract', parentGroup: '' }],
        idToName: {},
        clouderaTools: [],
        deepPropertyInventory: {
          filePaths: {}, urls: {}, jdbcUrls: {}, nifiEL: {},
          cronExprs: {}, credentialRefs: {}, hostPorts: {},
          dataFormats: new Set(), encodings: new Set()
        },
        sqlTables: [],
        sqlTableMeta: {}
      },
      tables: [],
      parse_warnings: warnings.length ? warnings : ['Parsed from document — content represents extracted references, not NiFi processors.'],
      _deferredProcessorWork: null
    };
  } catch (e) {
    return { error: 'Document flow building failed: ' + e.message };
  }
}
