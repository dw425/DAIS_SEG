/**
 * analyzers/resource-manifest.js — Build a complete resource manifest from parsed NiFi flow
 *
 * Extracted from index.html lines 1928-2136.
 *
 * @module analyzers/resource-manifest
 */

import { _RE } from '../constants/regex-patterns.js';

/**
 * Set of known NiFi Expression Language built-in functions.
 * Used to filter out resolved EL references from the unresolved parameter list.
 */
const NIFI_EL_FUNCS = new Set([
  'now','nextInt','UUID','hostname','IP','literal','thread','format','toDate',
  'substring','substringBefore','substringAfter','replace','replaceAll','replaceFirst',
  'replaceEmpty','replaceNull','toUpper','toLower','trim','length','isEmpty','equals',
  'equalsIgnoreCase','contains','startsWith','endsWith','append','prepend','plus',
  'minus','multiply','divide','mod','gt','ge','lt','le','and','or','not','ifElse',
  'toString','toNumber','math','getStateValue','count','padLeft','padRight',
  'escapeJson','escapeXml','escapeCsv','unescapeJson','unescapeXml','urlEncode',
  'urlDecode','base64Encode','base64Decode','toRadix','jsonPath','jsonPathDelete',
  'jsonPathAdd','jsonPathSet','jsonPathPut'
]);

/**
 * Build a comprehensive resource manifest from a parsed NiFi flow.
 *
 * Scans processors and controller services for directories, files, SQL tables,
 * tokens, signals, HTTP endpoints, Kafka topics, DB connections, scripts,
 * parameters, counters, and disconnected processors.
 *
 * @param {object} nifi — parsed NiFi flow
 * @returns {object} — full resource manifest
 */
export function buildResourceManifest(nifi) {
  const dirs = {}, files = {}, sqlTbls = {}, tokens = {}, signals = {};
  const httpEndpoints = [], kafkaTopics = [], dbConnections = [], scripts = [], parameters = [], counters = [];
  const connectedIds = new Set();

  function addDir(path, type, proc) {
    if (!path || path.includes('${')) return;
    if (!dirs[path]) dirs[path] = { type, processors: [] };
    dirs[path].processors.push(proc);
  }

  function addFile(path, role, proc, fmt) {
    if (!path) return;
    if (!files[path]) files[path] = { producers: [], consumers: [], format: fmt || 'unknown' };
    if (role === 'read') files[path].consumers.push(proc);
    else files[path].producers.push(proc);
  }

  function addSqlTable(name, role, proc) {
    if (!name || name === 'dual' || name.length < 2) return;
    const n = name.replace(/^["'`]+|["'`]+$/g, '').trim();
    if (!n || /^(waiting|because|account|log|Dates|LeadLag|startGrouping|grps|Temptation)$/i.test(n)) return;
    if (!sqlTbls[n]) sqlTbls[n] = { readers: [], writers: [] };
    if (role === 'read') sqlTbls[n].readers.push(proc);
    else sqlTbls[n].writers.push(proc);
  }

  function findUnresolvedEL(value, proc) {
    if (!value || typeof value !== 'string') return;
    const matches = value.match(/\$\{([^}]+)\}/g);
    if (!matches) return;
    matches.forEach(m => {
      const inner = m.slice(2, -1);
      const funcName = inner.split(':')[0].trim();
      const baseFuncName = funcName.replace(/\(.*$/, '');
      if (
        !NIFI_EL_FUNCS.has(funcName) &&
        !NIFI_EL_FUNCS.has(baseFuncName) &&
        !funcName.includes('.') &&
        !/^(filename|path|absolute\.path|uuid|fileSize|file\.size|entryDate|lineageStartDate|flowfile)/.test(funcName)
      ) {
        parameters.push({ expr: m, processor: proc, attrName: funcName, resolved: false });
      }
    });
  }

  // Scan connections for connected processor IDs
  (nifi.connections || []).forEach(c => {
    if (c.sourceType === 'PROCESSOR') connectedIds.add(c.sourceId || c.sourceName);
    if (c.destinationType === 'PROCESSOR') connectedIds.add(c.destinationId || c.destinationName);
    connectedIds.add(c.sourceName);
    connectedIds.add(c.destinationName);
  });

  // Scan each processor
  (nifi.processors || []).forEach(p => {
    const props = p.properties || {};
    const pName = p.name;
    const t = p.type;

    // Scan ALL properties for unresolved expressions
    Object.entries(props).forEach(([k, v]) => findUnresolvedEL(v, pName));

    // Per-type resource extraction
    if (t === 'GetFile') {
      const dir = props['Input Directory'];
      addDir(dir, 'input', pName);
      if (dir) addFile(dir + '/*.csv', 'read', pName, 'csv');
    } else if (t === 'ListFile') {
      const dir = props['Input Directory'];
      addDir(dir, 'input', pName);
    } else if (t === 'FetchFile') {
      const fp = props['File to Fetch'] || props['Filename'];
      addFile(fp || '(dynamic)', 'read', pName);
    } else if (t === 'PutFile') {
      const dir = props['Directory'];
      addDir(dir, 'output', pName);
    } else if (t === 'PutSFTP') {
      const host = props['Hostname'] || 'sftp-host';
      const rp = props['Remote Path'] || '/';
      addDir(`sftp://${host}${rp}`, 'output', pName);
      addFile(`sftp://${host}${rp}/(dynamic)`, 'write', pName);
    } else if (t === 'PutHDFS' || t === 'PutParquet') {
      const dir = props['Directory'] || props['directory'];
      addDir(dir, 'output', pName);
    } else if (t === 'ExecuteSQL' || t === 'ExecuteSQLRecord') {
      const sql = props['SQL select query'] || props['sql-select-query'] || '';
      const tblRefs = sql.match(/(?:FROM|JOIN)\s+([\w$.{}"]+)/gi);
      if (tblRefs) tblRefs.forEach(r => {
        const tn = r.replace(/^(FROM|JOIN)\s+/i, '').trim();
        addSqlTable(tn, 'read', pName);
      });
      const dbcp = props['Database Connection Pooling Service'] || props['dbcp-service'];
      if (dbcp) dbConnections.push({ name: dbcp, processor: pName, type: 'read' });
    } else if (t === 'PutDatabaseRecord' || t === 'PutSQL') {
      const tn = props['Table Name'] || props['table-name'] || props['put-db-record-table-name'];
      addSqlTable(tn, 'write', pName);
      const dbcp = props['Database Connection Pooling Service'] || props['dbcp-service'];
      if (dbcp) dbConnections.push({ name: dbcp, processor: pName, type: 'write' });
    } else if (t === 'ExecuteStreamCommand') {
      const cmd = props['Command'] || '';
      const args = props['Command Arguments'] || '';
      if (cmd) scripts.push({ path: cmd, args, processor: pName, _cloudera: null });
      const allCmd = (cmd + ' ' + args).toLowerCase();
      // HDFS operations
      if (/hdfs\s+dfs|dfs;/.test(allCmd) || /^dfs$/.test(cmd.trim())) {
        const hdfsMatch = args.match(/(?:-cp|-mv|-put|-get|-ls|-rm|-mkdir|-cat|-chmod|-chown|-touchz)\s*;?\s*([^\s;]+)/);
        if (hdfsMatch) addDir(hdfsMatch[1], /(-ls|-cat|-get)/.test(args) ? 'input' : 'output', pName);
        const destMatch = args.match(/(?:-put|-cp)\s*;?\s*[^\s;]+\s*;?\s*([^\s;]+)/);
        if (destMatch) addDir(destMatch[1], 'output', pName);
      }
      // Impala operations
      if (/impala-shell|impala/.test(allCmd)) {
        const tblMatches = args.match(/(?:refresh|invalidate\s+metadata|compute\s+stats|from|join|into|insert\s+into|insert\s+overwrite)\s+[;]?\s*([\w.${}]+)/gi);
        if (tblMatches) tblMatches.forEach(m => {
          const tn = m.replace(/^(refresh|invalidate\s+metadata|compute\s+stats|from|join|into|insert\s+into|insert\s+overwrite)\s+;?\s*/i, '').trim().replace(/[";]/g, '');
          if (tn && tn.length > 2) addSqlTable(tn, /insert|into/i.test(m) ? 'write' : 'read', pName);
        });
      }
      // Hive/Beeline operations
      if (/hive|beeline/.test(allCmd)) {
        const tblMatches = args.match(/(?:from|join|into|table)\s+([\w.]+)/gi);
        if (tblMatches) tblMatches.forEach(m => {
          const tn = m.replace(/^(from|join|into|table)\s+/i, '').trim();
          if (tn && tn.length > 2) addSqlTable(tn, /into/i.test(m) ? 'write' : 'read', pName);
        });
      }
      // Kerberos
      if (/kinit|keytab|kerberos|klist/.test(allCmd) && scripts.length && cmd) {
        scripts[scripts.length - 1]._cloudera = 'kerberos';
      }
      // Sqoop
      if (/sqoop/.test(allCmd)) {
        const tblMatch = args.match(/--table\s+(\S+)/);
        if (tblMatch) addSqlTable(tblMatch[1], /export/.test(allCmd) ? 'write' : 'read', pName);
      }
      // Phoenix
      if (/sqlline\.py|phoenix/.test(allCmd)) {
        const tblMatches = args.match(/(?:from|into|table|upsert\s+into)\s+([\w.]+)/gi);
        if (tblMatches) tblMatches.forEach(m => {
          const tn = m.replace(/^(from|into|table|upsert\s+into)\s+/i, '').trim();
          if (tn && tn.length > 2) addSqlTable(tn, /into|upsert/i.test(m) ? 'write' : 'read', pName);
        });
      }
      // Presto/Trino
      if (/presto|trino/.test(allCmd)) {
        const tblMatches = args.match(/(?:from|join|into)\s+([\w.]+)/gi);
        if (tblMatches) tblMatches.forEach(m => {
          const tn = m.replace(/^(from|join|into)\s+/i, '').trim();
          if (tn && tn.length > 2) addSqlTable(tn, /into/i.test(m) ? 'write' : 'read', pName);
        });
      }
    } else if (t === 'InvokeHTTP') {
      const url = props['Remote URL'] || props['remote-url'] || '';
      const method = props['HTTP Method'] || props['http-method'] || 'GET';
      if (url) httpEndpoints.push({ url, method, processor: pName });
    } else if (t === 'ConsumeKafka_2_6' || t === 'ConsumeKafka' || t === 'ConsumeKafkaRecord_2_6') {
      const topic = props['Topic Name(s)'] || props['topic'] || '';
      const brokers = props['Kafka Brokers'] || props['bootstrap.servers'] || '';
      if (topic) kafkaTopics.push({ topic, brokers, processor: pName, direction: 'consume' });
    } else if (t === 'PublishKafka_2_6' || t === 'PublishKafka' || t === 'PublishKafkaRecord_2_6') {
      const topic = props['Topic Name(s)'] || props['topic'] || '';
      const brokers = props['Kafka Brokers'] || props['bootstrap.servers'] || '';
      if (topic) kafkaTopics.push({ topic, brokers, processor: pName, direction: 'produce' });
    } else if (t === 'Wait') {
      const sn = props['Signal Counter Name'] || '';
      const target = parseInt(props['Target Signal Count']) || 1;
      if (sn) {
        if (!signals[sn]) signals[sn] = { senders: [], waiters: [], target };
        signals[sn].waiters.push(pName);
      }
    } else if (t === 'Notify') {
      const sn = props['Signal Counter Name'] || '';
      if (sn) {
        if (!signals[sn]) signals[sn] = { senders: [], waiters: [], target: 1 };
        signals[sn].senders.push(pName);
      }
    } else if (t === 'ControlRate') {
      const tn = 'rate_' + pName.replace(/\s+/g, '_');
      tokens[tn] = { acquirers: [pName], releasers: [pName] };
    } else if (t === 'LogMessage') {
      counters.push({ name: 'log_' + pName, processor: pName });
    } else if (t === 'LookupAttribute' || t === 'LookupRecord') {
      addSqlTable('lookup_' + pName.replace(/\s+/g, '_').toLowerCase(), 'read', pName);
    }
  });

  // Scan controller services for DB connections
  (nifi.controllerServices || []).forEach(cs => {
    const props = cs.properties || {};
    const url = props['Database Connection URL'] || props['database-connection-url'] || '';
    if (url || cs.type.includes('DBCP') || cs.type.includes('ConnectionPool')) {
      dbConnections.push({
        name: cs.name,
        url: url ? url.replace(/password=[^&;]+/gi, 'password=***') : '',
        processor: '(controller service)',
        type: 'service',
      });
    }
  });

  // Detect disconnected processors
  const disconnected = (nifi.processors || []).filter(p =>
    !connectedIds.has(p.name) && !connectedIds.has(p.id)
  );

  // Deduplicate parameters
  const uniqueParams = [];
  const seenParams = new Set();
  parameters.forEach(p => {
    const k = p.expr + '|' + p.processor;
    if (!seenParams.has(k)) {
      seenParams.add(k);
      uniqueParams.push(p);
    }
  });

  // Summary counts
  const totalResources = Object.keys(dirs).length + Object.keys(files).length + Object.keys(sqlTbls).length +
    Object.keys(tokens).length + Object.keys(signals).length + httpEndpoints.length + kafkaTopics.length +
    scripts.length + dbConnections.length + counters.length;

  // Collect Cloudera tools from parsed nifi data
  const clouderaTools = (nifi.clouderaTools || []);

  return {
    directories: dirs, files, sqlTables: sqlTbls, tokens, signals,
    httpEndpoints, kafkaTopics, dbConnections, scripts, clouderaTools,
    parameters: uniqueParams, counters, disconnected,
    disconnectedProcessors: disconnected.map(p => ({
      name: p.name, type: p.type, group: p.group || '(root)', noInbound: true, noOutbound: true,
    })),
    totalResources,
    warnings: [
      ...disconnected.map(p => `Processor "${p.name}" (${p.type}) has no connections — may never execute`),
      ...uniqueParams.filter(p => !p.resolved).slice(0, 10).map(p => `Unresolved expression ${p.expr} in "${p.processor}"`),
      ...(dbConnections.filter(d => d.type !== 'service').length > 0 && dbConnections.filter(d => d.type === 'service').length === 0
        ? ['Processors reference DB connections but no DBCP controller service found'] : []),
      ...(clouderaTools.length ? [`${clouderaTools.length} external system references detected — see External Systems & Tools inventory`] : []),
    ],
  };
}
