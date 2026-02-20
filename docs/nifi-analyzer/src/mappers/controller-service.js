/**
 * mappers/controller-service.js -- Controller Service Resolver
 *
 * Extracted from index.html lines 3039-3078.
 * Resolves NiFi controller service references to connection parameters
 * (JDBC URLs, SSL configs, cache hosts, record formats).
 */

/**
 * Resolve a NiFi controller service by name, extracting connection
 * parameters from its properties.
 *
 * @param {string} csName - Controller service name or type substring
 * @param {Array} controllerServices - Array of controller service objects
 * @returns {object|null} - Resolved service info or null
 */
export function resolveControllerService(csName, controllerServices) {
  if (!controllerServices || !csName) return null;
  const cs = controllerServices.find(s => s.name === csName || s.type.includes(csName));
  if (!cs) return null;
  const props = cs.properties || {};
  const result = { name: cs.name, type: cs.type, props: {} };

  // DBCP Connection Pool -> JDBC URL + driver
  if (/DBCP|ConnectionPool/i.test(cs.type)) {
    result.jdbcUrl = props['Database Connection URL'] || '';
    result.driver = props['Database Driver Class Name'] || '';
    result.user = props['Database User'] || '';
    result.maxConns = props['Max Total Connections'] || '10';
    if (/oracle/i.test(result.jdbcUrl)) result.dbType = 'oracle';
    else if (/postgresql/i.test(result.jdbcUrl)) result.dbType = 'postgresql';
    else if (/mysql/i.test(result.jdbcUrl)) result.dbType = 'mysql';
    else if (/sqlserver/i.test(result.jdbcUrl)) result.dbType = 'sqlserver';
    else if (/hive/i.test(result.jdbcUrl)) result.dbType = 'hive';
    else result.dbType = 'jdbc';
  }

  // SSL Context
  if (/SSL/i.test(cs.type)) {
    result.keystore = props['Keystore Filename'] || '';
    result.truststore = props['Truststore Filename'] || '';
    result.protocol = props['SSL Protocol'] || 'TLS';
  }

  // Distributed Cache
  if (/Cache/i.test(cs.type)) {
    result.cacheHost = props['Server Hostname'] || 'localhost';
    result.cachePort = props['Server Port'] || '4557';
  }

  // Record reader/writer
  if (/Reader|Writer/i.test(cs.type)) {
    result.schemaStrategy = props['Schema Access Strategy'] || 'Infer Schema';
    if (/CSV/i.test(cs.type)) result.format = 'csv';
    else if (/Json/i.test(cs.type)) result.format = 'json';
    else if (/Avro/i.test(cs.type)) result.format = 'avro';
    else if (/Parquet/i.test(cs.type)) result.format = 'parquet';
  }

  return result;
}
