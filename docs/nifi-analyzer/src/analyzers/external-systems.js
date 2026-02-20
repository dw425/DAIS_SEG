/**
 * analyzers/external-systems.js — Detect external systems from processor types & properties
 *
 * Extracted from index.html lines 3004-3012.
 * Uses PACKAGE_MAP (inline) for enrichment.
 *
 * @module analyzers/external-systems
 */

import { JDBC_TYPE_MAP } from '../constants/jdbc-type-map.js';
import { PACKAGE_MAP } from '../constants/package-map.js';

/**
 * Detect external systems referenced by NiFi processors and deep-property inventory.
 *
 * @param {object} nifi — parsed NiFi flow with processors[], deepPropertyInventory
 * @returns {object} — keyed by system key, each with name, processors[], jdbcUrls[], credentials[], packages[], dbxApproach
 */
export function detectExternalSystems(nifi) {
  const systems = {};
  const procs = nifi.processors || [];
  const dpi = nifi.deepPropertyInventory || {};

  const pats = [
    [/Kafka/i, 'Apache Kafka', 'kafka'],
    [/Oracle/i, 'Oracle Database', 'oracle'],
    [/MySQL/i, 'MySQL', 'mysql'],
    [/Postgres/i, 'PostgreSQL', 'postgresql'],
    [/Mongo/i, 'MongoDB', 'mongodb'],
    [/Elastic/i, 'Elasticsearch', 'elasticsearch'],
    [/Cassandra/i, 'Cassandra', 'cassandra'],
    [/HBase/i, 'HBase', 'hbase'],
    [/Kudu/i, 'Apache Kudu', 'kudu'],
    [/Hive/i, 'Hive', 'hive'],
    [/HDFS|Hadoop/i, 'HDFS', 'hdfs'],
    [/S3/i, 'AWS S3', 's3'],
    [/Azure.*Blob/i, 'Azure Blob', 'azure_blob'],
    [/Azure.*Lake|ADLS/i, 'Azure Data Lake', 'azure_adls'],
    [/Azure.*Event/i, 'Azure Event Hubs', 'azure_eventhub'],
    [/GCS|BigQuery/i, 'Google Cloud', 'gcs'],
    [/Snowflake/i, 'Snowflake', 'snowflake'],
    [/Redis/i, 'Redis', 'redis'],
    [/Solr/i, 'Solr', 'solr'],
    [/SFTP|FTP/i, 'SFTP/FTP', 'sftp'],
    [/HTTP|REST/i, 'HTTP/REST', 'http'],
    [/JMS/i, 'JMS', 'jms'],
    [/AMQP/i, 'AMQP', 'amqp'],
    [/MQTT/i, 'MQTT', 'mqtt'],
    [/Email|SMTP/i, 'Email', 'email'],
    [/Syslog/i, 'Syslog', 'syslog'],
    [/Slack/i, 'Slack', 'slack'],
    [/Splunk/i, 'Splunk', 'splunk'],
    [/InfluxDB/i, 'InfluxDB', 'influxdb'],
    [/Neo4j/i, 'Neo4j', 'neo4j'],
    [/Teradata/i, 'Teradata', 'teradata'],
    [/Iceberg/i, 'Iceberg', 'iceberg'],
    [/SQL|Database|JDBC/i, 'SQL/JDBC', 'sql_jdbc'],
  ];

  procs.forEach(p => {
    const dir = /^(Get|List|Consume|Listen|Fetch|Query|Scan|Select)/i.test(p.type)
      ? 'READ'
      : /^(Put|Publish|Send|Post|Insert|Write|Delete)/i.test(p.type)
        ? 'WRITE'
        : 'READ/WRITE';

    pats.forEach(([re, name, key]) => {
      if (re.test(p.type)) {
        if (!systems[key]) systems[key] = { name, key, processors: [], jdbcUrls: [], credentials: [], packages: [] };
        systems[key].processors.push({ name: p.name, type: p.type, direction: dir, group: p.group });
      }
    });
  });

  if (dpi.jdbcUrls) {
    Object.keys(dpi.jdbcUrls).forEach(url => {
      const m = url.match(/jdbc:(\w+):/);
      if (m) {
        const k = m[1].toLowerCase();
        const sk = { oracle: 'oracle', mysql: 'mysql', postgresql: 'postgresql', sqlserver: 'sqlserver', hive2: 'hive', teradata: 'teradata', snowflake: 'snowflake' }[k] || 'sql_jdbc';
        if (!systems[sk]) systems[sk] = { name: PACKAGE_MAP[sk] ? PACKAGE_MAP[sk].desc : sk, key: sk, processors: [], jdbcUrls: [], credentials: [], packages: [] };
        systems[sk].jdbcUrls.push(url);
      }
    });
  }

  if (dpi.credentialRefs) {
    Object.keys(dpi.credentialRefs).forEach(ref => {
      Object.values(systems).forEach(sys => {
        const names = sys.processors.map(p => p.name);
        const rp = dpi.credentialRefs[ref];
        if (Array.isArray(rp) && rp.some(r => names.includes(r))) sys.credentials.push(ref);
      });
    });
  }

  Object.values(systems).forEach(sys => {
    const pkg = PACKAGE_MAP[sys.key];
    sys.dbxApproach = pkg ? pkg.dbx : 'Custom implementation';
    sys.packages = pkg ? pkg.pip : [];
  });

  return systems;
}
