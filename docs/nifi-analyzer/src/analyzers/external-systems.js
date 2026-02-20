/**
 * analyzers/external-systems.js — Detect external systems from processor types & properties
 *
 * Extracted from index.html lines 3004-3012.
 * Uses PACKAGE_MAP (inline) for enrichment.
 *
 * @module analyzers/external-systems
 */

import { JDBC_TYPE_MAP } from '../constants/jdbc-type-map.js';

/**
 * Inline PACKAGE_MAP — maps system keys to pip packages, Databricks info, and descriptions.
 * Extracted from index.html lines 2912-2979.
 */
const PACKAGE_MAP = {
  kafka:{pip:['confluent-kafka'],dbx:'Pre-installed on DBR',desc:'Kafka'},
  oracle:{pip:['oracledb'],dbx:'JDBC driver via cluster library',desc:'Oracle Database'},
  mysql:{pip:['mysql-connector-python'],dbx:'JDBC driver via cluster library',desc:'MySQL'},
  postgresql:{pip:['psycopg2-binary'],dbx:'JDBC driver via cluster library',desc:'PostgreSQL'},
  sqlserver:{pip:['pymssql'],dbx:'JDBC driver via cluster library',desc:'SQL Server'},
  mongodb:{pip:['pymongo'],dbx:'MongoDB Spark Connector',desc:'MongoDB'},
  elasticsearch:{pip:['elasticsearch'],dbx:'Elasticsearch Spark library',desc:'Elasticsearch'},
  cassandra:{pip:['cassandra-driver'],dbx:'Spark Cassandra Connector',desc:'Cassandra'},
  redis:{pip:['redis'],dbx:'Custom library install',desc:'Redis'},
  hbase:{pip:[],dbx:'Delta Lake migration',desc:'HBase'},
  kudu:{pip:[],dbx:'Delta Lake (direct replacement)',desc:'Kudu'},
  s3:{pip:['boto3'],dbx:'Pre-installed on DBR',desc:'AWS S3'},
  azure_blob:{pip:['azure-storage-blob'],dbx:'Pre-installed on DBR',desc:'Azure Blob'},
  azure_adls:{pip:['azure-storage-file-datalake'],dbx:'Pre-installed on DBR',desc:'Azure Data Lake'},
  gcs:{pip:['google-cloud-storage'],dbx:'GCS Spark connector',desc:'GCS'},
  hdfs:{pip:[],dbx:'dbutils.fs (pre-installed)',desc:'HDFS'},
  sftp:{pip:['paramiko'],dbx:'Volumes-based staging',desc:'SFTP/FTP'},
  http:{pip:['requests'],dbx:'Pre-installed on DBR',desc:'HTTP/REST'},
  email:{pip:['sendgrid'],dbx:'Webhook notification',desc:'Email'},
  mqtt:{pip:['paho-mqtt'],dbx:'Custom library',desc:'MQTT'},
  jms:{pip:['stomp.py'],dbx:'Custom library',desc:'JMS/AMQP'},
  snowflake:{pip:['snowflake-connector-python'],dbx:'Snowflake Spark Connector',desc:'Snowflake'},
  neo4j:{pip:['neo4j'],dbx:'Neo4j Spark Connector',desc:'Neo4j'},
  splunk:{pip:['splunklib'],dbx:'Splunk Spark Add-on',desc:'Splunk'},
  influxdb:{pip:['influxdb-client'],dbx:'Custom library',desc:'InfluxDB'},
  solr:{pip:['pysolr'],dbx:'Solr Spark library',desc:'Solr'},
  hive:{pip:[],dbx:'Pre-installed (Spark SQL)',desc:'Hive'},
  iceberg:{pip:[],dbx:'Pre-installed on DBR 13+',desc:'Iceberg'},
  teradata:{pip:['teradatasql'],dbx:'JDBC driver',desc:'Teradata'},
  slack:{pip:['slack-sdk'],dbx:'Webhook integration',desc:'Slack'},
  kerberos:{pip:[],dbx:'Unity Catalog identity federation',desc:'Kerberos'},
  azure_eventhub:{pip:['azure-eventhub'],dbx:'Pre-installed on DBR',desc:'Azure Event Hubs'},
  azure_servicebus:{pip:['azure-servicebus'],dbx:'Custom library install',desc:'Azure Service Bus'},
  azure_cosmos:{pip:[],dbx:'Pre-installed (Cosmos Spark connector)',desc:'Azure Cosmos DB'},
  azure_queue:{pip:['azure-storage-queue'],dbx:'Custom library install',desc:'Azure Queue Storage'},
  gcp_pubsub:{pip:['google-cloud-pubsub'],dbx:'Custom library install',desc:'GCP Pub/Sub'},
  gcp_bigquery:{pip:['google-cloud-bigquery'],dbx:'BigQuery Spark connector',desc:'GCP BigQuery'},
  clickhouse:{pip:['clickhouse-driver'],dbx:'ClickHouse JDBC driver',desc:'ClickHouse'},
  druid:{pip:[],dbx:'Druid JDBC driver',desc:'Apache Druid'},
  hudi:{pip:[],dbx:'Pre-installed on DBR 13+',desc:'Apache Hudi'},
  kinesis:{pip:['boto3'],dbx:'Kinesis Spark connector',desc:'AWS Kinesis'},
  cloudwatch:{pip:['boto3'],dbx:'Pre-installed on DBR',desc:'AWS CloudWatch'},
  sqs:{pip:['boto3'],dbx:'Pre-installed on DBR',desc:'AWS SQS'},
  sns:{pip:['boto3'],dbx:'Pre-installed on DBR',desc:'AWS SNS'},
  dynamodb:{pip:['boto3'],dbx:'Pre-installed on DBR',desc:'AWS DynamoDB'},
  lambda_aws:{pip:['boto3'],dbx:'Pre-installed on DBR',desc:'AWS Lambda'},
  pagerduty:{pip:['pdpyras'],dbx:'Custom library install',desc:'PagerDuty'},
  opsgenie:{pip:['opsgenie-sdk'],dbx:'Custom library install',desc:'OpsGenie'},
  telegram:{pip:[],dbx:'HTTP API (no SDK needed)',desc:'Telegram'},
  geoip:{pip:['geoip2'],dbx:'Custom library install',desc:'GeoIP'},
  exchange:{pip:['exchangelib'],dbx:'Custom library install',desc:'Microsoft Exchange'},
  whois:{pip:['python-whois'],dbx:'Custom library install',desc:'WHOIS'},
  snmp:{pip:['pysnmp'],dbx:'Custom library install',desc:'SNMP'},
  datadog:{pip:['datadog-api-client'],dbx:'Datadog integration',desc:'Datadog'},
  prometheus:{pip:['prometheus-client'],dbx:'Custom library install',desc:'Prometheus'},
  grafana:{pip:[],dbx:'Grafana REST API',desc:'Grafana'},
  phoenix:{pip:[],dbx:'Phoenix JDBC driver',desc:'Apache Phoenix'},
  cockroachdb:{pip:['psycopg2-binary'],dbx:'PostgreSQL JDBC driver',desc:'CockroachDB'},
  timescaledb:{pip:['psycopg2-binary'],dbx:'PostgreSQL JDBC driver',desc:'TimescaleDB'},
  greenplum:{pip:['psycopg2-binary'],dbx:'PostgreSQL JDBC driver',desc:'Greenplum'},
  vertica:{pip:['vertica-python'],dbx:'Vertica JDBC driver',desc:'Vertica'},
  saphana:{pip:['hdbcli'],dbx:'SAP HANA JDBC driver',desc:'SAP HANA'},
  presto:{pip:[],dbx:'Presto JDBC driver',desc:'Presto'},
  trino:{pip:['trino'],dbx:'Trino JDBC driver',desc:'Trino'},
};

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
