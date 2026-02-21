"""
NiFi Controller Service Knowledge Base -- 150+ services mapped to Databricks.

Every NiFi Controller Service type is cataloged with its full Java class name,
functional category, Databricks-equivalent technology, and the list of
properties that should be treated as secrets (mapped to ``dbutils.secrets.get``
at code-generation time).

Usage
-----
>>> from app.knowledge.nifi_services_kb import lookup_service
>>> svc = lookup_service("DBCPConnectionPool")
>>> svc.databricks_equivalent
'Unity Catalog connection / spark.read.format("jdbc")'
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class ServiceDef:
    """Definition for a single NiFi Controller Service type."""

    short_type: str
    full_type: str
    category: str  # connection_pool, record_reader, record_writer, schema_registry,
                   # ssl, credential, cache, lookup, messaging, kerberos, oauth,
                   # http, monitoring, scripting, other
    description: str
    databricks_equivalent: str
    secret_properties: list[str] = field(default_factory=list)
    notes: str = ""


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

SERVICE_KB: dict[str, ServiceDef] = {}


def _register(svc: ServiceDef) -> None:
    """Register a service definition in the knowledge base."""
    SERVICE_KB[svc.short_type] = svc


# ===================================================================
# CONNECTION POOL SERVICES  (8 entries)
# ===================================================================

_register(ServiceDef(
    short_type="DBCPConnectionPool",
    full_type="org.apache.nifi.dbcp.DBCPConnectionPool",
    category="connection_pool",
    description="Standard JDBC connection pool using Apache DBCP.  Supports any JDBC driver.",
    databricks_equivalent="Unity Catalog connection / spark.read.format('jdbc')",
    secret_properties=["Password"],
    notes="Map 'Database Connection URL' to JDBC URL in Spark; use dbutils.secrets for password.",
))

_register(ServiceDef(
    short_type="HikariCPConnectionPool",
    full_type="org.apache.nifi.dbcp.HikariCPConnectionPool",
    category="connection_pool",
    description="High-performance JDBC connection pool using HikariCP.",
    databricks_equivalent="Unity Catalog connection / spark.read.format('jdbc')",
    secret_properties=["Password"],
    notes="Functionally equivalent to DBCPConnectionPool for migration purposes.",
))

_register(ServiceDef(
    short_type="DBCPConnectionPoolLookup",
    full_type="org.apache.nifi.dbcp.DBCPConnectionPoolLookup",
    category="connection_pool",
    description="Routes to different DBCP pools based on a FlowFile attribute lookup key.",
    databricks_equivalent="Parameterized Unity Catalog connection string using widgets / Spark conf",
    secret_properties=["Password"],
    notes="Map each sub-pool to a separate JDBC configuration; select at runtime via Spark conf or notebook widget.",
))

_register(ServiceDef(
    short_type="HiveConnectionPool",
    full_type="org.apache.nifi.dbcp.hive.HiveConnectionPool",
    category="connection_pool",
    description="JDBC connection pool for Apache Hive.",
    databricks_equivalent="spark.sql() / Unity Catalog (Hive-compatible metastore)",
    secret_properties=["Password"],
    notes="Databricks is Hive-metastore compatible; most Hive queries run directly via spark.sql().",
))

_register(ServiceDef(
    short_type="Hive3ConnectionPool",
    full_type="org.apache.nifi.dbcp.hive.Hive3ConnectionPool",
    category="connection_pool",
    description="JDBC connection pool for Apache Hive 3.x with ACID support.",
    databricks_equivalent="spark.sql() / Delta Lake (ACID natively)",
    secret_properties=["Password"],
))

_register(ServiceDef(
    short_type="PhoenixConnectionPool",
    full_type="org.apache.nifi.dbcp.phoenix.PhoenixConnectionPool",
    category="connection_pool",
    description="JDBC connection pool for Apache Phoenix (HBase SQL layer).",
    databricks_equivalent="spark.read.format('jdbc') with Phoenix JDBC driver",
    secret_properties=["Password"],
))

_register(ServiceDef(
    short_type="MongoDBControllerService",
    full_type="org.apache.nifi.mongodb.MongoDBControllerService",
    category="connection_pool",
    description="Provides MongoDB client connections.",
    databricks_equivalent="spark.read.format('mongodb') / pymongo with dbutils.secrets",
    secret_properties=["Password", "URI"],
    notes="Use the MongoDB Spark connector or pymongo for direct access.",
))

_register(ServiceDef(
    short_type="RedisConnectionPoolService",
    full_type="org.apache.nifi.redis.service.RedisConnectionPoolService",
    category="connection_pool",
    description="Connection pool for Redis.",
    databricks_equivalent="redis-py client with dbutils.secrets for connection string",
    secret_properties=["Password"],
))

# ===================================================================
# RECORD READER SERVICES  (16 entries)
# ===================================================================

_register(ServiceDef(
    short_type="JsonTreeReader",
    full_type="org.apache.nifi.json.JsonTreeReader",
    category="record_reader",
    description="Reads JSON data into NiFi Records using a tree (DOM) parser.",
    databricks_equivalent="spark.read.json() / F.from_json()",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="JsonPathReader",
    full_type="org.apache.nifi.json.JsonPathReader",
    category="record_reader",
    description="Reads JSON data using JSONPath expressions to extract fields.",
    databricks_equivalent="spark.read.json() + F.get_json_object()",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="CSVReader",
    full_type="org.apache.nifi.csv.CSVReader",
    category="record_reader",
    description="Reads CSV data into NiFi Records.",
    databricks_equivalent="spark.read.csv() / spark.read.format('csv')",
    secret_properties=[],
    notes="Map NiFi CSV properties (delimiter, quote char, escape char, header) to Spark CSV options.",
))

_register(ServiceDef(
    short_type="AvroReader",
    full_type="org.apache.nifi.avro.AvroReader",
    category="record_reader",
    description="Reads Avro-formatted data into NiFi Records.",
    databricks_equivalent="spark.read.format('avro')",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="ParquetReader",
    full_type="org.apache.nifi.parquet.ParquetReader",
    category="record_reader",
    description="Reads Apache Parquet data into NiFi Records.",
    databricks_equivalent="spark.read.parquet()",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="XMLReader",
    full_type="org.apache.nifi.xml.XMLReader",
    category="record_reader",
    description="Reads XML data into NiFi Records.",
    databricks_equivalent="spark.read.format('xml') (spark-xml library)",
    secret_properties=[],
    notes="Requires the com.databricks:spark-xml library on the cluster.",
))

_register(ServiceDef(
    short_type="ExcelReader",
    full_type="org.apache.nifi.excel.ExcelReader",
    category="record_reader",
    description="Reads Microsoft Excel (.xlsx) data into NiFi Records.",
    databricks_equivalent="pandas.read_excel() + spark.createDataFrame()",
    secret_properties=[],
    notes="No native Spark Excel reader; use pandas or openpyxl as intermediate.",
))

_register(ServiceDef(
    short_type="GrokReader",
    full_type="org.apache.nifi.grok.GrokReader",
    category="record_reader",
    description="Parses unstructured text using Grok patterns (similar to Logstash).",
    databricks_equivalent="F.regexp_extract() with equivalent regex patterns",
    secret_properties=[],
    notes="Convert Grok patterns to Python regex; each named capture group becomes a column.",
))

_register(ServiceDef(
    short_type="SyslogReader",
    full_type="org.apache.nifi.syslog.SyslogReader",
    category="record_reader",
    description="Parses Syslog (RFC 3164 / RFC 5424) formatted data.",
    databricks_equivalent="F.regexp_extract() with syslog regex patterns",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="Syslog5424Reader",
    full_type="org.apache.nifi.syslog.Syslog5424Reader",
    category="record_reader",
    description="Parses Syslog RFC 5424 formatted data with structured data support.",
    databricks_equivalent="F.regexp_extract() with RFC 5424 regex",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="ProtobufReader",
    full_type="org.apache.nifi.protobuf.ProtobufReader",
    category="record_reader",
    description="Reads Protocol Buffers encoded data using a .proto schema.",
    databricks_equivalent="from_protobuf() (Spark 3.4+) or protobuf-python UDF",
    secret_properties=[],
    notes="Spark 3.4+ has native protobuf support via from_protobuf/to_protobuf.",
))

_register(ServiceDef(
    short_type="YAMLTreeReader",
    full_type="org.apache.nifi.yaml.YAMLTreeReader",
    category="record_reader",
    description="Reads YAML data into NiFi Records.",
    databricks_equivalent="UDF with pyyaml: yaml.safe_load() + spark.createDataFrame()",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="WindowsEventLogReader",
    full_type="org.apache.nifi.processors.windows.event.log.reader.WindowsEventLogReader",
    category="record_reader",
    description="Reads Windows Event Log XML entries.",
    databricks_equivalent="spark.read.format('xml') with Windows Event schema",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="CEFReader",
    full_type="org.apache.nifi.cef.CEFReader",
    category="record_reader",
    description="Reads Common Event Format (CEF) data.",
    databricks_equivalent="F.regexp_extract() with CEF regex pattern",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="FreeFormTextRecordSetWriter",
    full_type="org.apache.nifi.text.FreeFormTextRecordSetWriter",
    category="record_reader",
    description="Reads free-form text using a configured schema and delimiters.",
    databricks_equivalent="spark.read.text() + F.regexp_extract()",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="ScriptedReader",
    full_type="org.apache.nifi.processors.script.ScriptedReader",
    category="record_reader",
    description="Uses a user-supplied script (Groovy, Python, etc.) to parse records.",
    databricks_equivalent="Custom PySpark UDF or pandas_udf for parsing",
    secret_properties=[],
    notes="The script logic must be manually translated to a PySpark UDF.",
))

# ===================================================================
# RECORD WRITER SERVICES  (12 entries)
# ===================================================================

_register(ServiceDef(
    short_type="JsonRecordSetWriter",
    full_type="org.apache.nifi.json.JsonRecordSetWriter",
    category="record_writer",
    description="Writes NiFi Records as JSON.",
    databricks_equivalent="df.write.json() / F.to_json()",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="CSVRecordSetWriter",
    full_type="org.apache.nifi.csv.CSVRecordSetWriter",
    category="record_writer",
    description="Writes NiFi Records as CSV.",
    databricks_equivalent="df.write.csv()",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="AvroRecordSetWriter",
    full_type="org.apache.nifi.avro.AvroRecordSetWriter",
    category="record_writer",
    description="Writes NiFi Records in Avro format.",
    databricks_equivalent="df.write.format('avro')",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="ParquetRecordSetWriter",
    full_type="org.apache.nifi.parquet.ParquetRecordSetWriter",
    category="record_writer",
    description="Writes NiFi Records in Parquet format.",
    databricks_equivalent="df.write.parquet()",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="XMLRecordSetWriter",
    full_type="org.apache.nifi.xml.XMLRecordSetWriter",
    category="record_writer",
    description="Writes NiFi Records as XML.",
    databricks_equivalent="df.write.format('xml') (spark-xml library)",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="FreeFormTextRecordSetWriter_w",
    full_type="org.apache.nifi.text.FreeFormTextRecordSetWriter",
    category="record_writer",
    description="Writes NiFi Records as free-form text using a template.",
    databricks_equivalent="df.select(F.format_string(...)).write.text()",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="RecordSetWriterLookup",
    full_type="org.apache.nifi.serialization.RecordSetWriterLookup",
    category="record_writer",
    description="Routes to different record writers based on a FlowFile attribute.",
    databricks_equivalent="Conditional df.write based on column value / partitionBy",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="ScriptedRecordSetWriter",
    full_type="org.apache.nifi.processors.script.ScriptedRecordSetWriter",
    category="record_writer",
    description="Uses a user-supplied script to serialize records.",
    databricks_equivalent="Custom PySpark UDF for serialization",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="ORCRecordSetWriter",
    full_type="org.apache.nifi.orc.ORCRecordSetWriter",
    category="record_writer",
    description="Writes NiFi Records in ORC format.",
    databricks_equivalent="df.write.format('orc')",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="ProtobufRecordSetWriter",
    full_type="org.apache.nifi.protobuf.ProtobufRecordSetWriter",
    category="record_writer",
    description="Writes NiFi Records as Protocol Buffers.",
    databricks_equivalent="to_protobuf() (Spark 3.4+) or protobuf-python UDF",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="SyslogRecordSetWriter",
    full_type="org.apache.nifi.syslog.SyslogRecordSetWriter",
    category="record_writer",
    description="Writes NiFi Records in Syslog format.",
    databricks_equivalent="F.format_string() with syslog template + df.write.text()",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="ELRecordSetWriter",
    full_type="org.apache.nifi.record.sink.lookup.ELRecordSetWriter",
    category="record_writer",
    description="Writes NiFi Records using Expression Language templates.",
    databricks_equivalent="df.select(F.format_string(...)).write.text()",
    secret_properties=[],
))

# ===================================================================
# SCHEMA REGISTRY SERVICES  (6 entries)
# ===================================================================

_register(ServiceDef(
    short_type="ConfluentSchemaRegistry",
    full_type="org.apache.nifi.confluent.schemaregistry.ConfluentSchemaRegistry",
    category="schema_registry",
    description="Connects to Confluent Schema Registry for Avro/JSON/Protobuf schemas.",
    databricks_equivalent="Confluent Schema Registry (same) via spark.readStream.format('kafka') with schema.registry.url",
    secret_properties=["Password", "SSL Keystore Password", "SSL Truststore Password"],
    notes="Databricks supports Confluent Schema Registry natively for Kafka streams.",
))

_register(ServiceDef(
    short_type="HortonworksSchemaRegistry",
    full_type="org.apache.nifi.schemaregistry.hortonworks.HortonworksSchemaRegistry",
    category="schema_registry",
    description="Connects to Hortonworks (Cloudera) Schema Registry.",
    databricks_equivalent="Confluent Schema Registry or Unity Catalog schema management",
    secret_properties=["Password"],
    notes="Migrate schemas to Confluent SR or manage via Unity Catalog.",
))

_register(ServiceDef(
    short_type="AvroSchemaRegistry",
    full_type="org.apache.nifi.avro.AvroSchemaRegistry",
    category="schema_registry",
    description="Local schema registry that stores Avro schemas as controller service properties.",
    databricks_equivalent="Inline StructType schema definition in PySpark",
    secret_properties=[],
    notes="Schemas defined as NiFi properties are converted to PySpark StructType at code-gen time.",
))

_register(ServiceDef(
    short_type="JsonSchemaRegistry",
    full_type="org.apache.nifi.json.JsonSchemaRegistry",
    category="schema_registry",
    description="Local schema registry that stores JSON schemas.",
    databricks_equivalent="Inline StructType schema definition derived from JSON Schema",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="DatabaseTableSchemaRegistry",
    full_type="org.apache.nifi.schemaregistry.DatabaseTableSchemaRegistry",
    category="schema_registry",
    description="Infers schema from a database table.",
    databricks_equivalent="spark.table('catalog.schema.table').schema",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="SchemaRegistryService",
    full_type="org.apache.nifi.schemaregistry.services.SchemaRegistryService",
    category="schema_registry",
    description="Generic schema registry service interface.",
    databricks_equivalent="Unity Catalog schema management",
    secret_properties=[],
))

# ===================================================================
# SSL / TLS SERVICES  (4 entries)
# ===================================================================

_register(ServiceDef(
    short_type="StandardSSLContextService",
    full_type="org.apache.nifi.ssl.StandardSSLContextService",
    category="ssl",
    description="Provides SSL/TLS context for secure connections (keystores, truststores).",
    databricks_equivalent="Cluster SSL configuration / init script for certs / dbutils.secrets for passwords",
    secret_properties=["Keystore Password", "Key Password", "Truststore Password"],
    notes="In Databricks, TLS is usually handled at the cluster level or via init scripts that install certificates.",
))

_register(ServiceDef(
    short_type="StandardRestrictedSSLContextService",
    full_type="org.apache.nifi.ssl.StandardRestrictedSSLContextService",
    category="ssl",
    description="SSL context that restricts to strong ciphers and protocols.",
    databricks_equivalent="Cluster SSL configuration with restricted cipher suites",
    secret_properties=["Keystore Password", "Key Password", "Truststore Password"],
))

_register(ServiceDef(
    short_type="SSLContextService",
    full_type="org.apache.nifi.ssl.SSLContextService",
    category="ssl",
    description="Generic SSL context service interface.",
    databricks_equivalent="Cluster-level TLS / dbutils.secrets for cert passwords",
    secret_properties=["Keystore Password", "Key Password", "Truststore Password"],
))

_register(ServiceDef(
    short_type="KeyStoreService",
    full_type="org.apache.nifi.security.KeyStoreService",
    category="ssl",
    description="Manages Java KeyStore files for certificate-based authentication.",
    databricks_equivalent="dbutils.secrets for keystore passwords; init script to install keystore files",
    secret_properties=["Keystore Password", "Key Password"],
))

# ===================================================================
# CREDENTIAL / PROVIDER SERVICES  (12 entries)
# ===================================================================

_register(ServiceDef(
    short_type="AWSCredentialsProviderControllerService",
    full_type="org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService",
    category="credential",
    description="Provides AWS credentials (access key, secret key, session token, assume role).",
    databricks_equivalent="Instance profile / dbutils.secrets.get(scope, 'aws-access-key') + dbutils.secrets.get(scope, 'aws-secret-key')",
    secret_properties=["Access Key", "Secret Key", "Session Token"],
    notes="Prefer IAM instance profiles on Databricks; fall back to dbutils.secrets.",
))

_register(ServiceDef(
    short_type="AWSCredentialsProviderService",
    full_type="org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService",
    category="credential",
    description="Interface for AWS credential providers.",
    databricks_equivalent="Instance profile / dbutils.secrets",
    secret_properties=["Access Key", "Secret Key"],
))

_register(ServiceDef(
    short_type="AzureStorageCredentialsControllerService",
    full_type="org.apache.nifi.services.azure.storage.AzureStorageCredentialsControllerService",
    category="credential",
    description="Provides Azure Storage credentials (account key, SAS token, managed identity).",
    databricks_equivalent="spark.conf.set('fs.azure.account.key...') via dbutils.secrets",
    secret_properties=["Storage Account Key", "SAS Token"],
    notes="Databricks supports Azure Storage access via account key, SAS, OAuth, or managed identity configured at the cluster or Unity Catalog external location level.",
))

_register(ServiceDef(
    short_type="AzureStorageCredentialsControllerService_v12",
    full_type="org.apache.nifi.services.azure.storage.AzureStorageCredentialsControllerServiceV12",
    category="credential",
    description="V12 Azure Storage credentials for newer Azure SDK.",
    databricks_equivalent="Unity Catalog external locations / dbutils.secrets for Azure credentials",
    secret_properties=["Storage Account Key", "SAS Token", "Client Secret"],
))

_register(ServiceDef(
    short_type="ADLSCredentialsControllerService",
    full_type="org.apache.nifi.services.azure.storage.ADLSCredentialsControllerService",
    category="credential",
    description="Provides ADLS Gen2 credentials (managed identity, service principal, shared key).",
    databricks_equivalent="Unity Catalog external locations for ADLS / spark.conf with abfss:// + dbutils.secrets",
    secret_properties=["Account Key", "Client Secret"],
))

_register(ServiceDef(
    short_type="GCPCredentialsControllerService",
    full_type="org.apache.nifi.processors.gcp.credentials.service.GCPCredentialsControllerService",
    category="credential",
    description="Provides Google Cloud Platform credentials (service account JSON key).",
    databricks_equivalent="spark.conf.set('fs.gs...') / dbutils.secrets for GCP service account key",
    secret_properties=["Service Account JSON"],
    notes="Databricks on GCP uses built-in identity; for cross-cloud access use dbutils.secrets.",
))

_register(ServiceDef(
    short_type="StandardSensitivePropertyProviderService",
    full_type="org.apache.nifi.security.StandardSensitivePropertyProviderService",
    category="credential",
    description="Provides encryption/decryption of sensitive NiFi properties.",
    databricks_equivalent="dbutils.secrets (Databricks secret scopes handle encryption at rest)",
    secret_properties=[],
    notes="All NiFi sensitive properties map to dbutils.secrets.get() in Databricks.",
))

_register(ServiceDef(
    short_type="HashiCorpVaultParameterProvider",
    full_type="org.apache.nifi.vault.HashiCorpVaultParameterProvider",
    category="credential",
    description="Retrieves parameters from HashiCorp Vault.",
    databricks_equivalent="dbutils.secrets with Vault-backed secret scope",
    secret_properties=["Vault Token"],
    notes="Databricks supports Vault-backed secret scopes natively.",
))

_register(ServiceDef(
    short_type="StandardParameterProvider",
    full_type="org.apache.nifi.parameter.StandardParameterProvider",
    category="credential",
    description="Provides parameters from environment variables or property files.",
    databricks_equivalent="Spark conf / dbutils.widgets / environment variables",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="KnoxParameterProvider",
    full_type="org.apache.nifi.parameter.KnoxParameterProvider",
    category="credential",
    description="Retrieves parameters from Apache Knox.",
    databricks_equivalent="dbutils.secrets / cluster init script for Knox tokens",
    secret_properties=["Knox Password"],
))

_register(ServiceDef(
    short_type="ProxyConfigurationService",
    full_type="org.apache.nifi.proxy.ProxyConfigurationService",
    category="credential",
    description="Configures HTTP proxy settings for outbound connections.",
    databricks_equivalent="Cluster proxy settings / spark.conf for http.proxyHost/Port",
    secret_properties=["Proxy Password"],
))

_register(ServiceDef(
    short_type="EnvironmentVariableParameterProvider",
    full_type="org.apache.nifi.parameter.EnvironmentVariableParameterProvider",
    category="credential",
    description="Provides parameters from system environment variables.",
    databricks_equivalent="Spark conf / cluster environment variables / dbutils.widgets",
    secret_properties=[],
))

# ===================================================================
# CACHE SERVICES  (8 entries)
# ===================================================================

_register(ServiceDef(
    short_type="DistributedMapCacheClientService",
    full_type="org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService",
    category="cache",
    description="Client for NiFi's distributed map cache server.",
    databricks_equivalent="Delta table as key-value store / broadcast variable / Redis",
    secret_properties=[],
    notes="Replace with a Delta table lookup, Spark broadcast variable, or external Redis cache.",
))

_register(ServiceDef(
    short_type="DistributedMapCacheServer",
    full_type="org.apache.nifi.distributed.cache.server.map.DistributedMapCacheServer",
    category="cache",
    description="NiFi's built-in distributed map cache server.",
    databricks_equivalent="Delta table or external cache (Redis / Memcached)",
    secret_properties=[],
    notes="No Databricks equivalent of embedded cache server; use external service or Delta.",
))

_register(ServiceDef(
    short_type="DistributedSetCacheClientService",
    full_type="org.apache.nifi.distributed.cache.client.DistributedSetCacheClientService",
    category="cache",
    description="Client for NiFi's distributed set cache server (deduplication).",
    databricks_equivalent="Delta table with MERGE for deduplication / dropDuplicates()",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="DistributedSetCacheServer",
    full_type="org.apache.nifi.distributed.cache.server.set.DistributedSetCacheServer",
    category="cache",
    description="NiFi's built-in distributed set cache server.",
    databricks_equivalent="External set service or Delta table",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="CacheClient",
    full_type="org.apache.nifi.distributed.cache.client.CacheClient",
    category="cache",
    description="Generic cache client interface.",
    databricks_equivalent="Delta table / broadcast variable / Redis",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="RedisDistributedMapCacheClientService",
    full_type="org.apache.nifi.redis.service.RedisDistributedMapCacheClientService",
    category="cache",
    description="Distributed map cache client backed by Redis.",
    databricks_equivalent="redis-py client with dbutils.secrets for connection",
    secret_properties=["Password"],
))

_register(ServiceDef(
    short_type="CouchbaseCacheService",
    full_type="org.apache.nifi.couchbase.CouchbaseCacheService",
    category="cache",
    description="Cache client backed by Couchbase.",
    databricks_equivalent="Couchbase SDK (couchbase-python) with dbutils.secrets",
    secret_properties=["Password"],
))

_register(ServiceDef(
    short_type="HBase_2_ClientMapCacheService",
    full_type="org.apache.nifi.hbase.HBase_2_ClientMapCacheService",
    category="cache",
    description="Cache client backed by Apache HBase 2.x.",
    databricks_equivalent="spark.read.format('hbase') or happybase client",
    secret_properties=[],
))

# ===================================================================
# LOOKUP SERVICES  (14 entries)
# ===================================================================

_register(ServiceDef(
    short_type="SimpleDatabaseLookupService",
    full_type="org.apache.nifi.lookup.db.SimpleDatabaseLookupService",
    category="lookup",
    description="Looks up a single value from a database table by key column.",
    databricks_equivalent="spark.sql('SELECT value FROM table WHERE key = ...') or broadcast join",
    secret_properties=[],
    notes="Replace with a broadcast hash join for performance, or a direct SQL lookup.",
))

_register(ServiceDef(
    short_type="DatabaseRecordLookupService",
    full_type="org.apache.nifi.lookup.db.DatabaseRecordLookupService",
    category="lookup",
    description="Looks up an entire record from a database table by key.",
    databricks_equivalent="spark.sql() lookup or broadcast join on lookup table",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="IPLookupService",
    full_type="org.apache.nifi.lookup.maxmind.IPLookupService",
    category="lookup",
    description="GeoIP lookup using MaxMind GeoIP2 database.",
    databricks_equivalent="UDF with geoip2 library or broadcast MaxMind DB",
    secret_properties=[],
    notes="Load the MaxMind .mmdb file via init script and use a pandas_udf for vectorized lookup.",
))

_register(ServiceDef(
    short_type="CSVRecordLookupService",
    full_type="org.apache.nifi.lookup.CSVRecordLookupService",
    category="lookup",
    description="Looks up records from a CSV file.",
    databricks_equivalent="spark.read.csv() + broadcast join",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="PropertiesFileLookupService",
    full_type="org.apache.nifi.lookup.PropertiesFileLookupService",
    category="lookup",
    description="Looks up values from a Java .properties file.",
    databricks_equivalent="spark.sparkContext.broadcast(dict_from_properties_file) or Spark conf",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="XMLFileLookupService",
    full_type="org.apache.nifi.lookup.XMLFileLookupService",
    category="lookup",
    description="Looks up values by XPath from an XML file.",
    databricks_equivalent="UDF with lxml / ElementTree XPath lookup",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="RestLookupService",
    full_type="org.apache.nifi.lookup.RestLookupService",
    category="lookup",
    description="Looks up records by calling a REST API endpoint.",
    databricks_equivalent="UDF with requests library for REST calls",
    secret_properties=["API Key", "Password"],
))

_register(ServiceDef(
    short_type="DistributedMapCacheLookupService",
    full_type="org.apache.nifi.lookup.DistributedMapCacheLookupService",
    category="lookup",
    description="Looks up values from NiFi's distributed map cache.",
    databricks_equivalent="Delta table lookup or broadcast variable",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="ScriptedLookupService",
    full_type="org.apache.nifi.lookup.script.ScriptedLookupService",
    category="lookup",
    description="User-defined lookup service using a script (Groovy, Python, etc.).",
    databricks_equivalent="Custom PySpark UDF implementing the lookup logic",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="MongoDBLookupService",
    full_type="org.apache.nifi.mongodb.MongoDBLookupService",
    category="lookup",
    description="Looks up records from a MongoDB collection.",
    databricks_equivalent="pymongo lookup UDF or MongoDB Spark connector join",
    secret_properties=["Password", "URI"],
))

_register(ServiceDef(
    short_type="ElasticSearchLookupService",
    full_type="org.apache.nifi.elasticsearch.ElasticSearchLookupService",
    category="lookup",
    description="Looks up documents from Elasticsearch by ID or query.",
    databricks_equivalent="elasticsearch-py client UDF or ES Spark connector",
    secret_properties=["Password", "API Key"],
))

_register(ServiceDef(
    short_type="HBase_2_RecordLookupService",
    full_type="org.apache.nifi.hbase.HBase_2_RecordLookupService",
    category="lookup",
    description="Looks up records from Apache HBase 2.x.",
    databricks_equivalent="happybase client UDF or HBase Spark connector join",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="CouchbaseLookupService",
    full_type="org.apache.nifi.couchbase.CouchbaseLookupService",
    category="lookup",
    description="Looks up documents from Couchbase by key.",
    databricks_equivalent="Couchbase Python SDK lookup UDF",
    secret_properties=["Password"],
))

_register(ServiceDef(
    short_type="KuduLookupService",
    full_type="org.apache.nifi.lookup.kudu.KuduLookupService",
    category="lookup",
    description="Looks up records from Apache Kudu.",
    databricks_equivalent="spark.read.format('kudu') + broadcast join",
    secret_properties=[],
))

# ===================================================================
# MESSAGING SERVICES  (10 entries)
# ===================================================================

_register(ServiceDef(
    short_type="StandardKafkaProducerService",
    full_type="org.apache.nifi.kafka.service.producer.StandardKafkaProducerService",
    category="messaging",
    description="Provides Kafka producer connections for publishing messages.",
    databricks_equivalent="df.write.format('kafka') / KafkaProducer from confluent_kafka",
    secret_properties=["Password", "SSL Keystore Password"],
    notes="Databricks has native Kafka integration via Structured Streaming.",
))

_register(ServiceDef(
    short_type="StandardKafkaConsumerService",
    full_type="org.apache.nifi.kafka.service.consumer.StandardKafkaConsumerService",
    category="messaging",
    description="Provides Kafka consumer connections for consuming messages.",
    databricks_equivalent="spark.readStream.format('kafka')",
    secret_properties=["Password", "SSL Keystore Password"],
))

_register(ServiceDef(
    short_type="KafkaConnectionService",
    full_type="org.apache.nifi.kafka.service.KafkaConnectionService",
    category="messaging",
    description="Shared Kafka connection configuration (bootstrap servers, security).",
    databricks_equivalent="Kafka options dict: {'kafka.bootstrap.servers': '...', 'kafka.security.protocol': '...'}",
    secret_properties=["Password", "SSL Keystore Password", "SSL Truststore Password", "SASL JAAS Config"],
))

_register(ServiceDef(
    short_type="JMSConnectionFactoryProvider",
    full_type="org.apache.nifi.jms.cf.JMSConnectionFactoryProvider",
    category="messaging",
    description="Provides JMS connection factory for ActiveMQ, IBM MQ, etc.",
    databricks_equivalent="UDF with stomp.py / pika (RabbitMQ) or vendor-specific Python SDK",
    secret_properties=["Password"],
    notes="JMS is Java-specific; translate to the Python equivalent of the underlying broker.",
))

_register(ServiceDef(
    short_type="AmazonSQSService",
    full_type="org.apache.nifi.processors.aws.sqs.AmazonSQSService",
    category="messaging",
    description="Provides Amazon SQS client connections.",
    databricks_equivalent="boto3.client('sqs') with dbutils.secrets for AWS credentials",
    secret_properties=["Access Key", "Secret Key"],
))

_register(ServiceDef(
    short_type="AmazonSNSService",
    full_type="org.apache.nifi.processors.aws.sns.AmazonSNSService",
    category="messaging",
    description="Provides Amazon SNS client connections.",
    databricks_equivalent="boto3.client('sns') with dbutils.secrets",
    secret_properties=["Access Key", "Secret Key"],
))

_register(ServiceDef(
    short_type="AmazonKinesisService",
    full_type="org.apache.nifi.processors.aws.kinesis.AmazonKinesisService",
    category="messaging",
    description="Provides Amazon Kinesis client connections.",
    databricks_equivalent="spark.readStream.format('kinesis') or boto3.client('kinesis')",
    secret_properties=["Access Key", "Secret Key"],
))

_register(ServiceDef(
    short_type="AzureEventHubService",
    full_type="org.apache.nifi.services.azure.eventhub.AzureEventHubService",
    category="messaging",
    description="Provides Azure Event Hub connections.",
    databricks_equivalent="spark.readStream.format('eventhubs') with dbutils.secrets for connection string",
    secret_properties=["Event Hub Connection String", "SAS Key"],
))

_register(ServiceDef(
    short_type="PubSubService",
    full_type="org.apache.nifi.processors.gcp.pubsub.PubSubService",
    category="messaging",
    description="Provides Google Cloud Pub/Sub connections.",
    databricks_equivalent="google-cloud-pubsub Python client with service account from dbutils.secrets",
    secret_properties=["Service Account JSON"],
))

_register(ServiceDef(
    short_type="RabbitMQConnectionService",
    full_type="org.apache.nifi.amqp.processors.RabbitMQConnectionService",
    category="messaging",
    description="Provides RabbitMQ / AMQP connections.",
    databricks_equivalent="pika library with dbutils.secrets for connection credentials",
    secret_properties=["Password"],
))

# ===================================================================
# KERBEROS SERVICES  (4 entries)
# ===================================================================

_register(ServiceDef(
    short_type="KeytabCredentialsService",
    full_type="org.apache.nifi.kerberos.KeytabCredentialsService",
    category="kerberos",
    description="Provides Kerberos authentication using a keytab file.",
    databricks_equivalent="Cluster-level Kerberos config / init script with kinit",
    secret_properties=["Keytab"],
    notes="Databricks handles Kerberos at the cluster level via init scripts or admin console.",
))

_register(ServiceDef(
    short_type="SelfContainedKerberosCredentialsService",
    full_type="org.apache.nifi.kerberos.SelfContainedKerberosCredentialsService",
    category="kerberos",
    description="Provides Kerberos authentication with embedded keytab data.",
    databricks_equivalent="Cluster-level Kerberos config",
    secret_properties=["Keytab"],
))

_register(ServiceDef(
    short_type="KerberosPasswordCredentialsService",
    full_type="org.apache.nifi.kerberos.KerberosPasswordCredentialsService",
    category="kerberos",
    description="Provides Kerberos authentication with a principal and password.",
    databricks_equivalent="Cluster-level Kerberos config / dbutils.secrets for password",
    secret_properties=["Password"],
))

_register(ServiceDef(
    short_type="KerberosUserService",
    full_type="org.apache.nifi.kerberos.KerberosUserService",
    category="kerberos",
    description="Generic Kerberos user/principal service.",
    databricks_equivalent="Cluster-level Kerberos configuration",
    secret_properties=["Keytab", "Password"],
))

# ===================================================================
# OAUTH SERVICES  (4 entries)
# ===================================================================

_register(ServiceDef(
    short_type="StandardOauth2AccessTokenProvider",
    full_type="org.apache.nifi.oauth2.StandardOauth2AccessTokenProvider",
    category="oauth",
    description="Obtains OAuth 2.0 access tokens (client credentials, authorization code, password).",
    databricks_equivalent="requests_oauthlib or msal library with dbutils.secrets for client_id/secret",
    secret_properties=["Client Secret", "Password", "Access Token"],
))

_register(ServiceDef(
    short_type="OAuth2TokenProvider",
    full_type="org.apache.nifi.oauth2.OAuth2TokenProvider",
    category="oauth",
    description="Interface for OAuth 2.0 token providers.",
    databricks_equivalent="requests_oauthlib or msal library",
    secret_properties=["Client Secret"],
))

_register(ServiceDef(
    short_type="OktaOAuth2AccessTokenProvider",
    full_type="org.apache.nifi.oauth2.OktaOAuth2AccessTokenProvider",
    category="oauth",
    description="Obtains OAuth 2.0 tokens from Okta identity provider.",
    databricks_equivalent="okta-jwt-verifier or requests_oauthlib with Okta endpoints + dbutils.secrets",
    secret_properties=["Client Secret", "API Token"],
))

_register(ServiceDef(
    short_type="AzureADOAuth2AccessTokenProvider",
    full_type="org.apache.nifi.oauth2.AzureADOAuth2AccessTokenProvider",
    category="oauth",
    description="Obtains OAuth 2.0 tokens from Azure Active Directory.",
    databricks_equivalent="msal library with dbutils.secrets for client_id/secret/tenant_id",
    secret_properties=["Client Secret"],
    notes="Databricks on Azure can use managed identity; for cross-tenant use MSAL.",
))

# ===================================================================
# HTTP SERVICES  (4 entries)
# ===================================================================

_register(ServiceDef(
    short_type="StandardHttpContextMap",
    full_type="org.apache.nifi.http.StandardHttpContextMap",
    category="http",
    description="Holds HTTP request/response context for HandleHttpRequest/Response processors.",
    databricks_equivalent="Not applicable (no embedded HTTP server); use Flask/FastAPI endpoint or Databricks SQL endpoint",
    secret_properties=[],
    notes="NiFi's embedded HTTP listener pattern maps to a separate web service in Databricks.",
))

_register(ServiceDef(
    short_type="OkHttpClientService",
    full_type="org.apache.nifi.http.OkHttpClientService",
    category="http",
    description="HTTP client service using OkHttp library.",
    databricks_equivalent="requests library / urllib3 with dbutils.secrets for auth",
    secret_properties=["Password", "Bearer Token"],
))

_register(ServiceDef(
    short_type="StandardHttpClientService",
    full_type="org.apache.nifi.http.StandardHttpClientService",
    category="http",
    description="HTTP client service using Java HttpURLConnection.",
    databricks_equivalent="requests library with dbutils.secrets for credentials",
    secret_properties=["Password", "Bearer Token"],
))

_register(ServiceDef(
    short_type="Jetty9WebSocketClientService",
    full_type="org.apache.nifi.websocket.jetty.Jetty9WebSocketClientService",
    category="http",
    description="WebSocket client using Jetty 9.",
    databricks_equivalent="websockets library (Python asyncio WebSocket client)",
    secret_properties=["Password"],
))

# ===================================================================
# MONITORING / REPORTING SERVICES  (6 entries)
# ===================================================================

_register(ServiceDef(
    short_type="SiteToSiteReportingTask",
    full_type="org.apache.nifi.reporting.SiteToSiteReportingTask",
    category="monitoring",
    description="Sends NiFi provenance and bulletins to another NiFi via Site-to-Site.",
    databricks_equivalent="Databricks audit logs / system tables (system.access.audit)",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="PrometheusReportingTask",
    full_type="org.apache.nifi.reporting.prometheus.PrometheusReportingTask",
    category="monitoring",
    description="Exposes NiFi metrics in Prometheus format.",
    databricks_equivalent="Databricks Ganglia metrics / custom Prometheus pushgateway from driver",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="AmbariReportingTask",
    full_type="org.apache.nifi.reporting.ambari.AmbariReportingTask",
    category="monitoring",
    description="Sends NiFi metrics to Apache Ambari.",
    databricks_equivalent="Databricks built-in monitoring / Ganglia / custom metrics",
    secret_properties=["Password"],
))

_register(ServiceDef(
    short_type="DataDogReportingTask",
    full_type="org.apache.nifi.reporting.datadog.DataDogReportingTask",
    category="monitoring",
    description="Sends NiFi metrics to Datadog.",
    databricks_equivalent="Datadog integration for Databricks / custom datadog-api-client from driver",
    secret_properties=["API Key"],
))

_register(ServiceDef(
    short_type="ElasticSearchReportingTask",
    full_type="org.apache.nifi.reporting.elasticsearch.ElasticSearchReportingTask",
    category="monitoring",
    description="Sends NiFi metrics and provenance to Elasticsearch.",
    databricks_equivalent="Log4j appender to Elasticsearch from driver / custom elasticsearch-py integration",
    secret_properties=["Password"],
))

_register(ServiceDef(
    short_type="SplunkReportingTask",
    full_type="org.apache.nifi.reporting.splunk.SplunkReportingTask",
    category="monitoring",
    description="Sends NiFi metrics to Splunk HEC.",
    databricks_equivalent="Splunk HEC integration from Databricks / splunk-sdk-python from driver",
    secret_properties=["HEC Token"],
))

# ===================================================================
# SCRIPTING / CUSTOM SERVICES  (4 entries)
# ===================================================================

_register(ServiceDef(
    short_type="ScriptedControllerService",
    full_type="org.apache.nifi.processors.script.ScriptedControllerService",
    category="scripting",
    description="User-defined controller service using a script.",
    databricks_equivalent="Custom Python class or PySpark UDF",
    secret_properties=[],
    notes="The script must be manually translated to Python.",
))

_register(ServiceDef(
    short_type="GroovyControllerService",
    full_type="org.apache.nifi.processors.groovy.GroovyControllerService",
    category="scripting",
    description="Controller service written in Groovy.",
    databricks_equivalent="Custom Python class (translate Groovy logic to Python)",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="JythonControllerService",
    full_type="org.apache.nifi.processors.jython.JythonControllerService",
    category="scripting",
    description="Controller service written in Jython (Python 2 on JVM).",
    databricks_equivalent="Native Python 3 class / module",
    secret_properties=[],
    notes="Jython scripts are near-Python; upgrade syntax to Python 3.",
))

_register(ServiceDef(
    short_type="ClojureControllerService",
    full_type="org.apache.nifi.processors.clojure.ClojureControllerService",
    category="scripting",
    description="Controller service written in Clojure.",
    databricks_equivalent="Custom Python class (translate Clojure logic to Python)",
    secret_properties=[],
))

# ===================================================================
# RECORD SINK SERVICES  (4 entries)
# ===================================================================

_register(ServiceDef(
    short_type="ProvenanceReportingRecordSink",
    full_type="org.apache.nifi.record.sink.ProvenanceReportingRecordSink",
    category="other",
    description="Sends provenance records to a record sink (e.g. database).",
    databricks_equivalent="Databricks system tables / Delta audit table",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="DatabaseRecordSink",
    full_type="org.apache.nifi.record.sink.db.DatabaseRecordSink",
    category="other",
    description="Writes records to a database table via JDBC.",
    databricks_equivalent="df.write.format('jdbc') or df.write.saveAsTable()",
    secret_properties=["Password"],
))

_register(ServiceDef(
    short_type="KafkaRecordSink",
    full_type="org.apache.nifi.record.sink.kafka.KafkaRecordSink",
    category="other",
    description="Writes records to a Kafka topic.",
    databricks_equivalent="df.write.format('kafka')",
    secret_properties=["Password"],
))

_register(ServiceDef(
    short_type="SiteToSiteRecordSink",
    full_type="org.apache.nifi.record.sink.SiteToSiteRecordSink",
    category="other",
    description="Sends records to a remote NiFi via Site-to-Site protocol.",
    databricks_equivalent="Not applicable; replace with direct target write (Delta, Kafka, JDBC)",
    secret_properties=[],
))

# ===================================================================
# ELASTICSEARCH SERVICES  (4 entries)
# ===================================================================

_register(ServiceDef(
    short_type="ElasticSearchClientServiceImpl",
    full_type="org.apache.nifi.elasticsearch.ElasticSearchClientServiceImpl",
    category="connection_pool",
    description="Client for Elasticsearch REST API.",
    databricks_equivalent="elasticsearch-py client with dbutils.secrets for auth",
    secret_properties=["Password", "API Key"],
))

_register(ServiceDef(
    short_type="ElasticSearchClientService",
    full_type="org.apache.nifi.elasticsearch.ElasticSearchClientService",
    category="connection_pool",
    description="Interface for Elasticsearch client services.",
    databricks_equivalent="elasticsearch-py client",
    secret_properties=["Password", "API Key"],
))

_register(ServiceDef(
    short_type="ElasticSearchStringLookupService",
    full_type="org.apache.nifi.elasticsearch.ElasticSearchStringLookupService",
    category="lookup",
    description="Looks up string values from Elasticsearch documents.",
    databricks_equivalent="elasticsearch-py get() UDF",
    secret_properties=["Password", "API Key"],
))

_register(ServiceDef(
    short_type="ElasticSearchRecordLookupService",
    full_type="org.apache.nifi.elasticsearch.ElasticSearchRecordLookupService",
    category="lookup",
    description="Looks up full records from Elasticsearch documents.",
    databricks_equivalent="elasticsearch-py search() UDF or ES Spark connector join",
    secret_properties=["Password", "API Key"],
))

# ===================================================================
# SPLUNK SERVICES  (2 entries)
# ===================================================================

_register(ServiceDef(
    short_type="SplunkClientService",
    full_type="org.apache.nifi.splunk.SplunkClientService",
    category="connection_pool",
    description="Client for Splunk HEC and REST API.",
    databricks_equivalent="splunk-sdk-python / requests with Splunk HEC token from dbutils.secrets",
    secret_properties=["HEC Token", "Password"],
))

_register(ServiceDef(
    short_type="SplunkSearchService",
    full_type="org.apache.nifi.splunk.SplunkSearchService",
    category="lookup",
    description="Executes Splunk searches via the REST API.",
    databricks_equivalent="splunk-sdk-python search UDF",
    secret_properties=["Password"],
))

# ===================================================================
# SOLR SERVICES  (2 entries)
# ===================================================================

_register(ServiceDef(
    short_type="SolrClientService",
    full_type="org.apache.nifi.solr.SolrClientService",
    category="connection_pool",
    description="Client for Apache Solr search platform.",
    databricks_equivalent="pysolr or requests with Solr REST API",
    secret_properties=["Password"],
))

_register(ServiceDef(
    short_type="Solr6ClientService",
    full_type="org.apache.nifi.solr.Solr6ClientService",
    category="connection_pool",
    description="Client for Apache Solr 6.x.",
    databricks_equivalent="pysolr with dbutils.secrets for auth",
    secret_properties=["Password"],
))

# ===================================================================
# CLOUD STORAGE SERVICES  (8 entries)
# ===================================================================

_register(ServiceDef(
    short_type="AmazonS3Client",
    full_type="org.apache.nifi.processors.aws.s3.AmazonS3Client",
    category="connection_pool",
    description="Provides Amazon S3 client connections.",
    databricks_equivalent="spark.read/write with s3:// path + instance profile or dbutils.secrets",
    secret_properties=["Access Key", "Secret Key"],
))

_register(ServiceDef(
    short_type="AzureBlobStorageService",
    full_type="org.apache.nifi.services.azure.storage.AzureBlobStorageService",
    category="connection_pool",
    description="Provides Azure Blob Storage connections.",
    databricks_equivalent="spark.read/write with wasbs:// path + dbutils.secrets for account key",
    secret_properties=["Storage Account Key", "SAS Token"],
))

_register(ServiceDef(
    short_type="AzureDataLakeStorageService",
    full_type="org.apache.nifi.services.azure.storage.AzureDataLakeStorageService",
    category="connection_pool",
    description="Provides Azure Data Lake Storage Gen2 connections.",
    databricks_equivalent="spark.read/write with abfss:// path + Unity Catalog external locations",
    secret_properties=["Account Key", "Client Secret"],
))

_register(ServiceDef(
    short_type="GCSClient",
    full_type="org.apache.nifi.processors.gcp.storage.GCSClient",
    category="connection_pool",
    description="Provides Google Cloud Storage client connections.",
    databricks_equivalent="spark.read/write with gs:// path + dbutils.secrets for service account",
    secret_properties=["Service Account JSON"],
))

_register(ServiceDef(
    short_type="HDFSConnectionService",
    full_type="org.apache.nifi.hadoop.HDFSConnectionService",
    category="connection_pool",
    description="Provides HDFS client connections.",
    databricks_equivalent="spark.read/write with dbfs:/ or abfss:/ (HDFS replaced by cloud storage)",
    secret_properties=[],
    notes="HDFS is typically replaced by DBFS, S3, ADLS, or GCS in Databricks.",
))

_register(ServiceDef(
    short_type="SFTPClient",
    full_type="org.apache.nifi.processors.standard.SFTPClient",
    category="connection_pool",
    description="Provides SFTP client connections.",
    databricks_equivalent="paramiko library with dbutils.secrets for private key / password",
    secret_properties=["Password", "Private Key Passphrase"],
))

_register(ServiceDef(
    short_type="FTPClient",
    full_type="org.apache.nifi.processors.standard.FTPClient",
    category="connection_pool",
    description="Provides FTP client connections.",
    databricks_equivalent="ftplib with dbutils.secrets for credentials",
    secret_properties=["Password"],
))

_register(ServiceDef(
    short_type="SMBClientProviderService",
    full_type="org.apache.nifi.services.smb.SMBClientProviderService",
    category="connection_pool",
    description="Provides SMB/CIFS file share connections.",
    databricks_equivalent="smbprotocol / pysmb with dbutils.secrets for credentials",
    secret_properties=["Password"],
))

# ===================================================================
# NOTIFICATION / ALERTING SERVICES  (4 entries)
# ===================================================================

_register(ServiceDef(
    short_type="EmailNotificationService",
    full_type="org.apache.nifi.notifications.EmailNotificationService",
    category="other",
    description="Sends email notifications on NiFi events.",
    databricks_equivalent="smtplib / sendgrid SDK with dbutils.secrets for SMTP credentials",
    secret_properties=["Password"],
))

_register(ServiceDef(
    short_type="SlackNotificationService",
    full_type="org.apache.nifi.notifications.SlackNotificationService",
    category="other",
    description="Sends notifications to a Slack channel via webhook.",
    databricks_equivalent="requests.post(webhook_url) with dbutils.secrets for webhook URL",
    secret_properties=["Webhook URL"],
))

_register(ServiceDef(
    short_type="PagerDutyNotificationService",
    full_type="org.apache.nifi.notifications.PagerDutyNotificationService",
    category="other",
    description="Triggers PagerDuty incidents.",
    databricks_equivalent="pdpyras (PagerDuty Python SDK) with dbutils.secrets for routing key",
    secret_properties=["Routing Key"],
))

_register(ServiceDef(
    short_type="JiraNotificationService",
    full_type="org.apache.nifi.notifications.JiraNotificationService",
    category="other",
    description="Creates Jira issues on NiFi events.",
    databricks_equivalent="jira library (Python Jira client) with dbutils.secrets for API token",
    secret_properties=["API Token", "Password"],
))

# ===================================================================
# MISCELLANEOUS SERVICES  (8 entries)
# ===================================================================

_register(ServiceDef(
    short_type="VolatileProvenanceRepository",
    full_type="org.apache.nifi.provenance.VolatileProvenanceRepository",
    category="other",
    description="In-memory provenance repository (volatile, not persisted).",
    databricks_equivalent="Spark event log / Delta audit table",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="FileSystemRepository",
    full_type="org.apache.nifi.provenance.FileSystemRepository",
    category="other",
    description="File-system-backed content repository.",
    databricks_equivalent="DBFS / cloud object storage (S3, ADLS, GCS)",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="StandardFlowFileSwapManager",
    full_type="org.apache.nifi.controller.StandardFlowFileSwapManager",
    category="other",
    description="Manages swapping FlowFiles to/from disk when queues are full.",
    databricks_equivalent="Spark disk spill / shuffle service (automatic)",
    secret_properties=[],
    notes="Spark handles memory/disk spilling automatically.",
))

_register(ServiceDef(
    short_type="RecordReaderLookup",
    full_type="org.apache.nifi.serialization.RecordReaderLookup",
    category="record_reader",
    description="Routes to different record readers based on a FlowFile attribute.",
    databricks_equivalent="Conditional spark.read based on file extension / attribute",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="RecordPathPropertyService",
    full_type="org.apache.nifi.record.path.RecordPathPropertyService",
    category="other",
    description="Provides RecordPath expressions for nested record field access.",
    databricks_equivalent="PySpark nested column access: F.col('parent.child') / F.col('array[0]')",
    secret_properties=[],
))

_register(ServiceDef(
    short_type="InfluxDatabaseService",
    full_type="org.apache.nifi.influxdb.InfluxDatabaseService",
    category="connection_pool",
    description="Provides InfluxDB client connections.",
    databricks_equivalent="influxdb-client-python with dbutils.secrets for token",
    secret_properties=["Token", "Password"],
))

_register(ServiceDef(
    short_type="CassandraSessionProvider",
    full_type="org.apache.nifi.cassandra.CassandraSessionProvider",
    category="connection_pool",
    description="Provides Apache Cassandra session connections.",
    databricks_equivalent="spark.read.format('org.apache.spark.sql.cassandra') or cassandra-driver",
    secret_properties=["Password"],
))

_register(ServiceDef(
    short_type="Neo4JClientService",
    full_type="org.apache.nifi.graph.Neo4JClientService",
    category="connection_pool",
    description="Provides Neo4j graph database connections.",
    databricks_equivalent="neo4j Python driver with dbutils.secrets for credentials",
    secret_properties=["Password"],
))


# ===================================================================
# LOOKUP / HELPER FUNCTIONS
# ===================================================================

def lookup_service(name: str) -> Optional[ServiceDef]:
    """Look up a controller service definition by short type name.

    Performs an exact match first, then a case-insensitive search, and finally
    checks whether *name* appears as a substring of any ``full_type``.

    Parameters
    ----------
    name:
        The short type name (e.g. ``"DBCPConnectionPool"``) or a fragment of
        the full Java class name.

    Returns
    -------
    ServiceDef | None
        The matching definition, or ``None`` if not found.
    """
    # Exact match on short_type
    if name in SERVICE_KB:
        return SERVICE_KB[name]

    # Case-insensitive match on short_type
    name_lower = name.lower()
    for key, svc in SERVICE_KB.items():
        if key.lower() == name_lower:
            return svc

    # Substring match on full_type
    for svc in SERVICE_KB.values():
        if name in svc.full_type:
            return svc

    # Case-insensitive substring on full_type
    for svc in SERVICE_KB.values():
        if name_lower in svc.full_type.lower():
            return svc

    return None


def get_all_services() -> dict[str, ServiceDef]:
    """Return the entire controller service knowledge base (defensive copy).

    Returns
    -------
    dict[str, ServiceDef]
        A shallow copy of the knowledge-base dictionary keyed by short type.
    """
    return dict(SERVICE_KB)


def get_services_by_category(category: str) -> list[ServiceDef]:
    """Return all controller services belonging to *category*.

    Parameters
    ----------
    category:
        One of: ``connection_pool``, ``record_reader``, ``record_writer``,
        ``schema_registry``, ``ssl``, ``credential``, ``cache``, ``lookup``,
        ``messaging``, ``kerberos``, ``oauth``, ``http``, ``monitoring``,
        ``scripting``, ``other``.

    Returns
    -------
    list[ServiceDef]
        Matching definitions sorted by short type name.
    """
    return sorted(
        [svc for svc in SERVICE_KB.values() if svc.category == category],
        key=lambda svc: svc.short_type,
    )


def get_service_categories() -> list[str]:
    """Return a sorted list of all distinct service categories.

    Returns
    -------
    list[str]
        Distinct category names in alphabetical order.
    """
    return sorted({svc.category for svc in SERVICE_KB.values()})


def get_secret_properties_for_service(name: str) -> list[str]:
    """Return the list of properties that should be treated as secrets.

    Parameters
    ----------
    name:
        The short type name of the controller service.

    Returns
    -------
    list[str]
        Property names that should map to ``dbutils.secrets.get()``, or an
        empty list if the service is not found or has no secret properties.
    """
    svc = lookup_service(name)
    return svc.secret_properties if svc else []


def search_services(query: str) -> list[ServiceDef]:
    """Full-text search across service types, descriptions, and equivalents.

    Parameters
    ----------
    query:
        Case-insensitive search term.

    Returns
    -------
    list[ServiceDef]
        All matching definitions, sorted by short type.
    """
    query_lower = query.lower()
    results = []
    for svc in SERVICE_KB.values():
        if (query_lower in svc.short_type.lower()
                or query_lower in svc.full_type.lower()
                or query_lower in svc.description.lower()
                or query_lower in svc.databricks_equivalent.lower()
                or query_lower in svc.notes.lower()):
            results.append(svc)
    return sorted(results, key=lambda svc: svc.short_type)
