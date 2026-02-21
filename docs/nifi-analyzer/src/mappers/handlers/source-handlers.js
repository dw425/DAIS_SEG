/**
 * mappers/handlers/source-handlers.js -- Source processor smart code generation
 *
 * Extracted from index.html lines ~4232-4294, 4306-4325, 4361-4365, 4708-4712.
 * Handles: ListenHTTP, HandleHttpRequest, ConsumeKafka*, GetSFTP/FTP, GetFile,
 * QueryDatabaseTable, ExecuteSQL, GenerateFlowFile, TailFile, ConsumeKinesisStream,
 * CaptureChangeMySQL, and generic Get, List, Listen, Fetch fallbacks.
 */

/**
 * Generate Databricks code for source-type NiFi processors.
 *
 * @param {object} p - Processor object { name, type, properties, ... }
 * @param {object} props - Processor properties (variable-resolved)
 * @param {string} varName - Sanitized output variable name
 * @param {string} inputVar - Sanitized input variable name
 * @param {string} existingCode - Code from template resolution (may be overridden)
 * @param {number} existingConf - Confidence from template resolution
 * @returns {{ code: string, conf: number }|null} - Generated code or null if not handled
 */
export function handleSourceProcessor(p, props, varName, inputVar, existingCode, existingConf) {
  let code = existingCode;
  let conf = existingConf;

  // -- HTTP Listeners (ListenHTTP, HandleHttpRequest) --
  if (p.type === 'ListenHTTP' || p.type === 'HandleHttpRequest') {
    const port = props['Listening Port'] || props['Port'] || '8080';
    const basePath = props['Base Path'] || '/api/v1';
    code = '# HTTP Endpoint: ' + p.name + '\n' +
      '# NiFi HTTP listener on port ' + port + ', path: ' + basePath + '\n' +
      '#\n' +
      '# DO NOT run a blocking web server (Flask/FastAPI) in a notebook cell.\n' +
      '# It will hang indefinitely and block all downstream execution.\n' +
      '#\n' +
      '# OPTION 1 (RECOMMENDED): Databricks Model Serving Endpoint\n' +
      '# Deploy as an MLflow model serving endpoint that writes to Delta table.\n' +
      '# See: https://docs.databricks.com/machine-learning/model-serving/\n' +
      '#\n' +
      '# OPTION 2: Databricks Apps (Gradio/Streamlit on port 8080)\n' +
      '# See: databricks.yml app deployment\n' +
      '#\n' +
      '# OPTION 3: External API Gateway -> Databricks Job trigger\n' +
      '# AWS API Gateway / Azure APIM -> triggers Databricks Job via REST API\n' +
      '#\n' +
      '# Implementation: Read from Delta landing table (populated by serving endpoint)\n' +
      'df_' + varName + ' = (spark.readStream\n' +
      '    .format("delta")\n' +
      '    .table("' + varName + '_incoming")\n' +
      ')\n' +
      'print(f"[HTTP] Endpoint: streaming from ' + varName + '_incoming Delta table")';
    conf = 0.92;
    return { code, conf };
  }

  // -- Kafka consumers --
  if (/^Consume(Kafka|KafkaRecord)/.test(p.type)) {
    const brokers = props['Kafka Brokers'] || props['bootstrap.servers'] || 'kafka:9092';
    const topic = props['Topic Name(s)'] || props['topic'] || 'default_topic';
    const groupId = props['Group ID'] || props['group.id'] || 'consumer_group';
    const offsetReset = props['Offset Reset'] || props['auto.offset.reset'] || 'earliest';
    const secProto = props['Security Protocol'] || '';
    let secOptions = '';
    if (secProto.includes('SASL')) secOptions = `\n  .option("kafka.security.protocol", "${secProto}")\n  .option("kafka.sasl.mechanism", "PLAIN")\n  .option("kafka.sasl.jaas.config", f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{_kafka_user}' password='{_kafka_pass}';")\n`;
    code = `# Kafka Consumer: ${p.name}\n# Topic: ${topic} | Group: ${groupId} | Brokers: ${brokers}\n_kafka_user = dbutils.secrets.get(scope='kafka', key='user')\n_kafka_pass = dbutils.secrets.get(scope='kafka', key='pass')\ndf_${varName} = (spark.readStream\n  .format("kafka")\n  .option("kafka.bootstrap.servers", "${brokers}")\n  .option("subscribe", "${topic}")\n  .option("kafka.group.id", "${groupId}")\n  .option("startingOffsets", "${offsetReset}")\n  .option("maxOffsetsPerTrigger", 10000)${secOptions}\n  .load()\n  .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value", "topic", "partition", "offset", "timestamp")\n)\nprint(f"[KAFKA] Consuming from ${topic} with group ${groupId}")`;
    conf = 0.95;
    return { code, conf };
  }

  // -- SFTP/FTP sources --
  if (/^(Get|Fetch|List)(SFTP|FTP)$/.test(p.type)) {
    const host = props['Hostname'] || 'sftp.example.com';
    const port = props['Port'] || '22';
    const user = props['Username'] || 'sftp_user';
    const remotePath = props['Remote Path'] || '/';
    const fileFilter = props['File Filter Regex'] || props['File Filter'] || '.*';
    code = `# ${p.type}: ${p.name}\n# Host: ${host}:${port} | Path: ${remotePath} | Filter: ${fileFilter}\nimport paramiko\n_ssh = paramiko.SSHClient()\n_ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())\n_ssh.connect("${host}", port=int("${port}"), username="${user}",\n    password=dbutils.secrets.get(scope="sftp", key="password"))\n_sftp = _ssh.open_sftp()\n\nimport re as _re\n_files = [f for f in _sftp.listdir("${remotePath}") if _re.match(r"${fileFilter}", f)]\n_data = []\nfor _fname in _files:\n    with _sftp.open(f"${remotePath}/{_fname}") as _f:\n        _data.append({"filename": _fname, "content": _f.read().decode("utf-8", errors="replace")})\n\ndf_${varName} = spark.createDataFrame(_data) if _data else spark.createDataFrame([], "filename STRING, content STRING")\n_sftp.close()\n_ssh.close()\nprint(f"[SFTP] Fetched {len(_files)} files from ${host}:${remotePath}")`;
    conf = 0.90;
    return { code, conf };
  }

  // -- Database queries (ExecuteSQL, QueryDatabaseTable, etc.) --
  if (/^(ExecuteSQL|QueryDatabase|GenerateTableFetch)/.test(p.type)) {
    const dbPool = props['Database Connection Pooling Service'] || props['JDBC Connection Pool'] || '';
    const query = props['SQL select query'] || props['SQL Statement'] || '';
    const table = props['Table Name'] || '';
    const maxRows = props['Max Rows Per Flow File'] || '0';
    let jdbcUrl = 'jdbc:database://host:port/db';
    let driver = 'com.database.Driver';
    if (/oracle/i.test(dbPool)) { jdbcUrl = 'jdbc:oracle:thin:@db_host:1521:db_sid'; driver = 'oracle.jdbc.driver.OracleDriver'; }
    else if (/postgres/i.test(dbPool)) { jdbcUrl = 'jdbc:postgresql://pg_host:5432/pg_db'; driver = 'org.postgresql.Driver'; }
    else if (/mysql/i.test(dbPool)) { jdbcUrl = 'jdbc:mysql://mysql_host:3306/mysql_db'; driver = 'com.mysql.cj.jdbc.Driver'; }
    else if (/hive/i.test(dbPool)) { jdbcUrl = ''; driver = ''; }
    if (jdbcUrl) {
      const sqlOrTable = query ? `"(` + query.replace(/"/g, '\\"').substring(0, 200) + `) AS subq"` : `"${table}"`;
      code = `# SQL Query: ${p.name}\n# Pool: ${dbPool} | Table: ${table || '(custom query)'}\ndf_${varName} = (spark.read\n  .format("jdbc")\n  .option("url", "${jdbcUrl}")\n  .option("dbtable", ${sqlOrTable})\n  .option("driver", "${driver}")\n  .option("user", dbutils.secrets.get(scope="db", key="user"))\n  .option("password", dbutils.secrets.get(scope="db", key="pass"))` + (maxRows !== '0' ? `\n  .option("fetchsize", "${maxRows}")` : '') + `\n  .load()\n)\nprint(f"[SQL] Read from ${table || 'query'}")`;
    } else {
      const sqlText = query || 'SELECT * FROM ' + table;
      const safeSqlText = sqlText.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, ' ');
      code = `# SQL Query: ${p.name} (via Hive/Spark SQL)\ndf_${varName} = spark.sql("${safeSqlText}")\nprint("[SQL] Read from Hive/Spark SQL")`;
    }
    conf = 0.92;
    return { code, conf };
  }

  // -- GenerateFlowFile --
  if (p.type === 'GenerateFlowFile') {
    const batch = props['Batch Size'] || '1';
    code = `# Generate Test Data: ${p.name}\nfrom pyspark.sql.functions import current_timestamp, lit\ndf_${varName} = spark.range(${batch}).toDF("id")\ndf_${varName} = df_${varName}.withColumn("_generated_at", current_timestamp())\nprint(f"[GEN] Generated ${batch} test records")`;
    conf = 0.95;
    return { code, conf };
  }

  // -- TailFile --
  if (p.type === 'TailFile') {
    const file = props['File(s) to Tail'] || props['File to Tail'] || '/var/log/app.log';
    code = `# TailFile: ${p.name}\n# File: ${file}\n# In Databricks, use Auto Loader for continuous file ingestion\ndf_${varName} = (spark.readStream\n  .format("cloudFiles")\n  .option("cloudFiles.format", "text")\n  .load("${file.replace(/[^/]*$/, '')}")\n)\nprint(f"[TAIL] Streaming from ${file}")`;
    conf = 0.92;
    return { code, conf };
  }

  // -- Kinesis --
  if (p.type === 'ConsumeKinesisStream') {
    const stream = props['Kinesis Stream Name'] || props['Amazon Kinesis Stream Name'] || 'stream';
    const region = props['Region'] || 'us-east-1';
    code = `# Kinesis: ${p.name}\ndf_${varName} = (spark.readStream\n  .format("kinesis")\n  .option("streamName", "${stream}")\n  .option("region", "${region}")\n  .option("initialPosition", "TRIM_HORIZON")\n  .load())`;
    conf = 0.92;
    return { code, conf };
  }

  // -- MySQL CDC (CaptureChangeMySQL) --
  if (p.type === 'CaptureChangeMySQL') {
    const host = props['MySQL Hostname'] || props['Hosts'] || 'mysql_host';
    const port = props['MySQL Port'] || props['Port'] || '3306';
    const db = props['Database/Schema'] || props['Database'] || 'source_db';
    const table = props['Table'] || props['Table Name Pattern'] || 'source_table';
    const serverId = props['Server ID'] || '1';
    code = `# CaptureChangeMySQL (CDC): ${p.name}\n# MySQL Host: ${host}:${port} | DB: ${db} | Table: ${table}\n#\n# OPTION 1 (RECOMMENDED): Use Auto Loader to ingest CDC events from a landing zone\n# Assumes MySQL CDC events (e.g., from Debezium) land as JSON files in a Volume\ndf_${varName} = (spark.readStream\n  .format("cloudFiles")\n  .option("cloudFiles.format", "json")\n  .option("cloudFiles.schemaLocation", "/Volumes/<catalog>/<schema>/tmp/cdc_schema/${db}_${table}")\n  .option("cloudFiles.inferColumnTypes", "true")\n  .load("/Volumes/<catalog>/<schema>/cdc/${db}/${table}/")\n)\nprint(f"[CDC] Auto Loader ingesting MySQL CDC events for ${db}.${table}")\n\n# OPTION 2: Read directly from MySQL via JDBC (batch, not streaming)\n# df_${varName} = (spark.read\n#   .format("jdbc")\n#   .option("url", f"jdbc:mysql://{dbutils.secrets.get(scope='mysql', key='host')}:${port}/${db}")\n#   .option("dbtable", "${table}")\n#   .option("driver", "com.mysql.cj.jdbc.Driver")\n#   .option("user", dbutils.secrets.get(scope="mysql", key="user"))\n#   .option("password", dbutils.secrets.get(scope="mysql", key="pass"))\n#   .load()\n# )`;
    conf = 0.85;
    return { code, conf };
  }

  return null; // Not handled
}
