/**
 * mappers/handlers/database-handlers.js -- Database processor smart code generation
 *
 * Handles: Cassandra, HBase, Hive, Kudu, Phoenix, Oracle, Teradata, Snowflake processors.
 * Extracted from index.html lines ~4989-4992, 5043-5047, 5273-5282.
 */

/**
 * Generate Databricks code for database-type NiFi processors.
 *
 * @param {object} p - Processor object
 * @param {object} props - Processor properties (variable-resolved)
 * @param {string} varName - Sanitized output variable name
 * @param {string} inputVar - Sanitized input variable name
 * @param {string} existingCode - Code from template resolution
 * @param {number} existingConf - Confidence from template resolution
 * @returns {{ code: string, conf: number }|null}
 */
export function handleDatabaseProcessor(p, props, varName, inputVar, existingCode, existingConf) {
  let code = existingCode;
  let conf = existingConf;

  // -- Cassandra --
  if (/^(Query|Put|Get)Cassandra/.test(p.type)) {
    const keyspace = props['Keyspace'] || 'my_keyspace';
    const table = props['Table'] || props['Table Name'] || 'my_table';
    const host = props['Contact Points'] || props['Cassandra Contact Points'] || 'cassandra_host';
    const isWrite = /^Put/.test(p.type);
    if (isWrite) {
      code = `# Cassandra Write: ${p.name}\n# Keyspace: ${keyspace} | Table: ${table}\n(df_${inputVar}.write\n  .format("org.apache.spark.sql.cassandra")\n  .option("keyspace", "${keyspace}")\n  .option("table", "${table}")\n  .option("spark.cassandra.connection.host", "${host}")\n  .mode("append")\n  .save()\n)\nprint(f"[CASSANDRA] Wrote to ${keyspace}.${table}")`;
    } else {
      code = `# Cassandra Read: ${p.name}\n# Keyspace: ${keyspace} | Table: ${table}\ndf_${varName} = (spark.read\n  .format("org.apache.spark.sql.cassandra")\n  .option("keyspace", "${keyspace}")\n  .option("table", "${table}")\n  .option("spark.cassandra.connection.host", "${host}")\n  .load()\n)\nprint(f"[CASSANDRA] Read from ${keyspace}.${table}")`;
    }
    conf = 0.92;
    return { code, conf };
  }

  // -- HBase --
  if (/^(Get|Put|Scan|Delete)HBase/.test(p.type)) {
    const tableName = props['Table Name'] || 'hbase_table';
    const host = props['HBase Hostname'] || props['HBase Client Service'] || 'hbase_host';
    const isWrite = /^Put/.test(p.type);
    const isDelete = /^Delete/.test(p.type);
    if (isDelete) {
      code = `# HBase Delete: ${p.name}\nimport happybase\n_conn = happybase.Connection("${host}")\n_table = _conn.table("${tableName}")\nfor row in df_${inputVar}.limit(1000).collect():\n    _table.delete(row["row_key"].encode())\n_conn.close()\nprint(f"[HBASE] Deleted from ${tableName}")`;
    } else if (isWrite) {
      code = `# HBase Write: ${p.name}\n# Table: ${tableName}\nimport happybase, json\n_conn = happybase.Connection("${host}")\n_table = _conn.table("${tableName}")\nfor row in df_${inputVar}.limit(10000).collect():\n    _d = row.asDict()\n    _rk = str(_d.get("row_key", _d.get("id", "")))\n    _table.put(_rk.encode(), {f"cf:{k}".encode(): str(v).encode() for k, v in _d.items()})\n_conn.close()\nprint(f"[HBASE] Wrote to ${tableName}")`;
    } else {
      code = `# HBase Read: ${p.name}\n# Table: ${tableName}\nimport happybase\n_conn = happybase.Connection("${host}")\n_table = _conn.table("${tableName}")\n_rows = [{**{"row_key": k.decode()}, **{c.decode(): v.decode() for c, v in d.items()}} for k, d in _table.scan(limit=50000)]\ndf_${varName} = spark.createDataFrame(_rows) if _rows else spark.createDataFrame([], "row_key STRING")\n_conn.close()\nprint(f"[HBASE] Read {len(_rows)} rows from ${tableName}")`;
    }
    conf = 0.90;
    return { code, conf };
  }

  // -- Hive --
  if (/^(SelectHiveQL|PutHiveQL|PutHiveStreaming|UpdateHiveTable)$/.test(p.type)) {
    const query = props['HiveQL Select Query'] || props['HiveQL Statement'] || '';
    const table = props['Table Name'] || 'hive_table';
    const isWrite = /^(Put|Update)/.test(p.type);
    if (isWrite) {
      code = `# Hive Write: ${p.name}\n(df_${inputVar}.write\n  .mode("append")\n  .saveAsTable("${table}")\n)\nprint(f"[HIVE] Wrote to ${table}")`;
    } else {
      const sql = query || `SELECT * FROM ${table}`;
      code = `# Hive Query: ${p.name}\ndf_${varName} = spark.sql("""${sql.substring(0, 300)}""")\nprint(f"[HIVE] Queried ${table}")`;
    }
    conf = 0.92;
    return { code, conf };
  }

  // -- Kudu --
  if (/^(Put|Get|Query)Kudu/.test(p.type)) {
    const master = props['Kudu Masters'] || 'kudu_master:7051';
    const table = props['Table Name'] || 'kudu_table';
    const isWrite = /^Put/.test(p.type);
    if (isWrite) {
      code = `# Kudu Write: ${p.name}\n# Master: ${master} | Table: ${table}\n(df_${inputVar}.write\n  .format("kudu")\n  .option("kudu.master", "${master}")\n  .option("kudu.table", "${table}")\n  .mode("append")\n  .save()\n)\nprint(f"[KUDU] Wrote to ${table}")`;
    } else {
      code = `# Kudu Read: ${p.name}\ndf_${varName} = (spark.read\n  .format("kudu")\n  .option("kudu.master", "${master}")\n  .option("kudu.table", "${table}")\n  .load()\n)\nprint(f"[KUDU] Read from ${table}")`;
    }
    conf = 0.90;
    return { code, conf };
  }

  // -- Phoenix (use JDBC — org.apache.phoenix.spark is deprecated/broken on Spark 3+) --
  if (/^(Query|Put)Phoenix/.test(p.type)) {
    const zkUrl = props['Phoenix URL'] || props['ZooKeeper Quorum'] || 'zk_host:2181';
    const table = props['Table Name'] || 'phoenix_table';
    const isWrite = /^Put/.test(p.type);
    if (isWrite) {
      code = `# Phoenix Write: ${p.name} (via JDBC — phoenix-spark connector is deprecated on Spark 3+)\n(df_${inputVar}.write\n  .format("jdbc")\n  .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")\n  .option("url", "jdbc:phoenix:${zkUrl}")\n  .option("dbtable", "${table}")\n  .mode("overwrite")\n  .save()\n)\nprint(f"[PHOENIX] Wrote to ${table} via JDBC")`;
    } else {
      code = `# Phoenix Read: ${p.name} (via JDBC — phoenix-spark connector is deprecated on Spark 3+)\ndf_${varName} = (spark.read\n  .format("jdbc")\n  .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")\n  .option("url", "jdbc:phoenix:${zkUrl}")\n  .option("dbtable", "${table}")\n  .load()\n)\nprint(f"[PHOENIX] Read from ${table} via JDBC")`;
    }
    conf = 0.70;
    return { code, conf };
  }

  // -- Oracle --
  if (/^(Query|Put)Oracle/.test(p.type)) {
    const table = props['Table Name'] || 'oracle_table';
    code = `# Oracle: ${p.name}\ndf_${varName} = (spark.read\n  .format("jdbc")\n  .option("url", dbutils.secrets.get(scope="oracle", key="jdbc-url"))\n  .option("dbtable", "${table}")\n  .option("driver", "oracle.jdbc.driver.OracleDriver")\n  .option("user", dbutils.secrets.get(scope="oracle", key="user"))\n  .option("password", dbutils.secrets.get(scope="oracle", key="pass"))\n  .load()\n)\nprint(f"[ORACLE] Read from ${table}")`;
    conf = 0.92;
    return { code, conf };
  }

  // -- Teradata --
  if (/^(Query|Put)Teradata/.test(p.type)) {
    const table = props['Table Name'] || 'teradata_table';
    code = `# Teradata: ${p.name}\ndf_${varName} = (spark.read\n  .format("jdbc")\n  .option("url", dbutils.secrets.get(scope="teradata", key="jdbc-url"))\n  .option("dbtable", "${table}")\n  .option("driver", "com.teradata.jdbc.TeraDriver")\n  .option("user", dbutils.secrets.get(scope="teradata", key="user"))\n  .option("password", dbutils.secrets.get(scope="teradata", key="pass"))\n  .load()\n)\nprint(f"[TERADATA] Read from ${table}")`;
    conf = 0.92;
    return { code, conf };
  }

  // -- Snowflake --
  if (/^(Get|Put|Query)Snowflake/.test(p.type)) {
    const db = props['Database'] || 'snowflake_db';
    const schema = props['Schema'] || 'public';
    const table = props['Table Name'] || 'snowflake_table';
    const isWrite = /^Put/.test(p.type);
    if (isWrite) {
      code = `# Snowflake Write: ${p.name}\n(df_${inputVar}.write\n  .format("snowflake")\n  .option("sfUrl", dbutils.secrets.get(scope="snowflake", key="url"))\n  .option("sfUser", dbutils.secrets.get(scope="snowflake", key="user"))\n  .option("sfPassword", dbutils.secrets.get(scope="snowflake", key="pass"))\n  .option("sfDatabase", "${db}")\n  .option("sfSchema", "${schema}")\n  .option("dbtable", "${table}")\n  .mode("append")\n  .save()\n)\nprint(f"[SNOWFLAKE] Wrote to ${db}.${schema}.${table}")`;
    } else {
      code = `# Snowflake Read: ${p.name}\ndf_${varName} = (spark.read\n  .format("snowflake")\n  .option("sfUrl", dbutils.secrets.get(scope="snowflake", key="url"))\n  .option("sfUser", dbutils.secrets.get(scope="snowflake", key="user"))\n  .option("sfPassword", dbutils.secrets.get(scope="snowflake", key="pass"))\n  .option("sfDatabase", "${db}")\n  .option("sfSchema", "${schema}")\n  .option("dbtable", "${table}")\n  .load()\n)\nprint(f"[SNOWFLAKE] Read from ${db}.${schema}.${table}")`;
    }
    conf = 0.92;
    return { code, conf };
  }

  return null; // Not handled
}
