/**
 * mappers/handlers/sink-handlers.js -- Sink processor smart code generation
 *
 * Extracted from index.html lines ~4278-4304, 4326-4340, 4557-4566.
 * Handles: PutFile, PutHDFS, PutDatabaseRecord, PutSQL, PutSFTP, PutFTP,
 * PutS3Object, PutEmail, PublishKafka, HandleHttpResponse.
 */

/**
 * Generate Databricks code for sink-type NiFi processors.
 *
 * @param {object} p - Processor object
 * @param {object} props - Processor properties (variable-resolved)
 * @param {string} varName - Sanitized output variable name
 * @param {string} inputVar - Sanitized input variable name
 * @param {string} existingCode - Code from template resolution
 * @param {number} existingConf - Confidence from template resolution
 * @returns {{ code: string, conf: number }|null}
 */
export function handleSinkProcessor(p, props, varName, inputVar, existingCode, existingConf) {
  let code = existingCode;
  let conf = existingConf;

  // -- HandleHttpResponse --
  if (p.type === 'HandleHttpResponse') {
    const statusCode = props['HTTP Status Code'] || '200';
    code = `# HTTP Response: ${p.name}\n# NiFi sends HTTP response with status ${statusCode}\nprint(f"[HTTP] Response: status=${statusCode} for request")`;
    conf = 0.92;
    return { code, conf };
  }

  // -- Kafka producers --
  if (/^(Publish|Put)(Kafka|KafkaRecord)/.test(p.type)) {
    const brokers = props['Kafka Brokers'] || props['bootstrap.servers'] || 'kafka:9092';
    const topic = props['Topic Name'] || props['topic'] || 'output_topic';
    const compression = props['Compression Type'] || 'snappy';
    code = `# Kafka Producer: ${p.name}\n# Topic: ${topic} | Brokers: ${brokers}\n(df_${inputVar}\n  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")\n  .write\n  .format("kafka")\n  .option("kafka.bootstrap.servers", "${brokers}")\n  .option("topic", "${topic}")\n  .option("kafka.compression.type", "${compression}")\n  .save()\n)\nprint(f"[KAFKA] Published to ${topic}")`;
    conf = 0.95;
    return { code, conf };
  }

  // -- SFTP/FTP sinks --
  if (/^Put(SFTP|FTP)$/.test(p.type)) {
    const host = props['Hostname'] || 'sftp.target.com';
    const port = props['Port'] || '22';
    const user = props['Username'] || 'sftp_user';
    const remotePath = props['Remote Path'] || '/exports/';
    code = `# ${p.type}: ${p.name}\n# Host: ${host}:${port} | Path: ${remotePath}\nimport paramiko\n_ssh = paramiko.SSHClient()\n_ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())\n_ssh.connect("${host}", port=int("${port}"), username="${user}",\n    password=dbutils.secrets.get(scope="sftp", key="password"))\n_sftp = _ssh.open_sftp()\n\n_pdf = df_${inputVar}.toPandas()\n_output_path = f"${remotePath}/{_pdf.shape[0]}_records.csv"\n_pdf.to_csv(f"/tmp/_sftp_out.csv", index=False)\n_sftp.put("/tmp/_sftp_out.csv", _output_path)\n\n_sftp.close()\n_ssh.close()\nprint(f"[SFTP] Uploaded {_pdf.shape[0]} records to ${host}:${remotePath}")`;
    conf = 0.90;
    return { code, conf };
  }

  // -- Database writes (PutDatabaseRecord, PutSQL) --
  if (/^(PutDatabaseRecord|PutSQL)$/.test(p.type) && !code.includes('spark.read')) {
    const dbPool = props['Database Connection Pooling Service'] || props['JDBC Connection Pool'] || '';
    const table = props['Table Name'] || 'target_table';
    const schema = props['Schema Name'] || '';
    const stmtType = props['Statement Type'] || 'INSERT';
    let jdbcUrl = 'jdbc:database://host:port/db';
    let driver = 'com.database.Driver';
    if (/oracle/i.test(dbPool)) { jdbcUrl = 'jdbc:oracle:thin:@db_host:1521:db_sid'; driver = 'oracle.jdbc.driver.OracleDriver'; }
    else if (/postgres/i.test(dbPool)) { jdbcUrl = 'jdbc:postgresql://pg_host:5432/pg_db'; driver = 'org.postgresql.Driver'; }
    else if (/mysql/i.test(dbPool)) { jdbcUrl = 'jdbc:mysql://mysql_host:3306/mysql_db'; driver = 'com.mysql.cj.jdbc.Driver'; }
    const fullTable = schema ? `${schema}.${table}` : table;
    code = `# DB Write: ${p.name} (${stmtType})\n# Pool: ${dbPool} | Table: ${fullTable}\n(df_${inputVar}.write\n  .format("jdbc")\n  .option("url", "${jdbcUrl}")\n  .option("dbtable", "${fullTable}")\n  .option("driver", "${driver}")\n  .option("user", dbutils.secrets.get(scope="db", key="user"))\n  .option("password", dbutils.secrets.get(scope="db", key="pass"))\n  .option("batchsize", 1000)\n  .mode("append")\n  .save()\n)\nprint(f"[DB] Wrote to ${fullTable}")`;
    conf = 0.92;
    return { code, conf };
  }

  // -- Email (PutEmail) --
  if (p.type === 'PutEmail') {
    const host = props['SMTP Hostname'] || 'smtp.example.com';
    const port = props['SMTP Port'] || '587';
    const from_ = props['From'] || 'noreply@example.com';
    const to = props['To'] || 'alerts@example.com';
    const subject = props['Subject'] || 'Pipeline notification';
    code = `# Email: ${p.name}\n# SMTP: ${host}:${port} | From: ${from_} | To: ${to}\nimport smtplib\nfrom email.mime.text import MIMEText\nfrom email.mime.multipart import MIMEMultipart\n\n_msg = MIMEMultipart()\n_msg["From"] = "${from_}"\n_msg["To"] = "${to}"\n_msg["Subject"] = f"${subject}"\n_msg.attach(MIMEText("Pipeline completed successfully.", "plain"))\n\nwith smtplib.SMTP("${host}", ${port}) as _smtp:\n    _smtp.starttls()\n    _smtp.login(dbutils.secrets.get(scope="email", key="user"),\n               dbutils.secrets.get(scope="email", key="pass"))\n    _smtp.send_message(_msg)\nprint(f"[EMAIL] Sent notification to ${to}")`;
    conf = 0.90;
    return { code, conf };
  }

  return null; // Not handled
}
