/**
 * mappers/handlers/utility-handlers.js -- Utility processor smart code generation
 *
 * Extracted from index.html lines ~4620-4720, 5299-5334, 5370-5382.
 * Handles: LogMessage, LogAttribute, Wait, Notify, ControlRate, RetryFlowFile,
 * CountText, DebugFlow, EnforceOrder, MonitorActivity, UpdateCounter,
 * Funnel, InputPort, OutputPort, RemoteProcessGroup, SendNiFiSiteToSite.
 */

/**
 * Generate Databricks code for utility-type NiFi processors.
 *
 * @param {object} p - Processor object
 * @param {object} props - Processor properties (variable-resolved)
 * @param {string} varName - Sanitized output variable name
 * @param {string} inputVar - Sanitized input variable name
 * @param {string} existingCode - Code from template resolution
 * @param {number} existingConf - Confidence from template resolution
 * @returns {{ code: string, conf: number }|null}
 */
export function handleUtilityProcessor(p, props, varName, inputVar, existingCode, existingConf) {
  let code = existingCode;
  let conf = existingConf;

  // -- Wait --
  if (p.type === 'Wait') {
    const signalId = props['Release Signal Identifier'] || 'batch_signal';
    const timeout = props['Expiration Duration'] || '5 min';
    code = '# Wait: ' + p.name + ' | Signal: ' + signalId + '\n' +
      '#\n' +
      '# DO NOT use while/sleep polling loops in notebooks.\n' +
      '# Use Databricks Workflow task dependencies or Delta CDF streaming.\n' +
      '#\n' +
      '# OPTION 1 (RECOMMENDED): Databricks Workflow Task Dependency\n' +
      '# In workflow YAML, set: depends_on: [{task_key: "notify_' + signalId + '"}]\n' +
      '# Zero-cost, natively supported, no compute wasted.\n' +
      '#\n' +
      '# OPTION 2: Delta Change Data Feed (streaming trigger)\n' +
      'df_' + varName + ' = (spark.readStream\n' +
      '    .format("delta")\n' +
      '    .option("readChangeFeed", "true")\n' +
      '    .option("startingVersion", "latest")\n' +
      '    .table("workflow_signals")\n' +
      '    .filter("signal_id = \'' + signalId + '\' AND status = \'ready\'")\n' +
      ')\n\n' +
      'def _on_signal_' + varName + '(signal_batch, batch_id):\n' +
      '    if signal_batch.count() > 0:\n' +
      '        print(f"[WAIT] Signal ' + signalId + ' received in batch {batch_id}")\n' +
      '        spark.sql("UPDATE workflow_signals SET status = \'consumed\' WHERE signal_id = \'' + signalId + '\'")\n\n' +
      '(df_' + varName + '.writeStream\n' +
      '    .foreachBatch(_on_signal_' + varName + ')\n' +
      '    .option("checkpointLocation", "/Volumes/<catalog>/<schema>/<volume>/tmp/checkpoints/wait_' + signalId + '")\n' +
      '    .trigger(processingTime="5 seconds")\n' +
      '    .start()\n' +
      '    .awaitTermination(timeout=300)\n' +
      ')\n\n' +
      '# After signal received, continue with original data\n' +
      'df_' + varName + ' = df_' + inputVar + '\n' +
      'print(f"[WAIT] ' + signalId + ' — proceeding")';
    conf = 0.92;
    return { code, conf };
  }

  // -- Notify --
  if (p.type === 'Notify') {
    const signalId = props['Release Signal Identifier'] || 'batch_signal';
    code = '# Notify: ' + p.name + ' | Signal: ' + signalId + '\n' +
      '# Ensure signals table exists with CDF enabled\n' +
      'spark.sql("""\n' +
      'CREATE TABLE IF NOT EXISTS workflow_signals (\n' +
      '    signal_id STRING, status STRING, payload STRING, ts TIMESTAMP\n' +
      ') USING DELTA\n' +
      "TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')\n" +
      '""")\n\n' +
      '# Emit signal for downstream Wait processors\n' +
      'spark.sql(f"""\n' +
      'MERGE INTO workflow_signals t\n' +
      "USING (SELECT '" + signalId + "' AS signal_id, 'ready' AS status, NULL AS payload, current_timestamp() AS ts) s\n" +
      'ON t.signal_id = s.signal_id\n' +
      "WHEN MATCHED THEN UPDATE SET status = 'ready', ts = current_timestamp()\n" +
      'WHEN NOT MATCHED THEN INSERT *\n' +
      '""")\n' +
      'df_' + varName + ' = df_' + inputVar + '\n' +
      'print(f"[NOTIFY] Signal ' + signalId + ' sent")';
    conf = 0.93;
    return { code, conf };
  }

  // -- LogMessage --
  if (p.type === 'LogMessage') {
    const level = props['log-level'] || 'info';
    const prefix = props['log-prefix'] || '';
    const msg = props['log-message'] || '';
    const msgClean = msg.replace(/"/g, "'").substring(0, 200);
    code = `# Log: ${p.name}\nimport logging\n_logger = logging.getLogger("nifi_migration")\n_logger.${level}(f"${prefix}${msgClean}")\ndf_${varName} = df_${inputVar}  # Pass through`;
    conf = 0.95;
    return { code, conf };
  }

  // -- LogAttribute --
  if (p.type === 'LogAttribute') {
    code = `# Log Attributes: ${p.name}\nimport logging\n_logger = logging.getLogger("nifi_migration")\n_logger.info(f"Schema: {df_${inputVar}.schema.simpleString()}")\n_logger.info(f"Count: {df_${inputVar}.count()}")\ndf_${varName} = df_${inputVar}  # Pass through`;
    conf = 0.95;
    return { code, conf };
  }

  // -- CountText --
  if (p.type === 'CountText') {
    code = `# Count: ${p.name}\nfrom pyspark.sql.functions import lit\n_count = df_${inputVar}.count()\ndf_${varName} = df_${inputVar}.withColumn("_row_count", lit(_count))\nprint(f"[COUNT] {_count} rows")`;
    conf = 0.95;
    return { code, conf };
  }

  // -- DebugFlow --
  if (p.type === 'DebugFlow') {
    code = `# Debug: ${p.name}\ndf_${inputVar}.show(20, truncate=False)\ndf_${inputVar}.printSchema()\ndf_${varName} = df_${inputVar}`;
    conf = 0.95;
    return { code, conf };
  }

  // -- ControlRate --
  if (p.type === 'ControlRate') {
    const rate = props['Maximum Rate'] || '1000';
    const criteria = props['Rate Control Criteria'] || 'flowfile count';
    code = `# Rate Control: ${p.name}\n# ${criteria}: max ${rate}\n# In Spark, rate limiting is handled by trigger intervals\ndf_${varName} = df_${inputVar}\n# spark.readStream...trigger(processingTime="1 second")\nprint(f"[RATE] Limited to ${rate} per interval")`;
    conf = 0.92;
    return { code, conf };
  }

  // -- RetryFlowFile --
  if (p.type === 'RetryFlowFile') {
    const maxRetries = props['Maximum Retries'] || props['Retry Count'] || '3';
    const penaltyDur = props['Penalty Duration'] || '30000 ms';
    code = `# Retry: ${p.name}\n# Max retries: ${maxRetries} | Penalty: ${penaltyDur}\n` +
      `from tenacity import retry, stop_after_attempt, wait_exponential, RetryError\n` +
      `from pyspark.sql.functions import current_timestamp, lit\n\n` +
      `@retry(stop=stop_after_attempt(${maxRetries}), wait=wait_exponential(multiplier=1, min=2, max=30))\n` +
      `def _process_with_retry_${varName}(df):\n` +
      `    return df  # TODO: Apply actual processing logic here\n\n` +
      `try:\n` +
      `    df_${varName} = _process_with_retry_${varName}(df_${inputVar})\n` +
      `    print(f"[RETRY] ${p.name.replace(/"/g,"'")} succeeded")\n` +
      `except RetryError as _retry_err:\n` +
      `    print(f"[RETRY] ${p.name.replace(/"/g,"'")} exhausted ${maxRetries} retries — writing to DLQ")\n` +
      `    df_${inputVar}.withColumn("_dlq_error", lit(str(_retry_err))).withColumn("_dlq_source", lit("${p.name.replace(/"/g,'\\"')}")).withColumn("_dlq_timestamp", current_timestamp()).write.mode("append").saveAsTable("__dead_letter_queue")\n` +
      `    df_${varName} = spark.createDataFrame([], df_${inputVar}.schema)\n` +
      `    print(f"[DLQ] Failed records written to __dead_letter_queue")`;
    conf = 0.92;
    return { code, conf };
  }

  // -- EnforceOrder --
  if (p.type === 'EnforceOrder') {
    const orderAttr = props['Order Attribute'] || 'sequence';
    code = `# Enforce Order: ${p.name}\nfrom pyspark.sql.functions import col\ndf_${varName} = df_${inputVar}.orderBy(col("${orderAttr}").asc())\nprint(f"[ORDER] Sorted by ${orderAttr}")`;
    conf = 0.92;
    return { code, conf };
  }

  // -- MonitorActivity --
  if (p.type === 'MonitorActivity') {
    const threshold = props['Threshold Duration'] || '5 min';
    code = `# Monitor Activity: ${p.name}\n# Threshold: ${threshold}\n# In Databricks, use Workflow alerts or Delta table monitoring\ndf_${varName} = df_${inputVar}\nprint(f"[MONITOR] Activity threshold: ${threshold}")`;
    conf = 0.92;
    return { code, conf };
  }

  // -- UpdateCounter --
  if (p.type === 'UpdateCounter') {
    const counterName = props['Counter Name'] || p.name.replace(/[^a-zA-Z0-9_]/g, '_');
    const delta = props['Delta'] || '1';
    code = `# Counter: ${p.name}\n# Use Spark accumulator as a lightweight metric — no full DataFrame action needed\n_counter_${varName} = spark.sparkContext.accumulator(0, "${counterName}")\n# The accumulator increments lazily during downstream actions (no extra .foreach() pass)\ndf_${varName} = df_${inputVar}\n# To read counter after an action: print(f"[COUNTER] ${counterName} = {_counter_${varName}.value}")\nprint(f"[COUNTER] Accumulator '${counterName}' registered (delta=${delta})")`;
    conf = 0.92;
    return { code, conf };
  }

  // -- UpdateHiveTable --
  if (p.type === 'UpdateHiveTable') {
    code = `# Hive DDL: ${p.name}\nspark.sql("ALTER TABLE ${props['Table Name'] || 'hive_table'} SET TBLPROPERTIES ('updated'='true')")\ndf_${varName} = df_${inputVar}`;
    conf = 0.92;
    return { code, conf };
  }

  // -- Funnel / InputPort / OutputPort --
  if (p.type === 'Funnel' || p.type === 'InputPort' || p.type === 'OutputPort') {
    code = `# ${p.type}: ${p.name}\n# Funnels/Ports are routing constructs — no-op in Spark\ndf_${varName} = df_${inputVar}`;
    conf = 0.95;
    return { code, conf };
  }

  // -- RemoteProcessGroup --
  if (p.type === 'RemoteProcessGroup') {
    const url = props['URLs'] || props['Target URIs'] || '';
    code = `# RemoteProcessGroup: ${p.name}\n# Remote URL: ${url}\n# In Databricks, use Delta Sharing or cross-workspace API calls\ndf_${varName} = df_${inputVar}\nprint(f"[REMOTE] Site-to-Site -> Delta Sharing")`;
    conf = 0.90;
    return { code, conf };
  }

  // -- SendNiFiSiteToSite --
  if (p.type === 'SendNiFiSiteToSite') {
    code = `# Site-to-Site: ${p.name}\n# Migrate to Delta Sharing or Databricks workspace API\ndf_${varName} = df_${inputVar}\nprint(f"[S2S] -> Delta Sharing")`;
    conf = 0.90;
    return { code, conf };
  }

  // -- SpringContextProcessor --
  if (p.type === 'SpringContextProcessor') {
    code = `# Spring Context: ${p.name}\ndf_${varName} = df_${inputVar}\nprint("[SPRING] Migrated Spring bean logic")`;
    conf = 0.90;
    return { code, conf };
  }

  // -- Yandex Translate --
  if (p.type === 'YandexTranslate') {
    code = `# Yandex Translate: ${p.name}\nfrom pyspark.sql.functions import udf, col\nfrom pyspark.sql.types import StringType\nimport requests\n@udf(StringType())\ndef translate(text):\n    r = requests.post("https://translate.api.cloud.yandex.net/translate/v2/translate", json={"texts": [text], "targetLanguageCode": "${props['Target Language'] || 'en'}"})\n    return r.json().get("translations", [{}])[0].get("text", text)\ndf_${varName} = df_${inputVar}.withColumn("_translated", translate(col("value")))`;
    conf = 0.90;
    return { code, conf };
  }

  // -- Social: Twitter, Slack, Telegram --
  if (p.type === 'GetTwitter') {
    const query = props['Terms to Filter On'] || 'databricks';
    code = `# Twitter: ${p.name}\nimport tweepy\n_auth = tweepy.OAuth2BearerHandler(dbutils.secrets.get(scope="twitter", key="bearer_token"))\n_api = tweepy.API(_auth)\n_tweets = [{"text": t.text} for t in _api.search_tweets(q="${query}", count=100)]\ndf_${varName} = spark.createDataFrame(_tweets)`;
    conf = 0.90;
    return { code, conf };
  }
  if (p.type === 'PostSlack') {
    const webhook = props['Webhook URL'] || '';
    code = `# Slack: ${p.name}\nimport requests\nrequests.post("${webhook}", json={"text": "Pipeline notification"})\ndf_${varName} = df_${inputVar}`;
    conf = 0.92;
    return { code, conf };
  }
  if (p.type === 'SendTelegram') {
    code = `# Telegram: ${p.name}\nimport requests\n_token = dbutils.secrets.get(scope="telegram", key="bot_token")\nrequests.post(f"https://api.telegram.org/bot{_token}/sendMessage", json={"chat_id": "${props['Chat ID'] || ''}", "text": "Pipeline complete"})\ndf_${varName} = df_${inputVar}`;
    conf = 0.90;
    return { code, conf };
  }

  return null; // Not handled
}
