/**
 * mappers/handlers/cloud-azure-handlers.js -- Azure processor smart code generation
 *
 * Extracted from index.html lines ~4541-4556.
 * Handles: PutAzureBlob, FetchAzureBlob, PutAzureDataLakeStorage,
 * PutEventHub, ConsumeAzureEventHub, PutAzureQueueStorage.
 */

/**
 * Generate Databricks code for Azure-type NiFi processors.
 *
 * @param {object} p - Processor object
 * @param {object} props - Processor properties (variable-resolved)
 * @param {string} varName - Sanitized output variable name
 * @param {string} inputVar - Sanitized input variable name
 * @param {string} existingCode - Code from template resolution
 * @param {number} existingConf - Confidence from template resolution
 * @returns {{ code: string, conf: number }|null}
 */
export function handleAzureProcessor(p, props, varName, inputVar, existingCode, existingConf) {
  let code = existingCode;
  let conf = existingConf;

  // -- Azure Blob/ADLS --
  if (/^(Put|Fetch|List|Delete)Azure(Blob|DataLake)/.test(p.type)) {
    const container = props['Container Name'] || props['Filesystem Name'] || 'mycontainer';
    const account = props['Storage Account Name'] || 'mystorageaccount';
    const path = props['Blob Name'] || props['Directory'] || 'data/';
    const isWrite = /^Put/.test(p.type);
    const isADLS = /DataLake/.test(p.type);
    const proto = isADLS ? 'abfss' : 'wasbs';
    const suffix = isADLS ? 'dfs.core.windows.net' : 'blob.core.windows.net';
    if (isWrite) {
      code = `# Azure Write: ${p.name}\n(df_${inputVar}.write\n  .format("delta")\n  .mode("append")\n  .save("${proto}://${container}@${account}.${suffix}/${path}")\n)\nprint(f"[AZURE] Wrote to ${proto}://${container}@${account}")`;
    } else {
      code = `# Azure Read: ${p.name}\ndf_${varName} = spark.read.format("delta").load("${proto}://${container}@${account}.${suffix}/${path}")\nprint(f"[AZURE] Read from Azure")`;
    }
    conf = 0.93;
    return { code, conf };
  }

  // -- ConsumeAzureEventHub --
  if (p.type === 'ConsumeAzureEventHub') {
    const namespace = props['Event Hub Namespace'] || 'my-eventhub-ns';
    const hubName = props['Event Hub Name'] || 'my-hub';
    const consumerGroup = props['Consumer Group'] || '$Default';
    const connStr = props['Event Hub Connection String'] || props['Connection String'] || '';
    code = `# Azure Event Hub Consumer: ${p.name}\n# Namespace: ${namespace} | Hub: ${hubName} | Group: ${consumerGroup}\nimport json as _json\n\n_conn_str = dbutils.secrets.get(scope="azure", key="eventhub-conn-str")\n# Use native PySpark EventHubs connector — pass connection string directly (encrypted at rest)\n_ehConf = {\n    "eventhubs.connectionString": _conn_str,\n    "eventhubs.consumerGroup": "${consumerGroup}",\n    "eventhubs.startingPosition": _json.dumps({"offset": "-1", "seqNo": -1, "enqueuedTime": None, "isInclusive": True})\n}\ndf_${varName} = (spark.readStream\n  .format("eventhubs")\n  .options(**_ehConf)\n  .load()\n  .selectExpr("CAST(body AS STRING) as value", "enqueuedTime as timestamp", "offset", "sequenceNumber")\n)\nprint(f"[EVENTHUB] Consuming from ${namespace}/${hubName}")`;
    conf = 0.92;
    return { code, conf };
  }

  // -- PutEventHub --
  if (p.type === 'PutEventHub' || p.type === 'PublishAzureEventHub') {
    const namespace = props['Event Hub Namespace'] || 'my-eventhub-ns';
    const hubName = props['Event Hub Name'] || 'my-hub';
    code = `# Azure Event Hub Producer: ${p.name}\n# Namespace: ${namespace} | Hub: ${hubName}\n_conn_str = dbutils.secrets.get(scope="azure", key="eventhub-conn-str")\n# Use native PySpark EventHubs connector — no JVM reflection needed\n_ehConf = {\n    "eventhubs.connectionString": _conn_str\n}\n(df_${inputVar}\n  .selectExpr("CAST(value AS STRING) as body")\n  .write\n  .format("eventhubs")\n  .options(**_ehConf)\n  .save()\n)\nprint(f"[EVENTHUB] Published to ${namespace}/${hubName}")`;
    conf = 0.92;
    return { code, conf };
  }

  // -- Azure Queue Storage --
  if (p.type === 'PutAzureQueueStorage' || p.type === 'GetAzureQueueStorage') {
    const queueName = props['Queue Name'] || 'my-queue';
    const account = props['Storage Account Name'] || 'mystorageaccount';
    const isWrite = /^Put/.test(p.type);
    if (isWrite) {
      code = `# Azure Queue Write: ${p.name}\nfrom azure.storage.queue import QueueClient\n_queue = QueueClient.from_connection_string(\n    dbutils.secrets.get(scope="azure", key="storage-conn-str"), "${queueName}")\nfor row in df_${inputVar}.limit(10000).collect():\n    _queue.send_message(str(row.asDict()))\nprint(f"[AZURE-QUEUE] Published to ${queueName}")`;
    } else {
      code = `# Azure Queue Read: ${p.name}\nfrom azure.storage.queue import QueueClient\n_queue = QueueClient.from_connection_string(\n    dbutils.secrets.get(scope="azure", key="storage-conn-str"), "${queueName}")\n_msgs = [{"body": m.content} for m in _queue.receive_messages(messages_per_page=32)]\ndf_${varName} = spark.createDataFrame(_msgs) if _msgs else spark.createDataFrame([], "body STRING")\nprint(f"[AZURE-QUEUE] Read {len(_msgs)} messages from ${queueName}")`;
    }
    conf = 0.90;
    return { code, conf };
  }

  return null; // Not handled
}
