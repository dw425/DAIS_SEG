/**
 * mappers/handlers/cloud-gcp-handlers.js -- GCP processor smart code generation
 *
 * Handles: PutGCS, ListGCS, FetchGCS, PutBigQueryBatch, ConsumeGCPubSub,
 * PublishGCPubSub.
 */

/**
 * Generate Databricks code for GCP-type NiFi processors.
 *
 * @param {object} p - Processor object
 * @param {object} props - Processor properties (variable-resolved)
 * @param {string} varName - Sanitized output variable name
 * @param {string} inputVar - Sanitized input variable name
 * @param {string} existingCode - Code from template resolution
 * @param {number} existingConf - Confidence from template resolution
 * @returns {{ code: string, conf: number }|null}
 */
export function handleGCPProcessor(p, props, varName, inputVar, existingCode, existingConf) {
  let code = existingCode;
  let conf = existingConf;

  // -- GCS operations --
  if (/^(Put|List|Fetch|Get|Delete)GCS/.test(p.type)) {
    const bucket = props['Bucket'] || props['GCS Bucket'] || 'gcs_bucket';
    const key = props['Key'] || props['Prefix'] || 'data/';
    const isList = /^List/.test(p.type);
    const isWrite = /^Put/.test(p.type);
    const isDelete = /^Delete/.test(p.type);

    if (isList) {
      code = `# GCS List: ${p.name}\n# Bucket: ${bucket} | Prefix: ${key}\ndf_${varName} = spark.createDataFrame(\n    [{"path": f.path, "name": f.name, "size": f.size}\n     for f in dbutils.fs.ls(f"gs://${bucket}/${key}")]\n)\nprint(f"[GCS] Listed objects from gs://${bucket}/${key}")`;
    } else if (isWrite) {
      code = `# GCS Write: ${p.name}\n# Bucket: ${bucket} | Key: ${key}\n(df_${inputVar}.write\n  .format("delta")\n  .mode("append")\n  .save(f"gs://${bucket}/${key}")\n)\nprint(f"[GCS] Wrote to gs://${bucket}/${key}")`;
    } else if (isDelete) {
      code = `# GCS Delete: ${p.name}\ndbutils.fs.rm(f"gs://${bucket}/${key}", recurse=True)\nprint(f"[GCS] Deleted gs://${bucket}/${key}")`;
    } else {
      code = `# GCS Read: ${p.name}\n# Bucket: ${bucket} | Key: ${key}\ndf_${varName} = spark.read.format("delta").load(f"gs://${bucket}/${key}")\nprint(f"[GCS] Read from gs://${bucket}/${key}")`;
    }
    conf = 0.93;
    return { code, conf };
  }

  // -- BigQuery --
  if (p.type === 'PutBigQueryBatch' || p.type === 'PutBigQuery') {
    const project = props['Project ID'] || props['Project'] || 'my-project';
    const dataset = props['Dataset'] || 'my_dataset';
    const table = props['Table Name'] || props['Table'] || 'my_table';
    code = `# BigQuery Write: ${p.name}\n# Project: ${project} | Dataset: ${dataset} | Table: ${table}\n(df_${inputVar}.write\n  .format("bigquery")\n  .option("project", "${project}")\n  .option("dataset", "${dataset}")\n  .option("table", "${table}")\n  .option("temporaryGcsBucket", "gs://${props['Temporary Bucket'] || 'temp-bucket'}/bq-staging")\n  .mode("append")\n  .save()\n)\nprint(f"[BQ] Wrote to ${project}.${dataset}.${table}")`;
    conf = 0.92;
    return { code, conf };
  }

  // -- GCP Pub/Sub --
  if (p.type === 'ConsumeGCPubSub') {
    const subscription = props['Subscription'] || props['Subscription Name'] || 'projects/my-project/subscriptions/my-sub';
    const project = props['Project ID'] || '';
    code = `# GCP Pub/Sub Consumer: ${p.name}\n# Subscription: ${subscription}\nfrom google.cloud import pubsub_v1\nimport json\n_subscriber = pubsub_v1.SubscriberClient()\n_msgs = []\ndef _callback(message):\n    _msgs.append({"data": message.data.decode("utf-8"), "attributes": dict(message.attributes)})\n    message.ack()\n_future = _subscriber.subscribe("${subscription}", callback=_callback)\nimport time; time.sleep(10)\n_future.cancel()\ndf_${varName} = spark.createDataFrame(_msgs) if _msgs else spark.createDataFrame([], "data STRING")\nprint(f"[PUBSUB] Consumed {len(_msgs)} messages")`;
    conf = 0.90;
    return { code, conf };
  }

  if (p.type === 'PublishGCPubSub') {
    const topic = props['Topic Name'] || props['Topic'] || 'projects/my-project/topics/my-topic';
    code = `# GCP Pub/Sub Publisher: ${p.name}\n# Topic: ${topic}\nfrom google.cloud import pubsub_v1\nimport json\n_publisher = pubsub_v1.PublisherClient()\nfor row in df_${inputVar}.limit(10000).collect():\n    _publisher.publish("${topic}", json.dumps(row.asDict()).encode("utf-8"))\nprint(f"[PUBSUB] Published to ${topic}")`;
    conf = 0.90;
    return { code, conf };
  }

  return null; // Not handled
}
