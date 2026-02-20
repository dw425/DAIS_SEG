/**
 * mappers/handlers/cloud-aws-handlers.js -- AWS processor smart code generation
 *
 * Extracted from index.html lines ~4509-4525, 4958-4964, 5048-5061.
 * Handles: PutSNS, GetSQS, PutDynamoDB, PutKinesisStream, PutLambda,
 * InvokeAWSGatewayApi, DeleteDynamoDB, DeleteSQS, S3 operations,
 * ConsumeKinesisStream.
 */

/**
 * Generate Databricks code for AWS-type NiFi processors.
 *
 * @param {object} p - Processor object
 * @param {object} props - Processor properties (variable-resolved)
 * @param {string} varName - Sanitized output variable name
 * @param {string} inputVar - Sanitized input variable name
 * @param {string} existingCode - Code from template resolution
 * @param {number} existingConf - Confidence from template resolution
 * @returns {{ code: string, conf: number }|null}
 */
export function handleAWSProcessor(p, props, varName, inputVar, existingCode, existingConf) {
  let code = existingCode;
  let conf = existingConf;

  // -- S3 operations --
  if (/^(List|Fetch|Get|Put|Delete)S3/.test(p.type)) {
    const bucket = props['Bucket'] || 's3_bucket';
    const key = props['Object Key'] || props['Prefix'] || 'data/';
    const isWrite = /^(Put|Delete)/.test(p.type);
    const isList = /^List/.test(p.type);
    if (isList) {
      code = `# S3 List: ${p.name}\n# Bucket: ${bucket} | Prefix: ${key}\ndf_${varName} = spark.createDataFrame(\n    [{"path": f.path, "name": f.name, "size": f.size}\n     for f in dbutils.fs.ls(f"s3://${bucket}/${key}")]\n)\nprint(f"[S3] Listed objects from s3://${bucket}/${key}")`;
    } else if (isWrite && p.type !== 'DeleteS3Object') {
      code = `# S3 Write: ${p.name}\n# Bucket: ${bucket} | Key: ${key}\n(df_${inputVar}.write\n  .format("delta")\n  .mode("append")\n  .save(f"s3://${bucket}/${key}")\n)\nprint(f"[S3] Wrote to s3://${bucket}/${key}")`;
    } else if (p.type === 'DeleteS3Object') {
      code = `# S3 Delete: ${p.name}\ndbutils.fs.rm(f"s3://${bucket}/${key}", recurse=True)\nprint(f"[S3] Deleted s3://${bucket}/${key}")`;
    } else {
      code = `# S3 Read: ${p.name}\n# Bucket: ${bucket} | Key: ${key}\ndf_${varName} = spark.read.format("delta").load(f"s3://${bucket}/${key}")\nprint(f"[S3] Read from s3://${bucket}/${key}")`;
    }
    conf = 0.93;
    return { code, conf };
  }

  // -- PutSNS --
  if (p.type === 'PutSNS') {
    const topicArn = props['Amazon Resource Name (ARN)'] || props['Topic ARN'] || 'arn:aws:sns:us-east-1:123456789:topic';
    const region = props['Region'] || 'us-east-1';
    code = `# SNS: ${p.name}\n# Topic: ${topicArn}\nimport boto3\n_sns = boto3.client("sns", region_name="${region}")\nfor row in df_${inputVar}.limit(10000).collect():\n    _sns.publish(TopicArn="${topicArn}", Message=str(row.asDict()))\nprint(f"[SNS] Published to ${topicArn}")`;
    conf = 0.90;
    return { code, conf };
  }

  // -- GetSQS --
  if (p.type === 'GetSQS') {
    const queueUrl = props['Queue URL'] || 'https://sqs.us-east-1.amazonaws.com/123456789/queue';
    const region = props['Region'] || 'us-east-1';
    code = `# SQS Consumer: ${p.name}\n# Queue: ${queueUrl}\nimport boto3\n_sqs = boto3.client("sqs", region_name="${region}")\n_msgs = []\nwhile True:\n    _resp = _sqs.receive_message(QueueUrl="${queueUrl}", MaxNumberOfMessages=10, WaitTimeSeconds=5)\n    _batch = _resp.get("Messages", [])\n    if not _batch: break\n    for m in _batch:\n        _msgs.append({"body": m["Body"], "receipt_handle": m["ReceiptHandle"]})\n        _sqs.delete_message(QueueUrl="${queueUrl}", ReceiptHandle=m["ReceiptHandle"])\n    if len(_msgs) >= 10000: break\ndf_${varName} = spark.createDataFrame(_msgs) if _msgs else spark.createDataFrame([], "body STRING")\nprint(f"[SQS] Consumed {len(_msgs)} messages")`;
    conf = 0.90;
    return { code, conf };
  }

  // -- PutDynamoDB / GetDynamoDB --
  if (p.type === 'PutDynamoDB' || p.type === 'GetDynamoDB') {
    const tableName = props['Table Name'] || 'dynamodb_table';
    const region = props['Region'] || 'us-east-1';
    const isWrite = p.type === 'PutDynamoDB';
    if (isWrite) {
      code = `# DynamoDB Write: ${p.name}\n# Table: ${tableName}\nimport boto3\n_dynamodb = boto3.resource("dynamodb", region_name="${region}")\n_table = _dynamodb.Table("${tableName}")\nwith _table.batch_writer() as _batch:\n    for row in df_${inputVar}.limit(10000).collect():\n        _batch.put_item(Item=row.asDict())\ndf_${varName} = df_${inputVar}\nprint(f"[DYNAMODB] Wrote to ${tableName}")`;
    } else {
      code = `# DynamoDB Read: ${p.name}\n# Table: ${tableName}\nimport boto3\n_dynamodb = boto3.resource("dynamodb", region_name="${region}")\n_table = _dynamodb.Table("${tableName}")\n_items = _table.scan().get("Items", [])\ndf_${varName} = spark.createDataFrame(_items) if _items else spark.createDataFrame([], "id STRING")\nprint(f"[DYNAMODB] Read {len(_items)} items from ${tableName}")`;
    }
    conf = 0.90;
    return { code, conf };
  }

  // -- DeleteDynamoDB --
  if (p.type === 'DeleteDynamoDB') {
    code = `# DynamoDB Delete: ${p.name}\nimport boto3\n_dynamodb = boto3.resource("dynamodb", region_name="${props['Region'] || 'us-east-1'}")\n_table = _dynamodb.Table("${props['Table Name'] || 'table'}")\nwith _table.batch_writer() as _batch:\n    for row in df_${inputVar}.limit(10000).collect():\n        _batch.delete_item(Key={"id": row["id"]})`;
    conf = 0.90;
    return { code, conf };
  }

  // -- DeleteSQS --
  if (p.type === 'DeleteSQS') {
    code = `# SQS Delete: ${p.name}\nimport boto3\n_sqs = boto3.client("sqs", region_name="${props['Region'] || 'us-east-1'}")\ndf_${varName} = df_${inputVar}`;
    conf = 0.90;
    return { code, conf };
  }

  // -- PutKinesisStream --
  if (p.type === 'PutKinesisStream') {
    const stream = props['Kinesis Stream Name'] || props['Amazon Kinesis Stream Name'] || 'stream';
    const region = props['Region'] || 'us-east-1';
    code = `# Kinesis Write: ${p.name}\n# Stream: ${stream}\nimport boto3, json\n_kinesis = boto3.client("kinesis", region_name="${region}")\nfor row in df_${inputVar}.limit(10000).collect():\n    _kinesis.put_record(StreamName="${stream}", Data=json.dumps(row.asDict()), PartitionKey=str(row[0]))\nprint(f"[KINESIS] Wrote to ${stream}")`;
    conf = 0.90;
    return { code, conf };
  }

  // -- PutLambda --
  if (p.type === 'PutLambda') {
    const funcName = props['Amazon Lambda Name'] || props['Function Name'] || 'lambda_function';
    const region = props['Region'] || 'us-east-1';
    code = `# Lambda: ${p.name}\n# Function: ${funcName}\nimport boto3, json\n_lambda = boto3.client("lambda", region_name="${region}")\nfor row in df_${inputVar}.limit(1000).collect():\n    _lambda.invoke(FunctionName="${funcName}", InvocationType="Event", Payload=json.dumps(row.asDict()))\nprint(f"[LAMBDA] Invoked ${funcName}")`;
    conf = 0.90;
    return { code, conf };
  }

  // -- InvokeAWSGatewayApi --
  if (p.type === 'InvokeAWSGatewayApi') {
    const url = props['API Gateway URL'] || props['URL'] || 'https://api.execute-api.amazonaws.com';
    code = `# AWS API GW: ${p.name}\nimport requests\n_response = requests.post("${url}", json=df_${inputVar}.limit(100).toPandas().to_dict(orient="records"))\ndf_${varName} = spark.createDataFrame([_response.json()] if isinstance(_response.json(), dict) else _response.json())`;
    conf = 0.90;
    return { code, conf };
  }

  return null; // Not handled
}
