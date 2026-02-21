/**
 * mappers/processor-classifier.js -- Pattern-based NiFi processor role classification
 *
 * Extracted from index.html lines 2316-2326.
 * Uses NIFI_PROCESSOR_META for known types with role, subcategory, and action,
 * then falls back to prefix-based regex matching.
 */

// ── Full processor metadata map: role + subcategory + action ──
const NIFI_PROCESSOR_META = {
  // ── Sources: file ──
  GetFile:       { role: 'source', subcategory: 'file-source', action: 'read' },
  GetSFTP:       { role: 'source', subcategory: 'file-source', action: 'read' },
  GetFTP:        { role: 'source', subcategory: 'file-source', action: 'read' },
  FetchFile:     { role: 'source', subcategory: 'file-source', action: 'read' },
  ListFile:      { role: 'source', subcategory: 'file-source', action: 'read' },
  ListSFTP:      { role: 'source', subcategory: 'file-source', action: 'read' },
  ListFTP:       { role: 'source', subcategory: 'file-source', action: 'read' },
  TailFile:      { role: 'source', subcategory: 'file-source', action: 'read' },
  // ── Sources: API ──
  GetHTTP:       { role: 'source', subcategory: 'api-source', action: 'read' },
  ListenHTTP:    { role: 'source', subcategory: 'api-source', action: 'read' },
  HandleHttpRequest: { role: 'source', subcategory: 'api-source', action: 'read' },
  // ── Sources: streaming ──
  ConsumeKafka:  { role: 'source', subcategory: 'streaming-source', action: 'read' },
  ConsumeKafka_2_6: { role: 'source', subcategory: 'streaming-source', action: 'read' },
  ConsumeKafkaRecord_2_6: { role: 'source', subcategory: 'streaming-source', action: 'read' },
  ConsumeJMS:    { role: 'source', subcategory: 'streaming-source', action: 'read' },
  ConsumeMQTT:   { role: 'source', subcategory: 'streaming-source', action: 'read' },
  ConsumeAMQP:   { role: 'source', subcategory: 'streaming-source', action: 'read' },
  ConsumeAzureEventHub: { role: 'source', subcategory: 'streaming-source', action: 'read' },
  ConsumeGCPubSub: { role: 'source', subcategory: 'streaming-source', action: 'read' },
  ConsumeKinesisStream: { role: 'source', subcategory: 'streaming-source', action: 'read' },
  GetSQS:        { role: 'source', subcategory: 'streaming-source', action: 'read' },
  // ── Sources: database ──
  QueryDatabaseTable: { role: 'source', subcategory: 'db-source', action: 'read' },
  QueryDatabaseTableRecord: { role: 'source', subcategory: 'db-source', action: 'read' },
  GetMongo:      { role: 'source', subcategory: 'db-source', action: 'read' },
  GetElasticsearch: { role: 'source', subcategory: 'db-source', action: 'read' },
  GetHBase:      { role: 'source', subcategory: 'db-source', action: 'read' },
  GetCouchbaseKey: { role: 'source', subcategory: 'db-source', action: 'read' },
  GetDynamoDB:   { role: 'source', subcategory: 'db-source', action: 'read' },
  GetCypher:     { role: 'source', subcategory: 'db-source', action: 'read' },
  // ── Sources: cloud storage ──
  ListS3:        { role: 'source', subcategory: 'cloud-source', action: 'read' },
  FetchS3Object: { role: 'source', subcategory: 'cloud-source', action: 'read' },
  FetchAzureBlobStorage: { role: 'source', subcategory: 'cloud-source', action: 'read' },
  FetchGCS:      { role: 'source', subcategory: 'cloud-source', action: 'read' },
  ListGCSBucket: { role: 'source', subcategory: 'cloud-source', action: 'read' },
  GetHDFS:       { role: 'source', subcategory: 'cloud-source', action: 'read' },
  FetchHDFS:     { role: 'source', subcategory: 'cloud-source', action: 'read' },
  ListHDFS:      { role: 'source', subcategory: 'cloud-source', action: 'read' },
  // ── Sources: generator ──
  GenerateFlowFile: { role: 'source', subcategory: 'generator', action: 'read' },

  // ── Transforms: content ──
  ReplaceText:       { role: 'transform', subcategory: 'content-transform', action: 'transform' },
  JoltTransformJSON: { role: 'transform', subcategory: 'content-transform', action: 'transform' },
  JoltTransformRecord: { role: 'transform', subcategory: 'content-transform', action: 'transform' },
  FlattenJson:       { role: 'transform', subcategory: 'content-transform', action: 'transform' },
  TransformXml:      { role: 'transform', subcategory: 'content-transform', action: 'transform' },
  ModifyBytes:       { role: 'transform', subcategory: 'content-transform', action: 'transform' },
  // ── Transforms: extract ──
  EvaluateJsonPath:  { role: 'transform', subcategory: 'extract', action: 'extract' },
  EvaluateXPath:     { role: 'transform', subcategory: 'extract', action: 'extract' },
  EvaluateXQuery:    { role: 'transform', subcategory: 'extract', action: 'extract' },
  ExtractText:       { role: 'transform', subcategory: 'extract', action: 'extract' },
  ExtractGrok:       { role: 'transform', subcategory: 'extract', action: 'extract' },
  ExtractHL7Attributes: { role: 'transform', subcategory: 'extract', action: 'extract' },
  ParseCEF:          { role: 'transform', subcategory: 'extract', action: 'extract' },
  ParseEvtx:         { role: 'transform', subcategory: 'extract', action: 'extract' },
  ParseNetflowv5:    { role: 'transform', subcategory: 'extract', action: 'extract' },
  ParseSyslog5424:   { role: 'transform', subcategory: 'extract', action: 'extract' },
  IdentifyMimeType:  { role: 'transform', subcategory: 'extract', action: 'extract' },
  // ── Transforms: attribute ──
  UpdateAttribute:   { role: 'transform', subcategory: 'attribute-transform', action: 'enrich' },
  PutAttribute:      { role: 'transform', subcategory: 'attribute-transform', action: 'enrich' },
  AttributesToJSON:  { role: 'transform', subcategory: 'attribute-transform', action: 'transform' },
  // ── Transforms: format conversion ──
  ConvertRecord:     { role: 'transform', subcategory: 'format-convert', action: 'transform' },
  ConvertAvroToJSON: { role: 'transform', subcategory: 'format-convert', action: 'transform' },
  ConvertAvroToORC:  { role: 'transform', subcategory: 'format-convert', action: 'transform' },
  ConvertCSVToAvro:  { role: 'transform', subcategory: 'format-convert', action: 'transform' },
  ConvertJSONToAvro: { role: 'transform', subcategory: 'format-convert', action: 'transform' },
  ConvertJSONToSQL:  { role: 'transform', subcategory: 'format-convert', action: 'transform' },
  ConvertCharacterSet: { role: 'transform', subcategory: 'format-convert', action: 'transform' },
  // ── Transforms: split/merge ──
  SplitJson:         { role: 'transform', subcategory: 'split-merge', action: 'transform' },
  SplitContent:      { role: 'transform', subcategory: 'split-merge', action: 'transform' },
  SplitXml:          { role: 'transform', subcategory: 'split-merge', action: 'transform' },
  SplitText:         { role: 'transform', subcategory: 'split-merge', action: 'transform' },
  SplitAvro:         { role: 'transform', subcategory: 'split-merge', action: 'transform' },
  SplitRecord:       { role: 'transform', subcategory: 'split-merge', action: 'transform' },
  MergeContent:      { role: 'transform', subcategory: 'split-merge', action: 'transform' },
  MergeRecord:       { role: 'transform', subcategory: 'split-merge', action: 'transform' },
  ForkRecord:        { role: 'transform', subcategory: 'split-merge', action: 'transform' },
  SampleRecord:      { role: 'transform', subcategory: 'split-merge', action: 'transform' },
  SegmentContent:    { role: 'transform', subcategory: 'split-merge', action: 'transform' },
  DuplicateFlowFile: { role: 'transform', subcategory: 'split-merge', action: 'transform' },
  UnpackContent:     { role: 'transform', subcategory: 'split-merge', action: 'transform' },
  // ── Transforms: encoding/security ──
  CompressContent:   { role: 'transform', subcategory: 'encoding', action: 'transform' },
  EncryptContent:    { role: 'transform', subcategory: 'encoding', action: 'transform' },
  HashAttribute:     { role: 'transform', subcategory: 'encoding', action: 'transform' },
  // ── Transforms: scripted ──
  ExecuteScript:     { role: 'transform', subcategory: 'scripted', action: 'transform' },
  ExecuteGroovyScript: { role: 'transform', subcategory: 'scripted', action: 'transform' },

  // ── Route ──
  RouteOnAttribute:  { role: 'route', subcategory: 'conditional', action: 'filter' },
  RouteOnContent:    { role: 'route', subcategory: 'conditional', action: 'filter' },
  RouteText:         { role: 'route', subcategory: 'conditional', action: 'filter' },
  RouteHL7:          { role: 'route', subcategory: 'conditional', action: 'filter' },
  ValidateRecord:    { role: 'route', subcategory: 'validation', action: 'validate' },
  DistributeLoad:    { role: 'route', subcategory: 'load-balance', action: 'filter' },
  DetectDuplicate:   { role: 'route', subcategory: 'dedup', action: 'filter' },

  // ── Process ──
  ExecuteSQL:        { role: 'process', subcategory: 'database', action: 'read' },
  InvokeHTTP:        { role: 'process', subcategory: 'api-call', action: 'enrich' },
  LookupRecord:      { role: 'process', subcategory: 'enrichment', action: 'enrich' },
  LookupAttribute:   { role: 'process', subcategory: 'enrichment', action: 'enrich' },
  HandleHttpResponse: { role: 'process', subcategory: 'api-call', action: 'write' },
  ExecuteStreamCommand: { role: 'process', subcategory: 'external-process', action: 'transform' },
  ExecuteProcess:    { role: 'process', subcategory: 'external-process', action: 'transform' },
  InvokeAWSGatewayApi: { role: 'process', subcategory: 'api-call', action: 'enrich' },

  // ── Sinks: file ──
  PutFile:           { role: 'sink', subcategory: 'file-sink', action: 'write' },
  PutSFTP:           { role: 'sink', subcategory: 'file-sink', action: 'write' },
  PutFTP:            { role: 'sink', subcategory: 'file-sink', action: 'write' },
  // ── Sinks: database ──
  PutDatabaseRecord: { role: 'sink', subcategory: 'db-sink', action: 'write' },
  PutSQL:            { role: 'sink', subcategory: 'db-sink', action: 'write' },
  PutMongo:          { role: 'sink', subcategory: 'db-sink', action: 'write' },
  PutElasticsearch:  { role: 'sink', subcategory: 'db-sink', action: 'write' },
  PutHBaseJSON:      { role: 'sink', subcategory: 'db-sink', action: 'write' },
  PutHBaseCell:      { role: 'sink', subcategory: 'db-sink', action: 'write' },
  PutDynamoDB:       { role: 'sink', subcategory: 'db-sink', action: 'write' },
  // ── Sinks: cloud storage ──
  PutS3Object:       { role: 'sink', subcategory: 'cloud-sink', action: 'write' },
  PutHDFS:           { role: 'sink', subcategory: 'cloud-sink', action: 'write' },
  PutAzureBlobStorage: { role: 'sink', subcategory: 'cloud-sink', action: 'write' },
  PutAzureDataLakeStorage: { role: 'sink', subcategory: 'cloud-sink', action: 'write' },
  PutGCSObject:      { role: 'sink', subcategory: 'cloud-sink', action: 'write' },
  // ── Sinks: streaming ──
  PublishKafka:      { role: 'sink', subcategory: 'streaming-sink', action: 'write' },
  PublishKafka_2_6:  { role: 'sink', subcategory: 'streaming-sink', action: 'write' },
  PublishKafkaRecord_2_6: { role: 'sink', subcategory: 'streaming-sink', action: 'write' },
  PutKinesisStream:  { role: 'sink', subcategory: 'streaming-sink', action: 'write' },
  PublishGCPubSub:   { role: 'sink', subcategory: 'streaming-sink', action: 'write' },
  PutSNS:            { role: 'sink', subcategory: 'streaming-sink', action: 'write' },
  // ── Sinks: notification ──
  PutEmail:          { role: 'sink', subcategory: 'notification-sink', action: 'write' },
  PutSyslog:         { role: 'sink', subcategory: 'notification-sink', action: 'write' },
  PutTCP:            { role: 'sink', subcategory: 'notification-sink', action: 'write' },

  // ── Utility ──
  LogMessage:        { role: 'utility', subcategory: 'logging', action: 'monitor' },
  LogAttribute:      { role: 'utility', subcategory: 'logging', action: 'monitor' },
  DebugFlow:         { role: 'utility', subcategory: 'logging', action: 'monitor' },
  CountText:         { role: 'utility', subcategory: 'metrics', action: 'monitor' },
  Wait:              { role: 'utility', subcategory: 'flow-control', action: 'monitor' },
  Notify:            { role: 'utility', subcategory: 'flow-control', action: 'monitor' },
};

// Backward-compatible flat role map derived from NIFI_PROCESSOR_META
const NIFI_ROLE_MAP = {};
Object.entries(NIFI_PROCESSOR_META).forEach(([type, meta]) => {
  NIFI_ROLE_MAP[type] = meta.role;
});

/**
 * Infer action type from processor type name prefix.
 * @param {string} type
 * @returns {string}
 */
function inferAction(type) {
  if (/^(Get|List|Consume|Fetch|Query|Tail)/i.test(type)) return 'read';
  if (/^(Put|Publish|Send|Post)/i.test(type)) return 'write';
  if (/^(Route|Distribute|Validate|Detect)/i.test(type)) return 'filter';
  if (/^(Convert|Split|Merge|Replace|Transform|Extract|Evaluate|Flatten|Compress|Encrypt|Hash|Parse|Fork|Sample|Unpack)/i.test(type)) return 'transform';
  if (/^(Lookup|Invoke)/i.test(type)) return 'enrich';
  if (/^(Log|Debug|Count|Wait|Notify)/i.test(type)) return 'monitor';
  return 'process';
}

/**
 * Infer subcategory from processor type name and role.
 * @param {string} type
 * @param {string} role
 * @returns {string}
 */
function inferSubcategory(type, role) {
  if (role === 'source') {
    if (/File|SFTP|FTP/i.test(type)) return 'file-source';
    if (/HTTP|API|Gateway/i.test(type)) return 'api-source';
    if (/Kafka|JMS|MQTT|AMQP|EventHub|PubSub|Kinesis|SQS/i.test(type)) return 'streaming-source';
    if (/SQL|Database|Mongo|Elasticsearch|HBase|Couchbase|DynamoDB|Cypher|Redis/i.test(type)) return 'db-source';
    if (/S3|Azure|GCS|HDFS|Cloud/i.test(type)) return 'cloud-source';
    return 'file-source';
  }
  if (role === 'sink') {
    if (/File|SFTP|FTP/i.test(type)) return 'file-sink';
    if (/SQL|Database|Mongo|Elasticsearch|HBase|Couchbase|DynamoDB|Redis/i.test(type)) return 'db-sink';
    if (/Kafka|JMS|MQTT|AMQP|EventHub|PubSub|Kinesis|SNS|SQS/i.test(type)) return 'streaming-sink';
    if (/S3|Azure|GCS|HDFS|Cloud/i.test(type)) return 'cloud-sink';
    if (/Email|Syslog|TCP|Slack/i.test(type)) return 'notification-sink';
    return 'cloud-sink';
  }
  if (role === 'transform') {
    if (/Extract|Evaluate|Parse|Grok|HL7|CEF|Syslog|Identify/i.test(type)) return 'extract';
    if (/Attribute|PutAttribute/i.test(type)) return 'attribute-transform';
    if (/Convert|CharacterSet/i.test(type)) return 'format-convert';
    if (/Split|Merge|Fork|Sample|Segment|Duplicate|Unpack/i.test(type)) return 'split-merge';
    if (/Compress|Encrypt|Hash/i.test(type)) return 'encoding';
    if (/Script|Groovy/i.test(type)) return 'scripted';
    return 'content-transform';
  }
  if (role === 'route') {
    if (/Validate/i.test(type)) return 'validation';
    if (/Duplicate|Dedup/i.test(type)) return 'dedup';
    if (/Distribute|Balance/i.test(type)) return 'load-balance';
    return 'conditional';
  }
  if (role === 'process') {
    if (/SQL|Database/i.test(type)) return 'database';
    if (/HTTP|API|Gateway|Invoke/i.test(type)) return 'api-call';
    if (/Lookup/i.test(type)) return 'enrichment';
    return 'external-process';
  }
  if (role === 'utility') {
    if (/Log|Debug/i.test(type)) return 'logging';
    if (/Count/i.test(type)) return 'metrics';
    return 'flow-control';
  }
  return role + '-unknown';
}

/**
 * Classify a NiFi processor type into a role category.
 * First checks the static map, then falls back to prefix-based regex patterns.
 *
 * @param {string} type - The NiFi processor type name
 * @returns {'source'|'sink'|'route'|'transform'|'process'|'utility'}
 */
export function classifyNiFiProcessor(type) {
  const meta = NIFI_PROCESSOR_META[type];
  if (meta) return meta.role;
  return (
    /^(Get|List|Consume|Listen|Fetch|Tail|Query)/i.test(type) ? 'source' :
    /^(Put|Publish|Send|Post)/i.test(type) ? 'sink' :
    /^(Route|Distribute|Control|Validate|Detect)/i.test(type) ? 'route' :
    /^(Convert|Split|Merge|Replace|Transform|Extract|Evaluate|Flatten|Compress|Encrypt|Hash)/i.test(type) ? 'transform' :
    /^(Execute|Invoke|Lookup|Handle)/i.test(type) ? 'process' :
    /^(Log|Debug|Count|Wait|Notify)/i.test(type) ? 'utility' :
    'process'
  );
}

/**
 * Classify a NiFi processor type with full metadata.
 * Returns role, subcategory, and action type for enhanced visualization.
 *
 * @param {string} type - The NiFi processor type name
 * @returns {{ role: string, subcategory: string, action: string }}
 */
export function classifyNiFiProcessorFull(type) {
  const meta = NIFI_PROCESSOR_META[type];
  if (meta) return { ...meta };
  const role = classifyNiFiProcessor(type);
  return { role, subcategory: inferSubcategory(type, role), action: inferAction(type) };
}

export { NIFI_ROLE_MAP, NIFI_PROCESSOR_META };
