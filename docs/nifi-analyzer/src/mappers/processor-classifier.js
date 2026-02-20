/**
 * mappers/processor-classifier.js -- Pattern-based NiFi processor role classification
 *
 * Extracted from index.html lines 2316-2326.
 * Uses NIFI_ROLE_MAP for known types, then falls back to prefix-based regex matching.
 */

// Static role map for well-known NiFi processor types
// Extracted from index.html lines 2273-2314
const NIFI_ROLE_MAP = {
  // Sources
  GetFile:'source',GetHTTP:'source',ConsumeKafka:'source',ConsumeKafka_2_6:'source',
  ConsumeKafkaRecord_2_6:'source',QueryDatabaseTable:'source',QueryDatabaseTableRecord:'source',
  ListenHTTP:'source',GetSFTP:'source',GetFTP:'source',GenerateFlowFile:'source',
  ListS3:'source',FetchS3Object:'source',ListFile:'source',ListSFTP:'source',
  ListFTP:'source',TailFile:'source',GetMongo:'source',FetchFile:'source',
  GetElasticsearch:'source',GetSQS:'source',ConsumeJMS:'source',ConsumeMQTT:'source',
  ConsumeAMQP:'source',GetHDFS:'source',FetchHDFS:'source',ListHDFS:'source',
  ConsumeAzureEventHub:'source',FetchAzureBlobStorage:'source',
  HandleHttpRequest:'source',ConsumeGCPubSub:'source',ConsumeKinesisStream:'source',
  GetDynamoDB:'source',FetchGCS:'source',ListGCSBucket:'source',
  GetHBase:'source',GetCouchbaseKey:'source',GetCypher:'source',
  // Transforms
  ReplaceText:'transform',EvaluateJsonPath:'transform',ConvertRecord:'transform',
  UpdateAttribute:'transform',FlattenJson:'transform',JoltTransformJSON:'transform',
  SplitJson:'transform',SplitContent:'transform',MergeContent:'transform',
  MergeRecord:'transform',CompressContent:'transform',
  ConvertAvroToJSON:'transform',ConvertAvroToORC:'transform',ConvertCSVToAvro:'transform',
  ConvertJSONToAvro:'transform',ConvertJSONToSQL:'transform',
  ExtractText:'transform',TransformXml:'transform',EvaluateXPath:'transform',EvaluateXQuery:'transform',
  EncryptContent:'transform',HashAttribute:'transform',
  SplitXml:'transform',SplitText:'transform',SplitAvro:'transform',SplitRecord:'transform',
  ExtractGrok:'transform',ExtractHL7Attributes:'transform',
  ExecuteScript:'transform',ExecuteGroovyScript:'transform',
  AttributesToJSON:'transform',ForkRecord:'transform',SampleRecord:'transform',
  JoltTransformRecord:'transform',ConvertCharacterSet:'transform',
  ParseCEF:'transform',ParseEvtx:'transform',ParseNetflowv5:'transform',ParseSyslog5424:'transform',
  UnpackContent:'transform',IdentifyMimeType:'transform',ModifyBytes:'transform',
  SegmentContent:'transform',DuplicateFlowFile:'transform',
  // Route
  RouteOnAttribute:'route',RouteOnContent:'route',RouteText:'route',RouteHL7:'route',
  ValidateRecord:'route',DistributeLoad:'route',DetectDuplicate:'route',
  // Process
  ExecuteSQL:'process',InvokeHTTP:'process',ExecuteStreamCommand:'process',
  LookupRecord:'process',LookupAttribute:'process',HandleHttpResponse:'process',
  ExecuteProcess:'process',InvokeAWSGatewayApi:'process',
  // Sinks
  PutFile:'sink',PutHDFS:'sink',PutDatabaseRecord:'sink',PutSQL:'sink',
  PutS3Object:'sink',PutSFTP:'sink',PutFTP:'sink',PutEmail:'sink',
  PublishKafka:'sink',PublishKafka_2_6:'sink',PublishKafkaRecord_2_6:'sink',
  PutAzureBlobStorage:'sink',PutAzureDataLakeStorage:'sink',
  PutElasticsearch:'sink',PutMongo:'sink',PutHBaseJSON:'sink',PutHBaseCell:'sink',
  PutSNS:'sink',PutDynamoDB:'sink',PutKinesisStream:'sink',
  PutGCSObject:'sink',PublishGCPubSub:'sink',
  PutDatabaseRecord:'sink',PutSyslog:'sink',PutTCP:'sink',
  // Utility
  LogMessage:'utility',LogAttribute:'utility',Wait:'utility',Notify:'utility',
  DebugFlow:'utility',CountText:'utility',AttributesToJSON:'utility',
};

/**
 * Classify a NiFi processor type into a role category.
 * First checks the static NIFI_ROLE_MAP, then falls back to
 * prefix-based regex patterns.
 *
 * @param {string} type - The NiFi processor type name
 * @returns {'source'|'sink'|'route'|'transform'|'process'|'utility'}
 */
export function classifyNiFiProcessor(type) {
  return NIFI_ROLE_MAP[type] || (
    /^(Get|List|Consume|Listen|Fetch|Tail|Query)/i.test(type) ? 'source' :
    /^(Put|Publish|Send|Post)/i.test(type) ? 'sink' :
    /^(Route|Distribute|Control|Validate|Detect)/i.test(type) ? 'route' :
    /^(Convert|Split|Merge|Replace|Transform|Extract|Evaluate|Flatten|Compress|Encrypt|Hash)/i.test(type) ? 'transform' :
    /^(Execute|Invoke|Lookup|Handle)/i.test(type) ? 'process' :
    /^(Log|Debug|Count|Wait|Notify)/i.test(type) ? 'utility' :
    'process'
  );
}

export { NIFI_ROLE_MAP };
