// NiFi processor type → role classification
export const NIFI_ROLE_MAP = {
  // Sources (Tier 1)
  GetFile:'source',GetHTTP:'source',GetSFTP:'source',GetFTP:'source',ConsumeKafka:'source',
  ConsumeKafka_2_6:'source',ConsumeKafkaRecord_2_6:'source',ListenHTTP:'source',
  QueryDatabaseTable:'source',QueryDatabaseTableRecord:'source',GenerateFlowFile:'source',
  GetHDFS:'source',ListS3:'source',FetchS3Object:'source',GetS3Object:'source',
  ListFile:'source',FetchFile:'source',ConsumeJMS:'source',ListenTCP:'source',
  ListenUDP:'source',GetMongo:'source',GetElasticsearch:'source',TailFile:'source',
  // Routing (Tier 2)
  RouteOnAttribute:'route',RouteOnContent:'route',DistributeLoad:'route',ControlRate:'route',
  RouteText:'route',DetectDuplicate:'route',ValidateRecord:'route',
  // Transform (Tier 2)
  UpdateAttribute:'transform',JoltTransformJSON:'transform',ReplaceText:'transform',
  ConvertRecord:'transform',SplitRecord:'transform',MergeContent:'transform',MergeRecord:'transform',
  ExecuteScript:'transform',ExecuteStreamCommand:'transform',ConvertJSONToSQL:'transform',
  TransformXml:'transform',SplitJson:'transform',SplitXml:'transform',SplitText:'transform',SplitContent:'transform',
  EvaluateJsonPath:'transform',ExtractText:'transform',CompressContent:'transform',
  EncryptContent:'transform',HashContent:'transform',Base64EncodeContent:'transform',
  ConvertCharacterSet:'transform',FlattenJson:'transform',ConvertAvroToJSON:'transform',
  ConvertJSONToAvro:'transform',
  // Processing (Tier 3)
  ExecuteSQL:'process',ExecuteSQLRecord:'process',PutDatabaseRecord:'process',
  LookupAttribute:'process',LookupRecord:'process',InvokeHTTP:'process',
  ExecuteProcess:'process',HandleHttpRequest:'process',HandleHttpResponse:'process',
  // Sinks (Tier 4)
  PutFile:'sink',PutHDFS:'sink',PutS3Object:'sink',PutSQL:'sink',PutKafka:'sink',
  PutKafkaRecord:'sink',PutEmail:'sink',PutSFTP:'sink',PutFTP:'sink',PublishKafka:'sink',
  PublishKafka_2_6:'sink',PublishKafkaRecord_2_6:'sink',PutMongo:'sink',PutElasticsearch:'sink',
  PutDatabaseRecord:'sink',PutSyslog:'sink',PutTCP:'sink',
  // Utility
  LogMessage:'utility',LogAttribute:'utility',Wait:'utility',Notify:'utility',
  DebugFlow:'utility',CountText:'utility',AttributesToJSON:'utility',
};

// Role tier ordering for display
export const ROLE_TIER_ORDER = ['source', 'route', 'transform', 'process', 'sink', 'utility'];

// Role tier colors for UI rendering
export const ROLE_TIER_COLORS = {
  source: '#3B82F6',
  route: '#EAB308',
  transform: '#A855F7',
  process: '#6366F1',
  sink: '#21C354',
  utility: '#808495'
};

// Role tier labels for section headings
export const ROLE_TIER_LABELS = {
  source: 'SOURCES — Ingestion & Acquisition',
  route: 'ROUTING — Distribution & Validation',
  transform: 'TRANSFORMS — Conversion & Enrichment',
  process: 'PROCESSING — Execution & Lookup',
  sink: 'SINKS — Output & Delivery',
  utility: 'UTILITY — Logging & Control'
};

