// Streaming source processor types — these produce streaming DataFrames
export const _STREAMING_SOURCE_TYPES = /^(ConsumeKafka|ConsumeKafkaRecord|ListenHTTP|ListenTCP|ListenUDP|ListenSyslog|ListenRELP|ListenSMTP|ListenGRPC|ListenWebSocket|ConsumeJMS|ConsumeMQTT|ConsumeAMQP|ConsumeGCPubSub|ConsumeAzureEventHub|ConsumeAzureServiceBus|ConsumeKinesisStream|TailFile|GetHTTP|GenerateFlowFile)/;

// Batch sink patterns — code patterns that are incompatible with streaming DataFrames
export const _BATCH_SINK_PATTERN = /\.toPandas\(\)|for\s+row\s+in\s+df_\w+\.(?:limit\(\d+\)\.)?collect\(\)|df_\w+\.limit\(\d+\)\.toPandas|df_\w+\.count\(\)|\.write\.format\(|\.saveAsTable\(|\.save\(|\.show\(|\.display\(/;

// Processors that break streaming propagation (batch-only operations)
export const _BATCH_BREAKING_TYPES = /^(ExecuteSQL|PutDatabaseRecord|PutFile|PutFTP|PutSFTP|PutHDFS|FetchFile|QueryDatabaseTable|ListDatabaseTables|GenerateTableFetch)$/;
