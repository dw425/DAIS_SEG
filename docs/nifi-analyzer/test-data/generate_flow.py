#!/usr/bin/env python3
"""Generate a realistic 1000-processor NiFi flow XML for testing."""
import uuid
import random
import os

PROC_TEMPLATES = {
    'GetFile': {
        'class': 'org.apache.nifi.processors.standard.GetFile',
        'props': lambda i: {'Input Directory': f'/data/input/source_{i}', 'File Filter': '[^\\\\.].*\\.csv', 'Keep Source File': 'false', 'Batch Size': '10'}
    },
    'ListFile': {
        'class': 'org.apache.nifi.processors.standard.ListFile',
        'props': lambda i: {'Input Directory': f'/data/input/list_{i}', 'Recurse Subdirectories': 'true', 'File Filter': '.*', 'Minimum File Age': '0 sec'}
    },
    'FetchFile': {
        'class': 'org.apache.nifi.processors.standard.FetchFile',
        'props': lambda i: {'File to Fetch': '${absolute.path}/${filename}', 'Completion Strategy': 'Move File', 'Move Destination Directory': f'/data/processed/{i}'}
    },
    'ConvertRecord': {
        'class': 'org.apache.nifi.processors.standard.ConvertRecord',
        'props': lambda i: {'Record Reader': 'CSVReader', 'Record Writer': 'JsonRecordSetWriter', 'Include Zero Record FlowFiles': 'true'}
    },
    'EvaluateJsonPath': {
        'class': 'org.apache.nifi.processors.standard.EvaluateJsonPath',
        'props': lambda i: {'Destination': 'flowfile-attribute', 'Return Type': 'auto-detect', f'field_{i}': f'$.data[{i%10}].value', 'id': '$.id', 'timestamp': '$.metadata.timestamp'}
    },
    'JoltTransformJSON': {
        'class': 'org.apache.nifi.processors.standard.JoltTransformJSON',
        'props': lambda i: {'Jolt Transformation DSL': 'Chain', 'Jolt Specification': '[{"operation":"shift","spec":{"data":{"*":{"value":"values[&1]"}}}}]'}
    },
    'RouteOnAttribute': {
        'class': 'org.apache.nifi.processors.standard.RouteOnAttribute',
        'props': lambda i: {'Routing Strategy': 'Route to Property name', 'valid': '${fileSize:gt(0)}', 'high_priority': '${priority:equals("HIGH")}', 'error': '${error:isEmpty():not()}'}
    },
    'RouteOnContent': {
        'class': 'org.apache.nifi.processors.standard.RouteOnContent',
        'props': lambda i: {'Match Requirement': 'content must contain match', 'Content Buffer Size': '1 MB', f'pattern_{i}': f'ERROR|CRITICAL|FATAL'}
    },
    'UpdateAttribute': {
        'class': 'org.apache.nifi.processors.standard.UpdateAttribute',
        'props': lambda i: {'processed_time': '${now():format("yyyy-MM-dd HH:mm:ss")}', f'batch_id_{i}': '${UUID()}', 'source_system': f'system_{i}'}
    },
    'ReplaceText': {
        'class': 'org.apache.nifi.processors.standard.ReplaceText',
        'props': lambda i: {'Search Value': '(?s)(^.*$)', 'Replacement Value': '$1', 'Replacement Strategy': 'Regex Replace', 'Evaluation Mode': 'Entire text'}
    },
    'ExecuteSQL': {
        'class': 'org.apache.nifi.processors.standard.ExecuteSQL',
        'props': lambda i: {'Database Connection Pooling Service': 'DBCPService', 'SQL select query': f'SELECT * FROM schema_{i}.table_{i} WHERE modified_date >= CURRENT_DATE - 1', 'Max Rows Per Flow File': '10000'}
    },
    'QueryDatabaseTable': {
        'class': 'org.apache.nifi.processors.standard.QueryDatabaseTable',
        'props': lambda i: {'Database Connection Pooling Service': 'DBCPService', 'Table Name': f'dbo.transactions_{i}', 'Maximum-value Columns': 'modified_date', 'Columns to Return': '*'}
    },
    'LookupRecord': {
        'class': 'org.apache.nifi.processors.standard.LookupRecord',
        'props': lambda i: {'Record Reader': 'JsonTreeReader', 'Record Writer': 'JsonRecordSetWriter', 'Lookup Service': f'SimpleDatabaseLookupService_{i}', 'Result RecordPath': f'/enriched_field_{i}'}
    },
    'MergeContent': {
        'class': 'org.apache.nifi.processors.standard.MergeContent',
        'props': lambda i: {'Merge Strategy': 'Bin-Packing Algorithm', 'Merge Format': 'Binary Concatenation', 'Minimum Number of Entries': '5', 'Maximum Number of Entries': '100', 'Max Bin Age': '5 min'}
    },
    'SplitJson': {
        'class': 'org.apache.nifi.processors.standard.SplitJson',
        'props': lambda i: {'JsonPath Expression': '$.records[*]'}
    },
    'SplitContent': {
        'class': 'org.apache.nifi.processors.standard.SplitContent',
        'props': lambda i: {'Byte Sequence': '\\n', 'Keep Byte Sequence': 'false', 'Byte Sequence Format': 'Text'}
    },
    'CompressContent': {
        'class': 'org.apache.nifi.processors.standard.CompressContent',
        'props': lambda i: {'Mode': 'compress', 'Compression Format': 'gzip', 'Compression Level': '6'}
    },
    'EncryptContent': {
        'class': 'org.apache.nifi.processors.standard.EncryptContent',
        'props': lambda i: {'Mode': 'Encrypt', 'Algorithm': 'AES/GCM/NoPadding', 'Key Derivation Function': 'PBKDF2'}
    },
    'PutFile': {
        'class': 'org.apache.nifi.processors.standard.PutFile',
        'props': lambda i: {'Directory': f'/data/output/target_{i}', 'Conflict Resolution Strategy': 'replace', 'Create Missing Directories': 'true'}
    },
    'PutDatabaseRecord': {
        'class': 'org.apache.nifi.processors.standard.PutDatabaseRecord',
        'props': lambda i: {'Database Connection Pooling Service': 'DBCPService', 'Table Name': f'warehouse.processed_{i}', 'Statement Type': 'INSERT', 'Record Reader': 'JsonTreeReader'}
    },
    'PutSFTP': {
        'class': 'org.apache.nifi.processors.standard.PutSFTP',
        'props': lambda i: {'Hostname': f'sftp-server-{i}.corp.com', 'Port': '22', 'Remote Path': f'/incoming/data_{i}', 'Username': '${sftp.user}', 'Password': '${sftp.password}'}
    },
    'PutS3Object': {
        'class': 'org.apache.nifi.processors.aws.s3.PutS3Object',
        'props': lambda i: {'Bucket': f'data-lake-bucket-{i%5}', 'Object Key': f'raw/source_{i}/${{filename}}', 'Region': 'us-east-1', 'Access Key ID': '${aws.access.key}'}
    },
    'PublishKafka': {
        'class': 'org.apache.nifi.processors.kafka.pubsub.PublishKafka_2_6',
        'props': lambda i: {'Kafka Brokers': 'kafka-broker-1:9092,kafka-broker-2:9092', 'Topic Name': f'processed-events-{i%10}', 'Delivery Guarantee': 'Guarantee Replicated Delivery', 'Key Attribute Encoding': 'UTF-8'}
    },
    'ConsumeKafka': {
        'class': 'org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_6',
        'props': lambda i: {'Kafka Brokers': 'kafka-broker-1:9092,kafka-broker-2:9092', 'Topic Name(s)': f'raw-events-{i%10}', 'Group ID': f'nifi-consumer-group-{i}', 'Offset Reset': 'latest'}
    },
    'InvokeHTTP': {
        'class': 'org.apache.nifi.processors.standard.InvokeHTTP',
        'props': lambda i: {'HTTP Method': random.choice(['GET','POST']), 'Remote URL': f'https://api.internal.corp.com/v2/data/{i}', 'Content-Type': 'application/json', 'Connection Timeout': '30 secs'}
    },
    'LogMessage': {
        'class': 'org.apache.nifi.processors.standard.LogMessage',
        'props': lambda i: {'Log Level': random.choice(['info','warn','debug']), 'Log Message': f'Processing batch ${{batch.id}} for pipeline {i}'}
    },
    'MonitorActivity': {
        'class': 'org.apache.nifi.processors.standard.MonitorActivity',
        'props': lambda i: {'Threshold Duration': '5 min', 'Continually Send Messages': 'false'}
    },
    'RetryFlowFile': {
        'class': 'org.apache.nifi.processors.standard.RetryFlowFile',
        'props': lambda i: {'Maximum Retries': '3', 'Penalize FlowFile': 'true', 'Penalty Duration': '30 sec'}
    },
    'ValidateRecord': {
        'class': 'org.apache.nifi.processors.standard.ValidateRecord',
        'props': lambda i: {'Record Reader': 'CSVReader', 'Record Writer': 'CSVWriter', 'Schema Access Strategy': 'Use Schema Name Property', 'Allow Extra Fields': 'false'}
    },
    'DetectDuplicate': {
        'class': 'org.apache.nifi.processors.standard.DetectDuplicate',
        'props': lambda i: {'Distributed Cache Service': 'DistributedMapCacheClientService', 'Cache Entry Identifier': '${hash.value}', 'FlowFile Description': '${filename}'}
    },
    'EvaluateXPath': {
        'class': 'org.apache.nifi.processors.standard.EvaluateXPath',
        'props': lambda i: {'Destination': 'flowfile-attribute', 'Return Type': 'auto-detect', 'record_id': '/root/record/@id', 'value': '/root/record/value/text()'}
    },
    'SplitXml': {
        'class': 'org.apache.nifi.processors.standard.SplitXml',
        'props': lambda i: {'Split Depth': '2'}
    },
    'ExtractText': {
        'class': 'org.apache.nifi.processors.standard.ExtractText',
        'props': lambda i: {'Enable DOTALL Mode': 'true', f'field_{i}': f'(?i)(error|warning|critical)', 'timestamp': '(\\d{{4}}-\\d{{2}}-\\d{{2}}T\\d{{2}}:\\d{{2}}:\\d{{2}})'}
    },
    'ListHDFS': {
        'class': 'org.apache.nifi.processors.hadoop.ListHDFS',
        'props': lambda i: {'Directory': f'/user/data/raw_{i}', 'Recurse Subdirectories': 'true', 'File Filter': '.*\\.parquet'}
    },
    'FetchHDFS': {
        'class': 'org.apache.nifi.processors.hadoop.FetchHDFS',
        'props': lambda i: {'HDFS Filename': '${absolute.hdfs.path}', 'Compression Codec': 'NONE'}
    },
    'PutHDFS': {
        'class': 'org.apache.nifi.processors.hadoop.PutHDFS',
        'props': lambda i: {'Directory': f'/user/data/processed_{i}', 'Conflict Resolution Strategy': 'replace', 'Block Size': '128 MB'}
    },
    'ExecuteScript': {
        'class': 'org.apache.nifi.processors.standard.ExecuteScript',
        'props': lambda i: {'Script Engine': 'python', 'Script Body': f'# Process record {i}\nimport json\nflowFile = session.get()\nif flowFile:\n    session.transfer(flowFile, REL_SUCCESS)'}
    },
    'ListS3': {
        'class': 'org.apache.nifi.processors.aws.s3.ListS3',
        'props': lambda i: {'Bucket': f'enterprise-datalake-{i%3}', 'Region': 'us-east-1', 'Prefix': f'raw/ingestion_{i}/', 'Listing Strategy': 'Tracking Timestamps'}
    },
    'FetchS3Object': {
        'class': 'org.apache.nifi.processors.aws.s3.FetchS3Object',
        'props': lambda i: {'Bucket': '${s3.bucket}', 'Object Key': '${filename}', 'Region': 'us-east-1'}
    },
    'PutAzureBlobStorage': {
        'class': 'org.apache.nifi.processors.azure.storage.PutAzureBlobStorage',
        'props': lambda i: {'Container Name': f'data-container-{i%3}', 'Storage Account Name': 'corpstorageaccount', 'Blob': f'uploads/${{filename}}'}
    },
    'PutGCSObject': {
        'class': 'org.apache.nifi.processors.gcp.storage.PutGCSObject',
        'props': lambda i: {'Bucket': f'analytics-bucket-{i%3}', 'Key': f'data/${{filename}}'}
    },
    'GenerateFlowFile': {
        'class': 'org.apache.nifi.processors.standard.GenerateFlowFile',
        'props': lambda i: {'File Size': '1 KB', 'Batch Size': '1', 'Data Format': 'Text', 'Unique FlowFiles': 'true'}
    },
}

PROCESS_GROUPS = [
    {
        'name': 'Oracle_Ingestion',
        'pipeline': ['GetFile','ValidateRecord','ConvertRecord','RouteOnAttribute','UpdateAttribute','ExecuteSQL','PutDatabaseRecord','LogMessage','MonitorActivity'],
        'count': 80
    },
    {
        'name': 'SQLServer_Ingestion',
        'pipeline': ['QueryDatabaseTable','ExecuteSQL','SplitJson','EvaluateJsonPath','ConvertRecord','RouteOnAttribute','UpdateAttribute','PutDatabaseRecord','RetryFlowFile','LogMessage'],
        'count': 80
    },
    {
        'name': 'Hadoop_Migration',
        'pipeline': ['ListHDFS','FetchHDFS','MergeContent','SplitContent','ConvertRecord','CompressContent','PutFile','PutHDFS','LogMessage'],
        'count': 80
    },
    {
        'name': 'Kafka_Streaming',
        'pipeline': ['ConsumeKafka','EvaluateJsonPath','JoltTransformJSON','UpdateAttribute','RouteOnAttribute','PublishKafka','PutDatabaseRecord','LogMessage'],
        'count': 80
    },
    {
        'name': 'API_Integration',
        'pipeline': ['InvokeHTTP','EvaluateJsonPath','SplitJson','LookupRecord','ConvertRecord','RouteOnContent','PutFile','UpdateAttribute'],
        'count': 80
    },
    {
        'name': 'Data_Quality',
        'pipeline': ['ValidateRecord','RouteOnAttribute','UpdateAttribute','ReplaceText','ConvertRecord','LogMessage','PutDatabaseRecord'],
        'count': 85
    },
    {
        'name': 'CDC_Processing',
        'pipeline': ['QueryDatabaseTable','DetectDuplicate','RouteOnAttribute','UpdateAttribute','MergeContent','PutDatabaseRecord','LogMessage'],
        'count': 60
    },
    {
        'name': 'File_Distribution',
        'pipeline': ['ListFile','FetchFile','CompressContent','EncryptContent','PutFile','PutSFTP','PutS3Object','LogMessage'],
        'count': 60
    },
    {
        'name': 'Error_Handling',
        'pipeline': ['RouteOnAttribute','RetryFlowFile','LogMessage','PutFile','UpdateAttribute','InvokeHTTP'],
        'count': 65
    },
    {
        'name': 'Monitoring_Audit',
        'pipeline': ['MonitorActivity','LogMessage','UpdateAttribute','PutDatabaseRecord','InvokeHTTP'],
        'count': 70
    },
    {
        'name': 'XML_Processing',
        'pipeline': ['GetFile','EvaluateXPath','SplitXml','ExtractText','ConvertRecord','RouteOnAttribute','PutDatabaseRecord','LogMessage'],
        'count': 60
    },
    {
        'name': 'Data_Enrichment',
        'pipeline': ['LookupRecord','ExecuteSQL','UpdateAttribute','ConvertRecord','MergeContent','PutDatabaseRecord','LogMessage'],
        'count': 60
    },
    {
        'name': 'Cloud_Sync',
        'pipeline': ['ListS3','FetchS3Object','ConvertRecord','PutAzureBlobStorage','PutGCSObject','LogMessage','UpdateAttribute'],
        'count': 70
    },
    {
        'name': 'Realtime_Analytics',
        'pipeline': ['ConsumeKafka','EvaluateJsonPath','ExecuteScript','UpdateAttribute','PublishKafka','PutDatabaseRecord','LogMessage','MonitorActivity'],
        'count': 70
    },
]

def gen_id():
    return str(uuid.uuid4())

def escape_xml(s):
    return s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;')

def make_processor_xml(proc_type, name, idx, group_name):
    pid = gen_id()
    tmpl = PROC_TEMPLATES[proc_type]
    props = tmpl['props'](idx)
    sched = random.choice(['TIMER_DRIVEN'] * 9 + ['CRON_DRIVEN'])
    period = random.choice(['0 sec', '5 sec', '10 sec', '30 sec', '1 min', '5 min'])
    if sched == 'CRON_DRIVEN':
        period = '0 */5 * * * ?'

    xml = f'    <processor>\n'
    xml += f'      <id>{pid}</id>\n'
    xml += f'      <name>{escape_xml(name)}</name>\n'
    xml += f'      <class>{tmpl["class"]}</class>\n'
    xml += f'      <schedulingStrategy>{sched}</schedulingStrategy>\n'
    xml += f'      <schedulingPeriod>{period}</schedulingPeriod>\n'
    xml += f'      <state>RUNNING</state>\n'
    for k, v in props.items():
        xml += f'      <property><name>{escape_xml(k)}</name><value>{escape_xml(str(v))}</value></property>\n'
    xml += f'    </processor>\n'
    return pid, xml

def make_connection_xml(src_id, dst_id, relationship='success'):
    cid = gen_id()
    xml = f'    <connection>\n'
    xml += f'      <id>{cid}</id>\n'
    xml += f'      <source><id>{src_id}</id><type>PROCESSOR</type></source>\n'
    xml += f'      <destination><id>{dst_id}</id><type>PROCESSOR</type></destination>\n'
    xml += f'      <relationship>{relationship}</relationship>\n'
    xml += f'    </connection>\n'
    return xml

def generate_flow():
    xml = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n'
    xml += '<flowController encoding-version="1.4">\n'
    xml += '  <rootGroup>\n'
    xml += '    <name>Enterprise_Data_Platform_1000</name>\n'

    all_group_last_ids = {}  # group_name -> last processor id (for cross-group connections)
    all_group_first_ids = {}  # group_name -> first processor id
    total_procs = 0

    for group in PROCESS_GROUPS:
        gname = group['name']
        pipeline = group['pipeline']
        target_count = group['count']
        xml += f'\n    <processGroup>\n      <name>{gname}</name>\n'

        proc_ids = []  # (id, type) pairs for connections
        proc_xmls = []
        conn_xmls = []

        # Generate processors — cycle through pipeline template
        for i in range(target_count):
            proc_type = pipeline[i % len(pipeline)]
            iteration = i // len(pipeline)
            suffix = f'_{iteration}' if iteration > 0 else ''
            name = f'{gname}_{proc_type}{suffix}_{i}'
            pid, pxml = make_processor_xml(proc_type, name, i + total_procs, gname)
            proc_ids.append((pid, proc_type))
            proc_xmls.append(pxml)

        # Generate connections within group — chain adjacent processors
        for i in range(len(proc_ids) - 1):
            src_id, src_type = proc_ids[i]
            dst_id, dst_type = proc_ids[i + 1]
            rel = 'success'
            if 'Route' in src_type:
                rel = random.choice(['success', 'valid', 'matched', 'high_priority'])
            elif 'Validate' in src_type:
                rel = random.choice(['valid', 'invalid'])
            conn_xmls.append(make_connection_xml(src_id, dst_id, rel))

        # Add some branch connections (Route processors connect to multiple destinations)
        for i, (pid, ptype) in enumerate(proc_ids):
            if 'Route' in ptype and i + 2 < len(proc_ids):
                conn_xmls.append(make_connection_xml(pid, proc_ids[i + 2][0], 'failure'))
            if 'Route' in ptype and i + 3 < len(proc_ids):
                conn_xmls.append(make_connection_xml(pid, proc_ids[i + 3][0], 'unmatched'))

        for pxml in proc_xmls:
            xml += pxml
        for cxml in conn_xmls:
            xml += cxml

        xml += '    </processGroup>\n'

        if proc_ids:
            all_group_first_ids[gname] = proc_ids[0][0]
            all_group_last_ids[gname] = proc_ids[-1][0]

        total_procs += len(proc_ids)

    # Cross-group connections
    cross_connections = [
        ('Oracle_Ingestion', 'Data_Quality'),
        ('SQLServer_Ingestion', 'Data_Quality'),
        ('Hadoop_Migration', 'Cloud_Sync'),
        ('Kafka_Streaming', 'Realtime_Analytics'),
        ('API_Integration', 'Data_Enrichment'),
        ('Data_Quality', 'Error_Handling'),
        ('CDC_Processing', 'Data_Enrichment'),
        ('File_Distribution', 'Monitoring_Audit'),
        ('Error_Handling', 'Monitoring_Audit'),
        ('XML_Processing', 'Data_Quality'),
        ('Data_Enrichment', 'Kafka_Streaming'),
        ('Cloud_Sync', 'Monitoring_Audit'),
        ('Realtime_Analytics', 'Error_Handling'),
    ]

    xml += '\n    <!-- Cross-group connections -->\n'
    for src_group, dst_group in cross_connections:
        if src_group in all_group_last_ids and dst_group in all_group_first_ids:
            xml += make_connection_xml(
                all_group_last_ids[src_group],
                all_group_first_ids[dst_group],
                'success'
            )

    # Controller services
    xml += '''
    <controllerService>
      <id>cs-dbcp-1</id>
      <name>DBCPService</name>
      <class>org.apache.nifi.dbcp.DBCPConnectionPool</class>
      <property><name>Database Connection URL</name><value>jdbc:oracle:thin:@oracle-prod:1521:ORCL</value></property>
      <property><name>Database Driver Class Name</name><value>oracle.jdbc.OracleDriver</value></property>
      <property><name>Database User</name><value>${db.user}</value></property>
      <property><name>Password</name><value>${db.password}</value></property>
      <property><name>Max Total Connections</name><value>20</value></property>
    </controllerService>
    <controllerService>
      <id>cs-dbcp-2</id>
      <name>SQLServerDBCP</name>
      <class>org.apache.nifi.dbcp.DBCPConnectionPool</class>
      <property><name>Database Connection URL</name><value>jdbc:sqlserver://sqlserver-prod:1433;databaseName=Analytics</value></property>
      <property><name>Database Driver Class Name</name><value>com.microsoft.sqlserver.jdbc.SQLServerDriver</value></property>
      <property><name>Database User</name><value>${sqlserver.user}</value></property>
      <property><name>Password</name><value>${sqlserver.password}</value></property>
    </controllerService>
    <controllerService>
      <id>cs-csv-reader</id>
      <name>CSVReader</name>
      <class>org.apache.nifi.csv.CSVReader</class>
      <property><name>Schema Access Strategy</name><value>Infer Schema</value></property>
      <property><name>Date Format</name><value>yyyy-MM-dd</value></property>
    </controllerService>
    <controllerService>
      <id>cs-json-writer</id>
      <name>JsonRecordSetWriter</name>
      <class>org.apache.nifi.json.JsonRecordSetWriter</class>
      <property><name>Schema Access Strategy</name><value>Inherit Record Schema</value></property>
      <property><name>Output Grouping</name><value>Array</value></property>
    </controllerService>
    <controllerService>
      <id>cs-json-reader</id>
      <name>JsonTreeReader</name>
      <class>org.apache.nifi.json.JsonTreeReader</class>
      <property><name>Schema Access Strategy</name><value>Infer Schema</value></property>
    </controllerService>
'''

    xml += '  </rootGroup>\n'
    xml += '</flowController>\n'

    return xml, total_procs

if __name__ == '__main__':
    xml, total = generate_flow()
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'large_flow_1000.xml')
    with open(out_path, 'w') as f:
        f.write(xml)
    print(f'Generated {total} processors -> {out_path}')
    print(f'File size: {len(xml):,} bytes')
