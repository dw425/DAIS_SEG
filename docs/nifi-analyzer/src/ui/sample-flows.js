/**
 * ui/sample-flows.js — Embedded sample NiFi flow XML for one-click testing
 *
 * Extracted from index.html lines 595-875.
 * Contains the SAMPLE_FLOWS constant and loader functions.
 */

import { setUploadedContent } from './file-upload.js';
import { escapeHTML } from '../security/html-sanitizer.js';

/**
 * Embedded sample NiFi flows for demo/testing.
 * Extracted from index.html lines 595-843.
 */
export const SAMPLE_FLOWS = {
  etl: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<flowController encoding-version="1.4">
  <rootGroup><name>ETL_Demo_Pipeline</name>
    <processor><id>p1</id><name>Read Source CSV</name><class>org.apache.nifi.processors.standard.GetFile</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>5 min</schedulingPeriod><state>RUNNING</state>
      <property><name>Input Directory</name><value>/data/input/sales</value></property>
      <property><name>File Filter</name><value>[^\\\\.].*\\\\.csv</value></property>
      <autoTerminatedRelationship>failure</autoTerminatedRelationship>
    </processor>
    <processor><id>p2</id><name>Validate Schema</name><class>org.apache.nifi.processors.standard.ValidateRecord</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
      <property><name>Record Reader</name><value>CSVReader</value></property>
      <property><name>Record Writer</name><value>CSVWriter</value></property>
    </processor>
    <processor><id>p3</id><name>Route by Region</name><class>org.apache.nifi.processors.standard.RouteOnAttribute</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
      <property><name>Routing Strategy</name><value>Route to Property name</value></property>
      <property><name>us_east</name><value>\${region:equals("US-East")}</value></property>
      <property><name>us_west</name><value>\${region:equals("US-West")}</value></property>
      <property><name>europe</name><value>\${region:equals("EU")}</value></property>
    </processor>
    <processor><id>p4</id><name>Transform Sales Data</name><class>org.apache.nifi.processors.standard.ReplaceText</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
      <property><name>Search Value</name><value>"amount":"(\\d+)"</value></property>
      <property><name>Replacement Value</name><value>"amount_cents":"\${1}00"</value></property>
      <property><name>Replacement Strategy</name><value>Regex Replace</value></property>
    </processor>
    <processor><id>p5</id><name>Query Sales Summary</name><class>org.apache.nifi.processors.standard.ExecuteSQL</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
      <property><name>Database Connection Pooling Service</name><value>DBCPService</value></property>
      <property><name>SQL select query</name><value>SELECT region, product, SUM(amount) as total, COUNT(*) as cnt FROM sales.transactions WHERE trade_date >= CURRENT_DATE - 7 GROUP BY region, product</value></property>
    </processor>
    <processor><id>p6</id><name>Update Attributes</name><class>org.apache.nifi.processors.standard.UpdateAttribute</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
      <property><name>output.filename</name><value>\${filename:substringBefore('.')}_processed_\${now():format('yyyyMMdd')}.csv</value></property>
      <property><name>batch.id</name><value>\${UUID()}</value></property>
    </processor>
    <processor><id>p7</id><name>Write to Data Lake</name><class>org.apache.nifi.processors.standard.PutFile</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
      <property><name>Directory</name><value>/data/output/processed_sales</value></property>
      <property><name>Conflict Resolution Strategy</name><value>replace</value></property>
    </processor>
    <processor><id>p8</id><name>Insert to Warehouse</name><class>org.apache.nifi.processors.standard.PutDatabaseRecord</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
      <property><name>Database Connection Pooling Service</name><value>DBCPService</value></property>
      <property><name>Table Name</name><value>warehouse.sales_processed</value></property>
      <property><name>Statement Type</name><value>INSERT</value></property>
    </processor>
    <processor><id>p9</id><name>Log Completion</name><class>org.apache.nifi.processors.standard.LogMessage</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
      <property><name>Log Level</name><value>info</value></property>
      <property><name>Log Message</name><value>ETL batch complete: \${batch.id}</value></property>
    </processor>
    <connection><id>c1</id><source><id>p1</id><type>PROCESSOR</type></source><destination><id>p2</id><type>PROCESSOR</type></destination><relationship>success</relationship></connection>
    <connection><id>c2</id><source><id>p2</id><type>PROCESSOR</type></source><destination><id>p3</id><type>PROCESSOR</type></destination><relationship>valid</relationship></connection>
    <connection><id>c3</id><source><id>p3</id><type>PROCESSOR</type></source><destination><id>p4</id><type>PROCESSOR</type></destination><relationship>us_east</relationship></connection>
    <connection><id>c4</id><source><id>p3</id><type>PROCESSOR</type></source><destination><id>p4</id><type>PROCESSOR</type></destination><relationship>us_west</relationship></connection>
    <connection><id>c5</id><source><id>p3</id><type>PROCESSOR</type></source><destination><id>p4</id><type>PROCESSOR</type></destination><relationship>europe</relationship></connection>
    <connection><id>c6</id><source><id>p4</id><type>PROCESSOR</type></source><destination><id>p5</id><type>PROCESSOR</type></destination><relationship>success</relationship></connection>
    <connection><id>c7</id><source><id>p5</id><type>PROCESSOR</type></source><destination><id>p6</id><type>PROCESSOR</type></destination><relationship>success</relationship></connection>
    <connection><id>c8</id><source><id>p6</id><type>PROCESSOR</type></source><destination><id>p7</id><type>PROCESSOR</type></destination><relationship>success</relationship></connection>
    <connection><id>c9</id><source><id>p6</id><type>PROCESSOR</type></source><destination><id>p8</id><type>PROCESSOR</type></destination><relationship>success</relationship></connection>
    <connection><id>c10</id><source><id>p8</id><type>PROCESSOR</type></source><destination><id>p9</id><type>PROCESSOR</type></destination><relationship>success</relationship></connection>
  </rootGroup>
  <controllerServices>
    <controllerService><id>cs1</id><name>DBCPService</name><class>org.apache.nifi.dbcp.DBCPConnectionPool</class><property><name>Database Connection URL</name><value>jdbc:postgresql://db.example.com:5432/analytics</value></property><property><name>Database User</name><value>etl_user</value></property><property><name>Password</name><value>s3cur3_p4ss</value></property></controllerService>
  </controllerServices>
</flowController>`,

  streaming: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<flowController encoding-version="1.4">
  <rootGroup><name>Streaming_IoT_Pipeline</name>
    <processGroup><name>IoT Ingestion</name>
      <processor><id>s1</id><name>Consume Kafka Events</name><class>org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_2_6</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>100 ms</schedulingPeriod><state>RUNNING</state>
        <property><name>Kafka Brokers</name><value>kafka-broker-1:9092,kafka-broker-2:9092</value></property>
        <property><name>Topic Name(s)</name><value>iot.sensor.readings</value></property>
        <property><name>Group ID</name><value>nifi-iot-consumer</value></property>
      </processor>
      <processor><id>s2</id><name>Parse JSON Payload</name><class>org.apache.nifi.processors.standard.EvaluateJsonPath</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
        <property><name>Destination</name><value>flowfile-attribute</value></property>
        <property><name>sensor_id</name><value>$.sensor_id</value></property>
        <property><name>temperature</name><value>$.readings.temperature</value></property>
        <property><name>humidity</name><value>$.readings.humidity</value></property>
        <property><name>timestamp</name><value>$.event_time</value></property>
      </processor>
      <processor><id>s3</id><name>Route by Threshold</name><class>org.apache.nifi.processors.standard.RouteOnAttribute</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
        <property><name>Routing Strategy</name><value>Route to Property name</value></property>
        <property><name>alert</name><value>\${temperature:gt(100):or(\${humidity:gt(95)})}</value></property>
        <property><name>normal</name><value>\${temperature:le(100):and(\${humidity:le(95)})}</value></property>
      </processor>
      <connection><id>sc1</id><source><id>s1</id><type>PROCESSOR</type></source><destination><id>s2</id><type>PROCESSOR</type></destination><relationship>success</relationship></connection>
      <connection><id>sc2</id><source><id>s2</id><type>PROCESSOR</type></source><destination><id>s3</id><type>PROCESSOR</type></destination><relationship>matched</relationship></connection>
    </processGroup>
    <processGroup><name>Alert Processing</name>
      <processor><id>s4</id><name>Enrich Alert Data</name><class>org.apache.nifi.processors.standard.LookupAttribute</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
        <property><name>Lookup Service</name><value>DeviceRegistry</value></property>
        <property><name>device.name</name><value>\${sensor_id}</value></property>
      </processor>
      <processor><id>s5</id><name>Format Alert Notification</name><class>org.apache.nifi.processors.standard.ReplaceText</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
        <property><name>Replacement Value</name><value>{"alert":"THRESHOLD_EXCEEDED","sensor":"\${sensor_id}","temp":"\${temperature}","humidity":"\${humidity}","device":"\${device.name}","time":"\${timestamp}"}</value></property>
        <property><name>Replacement Strategy</name><value>Always Replace</value></property>
      </processor>
      <processor><id>s6</id><name>Send Alert to API</name><class>org.apache.nifi.processors.standard.InvokeHTTP</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
        <property><name>Remote URL</name><value>https://alerts.example.com/api/v2/notify</value></property>
        <property><name>HTTP Method</name><value>POST</value></property>
        <property><name>Content-Type</name><value>application/json</value></property>
      </processor>
      <connection><id>sc3</id><source><id>s4</id><type>PROCESSOR</type></source><destination><id>s5</id><type>PROCESSOR</type></destination><relationship>success</relationship></connection>
      <connection><id>sc4</id><source><id>s5</id><type>PROCESSOR</type></source><destination><id>s6</id><type>PROCESSOR</type></destination><relationship>success</relationship></connection>
    </processGroup>
    <processGroup><name>Data Storage</name>
      <processor><id>s7</id><name>Batch Readings</name><class>org.apache.nifi.processors.standard.MergeContent</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
        <property><name>Merge Strategy</name><value>Bin-Packing Algorithm</value></property>
        <property><name>Minimum Number of Entries</name><value>100</value></property>
        <property><name>Maximum Number of Entries</name><value>1000</value></property>
        <property><name>Max Bin Age</name><value>30 sec</value></property>
      </processor>
      <processor><id>s8</id><name>Convert to Parquet</name><class>org.apache.nifi.processors.standard.ConvertRecord</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
        <property><name>Record Reader</name><value>JsonTreeReader</value></property>
        <property><name>Record Writer</name><value>ParquetRecordSetWriter</value></property>
      </processor>
      <processor><id>s9</id><name>Write to Delta Lake</name><class>org.apache.nifi.processors.standard.PutHDFS</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
        <property><name>Directory</name><value>/data/iot/sensor_readings/\${now():format('yyyy/MM/dd')}</value></property>
        <property><name>Conflict Resolution Strategy</name><value>replace</value></property>
      </processor>
      <processor><id>s10</id><name>Insert to Timeseries DB</name><class>org.apache.nifi.processors.standard.PutDatabaseRecord</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
        <property><name>Database Connection Pooling Service</name><value>TimeseriesDBCP</value></property>
        <property><name>Table Name</name><value>iot.sensor_readings</value></property>
        <property><name>Statement Type</name><value>INSERT</value></property>
      </processor>
      <connection><id>sc5</id><source><id>s7</id><type>PROCESSOR</type></source><destination><id>s8</id><type>PROCESSOR</type></destination><relationship>merged</relationship></connection>
      <connection><id>sc6</id><source><id>s8</id><type>PROCESSOR</type></source><destination><id>s9</id><type>PROCESSOR</type></destination><relationship>success</relationship></connection>
      <connection><id>sc7</id><source><id>s8</id><type>PROCESSOR</type></source><destination><id>s10</id><type>PROCESSOR</type></destination><relationship>success</relationship></connection>
    </processGroup>
    <connection><id>sc_g1</id><source><id>s3</id><type>PROCESSOR</type></source><destination><id>s4</id><type>PROCESSOR</type></destination><relationship>alert</relationship></connection>
    <connection><id>sc_g2</id><source><id>s3</id><type>PROCESSOR</type></source><destination><id>s7</id><type>PROCESSOR</type></destination><relationship>normal</relationship></connection>
    <connection><id>sc_g3</id><source><id>s3</id><type>PROCESSOR</type></source><destination><id>s7</id><type>PROCESSOR</type></destination><relationship>alert</relationship></connection>
  </rootGroup>
</flowController>`,

  full: `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<flowController encoding-version="1.4">
  <rootGroup><name>Manufacturing_Data_Pipeline</name>
    <processGroup><name>Data Ingestion</name>
      <processor><id>f1</id><name>Scan Input Directory</name><class>org.apache.nifi.processors.standard.GetFile</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>1 min</schedulingPeriod><state>RUNNING</state>
        <property><name>Input Directory</name><value>/data/mfg/incoming</value></property>
        <property><name>File Filter</name><value>.*\\.(csv|json|xml)</value></property>
        <property><name>Keep Source File</name><value>false</value></property>
      </processor>
      <processor><id>f2</id><name>List SFTP Uploads</name><class>org.apache.nifi.processors.standard.ListFile</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>5 min</schedulingPeriod><state>RUNNING</state>
        <property><name>Input Directory</name><value>/sftp/uploads/mfg_data</value></property>
        <property><name>File Filter</name><value>production_.*\\.csv</value></property>
      </processor>
      <processor><id>f3</id><name>Fetch Upload Contents</name><class>org.apache.nifi.processors.standard.FetchFile</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
        <property><name>File to Fetch</name><value>\${absolute.path}/\${filename}</value></property>
      </processor>
      <processor><id>f4</id><name>Query Production Metrics</name><class>org.apache.nifi.processors.standard.ExecuteSQL</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>10 min</schedulingPeriod><state>RUNNING</state>
        <property><name>Database Connection Pooling Service</name><value>MfgDBCP</value></property>
        <property><name>SQL select query</name><value>SELECT lot_id, wafer_id, step_name, measurement, result, operator, meas_time FROM mfg_data.production_steps WHERE meas_time >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR ORDER BY meas_time</value></property>
      </processor>
      <connection><id>fc1</id><source><id>f2</id><type>PROCESSOR</type></source><destination><id>f3</id><type>PROCESSOR</type></destination><relationship>success</relationship></connection>
    </processGroup>
    <processGroup><name>Data Transformation</name>
      <processor><id>f5</id><name>Route by File Type</name><class>org.apache.nifi.processors.standard.RouteOnAttribute</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
        <property><name>Routing Strategy</name><value>Route to Property name</value></property>
        <property><name>csv_files</name><value>\${filename:endsWith('.csv')}</value></property>
        <property><name>json_files</name><value>\${filename:endsWith('.json')}</value></property>
        <property><name>xml_files</name><value>\${filename:endsWith('.xml')}</value></property>
      </processor>
      <processor><id>f6</id><name>Parse JSON Metrics</name><class>org.apache.nifi.processors.standard.EvaluateJsonPath</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
        <property><name>Destination</name><value>flowfile-attribute</value></property>
        <property><name>lot_id</name><value>$.lot_id</value></property>
        <property><name>status</name><value>$.quality_status</value></property>
        <property><name>yield_pct</name><value>$.yield_percentage</value></property>
      </processor>
      <processor><id>f7</id><name>Normalize Data Format</name><class>org.apache.nifi.processors.standard.ConvertRecord</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
        <property><name>Record Reader</name><value>InferAvroReader</value></property>
        <property><name>Record Writer</name><value>CSVRecordSetWriter</value></property>
      </processor>
      <processor><id>f8</id><name>Add Processing Metadata</name><class>org.apache.nifi.processors.standard.UpdateAttribute</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
        <property><name>processing.timestamp</name><value>\${now():format('yyyy-MM-dd HH:mm:ss')}</value></property>
        <property><name>source.system</name><value>nifi_mfg_pipeline</value></property>
        <property><name>batch.id</name><value>\${UUID()}</value></property>
        <property><name>output.filename</name><value>\${filename:substringBefore('.')}_enriched_\${now():format('yyyyMMdd_HHmmss')}.csv</value></property>
      </processor>
      <connection><id>fc2</id><source><id>f5</id><type>PROCESSOR</type></source><destination><id>f6</id><type>PROCESSOR</type></destination><relationship>json_files</relationship></connection>
      <connection><id>fc3</id><source><id>f5</id><type>PROCESSOR</type></source><destination><id>f7</id><type>PROCESSOR</type></destination><relationship>csv_files</relationship></connection>
      <connection><id>fc4</id><source><id>f6</id><type>PROCESSOR</type></source><destination><id>f8</id><type>PROCESSOR</type></destination><relationship>matched</relationship></connection>
      <connection><id>fc5</id><source><id>f7</id><type>PROCESSOR</type></source><destination><id>f8</id><type>PROCESSOR</type></destination><relationship>success</relationship></connection>
    </processGroup>
    <processGroup><name>Data Loading</name>
      <processor><id>f9</id><name>Write to Staging</name><class>org.apache.nifi.processors.standard.PutFile</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
        <property><name>Directory</name><value>/data/mfg/staging</value></property>
        <property><name>Conflict Resolution Strategy</name><value>replace</value></property>
      </processor>
      <processor><id>f10</id><name>Upload to HDFS</name><class>org.apache.nifi.processors.hadoop.PutHDFS</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
        <property><name>Directory</name><value>/data/warehouse/mfg_production</value></property>
        <property><name>Conflict Resolution Strategy</name><value>replace</value></property>
      </processor>
      <processor><id>f11</id><name>Insert Production Records</name><class>org.apache.nifi.processors.standard.PutDatabaseRecord</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
        <property><name>Database Connection Pooling Service</name><value>MfgDBCP</value></property>
        <property><name>Table Name</name><value>mfg_data.production_processed</value></property>
        <property><name>Statement Type</name><value>INSERT</value></property>
      </processor>
      <processor><id>f12</id><name>Transfer to Partner SFTP</name><class>org.apache.nifi.processors.standard.PutSFTP</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
        <property><name>Hostname</name><value>sftp.partner.example.com</value></property>
        <property><name>Port</name><value>22</value></property>
        <property><name>Username</name><value>mfg_data_xfer</value></property>
        <property><name>Password</name><value>xfer_s3cret!</value></property>
        <property><name>Remote Path</name><value>/incoming/mfg/\${now():format('yyyyMMdd')}</value></property>
      </processor>
      <connection><id>fc6</id><source><id>f9</id><type>PROCESSOR</type></source><destination><id>f10</id><type>PROCESSOR</type></destination><relationship>success</relationship></connection>
      <connection><id>fc7</id><source><id>f9</id><type>PROCESSOR</type></source><destination><id>f11</id><type>PROCESSOR</type></destination><relationship>success</relationship></connection>
      <connection><id>fc8</id><source><id>f10</id><type>PROCESSOR</type></source><destination><id>f12</id><type>PROCESSOR</type></destination><relationship>success</relationship></connection>
    </processGroup>
    <processGroup><name>Orchestration</name>
      <processor><id>f13</id><name>Signal Data Ready</name><class>org.apache.nifi.processors.standard.Notify</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
        <property><name>Signal Counter Name</name><value>mfg_data_ready</value></property>
        <property><name>Signal Counter Delta</name><value>1</value></property>
      </processor>
      <processor><id>f14</id><name>Wait for All Sources</name><class>org.apache.nifi.processors.standard.Wait</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>5 sec</schedulingPeriod><state>RUNNING</state>
        <property><name>Signal Counter Name</name><value>mfg_data_ready</value></property>
        <property><name>Target Signal Count</name><value>3</value></property>
      </processor>
      <processor><id>f15</id><name>Run Aggregation Script</name><class>org.apache.nifi.processors.standard.ExecuteStreamCommand</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
        <property><name>Command</name><value>/opt/scripts/aggregate_mfg.sh</value></property>
        <property><name>Command Arguments</name><value>/data/mfg/staging /data/mfg/aggregated</value></property>
      </processor>
      <processor><id>f16</id><name>Refresh Impala Tables</name><class>org.apache.nifi.processors.standard.ExecuteStreamCommand</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
        <property><name>Command</name><value>impala-shell</value></property>
        <property><name>Command Arguments</name><value>-q INVALIDATE METADATA mfg_data.production_processed; COMPUTE STATS mfg_data.production_processed;</value></property>
      </processor>
      <processor><id>f17</id><name>Log Pipeline Status</name><class>org.apache.nifi.processors.standard.LogMessage</class><schedulingStrategy>TIMER_DRIVEN</schedulingStrategy><schedulingPeriod>0 sec</schedulingPeriod><state>RUNNING</state>
        <property><name>Log Level</name><value>info</value></property>
        <property><name>Log Message</name><value>Manufacturing pipeline complete: batch=\${batch.id}, files=\${file.count}, timestamp=\${processing.timestamp}</value></property>
      </processor>
      <connection><id>fc9</id><source><id>f14</id><type>PROCESSOR</type></source><destination><id>f15</id><type>PROCESSOR</type></destination><relationship>success</relationship></connection>
      <connection><id>fc10</id><source><id>f15</id><type>PROCESSOR</type></source><destination><id>f16</id><type>PROCESSOR</type></destination><relationship>success</relationship></connection>
      <connection><id>fc11</id><source><id>f16</id><type>PROCESSOR</type></source><destination><id>f17</id><type>PROCESSOR</type></destination><relationship>success</relationship></connection>
    </processGroup>
    <connection><id>fc_g1</id><source><id>f1</id><type>PROCESSOR</type></source><destination><id>f5</id><type>PROCESSOR</type></destination><relationship>success</relationship></connection>
    <connection><id>fc_g2</id><source><id>f3</id><type>PROCESSOR</type></source><destination><id>f5</id><type>PROCESSOR</type></destination><relationship>success</relationship></connection>
    <connection><id>fc_g3</id><source><id>f4</id><type>PROCESSOR</type></source><destination><id>f8</id><type>PROCESSOR</type></destination><relationship>success</relationship></connection>
    <connection><id>fc_g4</id><source><id>f8</id><type>PROCESSOR</type></source><destination><id>f9</id><type>PROCESSOR</type></destination><relationship>success</relationship></connection>
    <connection><id>fc_g5</id><source><id>f11</id><type>PROCESSOR</type></source><destination><id>f13</id><type>PROCESSOR</type></destination><relationship>success</relationship></connection>
    <connection><id>fc_g6</id><source><id>f12</id><type>PROCESSOR</type></source><destination><id>f13</id><type>PROCESSOR</type></destination><relationship>success</relationship></connection>
  </rootGroup>
  <controllerServices>
    <controllerService><id>cs_mfg</id><name>MfgDBCP</name><class>org.apache.nifi.dbcp.DBCPConnectionPool</class>
      <property><name>Database Connection URL</name><value>jdbc:oracle:thin:@mfg-db.example.com:1521/MFGPRD</value></property>
      <property><name>Database User</name><value>mfg_reader</value></property>
      <property><name>Password</name><value>mfg_r34d3r!</value></property>
      <property><name>Database Driver Class Name</name><value>oracle.jdbc.driver.OracleDriver</value></property>
    </controllerService>
  </controllerServices>
</flowController>`
};

/**
 * Load an embedded sample flow by type key and trigger parsing.
 * Extracted from index.html lines 845-855.
 *
 * @param {string}   flowType   — key in SAMPLE_FLOWS ('etl', 'streaming', 'full')
 * @param {Function} parseFn    — the parseInput function to call after loading
 */
export async function loadSampleFlow(flowType, parseFn) {
  const xml = SAMPLE_FLOWS[flowType];
  if (!xml) return;
  const labels = {
    etl: 'ETL Pipeline (9 processors)',
    streaming: 'Streaming IoT (10 processors)',
    full: 'Manufacturing Migration (17 processors)'
  };
  setUploadedContent(xml, `sample_${flowType}_flow.xml`);
  const fileNameEl = document.getElementById('fileName');
  if (fileNameEl) {
    fileNameEl.textContent = 'Sample: ' + (labels[flowType] || flowType);
    fileNameEl.classList.remove('hidden');
  }
  const pasteInput = document.getElementById('pasteInput');
  if (pasteInput) pasteInput.value = '';
  if (typeof parseFn === 'function') await parseFn();
}

/**
 * Load a sample flow file from a URL path and trigger parsing.
 * Extracted from index.html lines 857-875.
 *
 * @param {string}   path     — URL path to fetch
 * @param {string}   filename — display name for the file
 * @param {Function} parseFn  — the parseInput function to call after loading
 */
export async function loadSampleFile(path, filename, parseFn) {
  const el = document.getElementById('fileName');
  if (el) {
    el.textContent = 'Loading: ' + filename + '...';
    el.classList.remove('hidden');
  }
  try {
    const resp = await fetch(path);
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const text = await resp.text();
    setUploadedContent(text, filename);
    if (el) el.textContent = 'Sample: ' + filename;
    const pasteInput = document.getElementById('pasteInput');
    if (pasteInput) pasteInput.value = '';
    if (typeof parseFn === 'function') await parseFn();
  } catch (e) {
    if (el) {
      el.textContent = 'Failed to load ' + filename + ' — ' + e.message;
      el.style.color = 'var(--red)';
      setTimeout(() => { el.style.color = ''; }, 3000);
    }
  }
}
