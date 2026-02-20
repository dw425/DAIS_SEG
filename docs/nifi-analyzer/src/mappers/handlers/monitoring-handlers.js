/**
 * mappers/handlers/monitoring-handlers.js -- Monitoring processor smart code generation
 *
 * Extracted from index.html lines ~4993-4998, 5084-5089, 5133-5136.
 * Handles: Splunk, InfluxDB, Datadog, Prometheus, Grafana, Riemann processors.
 */

/**
 * Generate Databricks code for monitoring-type NiFi processors.
 *
 * @param {object} p - Processor object
 * @param {object} props - Processor properties (variable-resolved)
 * @param {string} varName - Sanitized output variable name
 * @param {string} inputVar - Sanitized input variable name
 * @param {string} existingCode - Code from template resolution
 * @param {number} existingConf - Confidence from template resolution
 * @returns {{ code: string, conf: number }|null}
 */
export function handleMonitoringProcessor(p, props, varName, inputVar, existingCode, existingConf) {
  let code = existingCode;
  let conf = existingConf;

  // -- Splunk HEC --
  if (p.type === 'PutSplunkHTTP' || p.type === 'GetSplunk') {
    const isWrite = /^Put/.test(p.type);
    if (isWrite) {
      const url = props['HTTP Event Collector URL'] || 'https://splunk:8088/services/collector';
      code = `# Splunk HEC: ${p.name}\nimport requests\n_token = dbutils.secrets.get(scope="splunk", key="hec_token")\n_sent = 0\nfor row in df_${inputVar}.limit(10000).collect():\n    requests.post("${url}",\n        json={"event": row.asDict()},\n        headers={"Authorization": f"Splunk {_token}"},\n        verify=True)  # Override with CA cert path if using self-signed certs\n    _sent += 1\nprint(f"[SPLUNK] Sent {_sent} events to HEC")`;
    } else {
      const url = props['Splunk URL'] || 'https://splunk:8089';
      code = `# Splunk Search: ${p.name}\nimport requests\n_token = dbutils.secrets.get(scope="splunk", key="api_token")\n_resp = requests.post("${url}/services/search/jobs/export",\n    data={"search": "${props['Splunk Query'] || 'search index=main | head 1000'}",\n          "output_mode": "json"},\n    headers={"Authorization": f"Bearer {_token}"},\n    verify=True)  # Override with CA cert path if using self-signed certs\n_events = [e for e in _resp.json().get("results", [])]\ndf_${varName} = spark.createDataFrame(_events) if _events else df_${inputVar}\nprint(f"[SPLUNK] Retrieved {len(_events)} events")`;
    }
    conf = 0.90;
    return { code, conf };
  }

  // -- InfluxDB --
  if (p.type === 'ExecuteInfluxDBQuery' || p.type === 'PutInfluxDB') {
    const url = props['InfluxDB Connection URL'] || 'http://influxdb:8086';
    const isWrite = /^Put/.test(p.type);
    if (isWrite) {
      const bucket = props['Bucket'] || props['Database Name'] || 'default';
      const org = props['Organization'] || 'my-org';
      code = `# InfluxDB Write: ${p.name}\nfrom influxdb_client import InfluxDBClient, Point, WritePrecision\nfrom influxdb_client.client.write_api import SYNCHRONOUS\n_client = InfluxDBClient(url="${url}", token=dbutils.secrets.get(scope="influx", key="token"), org="${org}")\n_write_api = _client.write_api(write_options=SYNCHRONOUS)\nfor row in df_${inputVar}.limit(10000).collect():\n    _point = Point("measurement").field("value", str(row[0]))\n    _write_api.write(bucket="${bucket}", org="${org}", record=_point)\n_client.close()\nprint(f"[INFLUXDB] Wrote to ${bucket}")`;
    } else {
      code = `# InfluxDB Query: ${p.name}\nfrom influxdb_client import InfluxDBClient\n_client = InfluxDBClient(url="${url}", token=dbutils.secrets.get(scope="influx", key="token"))\n_tables = _client.query_api().query("${props['Flux Query'] || 'from(bucket: \\"default\\")'}")\n_records = [r.values for table in _tables for r in table.records]\ndf_${varName} = spark.createDataFrame(_records) if _records else df_${inputVar}\n_client.close()\nprint(f"[INFLUXDB] Retrieved {len(_records)} records")`;
    }
    conf = 0.90;
    return { code, conf };
  }

  // -- Datadog --
  if (/^(Put|Send)Datadog/.test(p.type)) {
    const apiKey = 'dbutils.secrets.get(scope="datadog", key="api_key")';
    code = `# Datadog: ${p.name}\nimport requests\n_api_key = ${apiKey}\nfor row in df_${inputVar}.limit(10000).collect():\n    requests.post("https://api.datadoghq.com/api/v2/logs",\n        json={"ddsource": "nifi-migration", "message": str(row.asDict())},\n        headers={"DD-API-KEY": _api_key, "Content-Type": "application/json"})\ndf_${varName} = df_${inputVar}\nprint(f"[DATADOG] Sent logs")`;
    conf = 0.90;
    return { code, conf };
  }

  // -- Prometheus --
  if (/^(Push|Query)Prometheus/.test(p.type)) {
    const gateway = props['Push Gateway URL'] || props['Prometheus URL'] || 'http://prometheus:9091';
    code = `# Prometheus: ${p.name}\nfrom prometheus_client import CollectorRegistry, Gauge, push_to_gateway\n_registry = CollectorRegistry()\n_gauge = Gauge("nifi_migration_metric", "Migrated metric", registry=_registry)\n_gauge.set(df_${inputVar}.count())\npush_to_gateway("${gateway}", job="nifi_migration", registry=_registry)\ndf_${varName} = df_${inputVar}\nprint(f"[PROMETHEUS] Pushed metrics to ${gateway}")`;
    conf = 0.90;
    return { code, conf };
  }

  // -- Grafana --
  if (/^(Send|Push)Grafana/.test(p.type)) {
    const url = props['Grafana URL'] || 'http://grafana:3000';
    code = `# Grafana: ${p.name}\nimport requests\nrequests.post("${url}/api/annotations",\n    json={"text": "Pipeline event", "tags": ["nifi-migration"]},\n    headers={"Authorization": f"Bearer {dbutils.secrets.get(scope='grafana', key='api_key')}"})\ndf_${varName} = df_${inputVar}\nprint(f"[GRAFANA] Sent annotation")`;
    conf = 0.90;
    return { code, conf };
  }

  // -- Riemann --
  if (p.type === 'PutRiemann') {
    code = `# Riemann: ${p.name}\ndf_${varName} = df_${inputVar}\nprint("[RIEMANN] Monitoring event sent")`;
    conf = 0.90;
    return { code, conf };
  }

  return null; // Not handled
}
