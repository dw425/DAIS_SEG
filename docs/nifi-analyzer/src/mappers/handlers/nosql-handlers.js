/**
 * mappers/handlers/nosql-handlers.js -- NoSQL processor smart code generation
 *
 * Extracted from index.html lines ~4484-4508, 5010-5024, 5062-5083.
 * Handles: MongoDB, Elasticsearch, Redis, Couchbase, Solr, RethinkDB processors.
 */

/**
 * Generate Databricks code for NoSQL-type NiFi processors.
 *
 * @param {object} p - Processor object
 * @param {object} props - Processor properties (variable-resolved)
 * @param {string} varName - Sanitized output variable name
 * @param {string} inputVar - Sanitized input variable name
 * @param {string} existingCode - Code from template resolution
 * @param {number} existingConf - Confidence from template resolution
 * @returns {{ code: string, conf: number }|null}
 */
export function handleNoSQLProcessor(p, props, varName, inputVar, existingCode, existingConf) {
  let code = existingCode;
  let conf = existingConf;

  // -- Elasticsearch --
  if (/^(Put|Fetch|Get|Query|Scroll|JsonQuery|Delete)Elasticsearch/.test(p.type)) {
    const esUrl = props['Elasticsearch URL'] || props['HTTP Hosts'] || 'https://es_host:9200';
    const index = props['Index'] || 'default_index';
    const isWrite = /^Put/.test(p.type);
    const isDelete = /^Delete/.test(p.type);

    if (isWrite) {
      code = `# Elasticsearch Write: ${p.name}\n# URL: ${esUrl} | Index: ${index}\nfrom elasticsearch import Elasticsearch, helpers\n_es = Elasticsearch("${esUrl}",\n    basic_auth=(dbutils.secrets.get(scope="es", key="user"), dbutils.secrets.get(scope="es", key="pass")),\n    verify_certs=False)\n\n_records = df_${inputVar}.limit(10000).toPandas().to_dict(orient="records")\n_actions = [{"_index": "${index}", "_source": r} for r in _records]\nhelpers.bulk(_es, _actions, chunk_size=500, request_timeout=60)\nprint(f"[ES] Indexed {len(_records)} documents to ${index}")`;
    } else if (isDelete) {
      code = `# Elasticsearch Delete: ${p.name}\nfrom elasticsearch import Elasticsearch\n_es = Elasticsearch("${esUrl}", basic_auth=(dbutils.secrets.get(scope="es", key="user"), dbutils.secrets.get(scope="es", key="pass")))\n_es.delete_by_query(index="${index}", body={"query": {"match_all": {}}})\ndf_${varName} = df_${inputVar}\nprint(f"[ES] Deleted from ${index}")`;
    } else {
      code = `# Elasticsearch Read: ${p.name}\n# URL: ${esUrl} | Index: ${index}\nfrom elasticsearch import Elasticsearch\n_es = Elasticsearch("${esUrl}",\n    basic_auth=(dbutils.secrets.get(scope="es", key="user"), dbutils.secrets.get(scope="es", key="pass")),\n    verify_certs=False)\n\n_result = _es.search(index="${index}", body={"query": {"match_all": {}}}, size=10000, scroll="2m")\n_hits = [h["_source"] for h in _result["hits"]["hits"]]\ndf_${varName} = spark.createDataFrame(_hits) if _hits else spark.createDataFrame([], "id STRING")\nprint(f"[ES] Read {len(_hits)} documents from ${index}")`;
    }
    conf = 0.90;
    return { code, conf };
  }

  // -- MongoDB --
  if (/^(Get|Put|Delete)Mongo/.test(p.type)) {
    const uri = props['Mongo URI'] ? `"${props['Mongo URI']}"` : 'dbutils.secrets.get(scope="nosql", key="mongo-uri")';
    const db = props['Mongo Database Name'] || 'mydb';
    const coll = props['Mongo Collection Name'] || 'mycollection';
    const isWrite = /^(Put|Delete)/.test(p.type);
    if (isWrite && p.type !== 'DeleteMongo') {
      code = `# MongoDB Write: ${p.name}\n# DB: ${db} | Collection: ${coll}\nfrom pymongo import MongoClient\n_mongo_uri = ${uri}\n_client = MongoClient(_mongo_uri)\n_db = _client["${db}"]\n_coll = _db["${coll}"]\n\n_records = df_${inputVar}.limit(10000).toPandas().to_dict(orient="records")\n_result = _coll.insert_many(_records)\nprint(f"[MONGO] Inserted {len(_result.inserted_ids)} documents into ${db}.${coll}")\n_client.close()`;
    } else if (p.type === 'DeleteMongo') {
      code = `# MongoDB Delete: ${p.name}\nfrom pymongo import MongoClient\n_mongo_uri = ${uri}\n_client = MongoClient(_mongo_uri)\n_coll = _client["${db}"]["${coll}"]\n_result = _coll.delete_many({})\nprint(f"[MONGO] Deleted {_result.deleted_count} documents from ${db}.${coll}")\n_client.close()\ndf_${varName} = df_${inputVar}`;
    } else {
      code = `# MongoDB Read: ${p.name}\n# DB: ${db} | Collection: ${coll}\nfrom pymongo import MongoClient\n_mongo_uri = ${uri}\n_client = MongoClient(_mongo_uri)\n_db = _client["${db}"]\n_coll = _db["${coll}"]\n\n_docs = list(_coll.find({}, {"_id": 0}).limit(50000))\ndf_${varName} = spark.createDataFrame(_docs) if _docs else spark.createDataFrame([], "id STRING")\nprint(f"[MONGO] Read {len(_docs)} documents from ${db}.${coll}")\n_client.close()`;
    }
    conf = 0.90;
    return { code, conf };
  }

  // -- GetMongoRecord --
  if (p.type === 'GetMongoRecord') {
    const uri = props['Mongo URI'] ? `"${props['Mongo URI']}"` : 'dbutils.secrets.get(scope="nosql", key="mongo-uri")';
    const db = props['Mongo Database Name'] || 'mydb';
    const coll = props['Mongo Collection Name'] || 'collection';
    code = `# Mongo Record: ${p.name}\nfrom pymongo import MongoClient\n_mongo_uri = ${uri}\n_client = MongoClient(_mongo_uri)\n_docs = list(_client["${db}"]["${coll}"].find({}, {"_id": 0}).limit(50000))\ndf_${varName} = spark.createDataFrame(_docs) if _docs else spark.createDataFrame([], "id STRING")\n_client.close()`;
    conf = 0.90;
    return { code, conf };
  }

  // -- RunMongoAggregation --
  if (p.type === 'RunMongoAggregation') {
    const uri = props['Mongo URI'] ? `"${props['Mongo URI']}"` : 'dbutils.secrets.get(scope="nosql", key="mongo-uri")';
    const db = props['Mongo Database Name'] || 'mydb';
    const coll = props['Mongo Collection Name'] || 'collection';
    code = `# Mongo Aggregation: ${p.name}\nfrom pymongo import MongoClient\n_mongo_uri = ${uri}\n_client = MongoClient(_mongo_uri)\n_results = list(_client["${db}"]["${coll}"].aggregate([]))\ndf_${varName} = spark.createDataFrame(_results) if _results else df_${inputVar}\n_client.close()`;
    conf = 0.90;
    return { code, conf };
  }

  // -- GridFS --
  if (/^(Fetch|Put|Delete)GridFS$/.test(p.type)) {
    const uri = props['Mongo URI'] ? `"${props['Mongo URI']}"` : 'dbutils.secrets.get(scope="nosql", key="mongo-uri")';
    const db = props['Mongo Database Name'] || 'files_db';
    code = `# GridFS ${p.type}: ${p.name}\nfrom pymongo import MongoClient\nimport gridfs\n_mongo_uri = ${uri}\n_client = MongoClient(_mongo_uri)\n_fs = gridfs.GridFS(_client["${db}"])\ndf_${varName} = df_${inputVar}\n_client.close()`;
    conf = 0.90;
    return { code, conf };
  }

  // -- Redis --
  if (/^(Put|Get|Fetch|Delete)Redis/.test(p.type)) {
    const host = props['Redis Connection Pool'] || props['Hostname'] || 'redis_host';
    const port = props['Port'] || '6379';
    const isWrite = /^(Put|Delete)/.test(p.type);
    if (isWrite) {
      code = `# Redis Write: ${p.name}\nimport redis, json\n_r = redis.Redis(host="${host}", port=${port},\n    password=dbutils.secrets.get(scope="redis", key="pass"))\nfor row in df_${inputVar}.limit(10000).collect():\n    _d = row.asDict()\n    _r.set(str(_d.get("id", "")), json.dumps(_d))\nprint(f"[REDIS] Wrote to Redis")\ndf_${varName} = df_${inputVar}`;
    } else {
      code = `# Redis Read: ${p.name}\nimport redis, json\n_r = redis.Redis(host="${host}", port=${port},\n    password=dbutils.secrets.get(scope="redis", key="pass"))\n_keys = _r.keys("*")\n_data = [json.loads(_r.get(k)) for k in _keys[:50000] if _r.get(k)]\ndf_${varName} = spark.createDataFrame(_data) if _data else spark.createDataFrame([], "id STRING")\nprint(f"[REDIS] Read {len(_data)} keys")`;
    }
    conf = 0.90;
    return { code, conf };
  }

  // -- Couchbase --
  if (/^(Get|Put)Couchbase/.test(p.type)) {
    const host = props['Cluster Hostname'] || 'couchbase_host';
    const bucket = props['Bucket Name'] || 'default';
    const isWrite = /^Put/.test(p.type);
    if (isWrite) {
      code = `# Couchbase Write: ${p.name}\nfrom couchbase.cluster import Cluster\nfrom couchbase.auth import PasswordAuthenticator\n_auth = PasswordAuthenticator(\n    dbutils.secrets.get(scope="couchbase", key="user"),\n    dbutils.secrets.get(scope="couchbase", key="pass"))\n_cluster = Cluster(f"couchbase://${host}", authenticator=_auth)\n_bucket = _cluster.bucket("${bucket}")\n_coll = _bucket.default_collection()\nfor row in df_${inputVar}.limit(10000).collect():\n    _d = row.asDict()\n    _coll.upsert(str(_d.get("id", "")), _d)\ndf_${varName} = df_${inputVar}\nprint(f"[COUCHBASE] Wrote to ${bucket}")`;
    } else {
      code = `# Couchbase Read: ${p.name}\nfrom couchbase.cluster import Cluster\nfrom couchbase.auth import PasswordAuthenticator\n_auth = PasswordAuthenticator(\n    dbutils.secrets.get(scope="couchbase", key="user"),\n    dbutils.secrets.get(scope="couchbase", key="pass"))\n_cluster = Cluster(f"couchbase://${host}", authenticator=_auth)\n_result = _cluster.query("SELECT * FROM \`${bucket}\` LIMIT 50000")\n_rows = [r for r in _result]\ndf_${varName} = spark.createDataFrame(_rows) if _rows else spark.createDataFrame([], "id STRING")\nprint(f"[COUCHBASE] Read from ${bucket}")`;
    }
    conf = 0.90;
    return { code, conf };
  }

  // -- Solr --
  if (/^(Put|Query|Get)Solr/.test(p.type)) {
    const url = props['Solr Location'] || props['Solr URL'] || 'http://solr:8983/solr';
    const collection = props['Collection'] || 'default_collection';
    const isWrite = /^Put/.test(p.type);
    if (isWrite) {
      code = `# Solr Write: ${p.name}\nimport requests, json\nfor row in df_${inputVar}.limit(10000).collect():\n    requests.post(f"${url}/${collection}/update/json/docs",\n        json=row.asDict(),\n        params={"commit": "true"})\ndf_${varName} = df_${inputVar}\nprint(f"[SOLR] Indexed to ${collection}")`;
    } else {
      code = `# Solr Read: ${p.name}\nimport requests\n_resp = requests.get(f"${url}/${collection}/select", params={"q": "*:*", "rows": 50000, "wt": "json"})\n_docs = _resp.json().get("response", {}).get("docs", [])\ndf_${varName} = spark.createDataFrame(_docs) if _docs else spark.createDataFrame([], "id STRING")\nprint(f"[SOLR] Read {len(_docs)} documents from ${collection}")`;
    }
    conf = 0.90;
    return { code, conf };
  }

  // -- RethinkDB --
  if (/^(Get|Put|Delete)RethinkDB$/.test(p.type)) {
    const host = props['Hostname'] || 'rethinkdb_host';
    const dbName = props['DB Name'] || '<database_name>';
    const tbl = props['Table Name'] || 'data';
    code = `# RethinkDB: ${p.name}\nfrom rethinkdb import r\n_conn = r.connect(host="${host}", port=28015, db="${dbName}")\n_docs = list(r.table("${tbl}").limit(50000).run(_conn))\ndf_${varName} = spark.createDataFrame(_docs) if _docs else df_${inputVar}\n_conn.close()`;
    conf = 0.90;
    return { code, conf };
  }

  return null; // Not handled
}
