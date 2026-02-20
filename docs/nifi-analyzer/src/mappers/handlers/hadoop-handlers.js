/**
 * mappers/handlers/hadoop-handlers.js -- Hadoop ecosystem processor smart code generation
 *
 * Extracted from index.html lines ~4526-4540, 5030-5042.
 * Handles: HDFS, ORC, Parquet, SequenceFile, Flume, HBase read/write,
 * GetHDFSEvents, GetHDFSFileInfo, GetHDFSSequenceFile.
 */

/**
 * Generate Databricks code for Hadoop ecosystem NiFi processors.
 *
 * @param {object} p - Processor object
 * @param {object} props - Processor properties (variable-resolved)
 * @param {string} varName - Sanitized output variable name
 * @param {string} inputVar - Sanitized input variable name
 * @param {string} existingCode - Code from template resolution
 * @param {number} existingConf - Confidence from template resolution
 * @returns {{ code: string, conf: number }|null}
 */
export function handleHadoopProcessor(p, props, varName, inputVar, existingCode, existingConf) {
  let code = existingCode;
  let conf = existingConf;

  // -- HDFS operations --
  if (/^(Get|Put|Fetch|List|Move|Delete)HDFS$/.test(p.type)) {
    const dir = props['Directory'] || '/data';
    const isWrite = /^(Put|Move|Delete)/.test(p.type);
    if (isWrite && p.type === 'PutHDFS') {
      code = `# HDFS Write: ${p.name}\n# Directory: ${dir}\n(df_${inputVar}.write\n  .format("delta")\n  .mode("append")\n  .save("${dir}")\n)\nprint(f"[HDFS] Wrote to ${dir}")`;
    } else if (p.type === 'MoveHDFS') {
      code = `# HDFS Move: ${p.name}\ndbutils.fs.mv("${dir}/source", "${dir}/dest", recurse=True)\nprint(f"[HDFS] Moved files")`;
    } else if (p.type === 'DeleteHDFS') {
      code = `# HDFS Delete: ${p.name}\ndbutils.fs.rm("${dir}", recurse=True)\nprint(f"[HDFS] Deleted ${dir}")`;
    } else {
      code = `# HDFS Read: ${p.name}\n# Directory: ${dir}\ndf_${varName} = spark.read.format("delta").load("${dir}")\nprint(f"[HDFS] Read from ${dir}")`;
    }
    conf = 0.93;
    return { code, conf };
  }

  // -- HDFS Events --
  if (p.type === 'GetHDFSEvents') {
    code = `# HDFS Events: ${p.name}\ndf_${varName} = (spark.readStream\n  .format("cloudFiles")\n  .option("cloudFiles.format", "json")\n  .load("${props['HDFS Path'] || '/data'}"))\nprint(f"[HDFS] Streaming events via Auto Loader")`;
    conf = 0.92;
    return { code, conf };
  }

  // -- HDFS File Info --
  if (p.type === 'GetHDFSFileInfo') {
    code = `# HDFS Info: ${p.name}\n_files = dbutils.fs.ls("${props['Directory'] || '/data'}")\ndf_${varName} = spark.createDataFrame([{"path": f.path, "name": f.name, "size": f.size} for f in _files])`;
    conf = 0.93;
    return { code, conf };
  }

  // -- SequenceFile --
  if (p.type === 'GetHDFSSequenceFile') {
    code = `# SequenceFile: ${p.name}\ndf_${varName} = spark.sparkContext.sequenceFile("${props['Directory'] || '/data'}", "org.apache.hadoop.io.Text", "org.apache.hadoop.io.Text").toDF(["key", "value"])`;
    conf = 0.90;
    return { code, conf };
  }

  // -- ORC --
  if (/^(Put|Get|Fetch)ORC$/.test(p.type) || p.type === 'ConvertAvroToORC') {
    const path = props['Directory'] || props['Path'] || '/Volumes/<catalog>/<schema>/data';
    const isWrite = /^Put/.test(p.type);
    if (isWrite) {
      code = `# ORC Write: ${p.name}\n(df_${inputVar}.write\n  .format("orc")\n  .mode("append")\n  .save("${path}")\n)\nprint(f"[ORC] Wrote to ${path}")`;
    } else {
      code = `# ORC Read: ${p.name}\ndf_${varName} = spark.read.format("orc").load("${path}")\nprint(f"[ORC] Read from ${path}")`;
    }
    conf = 0.93;
    return { code, conf };
  }

  // -- Parquet --
  if (/^(Put|Get|Fetch)Parquet$/.test(p.type)) {
    const path = props['Directory'] || props['Path'] || '/Volumes/<catalog>/<schema>/data';
    const isWrite = /^Put/.test(p.type);
    if (isWrite) {
      code = `# Parquet Write: ${p.name}\n(df_${inputVar}.write\n  .format("parquet")\n  .mode("append")\n  .save("${path}")\n)\nprint(f"[PARQUET] Wrote to ${path}")`;
    } else {
      code = `# Parquet Read: ${p.name}\ndf_${varName} = spark.read.format("parquet").load("${path}")\nprint(f"[PARQUET] Read from ${path}")`;
    }
    conf = 0.95;
    return { code, conf };
  }

  // -- Flume --
  if (/^(Put|Get|Listen)Flume/.test(p.type)) {
    const host = props['Hostname'] || 'flume_host';
    const port = props['Port'] || '41414';
    code = `# Flume: ${p.name}\n# Flume is deprecated — use Spark Structured Streaming directly\n# Host: ${host}:${port}\ndf_${varName} = (spark.readStream\n  .format("socket")\n  .option("host", "${host}")\n  .option("port", "${port}")\n  .load()\n)\nprint(f"[FLUME] Streaming from ${host}:${port} — consider migrating to native Spark source")`;
    conf = 0.85;
    return { code, conf };
  }

  // -- Hudi --
  if (/^Put(Hudi|HoodieRecord)$/.test(p.type)) {
    const table = props['Table Name'] || 'hudi_table';
    const path = props['Base Path'] || '/Volumes/<catalog>/<schema>/hudi';
    const recordKey = props['Record Key Field'] || 'id';
    const precombineKey = props['Precombine Key Field'] || 'timestamp';
    code = `# Hudi Write: ${p.name}\n(df_${inputVar}.write\n  .format("hudi")\n  .option("hoodie.table.name", "${table}")\n  .option("hoodie.datasource.write.recordkey.field", "${recordKey}")\n  .option("hoodie.datasource.write.precombine.field", "${precombineKey}")\n  .option("hoodie.datasource.write.operation", "upsert")\n  .mode("append")\n  .save("${path}/${table}")\n)\nprint(f"[HUDI] Wrote to ${table}")`;
    conf = 0.92;
    return { code, conf };
  }

  // -- Iceberg --
  if (/^PutIceberg$/.test(p.type)) {
    const table = props['Table Name'] || 'iceberg_table';
    code = `# Iceberg Write: ${p.name}\n(df_${inputVar}.writeTo("${table}")\n  .using("iceberg")\n  .append()\n)\nprint(f"[ICEBERG] Wrote to ${table}")`;
    conf = 0.92;
    return { code, conf };
  }

  return null; // Not handled
}
