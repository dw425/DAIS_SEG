// Role-based fallback templates — used when no specific processor mapping exists
// Generates real PySpark code for each role type
export const ROLE_FALLBACK_TEMPLATES = {
  source: {
    tpl: '# Source processor: {type}\n# Auto Loader ingestion from landing zone\ndf_{v} = (spark.readStream\n  .format("cloudFiles")\n  .option("cloudFiles.format", "json")\n  .option("cloudFiles.schemaLocation", "/Volumes/{catalog}/{schema}/_schema/{v}")\n  .option("cloudFiles.inferColumnTypes", "true")\n  .load("/Volumes/{catalog}/{schema}/landing/{v}"))\nprint(f"[SOURCE] {v}: streaming from landing zone")',
    desc: 'Source - Auto Loader ingestion',
    conf: 0.70
  },
  sink: {
    tpl: '# Sink processor: {type}\n# Delta Lake write with merge semantics\n(df_{in}.write\n  .format("delta")\n  .mode("append")\n  .option("mergeSchema", "true")\n  .saveAsTable("{catalog}.{schema}.{v}"))\nprint(f"[SINK] {v}: written to Delta table")',
    desc: 'Sink - Delta Lake write',
    conf: 0.70
  },
  transform: {
    tpl: '# Transform processor: {type}\n# DataFrame transformation pipeline\nfrom pyspark.sql.functions import col, lit, current_timestamp\ndf_{v} = (df_{in}\n  .withColumn("_processed_at", current_timestamp())\n  .withColumn("_source_processor", lit("{type}")))\nprint(f"[TRANSFORM] {v}: applied transformation")',
    desc: 'Transform - DataFrame transformation',
    conf: 0.70
  },
  route: {
    tpl: '# Route processor: {type}\n# Conditional DataFrame routing via filter\nfrom pyspark.sql.functions import col\n_condition = "1=1"  # TODO: translate NiFi routing rules\ndf_{v}_matched = df_{in}.filter(_condition)\ndf_{v}_unmatched = df_{in}.filter(f"NOT ({_condition})")\ndf_{v} = df_{v}_matched\nprint(f"[ROUTE] {v}: matched={{df_{v}_matched.count()}}, unmatched={{df_{v}_unmatched.count()}}")',
    desc: 'Route - DataFrame filter routing',
    conf: 0.70
  },
  process: {
    tpl: '# Process processor: {type}\n# Custom processing step\nfrom pyspark.sql.functions import col, expr, current_timestamp\ndf_{in}.createOrReplaceTempView("tmp_{v}")\ndf_{v} = spark.sql("SELECT *, current_timestamp() AS _processed_at FROM tmp_{v}")\nprint(f"[PROCESS] {v}: processed {{df_{v}.count()}} rows")',
    desc: 'Process - Custom processing via Spark SQL',
    conf: 0.70
  },
  utility: {
    tpl: '# Utility processor: {type}\n# Logging and inspection\nprint(f"[UTILITY] {v}: {{df_{in}.count()}} rows")\ndf_{in}.printSchema()\ndisplay(df_{in}.limit(5))\ndf_{v} = df_{in}',
    desc: 'Utility - logging and inspection',
    conf: 0.80
  }
};
