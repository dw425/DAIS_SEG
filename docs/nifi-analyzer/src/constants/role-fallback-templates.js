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
    tpl: '# Route processor: {type}\n# Conditional DataFrame routing via filter\nfrom pyspark.sql.functions import col, lit\n\n# Parse routing conditions from processor properties\n_route_conditions = {routeConditions}\nif _route_conditions:\n    for _route_name, _route_expr in _route_conditions.items():\n        try:\n            globals()[f"df_{v}_{_route_name}"] = df_{in}.filter(_route_expr)\n        except Exception as _route_err:\n            raise NotImplementedError(\n                f"Route condition for {type}/{_route_name} requires manual translation: "\n                f"{_route_expr} (error: {_route_err})"\n            )\n    # Default: rows not matching any named route\n    _matched_union = None\n    for _rn in _route_conditions:\n        _rdf = globals().get(f"df_{v}_{_rn}")\n        if _rdf is not None:\n            _matched_union = _rdf if _matched_union is None else _matched_union.union(_rdf)\n    df_{v}_unmatched = df_{in}.subtract(_matched_union) if _matched_union is not None else df_{in}\n    df_{v} = df_{in}  # Pass all through; downstream uses named route DataFrames\nelse:\n    raise NotImplementedError(\n        f"Route processor {type} has no translatable routing conditions. "\n        f"Review NiFi processor properties and add manual PySpark filter expressions."\n    )\nprint(f"[ROUTE] {v}: routing applied with {{len(_route_conditions)}} condition(s)")',
    desc: 'Route - DataFrame filter routing with parsed conditions',
    conf: 0.65
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
