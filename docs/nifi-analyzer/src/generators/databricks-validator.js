/**
 * generators/databricks-validator.js -- Validates generated notebook code
 * for Databricks/PySpark compatibility.
 *
 * @module generators/databricks-validator
 */

// Valid PySpark DataFrame methods
const VALID_DF_METHODS = new Set([
  'select', 'filter', 'where', 'groupBy', 'agg', 'orderBy', 'sort',
  'join', 'union', 'unionAll', 'unionByName', 'crossJoin',
  'drop', 'dropDuplicates', 'distinct', 'limit', 'sample',
  'withColumn', 'withColumnRenamed', 'alias',
  'collect', 'count', 'show', 'display', 'head', 'first', 'take',
  'toPandas', 'toDF', 'cache', 'persist', 'unpersist', 'checkpoint',
  'write', 'writeStream', 'read', 'readStream',
  'createOrReplaceTempView', 'createTempView', 'createGlobalTempView',
  'repartition', 'coalesce', 'rdd', 'schema', 'columns', 'dtypes',
  'describe', 'summary', 'printSchema', 'explain',
  'na', 'stat', 'cube', 'rollup', 'pivot',
  'foreach', 'foreachPartition', 'mapInPandas', 'applyInPandas',
  'transform', 'observe', 'hint', 'exceptAll', 'intersectAll',
  'subtract', 'isEmpty', 'isLocal', 'isStreaming',
  'option', 'options', 'format', 'mode', 'partitionBy', 'bucketBy',
  'save', 'saveAsTable', 'insertInto', 'load', 'table', 'csv', 'json',
  'parquet', 'orc', 'text', 'jdbc',
]);

// Common PySpark imports
const KNOWN_IMPORTS = {
  'col': 'pyspark.sql.functions', 'lit': 'pyspark.sql.functions',
  'when': 'pyspark.sql.functions', 'expr': 'pyspark.sql.functions',
  'struct': 'pyspark.sql.functions', 'array': 'pyspark.sql.functions',
  'explode': 'pyspark.sql.functions', 'split': 'pyspark.sql.functions',
  'concat': 'pyspark.sql.functions', 'coalesce': 'pyspark.sql.functions',
  'to_json': 'pyspark.sql.functions', 'from_json': 'pyspark.sql.functions',
  'to_date': 'pyspark.sql.functions', 'to_timestamp': 'pyspark.sql.functions',
  'regexp_replace': 'pyspark.sql.functions', 'regexp_extract': 'pyspark.sql.functions',
  'upper': 'pyspark.sql.functions', 'lower': 'pyspark.sql.functions',
  'trim': 'pyspark.sql.functions', 'length': 'pyspark.sql.functions',
  'substring': 'pyspark.sql.functions', 'concat_ws': 'pyspark.sql.functions',
  'count': 'pyspark.sql.functions', 'sum': 'pyspark.sql.functions',
  'avg': 'pyspark.sql.functions', 'max': 'pyspark.sql.functions',
  'min': 'pyspark.sql.functions', 'collect_list': 'pyspark.sql.functions',
  'collect_set': 'pyspark.sql.functions', 'first': 'pyspark.sql.functions',
  'last': 'pyspark.sql.functions', 'window': 'pyspark.sql.functions',
  'row_number': 'pyspark.sql.functions', 'rank': 'pyspark.sql.functions',
  'dense_rank': 'pyspark.sql.functions', 'lead': 'pyspark.sql.functions',
  'lag': 'pyspark.sql.functions', 'ntile': 'pyspark.sql.functions',
  'aes_encrypt': 'pyspark.sql.functions', 'aes_decrypt': 'pyspark.sql.functions',
  'sha2': 'pyspark.sql.functions', 'md5': 'pyspark.sql.functions',
  'base64': 'pyspark.sql.functions', 'unbase64': 'pyspark.sql.functions',
  'current_timestamp': 'pyspark.sql.functions', 'current_date': 'pyspark.sql.functions',
  'date_format': 'pyspark.sql.functions', 'datediff': 'pyspark.sql.functions',
  'months_between': 'pyspark.sql.functions', 'add_months': 'pyspark.sql.functions',
  'input_file_name': 'pyspark.sql.functions',
  'StructType': 'pyspark.sql.types', 'StructField': 'pyspark.sql.types',
  'StringType': 'pyspark.sql.types', 'IntegerType': 'pyspark.sql.types',
  'LongType': 'pyspark.sql.types', 'DoubleType': 'pyspark.sql.types',
  'FloatType': 'pyspark.sql.types', 'BooleanType': 'pyspark.sql.types',
  'TimestampType': 'pyspark.sql.types', 'DateType': 'pyspark.sql.types',
  'ArrayType': 'pyspark.sql.types', 'MapType': 'pyspark.sql.types',
  'BinaryType': 'pyspark.sql.types', 'DecimalType': 'pyspark.sql.types',
  'Window': 'pyspark.sql.window',
  'pandas_udf': 'pyspark.sql.functions', 'PandasUDFType': 'pyspark.sql.functions',
  'udf': 'pyspark.sql.functions',
  'requests': 'requests', 'boto3': 'boto3', 'paramiko': 'paramiko',
  'MongoClient': 'pymongo', 'Elasticsearch': 'elasticsearch',
  'redis': 'redis', 'json': 'json', 'os': 'os', 're': 're',
  'datetime': 'datetime', 'timedelta': 'datetime',
  'pandas': 'pandas', 'numpy': 'numpy',
  'InfluxDBClient': 'influxdb_client',
  'Fernet': 'cryptography.fernet',
  'hashlib': 'hashlib', 'hmac': 'hmac',
};

/**
 * Validate all generated notebook cells.
 * @param {Array} cells - notebook cells [{source, type, label}]
 * @returns {{ issues: Array<{cell: number, severity: string, code: string, message: string, fix: string}>, score: number }}
 */
export function validateNotebookCode(cells) {
  const issues = [];

  cells.forEach((cell, idx) => {
    if (cell.type !== 'code') return;
    const src = cell.source || '';

    // 1. Check for TODO/FIXME/PLACEHOLDER
    const todoMatches = src.match(/# *(TODO|FIXME|PLACEHOLDER|HACK)[: ].*/gi);
    if (todoMatches) {
      todoMatches.forEach(m => {
        issues.push({
          cell: idx, severity: 'high', code: 'GEN_TODO_STUB',
          message: `Stub code found: ${m.trim().substring(0, 80)}`,
          fix: 'Replace with real implementation or explicit NotImplementedError'
        });
      });
    }

    // 2. Check for unresolved placeholders {var} (excluding f-string expressions)
    const placeholders = src.match(/\{[a-z_]+\}/gi);
    if (placeholders) {
      const unique = [...new Set(placeholders)];
      // Filter out f-string interpolations by checking if they appear inside f-strings
      const real = unique.filter(p => {
        // Check if every occurrence of this placeholder is inside an f-string
        const escapedP = p.replace(/[{}]/g, '\\$&');
        const inFStringRe = new RegExp(`[f]['\"].*${escapedP}.*['\"]`, 'g');
        const totalOccurrences = (src.match(new RegExp(escapedP, 'g')) || []).length;
        const fstringOccurrences = (src.match(inFStringRe) || []).length;
        return fstringOccurrences < totalOccurrences;
      });
      if (real.length > 0) {
        issues.push({
          cell: idx, severity: 'high', code: 'GEN_UNRESOLVED_PLACEHOLDER',
          message: `Unresolved placeholders: ${real.join(', ')}`,
          fix: 'Configure Databricks settings or check template resolution'
        });
      }
    }

    // 3. Check for unresolved NiFi EL: ${...}
    const elPatterns = src.match(/\$\{[^}]+\}/g);
    if (elPatterns) {
      const realEL = elPatterns.filter(p => {
        const line = src.split('\n').find(l => l.includes(p));
        return line && !line.trimStart().startsWith('#');
      });
      if (realEL.length > 0) {
        issues.push({
          cell: idx, severity: 'high', code: 'GEN_UNRESOLVED_EL',
          message: `Unresolved NiFi EL expressions: ${realEL.slice(0, 3).join(', ')}`,
          fix: 'NiFi Expression Language was not translated to PySpark'
        });
      }
    }

    // 4. Check for bare except:pass
    if (/except\s*:\s*\n\s*pass/g.test(src)) {
      issues.push({
        cell: idx, severity: 'medium', code: 'GEN_BARE_EXCEPT',
        message: 'Bare except:pass swallows all errors silently',
        fix: 'Add specific exception types and logging'
      });
    }

    // 5. Check for eval/exec
    if (/\beval\s*\(/.test(src) || /\bexec\s*\(/.test(src)) {
      issues.push({
        cell: idx, severity: 'critical', code: 'GEN_UNSAFE_EVAL',
        message: 'eval() or exec() detected -- security risk',
        fix: 'Replace with safe alternative (literal parsing, ast.literal_eval, etc.)'
      });
    }

    // 6. Check for hardcoded credentials
    const credPatterns = [
      /password\s*=\s*["'][^"']+["']/i,
      /api_key\s*=\s*["'][^"']+["']/i,
      /secret\s*=\s*["'][^"']+["']/i,
      /token\s*=\s*["'][A-Za-z0-9+/=]{20,}["']/i,
    ];
    credPatterns.forEach(pat => {
      if (pat.test(src)) {
        const match = src.match(pat);
        issues.push({
          cell: idx, severity: 'critical', code: 'GEN_HARDCODED_CRED',
          message: `Possible hardcoded credential: ${(match?.[0] || '').substring(0, 40)}...`,
          fix: 'Use dbutils.secrets.get(scope="...", key="...") instead'
        });
      }
    });

    // 7. Check for deprecated DBFS paths
    if (/["']\/dbfs\//.test(src) || /["']\/mnt\//.test(src)) {
      issues.push({
        cell: idx, severity: 'medium', code: 'GEN_DEPRECATED_PATH',
        message: 'Legacy DBFS/mount path detected',
        fix: 'Use Unity Catalog Volumes: /Volumes/catalog/schema/volume/'
      });
    }

    // 8. Check for .collect() without .limit()
    if (/\.collect\(\)/.test(src) && !/\.limit\(\d+\).*\.collect\(\)/.test(src)) {
      issues.push({
        cell: idx, severity: 'medium', code: 'GEN_UNBOUNDED_COLLECT',
        message: '.collect() without .limit() may cause OOM on large DataFrames',
        fix: 'Add .limit(N) before .collect() or use .toPandas() with spark.conf row limits'
      });
    }

    // 9. Check unbalanced parens/brackets
    let parenDepth = 0, bracketDepth = 0, braceDepth = 0;
    for (const ch of src) {
      if (ch === '(') parenDepth++; else if (ch === ')') parenDepth--;
      if (ch === '[') bracketDepth++; else if (ch === ']') bracketDepth--;
      if (ch === '{') braceDepth++; else if (ch === '}') braceDepth--;
    }
    if (Math.abs(parenDepth) > 1) {
      issues.push({
        cell: idx, severity: 'high', code: 'GEN_UNBALANCED_PARENS',
        message: `Unbalanced parentheses (depth: ${parenDepth})`,
        fix: 'Check for missing closing parentheses'
      });
    }

    // 10. Check for passthrough stubs: df_out = df_in with no processing
    const passthroughMatch = src.match(/df_(\w+)\s*=\s*df_(\w+)\s*\n/g);
    if (passthroughMatch && src.split('\n').filter(l => !l.startsWith('#') && l.trim()).length <= 3) {
      issues.push({
        cell: idx, severity: 'medium', code: 'GEN_PASSTHROUGH',
        message: 'Cell appears to be a passthrough with no real processing',
        fix: 'Implement actual transformation or mark processor as unsupported'
      });
    }
  });

  // Calculate score: 100 - (weighted issues)
  const weights = { critical: 15, high: 8, medium: 3, low: 1 };
  const totalPenalty = issues.reduce((sum, i) => sum + (weights[i.severity] || 3), 0);
  const score = Math.max(0, Math.min(100, 100 - totalPenalty));

  return { issues, score };
}

export { VALID_DF_METHODS, KNOWN_IMPORTS };
