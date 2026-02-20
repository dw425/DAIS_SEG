/**
 * security/phi-safe-dlq.js — PHI-safe Dead Letter Queue code generator
 *
 * Extracted from index.html lines 3604-3617.
 * Generates PySpark code that hashes PHI/HIPAA fields (SHA-256)
 * before writing to a dead letter queue, preventing PHI exposure.
 *
 * @module security/phi-safe-dlq
 */

/**
 * Generate PySpark code to hash PHI fields before DLQ write.
 *
 * @param {string} varName   - The DataFrame variable name suffix (e.g. 'my_proc')
 * @param {Array}  phiFields - Array of {key:string, category:string} PHI field descriptors
 * @returns {string} PySpark code block (empty string if no PHI fields)
 */
export function generatePHISafeDLQ(varName, phiFields) {
  if (!phiFields || phiFields.length === 0) return '';
  const lines = [
    '# \u26A0 PHI/HIPAA WARNING: This processor handles protected health information',
    '# The following fields are hashed (SHA-256) before DLQ write to prevent PHI exposure:',
    '# ' + phiFields.map(f => f.key + ' (' + f.category + ')').join(', '),
    'from pyspark.sql.functions import sha2, col',
  ];
  phiFields.forEach(f => {
    const colName = f.key.replace(/[^a-zA-Z0-9_]/g, '_');
    lines.push(`df_${varName}_dlq = df_${varName}_dlq.withColumn("${colName}", sha2(col("${colName}").cast("string"), 256))`);
  });
  return lines.join('\n');
}
