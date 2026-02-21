/**
 * generators/runtime-compat.js -- Check generated code against Databricks Runtime compatibility
 *
 * @module generators/runtime-compat
 */

const COMPAT_RULES = [
  { pattern: /aes_encrypt|aes_decrypt/, minDBR: '10.4', feature: 'AES encryption functions' },
  { pattern: /MERGE\s+INTO/i, minDBR: '5.1', feature: 'MERGE INTO (Delta Lake)' },
  { pattern: /CREATE\s+VOLUME/i, minDBR: '13.3', feature: 'Unity Catalog Volumes' },
  { pattern: /cloudFiles/, minDBR: '8.1', feature: 'Auto Loader (cloudFiles)' },
  { pattern: /pandas_udf/, minDBR: '5.3', feature: 'Pandas UDFs' },
  { pattern: /mapInPandas|applyInPandas/, minDBR: '10.0', feature: 'mapInPandas/applyInPandas' },
  { pattern: /OPTIMIZE\s+.*ZORDER/i, minDBR: '5.1', feature: 'OPTIMIZE ZORDER' },
  { pattern: /VACUUM/i, minDBR: '5.1', feature: 'Delta VACUUM' },
  { pattern: /CREATE\s+STREAMING\s+TABLE/i, minDBR: '13.1', feature: 'DLT Streaming Tables' },
  { pattern: /\.observe\(/, minDBR: '11.0', feature: 'DataFrame.observe()' },
  { pattern: /spark\.readStream\.format\("kafka"\)/, minDBR: '5.1', feature: 'Kafka Structured Streaming' },
  { pattern: /IDENTITY\s+COLUMN/i, minDBR: '13.3', feature: 'Identity columns' },
  { pattern: /CLUSTER\s+BY/i, minDBR: '13.3', feature: 'Liquid clustering' },
];

/**
 * Check cells for runtime compatibility issues.
 * @param {Array} cells - notebook cells
 * @param {string} targetDBR - target Databricks Runtime version (e.g., "14.3")
 * @returns {{ minRequired: string, issues: Array<{cell: number, feature: string, minDBR: string}> }}
 */
export function checkRuntimeCompat(cells, targetDBR) {
  const targetMajor = parseFloat(targetDBR) || 14.3;
  const issues = [];
  let maxRequired = 0;

  cells.forEach((cell, idx) => {
    if (cell.type !== 'code') return;
    const src = cell.source || '';
    COMPAT_RULES.forEach(rule => {
      if (rule.pattern.test(src)) {
        const reqMajor = parseFloat(rule.minDBR);
        if (reqMajor > targetMajor) {
          issues.push({ cell: idx, feature: rule.feature, minDBR: rule.minDBR });
        }
        maxRequired = Math.max(maxRequired, reqMajor);
      }
    });
  });

  return { minRequired: String(maxRequired || '14.0'), issues };
}

/**
 * Generate a runtime version check cell.
 * @param {string} minDBR - minimum required DBR version
 * @returns {string} Python code for version assertion
 */
export function generateRuntimeCheck(minDBR) {
  const minVersion = parseFloat(minDBR) || 14.0;
  return `# Runtime Version Check
import re as _rc_re
_dbr_version = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "0.0")
_dbr_match = _rc_re.search(r"(\\d+\\.\\d+)", _dbr_version)
_dbr_major = float(_dbr_match.group(1)) if _dbr_match else 0.0
if _dbr_major < ${minVersion}:
    raise RuntimeError(
        f"This notebook requires Databricks Runtime ${minDBR}+, "
        f"but cluster is running {_dbr_version}. "
        f"Please upgrade or select a compatible cluster."
    )
print(f"[RUNTIME] Databricks Runtime {_dbr_version} (>= ${minDBR} required)")`;
}

export { COMPAT_RULES };
