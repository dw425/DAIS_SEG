/**
 * generators/cell-builders/preflight-cell.js -- Generate pre-flight validation cell
 *
 * @module generators/cell-builders/preflight-cell
 */

/**
 * Build a preflight check cell that validates environment before pipeline runs.
 * @param {Object} config - { catalog, schema, secretScope }
 * @param {Array} requiredPackages - packages detected from notebook cells
 * @param {Array} secretKeys - secret keys detected from dbutils.secrets.get() calls
 * @returns {Object} cell object
 */
export function buildPreflightCell(config, requiredPackages, secretKeys) {
  const cat = config.catalog || 'main';
  const sch = config.schema || 'default';
  const scope = config.secretScope || 'migration_secrets';

  const pkgChecks = (requiredPackages || [])
    .filter(p => !['pyspark', 'delta', 'dbutils'].includes(p))
    .map(p => `    "${p}"`)
    .join(',\n');

  const secretChecks = (secretKeys || [])
    .map(k => `    ("${scope}", "${k}")`)
    .join(',\n');

  const source = `# ============================================================
# PRE-FLIGHT VALIDATION
# Checks environment readiness before pipeline execution.
# ============================================================
import importlib, time as _pf_time

_pf_start = _pf_time.time()
_pf_errors = []
_pf_warnings = []

# 1. Verify catalog and schema exist
try:
    spark.sql(f"USE CATALOG ${cat}")
    spark.sql(f"USE SCHEMA ${cat}.${sch}")
    print(f"[PREFLIGHT] Catalog ${cat}.${sch} verified")
except Exception as _e:
    _pf_errors.append(f"Catalog/schema ${cat}.${sch} not accessible: {_e}")

# 2. Create execution tracking tables if missing
for _tbl_name, _tbl_ddl in [
    ("__execution_log", """CREATE TABLE IF NOT EXISTS ${cat}.${sch}.__execution_log (
        run_id STRING, processor STRING, proc_type STRING, role STRING,
        timestamp TIMESTAMP, status STRING, error STRING,
        rows LONG, duration_ms LONG, confidence DOUBLE, upstream STRING
    ) USING DELTA"""),
    ("__dead_letter_queue", """CREATE TABLE IF NOT EXISTS ${cat}.${sch}.__dead_letter_queue (
        run_id STRING, processor STRING, error STRING, timestamp TIMESTAMP,
        record_count LONG, sample_data STRING
    ) USING DELTA"""),
]:
    try:
        spark.sql(f"DESCRIBE TABLE ${cat}.${sch}.{_tbl_name}")
    except Exception:
        try:
            spark.sql(_tbl_ddl)
            print(f"[PREFLIGHT] Created {_tbl_name}")
        except Exception as _e2:
            _pf_warnings.append(f"Could not create {_tbl_name}: {_e2}")

# 3. Check required Python packages
_pf_missing_pkgs = []
for _pkg in [
${pkgChecks}
]:
    try:
        importlib.import_module(_pkg.split(".")[0])
    except ImportError:
        _pf_missing_pkgs.append(_pkg)

if _pf_missing_pkgs:
    _pf_warnings.append(f"Missing packages (install via %pip): {', '.join(_pf_missing_pkgs)}")

# 4. Check secret scopes
_pf_secret_checks = [
${secretChecks}
]
for _scope, _key in _pf_secret_checks:
    try:
        dbutils.secrets.get(scope=_scope, key=_key)
    except Exception as _e:
        _pf_errors.append(f"Secret {_scope}/{_key} not accessible: {_e}")

# 5. Generate unique run ID for this execution
import uuid as _pf_uuid
_RUN_ID = str(_pf_uuid.uuid4())[:8]
print(f"[PREFLIGHT] Run ID: {_RUN_ID}")

# Report results
_pf_elapsed = round((_pf_time.time() - _pf_start) * 1000)
if _pf_errors:
    print(f"\\n{'='*60}")
    print(f"PREFLIGHT FAILED ({len(_pf_errors)} errors, {len(_pf_warnings)} warnings)")
    for _e in _pf_errors:
        print(f"  ERROR: {_e}")
    for _w in _pf_warnings:
        print(f"  WARN:  {_w}")
    print(f"{'='*60}")
    raise RuntimeError(f"Pre-flight validation failed: {len(_pf_errors)} errors. Fix issues above before running pipeline.")
else:
    if _pf_warnings:
        for _w in _pf_warnings:
            print(f"  WARN: {_w}")
    print(f"[PREFLIGHT] All checks passed ({_pf_elapsed}ms)")`;

  return {
    type: 'code',
    label: 'Pre-Flight Validation',
    source,
    role: 'config',
    processor: '__preflight',
    procType: 'PreflightCheck',
    confidence: 1.0,
    mapped: true,
  };
}
