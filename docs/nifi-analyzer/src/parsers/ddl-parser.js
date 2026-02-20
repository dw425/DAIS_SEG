// ================================================================
// ddl-parser.js — Parse CREATE TABLE / CREATE VIEW DDL statements
// GAP FIX: Real DDL parsing (not just regex table-name extraction)
// ================================================================

/**
 * Parse a CREATE TABLE or CREATE VIEW statement and extract the table name,
 * columns with types, and basic constraints.
 *
 * Supports:
 *   - CREATE [EXTERNAL] TABLE [IF NOT EXISTS] [schema.]name (col type, ...)
 *   - CREATE [OR REPLACE] VIEW [schema.]name AS SELECT ...
 *   - Column constraints: NOT NULL, PRIMARY KEY, DEFAULT, UNIQUE, CHECK
 *   - Hive-style: STORED AS, PARTITIONED BY, ROW FORMAT, LOCATION
 *   - Impala-style: COMMENT, TBLPROPERTIES
 *
 * @param {string} sql - DDL statement text
 * @returns {{ tableName: string, schema: string, tableType: string, columns: Array, partitionColumns: Array, storedAs: string, location: string } | null}
 */
export function parseDDL(sql) {
  if (!sql) return null;
  const trimmed = sql.trim();

  // Match CREATE TABLE or CREATE VIEW
  const createMatch = trimmed.match(
    /CREATE\s+(?:OR\s+REPLACE\s+)?(?:EXTERNAL\s+)?(?:TEMPORARY\s+)?(TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:`?(\w+)`?\.)?`?(\w+)`?/i
  );
  if (!createMatch) return null;

  const tableType = createMatch[1].toUpperCase(); // TABLE or VIEW
  const schema = createMatch[2] || '';
  const tableName = createMatch[3];

  const result = {
    tableName,
    schema,
    tableType,
    columns: [],
    partitionColumns: [],
    storedAs: '',
    location: ''
  };

  // For VIEWs, there are no column definitions in parens — extract from AS SELECT if possible
  if (tableType === 'VIEW') {
    const asMatch = trimmed.match(/\bAS\s+(SELECT\s+[\s\S]+)/i);
    if (asMatch) {
      result._viewQuery = asMatch[1];
      // Try to extract column aliases from SELECT list
      const selMatch = asMatch[1].match(/SELECT\s+([\s\S]*?)\s+FROM/i);
      if (selMatch && selMatch[1].trim() !== '*') {
        selMatch[1].split(',').forEach(c => {
          const alias = c.trim().replace(/.*\s+AS\s+/i, '').replace(/.*\./, '').replace(/[()]/g, '').trim();
          if (alias && alias !== '*' && alias.length < 60) {
            result.columns.push({
              name: alias,
              data_type: 'unknown',
              raw_type: 'unknown',
              nullable: true,
              is_primary_key: false,
              is_unique: false,
              check_constraints: [],
              max_length: null,
              precision: null,
              scale: null,
              default_value: null
            });
          }
        });
      }
    }
    return result;
  }

  // Extract column definitions from parenthesized block
  const colBlockMatch = trimmed.match(/\(\s*([\s\S]*?)\s*\)\s*(?:COMMENT|STORED|ROW|PARTITIONED|LOCATION|TBLPROPERTIES|WITH|ENGINE|;|$)/i);
  if (!colBlockMatch) return result;

  const colBlock = colBlockMatch[1];

  // Split on commas that are not inside parentheses
  const colDefs = [];
  let current = '';
  let depth = 0;
  for (let i = 0; i < colBlock.length; i++) {
    const ch = colBlock[i];
    if (ch === '(') depth++;
    else if (ch === ')') depth--;
    else if (ch === ',' && depth === 0) {
      colDefs.push(current.trim());
      current = '';
      continue;
    }
    current += ch;
  }
  if (current.trim()) colDefs.push(current.trim());

  // Parse each column definition
  for (const def of colDefs) {
    // Skip table-level constraints
    if (/^\s*(PRIMARY\s+KEY|UNIQUE|CHECK|CONSTRAINT|INDEX|FOREIGN\s+KEY)/i.test(def)) continue;

    // Match: column_name data_type [constraints...]
    const colMatch = def.match(/^\s*`?(\w+)`?\s+(\w+(?:\s*\([^)]*\))?)/i);
    if (!colMatch) continue;

    const colName = colMatch[1];
    const rawType = colMatch[2].trim();
    const dataType = rawType.replace(/\(.*\)/, '').toLowerCase();

    // Extract constraints
    const nullable = !/\bNOT\s+NULL\b/i.test(def);
    const isPrimaryKey = /\bPRIMARY\s+KEY\b/i.test(def);
    const isUnique = /\bUNIQUE\b/i.test(def) || isPrimaryKey;

    // Extract default value
    let defaultValue = null;
    const defaultMatch = def.match(/\bDEFAULT\s+([^\s,]+)/i);
    if (defaultMatch) defaultValue = defaultMatch[1].replace(/^['"]|['"]$/g, '');

    // Extract precision/scale from type like DECIMAL(10,2) or VARCHAR(200)
    let maxLength = null, precision = null, scale = null;
    const typeParenMatch = rawType.match(/\((\d+)(?:\s*,\s*(\d+))?\)/);
    if (typeParenMatch) {
      if (/decimal|numeric/i.test(dataType)) {
        precision = parseInt(typeParenMatch[1]);
        scale = typeParenMatch[2] ? parseInt(typeParenMatch[2]) : 0;
      } else {
        maxLength = parseInt(typeParenMatch[1]);
      }
    }

    // Extract CHECK constraints
    const checkConstraints = [];
    const checkMatch = def.match(/\bCHECK\s*\(([^)]+)\)/i);
    if (checkMatch) checkConstraints.push(checkMatch[1].trim());

    result.columns.push({
      name: colName,
      data_type: dataType,
      raw_type: rawType,
      nullable,
      is_primary_key: isPrimaryKey,
      is_unique: isUnique,
      check_constraints: checkConstraints,
      max_length: maxLength,
      precision,
      scale,
      default_value: defaultValue
    });
  }

  // Extract PARTITIONED BY columns
  const partMatch = trimmed.match(/PARTITIONED\s+BY\s*\(([^)]+)\)/i);
  if (partMatch) {
    partMatch[1].split(',').forEach(p => {
      const pm = p.trim().match(/`?(\w+)`?(?:\s+(\w+))?/);
      if (pm) result.partitionColumns.push({ name: pm[1], data_type: pm[2] || 'string' });
    });
  }

  // Extract STORED AS format
  const storedMatch = trimmed.match(/STORED\s+AS\s+(\w+)/i);
  if (storedMatch) result.storedAs = storedMatch[1].toUpperCase();

  // Extract LOCATION
  const locMatch = trimmed.match(/LOCATION\s+['"]([^'"]+)['"]/i);
  if (locMatch) result.location = locMatch[1];

  return result;
}
