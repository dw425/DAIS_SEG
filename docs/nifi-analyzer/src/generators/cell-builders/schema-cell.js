/**
 * generators/cell-builders/schema-cell.js — Unity Catalog DDL Cell Builder
 *
 * Builds the SQL cell that creates the Unity Catalog, schema, and
 * Delta tables from the blueprint.
 *
 * Extracted from generateDatabricksNotebook DDL logic (index.html ~line 5803-5814).
 *
 * @module generators/cell-builders/schema-cell
 */

/**
 * Build the Unity Catalog setup SQL cell.
 *
 * @param {Object} options
 * @param {string} options.catalogName — Unity Catalog name (may be empty)
 * @param {string} options.schemaName — schema/database name
 * @param {string} options.qualifiedSchema — fully qualified schema name
 * @param {Array<Object>} options.tables — blueprint table definitions
 * @returns {Object|null} — cell object { type, label, source, role } or null if no tables
 */
export function buildSchemaCell({ catalogName, schemaName, qualifiedSchema, tables }) {
  if (!tables || tables.length === 0) return null;

  let ddl = '';
  if (catalogName) ddl += `CREATE CATALOG IF NOT EXISTS ${catalogName};\nUSE CATALOG ${catalogName};\n`;
  ddl += `CREATE SCHEMA IF NOT EXISTS ${schemaName};\nUSE SCHEMA ${schemaName};\n`;
  tables.forEach(t => {
    ddl += `\nCREATE TABLE IF NOT EXISTS ${qualifiedSchema}.${t.name} (\n`;
    ddl += t.columns.map(c => `  ${c.name} ${(c.data_type || c.type || 'STRING').toUpperCase()}`).join(',\n');
    ddl += '\n) USING DELTA;';
  });
  return { type: 'sql', label: 'Unity Catalog Setup', source: ddl, role: 'config' };
}
