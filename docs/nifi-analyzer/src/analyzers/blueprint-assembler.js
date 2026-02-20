/**
 * analyzers/blueprint-assembler.js — Assemble a SEG-compatible blueprint from parsed DDL
 *
 * Extracted from index.html lines 2245-2281.
 *
 * @module analyzers/blueprint-assembler
 */

/**
 * Generate synthetic statistics for a column, based on its data type.
 *
 * @param {object} col — column definition (data_type, nullable, is_primary_key, max_length, precision, scale, check_constraints)
 * @param {number} rowCount — estimated row count for the table
 * @returns {object} — statistics object
 */
export function genStats(col, rowCount) {
  const dt = col.data_type.toLowerCase();
  const s = { null_ratio: col.nullable ? 0.05 : 0.0 };

  if (col.check_constraints && col.check_constraints.length) {
    const n = col.check_constraints.length;
    s.top_values = col.check_constraints.map(v => ({ value: v, frequency: Math.round(1 / n * 10000) / 10000 }));
    s.distinct_count = n;
    return s;
  }

  if (['int', 'integer', 'smallint', 'tinyint'].includes(dt)) {
    if (col.is_primary_key) Object.assign(s, { min: 1, max: rowCount, mean: rowCount / 2, stddev: rowCount / 6, distinct_count: rowCount });
    else Object.assign(s, { min: 1, max: 1000, mean: 500, stddev: 300, distinct_count: Math.min(500, rowCount) });
  } else if (['bigint', 'long'].includes(dt)) {
    Object.assign(s, { min: 1, max: 100000, mean: 50000, stddev: 30000, distinct_count: Math.min(10000, rowCount) });
  } else if (['float', 'double'].includes(dt)) {
    Object.assign(s, { min: 0, max: 10000, mean: 100, stddev: 50, distinct_count: rowCount });
  } else if (['decimal', 'numeric'].includes(dt)) {
    const mx = Math.pow(10, (col.precision || 10) - (col.scale || 2)) - 1;
    Object.assign(s, { min: 0, max: mx, mean: mx / 10, stddev: mx / 20, distinct_count: rowCount });
  } else if (['varchar', 'char', 'text', 'string'].includes(dt)) {
    Object.assign(s, { min_length: 3, max_length: Math.min(col.max_length || 50, 100), distinct_count: rowCount });
  } else if (dt === 'date') {
    Object.assign(s, { min: '2020-01-01', max: '2025-12-31', distinct_count: Math.min(rowCount, 2000) });
  } else if (['timestamp', 'datetime'].includes(dt)) {
    Object.assign(s, { min: '2020-01-01', max: '2025-12-31', distinct_count: rowCount });
  } else if (['boolean', 'bool'].includes(dt)) {
    s.top_values = [{ value: true, frequency: 0.5 }, { value: false, frequency: 0.5 }];
    s.distinct_count = 2;
  } else {
    Object.assign(s, { min_length: 5, max_length: 20, distinct_count: rowCount });
  }
  return s;
}

/**
 * Assemble a SEG-compatible blueprint from parsed table definitions.
 *
 * @param {object} parsed — { tables: [...], source_name, source_type }
 * @returns {object} — blueprint with blueprint_id, source_system, tables, relationships
 */
export function assembleBlueprint(parsed) {
  const bid = (typeof crypto !== 'undefined' && crypto.randomUUID)
    ? crypto.randomUUID()
    : 'bp-' + Math.random().toString(36).substring(2, 10);

  const tables = parsed.tables.map(pt => {
    const rc = pt.row_count || 1000;
    const cols = pt.columns.map(c => ({
      name: c.name,
      data_type: c.data_type,
      nullable: c.nullable,
      is_primary_key: c.is_primary_key,
      stats: genStats(c, rc),
    }));
    const fks = pt.foreign_keys.map(fk => ({
      column: fk.fk_column,
      references_table: fk.referenced_table,
      references_column: fk.referenced_column,
    }));
    return { name: pt.name, schema: pt.schema, row_count: rc, columns: cols, foreign_keys: fks };
  });

  const rels = [];
  parsed.tables.forEach(pt =>
    pt.foreign_keys.forEach(fk =>
      rels.push({
        from_table: `${pt.schema}.${pt.name}`,
        to_table: `${pt.schema}.${fk.referenced_table}`,
        relationship_type: 'one_to_many',
        join_columns: [{ from_column: fk.fk_column, to_column: fk.referenced_column }],
      })
    )
  );

  return {
    blueprint_id: bid,
    source_system: { name: parsed.source_name, type: parsed.source_type },
    tables,
    relationships: rels,
  };
}
