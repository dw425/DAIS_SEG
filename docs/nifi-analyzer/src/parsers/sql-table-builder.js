// ================================================================
// sql-table-builder.js — Build table blueprints from SQL references
// Extracted from monolith lines 1746-1801
// ================================================================

/**
 * Build table blueprint objects from SQL table references that were not
 * already defined by DDL parsing. Infers columns from SQL context and
 * table-name patterns (semiconductor/manufacturing domain defaults).
 *
 * @param {Set} sqlTables - Set of discovered SQL table names
 * @param {Object} sqlTableMeta - Map of table metadata (readers, writers, columns, etc.)
 * @param {Array} tables - Existing tables array (will be mutated with new entries)
 * @param {number} processorCount - Number of processors in the flow
 * @returns {void} Mutates the tables array in-place
 */
export function buildSqlTableBlueprints(sqlTables, sqlTableMeta, tables, processorCount) {
  const existingTableNames = new Set(tables.map(t => t.name));
  if (sqlTables.size && processorCount <= 30) {
    [...sqlTables].forEach(ref => {
      const meta = sqlTableMeta[ref] || { readers:[], writers:[], columns: new Set(), sqlContexts:[], parameterized: ref.includes('${') };
      // Determine schema and table name from dotted ref
      const parts = ref.replace(/\$\{|\}/g,'').split('.');
      const tblName = parts[parts.length - 1];
      const schemaName = parts.length >= 2 ? parts[parts.length - 2] : 'nifi_source';
      if (existingTableNames.has(tblName)) return; // skip if already defined by DDL parse
      existingTableNames.add(tblName);
      // Build columns from SQL context + name-based inference
      const cols = [];
      const inferredCols = meta.columns.size > 0 ? [...meta.columns] : [];
      // Always add a primary key
      const pkName = tblName.replace(/^nifi_/,'').replace(/s$/,'') + '_id';
      cols.push({name:pkName, data_type:'int', raw_type:'int', nullable:false, is_primary_key:true, is_unique:true, check_constraints:[], max_length:null, precision:null, scale:null, default_value:null});
      // Add inferred columns from SQL
      inferredCols.forEach(c => {
        if (c === pkName) return;
        const dtype = /date|time|timestamp|created|modified/.test(c) ? 'timestamp' : /count|num|qty|amount|total|size/.test(c) ? 'int' : /price|cost|rate|pct|percent|ratio|score/.test(c) ? 'decimal' : /flag|is_|has_|enabled|active/.test(c) ? 'boolean' : 'varchar';
        cols.push({name:c, data_type:dtype, raw_type:dtype==='varchar'?'string':dtype, nullable:true, is_primary_key:false, is_unique:false, check_constraints:[], max_length:dtype==='varchar'?200:null, precision:dtype==='decimal'?10:null, scale:dtype==='decimal'?2:null, default_value:null});
      });
      // If no columns inferred, add common columns based on table name patterns
      if (inferredCols.length === 0) {
        const commonCols = [];
        if (/load_log/.test(tblName)) commonCols.push('load_timestamp','status','filename','row_count','error_message','duration_ms');
        else if (/load_token/.test(tblName)) commonCols.push('token_name','acquired_by','acquired_at','released_at','status');
        else if (/load_depend/.test(tblName)) commonCols.push('parent_load','child_load','dependency_type','status');
        else if (/experiment/.test(tblName)) commonCols.push('experiment_name','start_date','end_date','status','result');
        else if (/lot_status/.test(tblName)) commonCols.push('lot_id','wafer_id','status','step_name','update_time');
        else if (/production_step/.test(tblName)) commonCols.push('lot_id','wafer_id','step_name','measurement','result','operator','meas_time');
        else if (/scrap/.test(tblName)) commonCols.push('lot_id','wafer_id','scrap_code','scrap_reason','quantity','scrap_date');
        else if (/metrology/.test(tblName)) commonCols.push('lot_id','wafer_id','site_id','parameter','value','measurement_date');
        else if (/diamond/.test(tblName)) commonCols.push('lot_id','wafer_id','component_id','measurement','value','timestamp');
        else if (/e3_sum|e3_uva/.test(tblName)) commonCols.push('lot_id','wafer_id','parameter','value','uva_value','summary_date');
        else if (/pcm/.test(tblName)) commonCols.push('lot_id','wafer_id','site_id','parameter','value','measurement_date');
        else if (/overlay/.test(tblName)) commonCols.push('lot_id','wafer_id','tool_id','overlay_x','overlay_y','measurement_date');
        else if (/qualification/.test(tblName)) commonCols.push('qualification_id','tool_id','recipe','status','qualified_date');
        else if (/parameter/.test(tblName)) commonCols.push('parameter_name','parameter_value','unit','min_spec','max_spec');
        else if (/runcard/.test(tblName)) commonCols.push('runcard_id','lot_id','recipe','step_name','status','created_date');
        else if (/wafer_suffix/.test(tblName)) commonCols.push('wafer_id','suffix','description');
        else if (/xsite/.test(tblName)) commonCols.push('lot_id','wafer_id','site','state','timestamp');
        else if (/temptation/.test(tblName)) commonCols.push('lot_id','wafer_id','parameter','value','summary_date');
        else if (/deviation/.test(tblName)) commonCols.push('parameter_name','deviation_value','lot_id','wafer_id','detection_date');
        else commonCols.push('name','value','status','created_date','updated_date');
        commonCols.forEach(c => {
          const dtype = /date|time|timestamp/.test(c) ? 'timestamp' : /count|qty|quantity|size/.test(c) ? 'int' : /value|measurement|overlay/.test(c) ? 'decimal' : 'varchar';
          cols.push({name:c, data_type:dtype, raw_type:dtype==='varchar'?'string':dtype, nullable:true, is_primary_key:false, is_unique:false, check_constraints:[], max_length:dtype==='varchar'?200:null, precision:dtype==='decimal'?10:null, scale:dtype==='decimal'?2:null, default_value:null});
        });
      }
      tables.push({
        name: tblName, schema: schemaName, row_count: 1000,
        columns: cols, foreign_keys: [],
        _sql_meta: { full_reference: ref, parameterized: meta.parameterized, readers: meta.readers, writers: meta.writers, sql_contexts: meta.sqlContexts.slice(0,3) }
      });
    });
  }
}
