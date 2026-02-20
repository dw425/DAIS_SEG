// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { extractColumnsFromSQL, convertImpalaSqlToSpark, addSqlTableMeta } from '../src/parsers/sql-extractor.js';

describe('extractColumnsFromSQL', () => {
  it('should extract columns from a SELECT statement', () => {
    const sql = 'SELECT id, name, email FROM users WHERE active = 1';
    const cols = extractColumnsFromSQL(sql);
    expect(cols.has('id')).toBe(true);
    expect(cols.has('name')).toBe(true);
    expect(cols.has('email')).toBe(true);
  });

  it('should extract columns with aliases', () => {
    const sql = 'SELECT u.first_name AS fname, u.last_name AS lname FROM users u';
    const cols = extractColumnsFromSQL(sql);
    expect(cols.has('fname')).toBe(true);
    expect(cols.has('lname')).toBe(true);
  });

  it('should extract WHERE clause column references', () => {
    const sql = 'SELECT * FROM orders WHERE status = "active" AND created_at > "2024-01-01"';
    const cols = extractColumnsFromSQL(sql);
    expect(cols.has('status')).toBe(true);
    expect(cols.has('created_at')).toBe(true);
  });
});

describe('addSqlTableMeta', () => {
  it('should track table references with role and processor', () => {
    const sqlTables = new Set();
    const sqlTableMeta = {};
    addSqlTableMeta('customer_orders', 'read', 'QueryDB', 'SELECT * FROM customer_orders', sqlTables, sqlTableMeta);
    expect(sqlTables.has('customer_orders')).toBe(true);
    expect(sqlTableMeta['customer_orders'].readers).toContain('QueryDB');
  });

  it('should skip null, empty, or noise-word table names', () => {
    const sqlTables = new Set();
    const sqlTableMeta = {};
    addSqlTableMeta('', 'read', 'P1', '', sqlTables, sqlTableMeta);
    addSqlTableMeta(null, 'read', 'P1', '', sqlTables, sqlTableMeta);
    addSqlTableMeta('a', 'read', 'P1', '', sqlTables, sqlTableMeta); // too short
    expect(sqlTables.size).toBe(0);
  });
});

describe('convertImpalaSqlToSpark', () => {
  it('should convert COMPUTE STATS to ANALYZE TABLE', () => {
    const result = convertImpalaSqlToSpark('COMPUTE STATS my_table');
    expect(result).toBe('ANALYZE TABLE my_table COMPUTE STATISTICS');
  });

  it('should convert STRING_AGG to CONCAT_WS + COLLECT_LIST', () => {
    const result = convertImpalaSqlToSpark("STRING_AGG(name, ',')");
    expect(result).toContain('CONCAT_WS');
    expect(result).toContain('COLLECT_LIST');
  });

  it('should convert INVALIDATE METADATA to REFRESH TABLE', () => {
    const result = convertImpalaSqlToSpark('INVALIDATE METADATA my_db.my_table');
    expect(result).toContain('REFRESH TABLE');
  });

  it('should return null/empty input unchanged', () => {
    expect(convertImpalaSqlToSpark(null)).toBe(null);
    expect(convertImpalaSqlToSpark('')).toBe('');
  });
});
