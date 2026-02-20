// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { sanitizeVarName, truncate, pluralize } from '../src/utils/string-helpers.js';

describe('sanitizeVarName', () => {
  it('should remove special characters and lowercase', () => {
    expect(sanitizeVarName('My Processor-Name!')).toBe('my_processor_name_');
  });

  it('should prepend underscore for names starting with digit', () => {
    expect(sanitizeVarName('3rd_step')).toBe('_3rd_step');
  });

  it('should truncate to 40 characters', () => {
    const longName = 'a'.repeat(60);
    expect(sanitizeVarName(longName)).toHaveLength(40);
  });

  it('should return _empty for null/undefined/empty input', () => {
    expect(sanitizeVarName(null)).toBe('_empty');
    expect(sanitizeVarName(undefined)).toBe('_empty');
    expect(sanitizeVarName('')).toBe('_empty');
  });
});

describe('truncate', () => {
  it('should not truncate strings shorter than max', () => {
    expect(truncate('hello', 80)).toBe('hello');
  });

  it('should truncate and append suffix', () => {
    const result = truncate('This is a very long string that exceeds the limit', 20);
    expect(result).toHaveLength(20);
    expect(result).toMatch(/\.\.\.$/);
  });

  it('should return empty string for falsy input', () => {
    expect(truncate(null)).toBe('');
    expect(truncate('')).toBe('');
  });
});

describe('pluralize', () => {
  it('should return singular for count of 1', () => {
    expect(pluralize('processor', 1)).toBe('1 processor');
  });

  it('should return plural for count of 0', () => {
    expect(pluralize('processor', 0)).toBe('0 processors');
  });

  it('should return plural for count of many', () => {
    expect(pluralize('processor', 5)).toBe('5 processors');
  });

  it('should handle words ending in s', () => {
    expect(pluralize('class', 3)).toBe('3 classes');
  });

  it('should use explicit plural form when provided', () => {
    expect(pluralize('child', 3, 'children')).toBe('3 children');
  });
});
