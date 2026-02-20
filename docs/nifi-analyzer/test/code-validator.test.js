// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { validateGeneratedCode } from '../src/generators/code-validator.js';

describe('validateGeneratedCode', () => {
  it('should report valid for clean Python code', () => {
    const cells = [
      'import pyspark\nfrom pyspark.sql import functions as F\ndf = spark.read.parquet("/data")',
    ];
    const results = validateGeneratedCode(cells);
    expect(results).toHaveLength(1);
    expect(results[0].valid).toBe(true);
    expect(results[0].issues).toHaveLength(0);
  });

  it('should detect unresolved NiFi EL expressions', () => {
    const cells = [
      'df = spark.read.option("path", "${file.path}").load()',
    ];
    const results = validateGeneratedCode(cells);
    expect(results[0].valid).toBe(false);
    expect(results[0].issues.some(i => i.includes('NiFi EL'))).toBe(true);
  });

  it('should detect missing imports for known third-party modules', () => {
    const cells = [
      'response = requests.get("https://api.example.com")\ndata = response.json()',
    ];
    const results = validateGeneratedCode(cells);
    expect(results[0].valid).toBe(false);
    expect(results[0].issues.some(i => i.includes('Missing imports') && i.includes('requests'))).toBe(true);
  });

  it('should not flag imports that are present', () => {
    const cells = [
      'import requests\nresponse = requests.get("https://api.example.com")',
    ];
    const results = validateGeneratedCode(cells);
    const importIssues = results[0].issues.filter(i => i.includes('Missing imports'));
    expect(importIssues).toHaveLength(0);
  });

  it('should detect unbalanced parentheses beyond tolerance', () => {
    const cells = [
      'df = spark.read.option((("a", ("b")',  // 4 open, 1 close -> diff of 3
    ];
    const results = validateGeneratedCode(cells);
    expect(results[0].valid).toBe(false);
    expect(results[0].issues.some(i => i.includes('parentheses'))).toBe(true);
  });

  it('should handle null input gracefully', () => {
    expect(validateGeneratedCode(null)).toEqual([]);
  });
});
