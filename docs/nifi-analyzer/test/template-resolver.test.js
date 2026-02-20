// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { resolveTemplate } from '../src/mappers/template-resolver.js';

describe('resolveTemplate', () => {
  const noopTranslate = (s) => s;
  const noopPostProcess = (s) => s;

  it('should replace {v} and {in} placeholders in template', () => {
    const mapEntry = { tpl: '{v} = {in}.select("*")', conf: 0.9 };
    const result = resolveTemplate(
      mapEntry, 'df_output', 'df_input', ['Input Proc'],
      {}, noopTranslate, noopPostProcess, 'TestProc'
    );
    expect(result.code).toBe('df_output = df_input.select("*")');
    expect(result.conf).toBe(0.9);
  });

  it('should substitute processor properties into template', () => {
    const mapEntry = { tpl: '{v} = spark.read.format("{file_format}").load("{path}")', conf: 0.85 };
    const result = resolveTemplate(
      mapEntry, 'df_src', 'df_in', [],
      { 'File Format': 'parquet', 'Path': '/data/raw' },
      noopTranslate, noopPostProcess, 'GetFile'
    );
    expect(result.code).toContain('parquet');
    expect(result.code).toContain('/data/raw');
  });

  it('should handle missing variable with {in2} fallback', () => {
    const mapEntry = { tpl: '{v} = {in1}.union({in2})', conf: 0.7 };
    const result = resolveTemplate(
      mapEntry, 'df_merged', 'df_a', ['ProcA'],
      {}, noopTranslate, noopPostProcess, 'MergeProc'
    );
    // Only one input proc, so {in2} should fall back to 'input2'
    expect(result.code).toBe('df_merged = df_a.union(input2)');
  });

  it('should handle template with multiple input procs', () => {
    const mapEntry = { tpl: '{v} = {in1}.join({in2}, "id")', conf: 0.8 };
    const result = resolveTemplate(
      mapEntry, 'df_joined', 'df_left', ['Left Source', 'Right Source'],
      {}, noopTranslate, noopPostProcess, 'JoinProc'
    );
    expect(result.code).toContain('df_left');
    expect(result.code).toContain('right_source');
  });
});
