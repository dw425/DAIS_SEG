// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { cleanInput } from '../src/parsers/clean-input.js';

describe('cleanInput', () => {
  it('should strip BOM from the beginning of content', () => {
    const bom = '\uFEFF';
    const result = cleanInput(bom + '<root>hello</root>');
    expect(result).toBe('<root>hello</root>');
    expect(result.charCodeAt(0)).not.toBe(0xFEFF);
  });

  it('should normalize line endings and whitespace', () => {
    const input = 'line1\r\nline2\rline3\n';
    const result = cleanInput(input);
    expect(result).toBe('line1\nline2\nline3');
    expect(result).not.toContain('\r');
  });

  it('should replace non-breaking spaces and smart quotes', () => {
    const nbsp = 'hello\u00A0world';
    expect(cleanInput(nbsp)).toBe('hello world');

    const smartQuotes = '\u201CHello\u201D \u2018World\u2019';
    expect(cleanInput(smartQuotes)).toBe('"Hello" \'World\'');
  });

  it('should remove NULL bytes', () => {
    const withNulls = 'abc\x00def\x00ghi';
    expect(cleanInput(withNulls)).toBe('abcdefghi');
  });

  it('should return empty string for null/undefined/empty input', () => {
    expect(cleanInput(null)).toBe('');
    expect(cleanInput(undefined)).toBe('');
    expect(cleanInput('')).toBe('');
  });
});
