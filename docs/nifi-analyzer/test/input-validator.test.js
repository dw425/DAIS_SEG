// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { validateXML, validateJSON, sanitizeInput } from '../src/security/input-validator.js';

describe('validateXML', () => {
  it('should accept valid XML', () => {
    const result = validateXML('<root><child>text</child></root>');
    expect(result.valid).toBe(true);
    expect(result.error).toBeNull();
    expect(result.doc).not.toBeNull();
  });

  it('should reject XXE attempts with DOCTYPE', () => {
    const xxe = '<!DOCTYPE foo [<!ENTITY xxe SYSTEM "file:///etc/passwd">]><root>&xxe;</root>';
    const result = validateXML(xxe);
    expect(result.valid).toBe(false);
    expect(result.error).toContain('DOCTYPE');
  });

  it('should reject empty or non-string input', () => {
    expect(validateXML('')).toEqual({ valid: false, error: expect.any(String), doc: null });
    expect(validateXML(null)).toEqual({ valid: false, error: expect.any(String), doc: null });
    expect(validateXML(undefined)).toEqual({ valid: false, error: expect.any(String), doc: null });
  });

  it('should reject malformed XML', () => {
    const result = validateXML('<root><unclosed>');
    expect(result.valid).toBe(false);
    expect(result.error).toBeTruthy();
  });
});

describe('validateJSON', () => {
  it('should accept valid JSON', () => {
    const result = validateJSON('{"key": "value", "count": 42}');
    expect(result.valid).toBe(true);
    expect(result.data).toEqual({ key: 'value', count: 42 });
    expect(result.error).toBeNull();
  });

  it('should reject invalid JSON', () => {
    const result = validateJSON('{bad json}');
    expect(result.valid).toBe(false);
    expect(result.error).toBeTruthy();
    expect(result.data).toBeNull();
  });

  it('should reject empty or non-string input', () => {
    expect(validateJSON('')).toEqual({ valid: false, error: expect.any(String), data: null });
    expect(validateJSON(null)).toEqual({ valid: false, error: expect.any(String), data: null });
  });

  it('should accept JSON arrays', () => {
    const result = validateJSON('[1, 2, 3]');
    expect(result.valid).toBe(true);
    expect(result.data).toEqual([1, 2, 3]);
  });
});

describe('sanitizeInput', () => {
  it('should strip null bytes and control characters', () => {
    const input = 'hello\x00world\x01test';
    const result = sanitizeInput(input);
    expect(result).toBe('helloworld test'.replace(' ', '')); // \x01 removed
    expect(result).not.toContain('\x00');
  });

  it('should enforce max length', () => {
    const longStr = 'a'.repeat(20000);
    const result = sanitizeInput(longStr, { maxLength: 100 });
    expect(result).toHaveLength(100);
  });
});
