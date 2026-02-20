// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { escapeHTML, safeInnerHTML, createSafeEl } from '../src/security/html-sanitizer.js';

describe('escapeHTML', () => {
  it('should escape XSS script tags', () => {
    const result = escapeHTML('<script>alert(1)</script>');
    expect(result).toBe('&lt;script&gt;alert(1)&lt;/script&gt;');
    expect(result).not.toContain('<script>');
  });

  it('should escape quotes and ampersands', () => {
    const result = escapeHTML('He said "hello" & \'goodbye\'');
    expect(result).toBe('He said &quot;hello&quot; &amp; &#39;goodbye&#39;');
  });

  it('should handle null and undefined', () => {
    expect(escapeHTML(null)).toBe('');
    expect(escapeHTML(undefined)).toBe('');
  });

  it('should pass through normal text unchanged', () => {
    expect(escapeHTML('Hello World')).toBe('Hello World');
    expect(escapeHTML('abc 123')).toBe('abc 123');
  });
});

describe('safeInnerHTML', () => {
  it('should set textContent instead of innerHTML when not trusted', () => {
    const el = document.createElement('div');
    safeInnerHTML(el, '<script>alert(1)</script>');
    // textContent assignment means the script tag is NOT parsed as HTML
    expect(el.textContent).toBe('<script>alert(1)</script>');
    expect(el.innerHTML).not.toContain('<script>');
  });

  it('should set innerHTML when trusted flag is true', () => {
    const el = document.createElement('div');
    safeInnerHTML(el, '<b>bold</b>', { trusted: true });
    expect(el.innerHTML).toBe('<b>bold</b>');
  });

  it('should handle null element gracefully', () => {
    // Should not throw
    expect(() => safeInnerHTML(null, 'test')).not.toThrow();
  });
});
