// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { translateNELtoPySpark, translateNiFiELtoPython } from '../src/parsers/nel/index.js';

describe('translateNELtoPySpark', () => {
  it('should translate ${hostname()} system function', () => {
    const result = translateNELtoPySpark('${hostname()}', 'col');
    expect(result).toContain('clusterName');
    expect(result).not.toContain('${');
  });

  it('should pass through literals without ${', () => {
    const result = translateNELtoPySpark('plain text value', 'col');
    expect(result).toBe('plain text value');
  });

  it('should return null/undefined input as-is', () => {
    expect(translateNELtoPySpark(null)).toBe(null);
    expect(translateNELtoPySpark(undefined)).toBe(undefined);
    expect(translateNELtoPySpark('')).toBe('');
  });

  it('should translate ${now()} to current_timestamp in col mode', () => {
    const result = translateNELtoPySpark('${now()}', 'col');
    expect(result).toBe('current_timestamp()');
  });

  it('should translate ${now()} to datetime.now in python mode', () => {
    const result = translateNELtoPySpark('${now()}', 'python');
    expect(result).toBe('datetime.now()');
  });
});

describe('translateNiFiELtoPython', () => {
  it('should be an alias for translateNELtoPySpark with python mode', () => {
    const result = translateNiFiELtoPython('${UUID()}');
    expect(result).toContain('uuid');
  });
});
