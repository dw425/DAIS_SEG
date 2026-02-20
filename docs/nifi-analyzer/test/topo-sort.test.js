// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { topologicalSortMappings } from '../src/generators/topo-sort.js';

describe('topologicalSortMappings', () => {
  it('should produce correct order for a linear chain', () => {
    const mappings = [
      { name: 'C', role: 'sink' },
      { name: 'A', role: 'source' },
      { name: 'B', role: 'transform' },
    ];
    const nifi = {
      connections: [
        { sourceName: 'A', destinationName: 'B' },
        { sourceName: 'B', destinationName: 'C' },
      ],
    };
    const sorted = topologicalSortMappings(mappings, nifi);
    const names = sorted.map(m => m.name);

    expect(names.indexOf('A')).toBeLessThan(names.indexOf('B'));
    expect(names.indexOf('B')).toBeLessThan(names.indexOf('C'));
  });

  it('should produce valid topological order for branching graph', () => {
    const mappings = [
      { name: 'Sink', role: 'sink' },
      { name: 'Source', role: 'source' },
      { name: 'T1', role: 'transform' },
      { name: 'T2', role: 'transform' },
    ];
    const nifi = {
      connections: [
        { sourceName: 'Source', destinationName: 'T1' },
        { sourceName: 'Source', destinationName: 'T2' },
        { sourceName: 'T1', destinationName: 'Sink' },
        { sourceName: 'T2', destinationName: 'Sink' },
      ],
    };
    const sorted = topologicalSortMappings(mappings, nifi);
    const names = sorted.map(m => m.name);

    // Source must come before all others
    expect(names.indexOf('Source')).toBe(0);
    // Sink must come after T1 and T2
    expect(names.indexOf('Sink')).toBeGreaterThan(names.indexOf('T1'));
    expect(names.indexOf('Sink')).toBeGreaterThan(names.indexOf('T2'));
  });

  it('should handle cycles gracefully by appending unvisited nodes', () => {
    const mappings = [
      { name: 'A', role: 'source' },
      { name: 'B', role: 'transform' },
      { name: 'C', role: 'transform' },
    ];
    const nifi = {
      connections: [
        { sourceName: 'A', destinationName: 'B' },
        { sourceName: 'B', destinationName: 'C' },
        { sourceName: 'C', destinationName: 'A' },
      ],
    };
    const sorted = topologicalSortMappings(mappings, nifi);

    // Should still return all mappings even though there is a cycle
    expect(sorted).toHaveLength(3);
    const names = sorted.map(m => m.name);
    expect(names).toEqual(expect.arrayContaining(['A', 'B', 'C']));
  });

  it('should handle empty mappings', () => {
    const sorted = topologicalSortMappings([], { connections: [] });
    expect(sorted).toHaveLength(0);
  });
});
