// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { detectCyclesSCC } from '../src/analyzers/cycle-detection.js';

describe('detectCyclesSCC', () => {
  it('should return empty array for a DAG (no cycles)', () => {
    const adj = {
      A: ['B'],
      B: ['C'],
      C: [],
    };
    const sccs = detectCyclesSCC(adj);
    expect(sccs).toHaveLength(0);
  });

  it('should detect a simple cycle', () => {
    const adj = {
      A: ['B'],
      B: ['C'],
      C: ['A'],
    };
    const sccs = detectCyclesSCC(adj);
    expect(sccs).toHaveLength(1);
    expect(sccs[0]).toEqual(expect.arrayContaining(['A', 'B', 'C']));
    expect(sccs[0]).toHaveLength(3);
  });

  it('should detect multiple SCCs', () => {
    const adj = {
      A: ['B'],
      B: ['A'],      // cycle 1: A <-> B
      C: ['D'],
      D: ['E'],
      E: ['C'],      // cycle 2: C -> D -> E -> C
      F: [],          // isolated node, no cycle
    };
    const sccs = detectCyclesSCC(adj);
    expect(sccs).toHaveLength(2);
    const sizes = sccs.map(s => s.length).sort();
    expect(sizes).toEqual([2, 3]);
  });

  it('should handle empty adjacency map', () => {
    const sccs = detectCyclesSCC({});
    expect(sccs).toHaveLength(0);
  });
});
