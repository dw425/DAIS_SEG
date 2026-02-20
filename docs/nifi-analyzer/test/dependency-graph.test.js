// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { buildDependencyGraph } from '../src/analyzers/dependency-graph.js';

describe('buildDependencyGraph', () => {
  it('should build a linear dependency chain', () => {
    const nifi = {
      processors: [
        { name: 'A' },
        { name: 'B' },
        { name: 'C' },
      ],
      connections: [
        { sourceName: 'A', destinationName: 'B' },
        { sourceName: 'B', destinationName: 'C' },
      ],
    };
    const graph = buildDependencyGraph(nifi);

    expect(graph.downstream['A']).toContain('B');
    expect(graph.downstream['B']).toContain('C');
    expect(graph.upstream['C']).toContain('B');
    expect(graph.upstream['B']).toContain('A');
    expect(graph.fullDownstream['A']).toEqual(expect.arrayContaining(['B', 'C']));
    expect(graph.fullUpstream['C']).toEqual(expect.arrayContaining(['A', 'B']));
  });

  it('should handle branching dependencies', () => {
    const nifi = {
      processors: [
        { name: 'Source' },
        { name: 'Branch1' },
        { name: 'Branch2' },
        { name: 'Sink' },
      ],
      connections: [
        { sourceName: 'Source', destinationName: 'Branch1' },
        { sourceName: 'Source', destinationName: 'Branch2' },
        { sourceName: 'Branch1', destinationName: 'Sink' },
        { sourceName: 'Branch2', destinationName: 'Sink' },
      ],
    };
    const graph = buildDependencyGraph(nifi);

    expect(graph.downstream['Source']).toHaveLength(2);
    expect(graph.upstream['Sink']).toHaveLength(2);
    expect(graph.fullDownstream['Source']).toEqual(
      expect.arrayContaining(['Branch1', 'Branch2', 'Sink'])
    );
  });

  it('should handle empty processor list', () => {
    const nifi = { processors: [], connections: [] };
    const graph = buildDependencyGraph(nifi);

    expect(Object.keys(graph.downstream)).toHaveLength(0);
    expect(Object.keys(graph.upstream)).toHaveLength(0);
    expect(Object.keys(graph.fullDownstream)).toHaveLength(0);
    expect(Object.keys(graph.fullUpstream)).toHaveLength(0);
  });
});
