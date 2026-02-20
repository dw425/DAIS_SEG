// @vitest-environment jsdom
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { bfsShortestPath, bfsReachable } from '../src/utils/bfs.js';

describe('bfsShortestPath', () => {
  it('should find shortest path in a simple graph', () => {
    const connections = [
      { from: 'A', to: 'B' },
      { from: 'B', to: 'C' },
      { from: 'A', to: 'C' },  // direct shortcut
    ];
    const result = bfsShortestPath(connections, 'A', 'C');
    expect(result.found).toBe(true);
    // BFS should find direct path A -> C (length 2) rather than A -> B -> C (length 3)
    expect(result.pathNodes).toEqual(['A', 'C']);
    expect(result.pathEdgeKeys).toEqual(['A|C']);
  });

  it('should find path through intermediate nodes', () => {
    const connections = [
      { from: 'A', to: 'B' },
      { from: 'B', to: 'C' },
      { from: 'C', to: 'D' },
    ];
    const result = bfsShortestPath(connections, 'A', 'D');
    expect(result.found).toBe(true);
    expect(result.pathNodes).toEqual(['A', 'B', 'C', 'D']);
  });

  it('should return found=false when no path exists', () => {
    const connections = [
      { from: 'A', to: 'B' },
      { from: 'C', to: 'D' },
    ];
    const result = bfsShortestPath(connections, 'A', 'D');
    expect(result.found).toBe(false);
    expect(result.pathNodes).toEqual([]);
    expect(result.pathEdgeKeys).toEqual([]);
  });

  it('should handle start === end', () => {
    const connections = [{ from: 'A', to: 'B' }];
    const result = bfsShortestPath(connections, 'A', 'A');
    expect(result.found).toBe(true);
    expect(result.pathNodes).toEqual(['A']);
  });
});

describe('bfsReachable', () => {
  it('should find all connected nodes (upstream and downstream)', () => {
    const connections = [
      { from: 'A', to: 'B' },
      { from: 'B', to: 'C' },
      { from: 'D', to: 'B' },
    ];
    const result = bfsReachable('B', connections);
    expect(result.reachNodes.has('A')).toBe(true);   // upstream
    expect(result.reachNodes.has('B')).toBe(true);   // self
    expect(result.reachNodes.has('C')).toBe(true);   // downstream
    expect(result.reachNodes.has('D')).toBe(true);   // upstream
  });

  it('should return only the node itself when isolated', () => {
    const connections = [
      { from: 'X', to: 'Y' },
    ];
    const result = bfsReachable('Z', connections);
    expect(result.reachNodes.size).toBe(1);
    expect(result.reachNodes.has('Z')).toBe(true);
    expect(result.reachEdges.size).toBe(0);
  });

  it('should include edge keys for reachable connections', () => {
    const connections = [
      { from: 'A', to: 'B' },
      { from: 'B', to: 'C' },
    ];
    const result = bfsReachable('A', connections);
    expect(result.reachEdges.has('A|B')).toBe(true);
    expect(result.reachEdges.has('B|C')).toBe(true);
  });
});
