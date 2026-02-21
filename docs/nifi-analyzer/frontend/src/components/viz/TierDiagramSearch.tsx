import React, { useState, useMemo, useCallback, useEffect, useRef } from 'react';

// ── Role options matching TierDiagram ──
const ROLE_OPTIONS = ['all', 'source', 'transform', 'process', 'route', 'sink', 'utility'] as const;

interface Processor {
  name: string;
  type: string;
  group: string;
}

interface Mapping {
  name: string;
  type: string;
  role: string;
  category: string;
  mapped: boolean;
  confidence: number;
}

export interface SearchFilterState {
  query: string;
  role: string;
  confidenceMin: number;
  confidenceMax: number;
  matchCount: number;
  matchedNames: Set<string>;
}

export function useSearchFilter(
  processors: Processor[],
  mappings?: Mapping[],
): SearchFilterState & {
  setQuery: (q: string) => void;
  setRole: (r: string) => void;
  setConfidenceRange: (min: number, max: number) => void;
  reset: () => void;
} {
  const [query, setQuery] = useState('');
  const [role, setRole] = useState('all');
  const [confidenceMin, setConfidenceMin] = useState(0);
  const [confidenceMax, setConfidenceMax] = useState(100);

  const mappingsByName = useMemo(() => {
    const map = new Map<string, Mapping>();
    mappings?.forEach((m) => map.set(m.name, m));
    return map;
  }, [mappings]);

  const { matchedNames, matchCount } = useMemo(() => {
    const lowerQuery = query.toLowerCase();
    const matched = new Set<string>();

    processors.forEach((p) => {
      const mapping = mappingsByName.get(p.name);
      // Text filter
      if (lowerQuery) {
        const haystack = `${p.name} ${p.type} ${p.group}`.toLowerCase();
        if (!haystack.includes(lowerQuery)) return;
      }
      // Role filter
      if (role !== 'all' && mapping) {
        if (mapping.role !== role) return;
      }
      // Confidence filter
      if (mapping) {
        const conf = mapping.confidence <= 1 ? mapping.confidence * 100 : mapping.confidence;
        if (conf < confidenceMin || conf > confidenceMax) return;
      }
      matched.add(p.name);
    });

    return { matchedNames: matched, matchCount: matched.size };
  }, [processors, mappingsByName, query, role, confidenceMin, confidenceMax]);

  const setConfidenceRange = useCallback((min: number, max: number) => {
    setConfidenceMin(min);
    setConfidenceMax(max);
  }, []);

  const reset = useCallback(() => {
    setQuery('');
    setRole('all');
    setConfidenceMin(0);
    setConfidenceMax(100);
  }, []);

  return {
    query, setQuery,
    role, setRole,
    confidenceMin, confidenceMax, setConfidenceRange,
    matchCount, matchedNames,
    reset,
  };
}

export interface TierDiagramSearchProps {
  processors: Processor[];
  mappings?: Mapping[];
  onFilterChange?: (matchedNames: Set<string>) => void;
}

export default function TierDiagramSearch({ processors, mappings, onFilterChange }: TierDiagramSearchProps) {
  const filter = useSearchFilter(processors, mappings);
  const prevRef = useRef<Set<string>>(filter.matchedNames);

  useEffect(() => {
    if (prevRef.current !== filter.matchedNames) {
      prevRef.current = filter.matchedNames;
      onFilterChange?.(filter.matchedNames);
    }
  }, [filter.matchedNames, onFilterChange]);

  const hasFilters = filter.query || filter.role !== 'all' || filter.confidenceMin > 0 || filter.confidenceMax < 100;

  return (
    <div className="flex items-center gap-3 flex-wrap rounded-lg bg-gray-800/50 border border-border px-4 py-3 mb-4">
      {/* Search input */}
      <div className="relative flex-1 min-w-[200px]">
        <input
          type="text"
          placeholder="Search processors by name, type, or group..."
          value={filter.query}
          onChange={(e) => filter.setQuery(e.target.value)}
          className="w-full px-3 py-1.5 rounded-lg bg-gray-900 border border-border text-sm text-gray-200 placeholder-gray-500 focus:outline-none focus:border-gray-600"
        />
      </div>

      {/* Role filter */}
      <select
        value={filter.role}
        onChange={(e) => filter.setRole(e.target.value)}
        className="px-2 py-1.5 rounded-lg bg-gray-900 border border-border text-sm text-gray-300 focus:outline-none"
      >
        {ROLE_OPTIONS.map((r) => (
          <option key={r} value={r}>{r === 'all' ? 'All Roles' : r.charAt(0).toUpperCase() + r.slice(1)}</option>
        ))}
      </select>

      {/* Confidence range */}
      <div className="flex items-center gap-2 text-xs text-gray-400">
        <span>Conf:</span>
        <input
          type="number" min={0} max={100} step={10}
          value={filter.confidenceMin}
          onChange={(e) => filter.setConfidenceRange(Number(e.target.value), filter.confidenceMax)}
          className="w-14 px-1.5 py-1 rounded bg-gray-900 border border-border text-sm text-gray-300 text-center"
        />
        <span>-</span>
        <input
          type="number" min={0} max={100} step={10}
          value={filter.confidenceMax}
          onChange={(e) => filter.setConfidenceRange(filter.confidenceMin, Number(e.target.value))}
          className="w-14 px-1.5 py-1 rounded bg-gray-900 border border-border text-sm text-gray-300 text-center"
        />
        <span>%</span>
      </div>

      {/* Match counter */}
      <span className="text-xs text-gray-500 tabular-nums whitespace-nowrap">
        {filter.matchCount} match{filter.matchCount !== 1 ? 'es' : ''}
      </span>

      {/* Reset */}
      {hasFilters && (
        <button onClick={filter.reset} className="text-xs text-gray-500 hover:text-gray-300 transition">
          Clear
        </button>
      )}
    </div>
  );
}
