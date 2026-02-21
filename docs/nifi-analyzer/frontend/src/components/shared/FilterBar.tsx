import React, { useState, useEffect, useMemo, useCallback } from 'react';

export interface FilterableItem {
  name?: string;
  type?: string;
  role?: string;
  category?: string;
  mapped?: boolean;
  confidence?: number;
  [key: string]: unknown;
}

interface FilterBarProps {
  items: FilterableItem[];
  onFilter: (filtered: FilterableItem[]) => void;
}

const ROLES = ['all', 'source', 'transform', 'process', 'route', 'sink', 'utility'];
const MAPPED_OPTIONS = ['all', 'mapped', 'unmapped'] as const;

export default function FilterBar({ items, onFilter }: FilterBarProps) {
  const [search, setSearch] = useState('');
  const [minConfidence, setMinConfidence] = useState(0);
  const [maxConfidence, setMaxConfidence] = useState(100);
  const [role, setRole] = useState('all');
  const [mappedFilter, setMappedFilter] = useState<typeof MAPPED_OPTIONS[number]>('all');

  const filtered = useMemo(() => {
    return items.filter((item) => {
      // Text search
      if (search) {
        const q = search.toLowerCase();
        const text = [item.name, item.type, item.category, item.role]
          .filter(Boolean)
          .join(' ')
          .toLowerCase();
        if (!text.includes(q)) return false;
      }

      // Confidence range
      if (typeof item.confidence === 'number') {
        if (item.confidence < minConfidence || item.confidence > maxConfidence) return false;
      }

      // Role filter
      if (role !== 'all' && item.role) {
        if (item.role.toLowerCase() !== role) return false;
      }

      // Mapped status
      if (mappedFilter !== 'all' && typeof item.mapped === 'boolean') {
        if (mappedFilter === 'mapped' && !item.mapped) return false;
        if (mappedFilter === 'unmapped' && item.mapped) return false;
      }

      return true;
    });
  }, [items, search, minConfidence, maxConfidence, role, mappedFilter]);

  const stableOnFilter = useCallback(onFilter, [onFilter]);

  useEffect(() => {
    stableOnFilter(filtered);
  }, [filtered, stableOnFilter]);

  return (
    <div className="flex flex-wrap items-center gap-3 p-3 bg-gray-900/50 border border-border rounded-lg">
      {/* Search */}
      <div className="flex-1 min-w-[180px]">
        <input
          type="text"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Search..."
          className="w-full px-3 py-1.5 rounded-lg bg-gray-800 border border-border text-sm text-gray-200
            placeholder:text-gray-600 focus:outline-none focus:border-primary transition"
        />
      </div>

      {/* Confidence range */}
      <div className="flex items-center gap-2 text-xs text-gray-400">
        <span>Confidence:</span>
        <input
          type="range"
          min={0}
          max={100}
          value={minConfidence}
          onChange={(e) => setMinConfidence(Number(e.target.value))}
          className="w-16 accent-primary"
        />
        <span className="w-8 text-center text-gray-300">{minConfidence}</span>
        <span>-</span>
        <input
          type="range"
          min={0}
          max={100}
          value={maxConfidence}
          onChange={(e) => setMaxConfidence(Number(e.target.value))}
          className="w-16 accent-primary"
        />
        <span className="w-8 text-center text-gray-300">{maxConfidence}</span>
      </div>

      {/* Role dropdown */}
      <select
        value={role}
        onChange={(e) => setRole(e.target.value)}
        className="px-3 py-1.5 rounded-lg bg-gray-800 border border-border text-sm text-gray-200
          focus:outline-none focus:border-primary transition"
      >
        {ROLES.map((r) => (
          <option key={r} value={r}>{r === 'all' ? 'All Roles' : r.charAt(0).toUpperCase() + r.slice(1)}</option>
        ))}
      </select>

      {/* Mapped toggle */}
      <div className="flex rounded-lg border border-border overflow-hidden">
        {MAPPED_OPTIONS.map((opt) => (
          <button
            key={opt}
            onClick={() => setMappedFilter(opt)}
            className={`px-3 py-1.5 text-xs font-medium transition
              ${mappedFilter === opt ? 'bg-primary text-white' : 'bg-gray-800 text-gray-400 hover:text-gray-200'}`}
          >
            {opt === 'all' ? 'All' : opt.charAt(0).toUpperCase() + opt.slice(1)}
          </button>
        ))}
      </div>

      {/* Count */}
      <span className="text-xs text-gray-500">
        {filtered.length} of {items.length} items
      </span>
    </div>
  );
}
