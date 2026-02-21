import React, { useState, useEffect, useRef, useMemo } from 'react';
import { useUIStore } from '../../store/ui';
import { usePipelineStore } from '../../store/pipeline';

interface SearchResult {
  category: string;
  icon: string;
  name: string;
  type: string;
  tag: string;
  targetStep: number;
}

const CATEGORY_ORDER = ['Processors', 'Mappings', 'Security Findings', 'Gaps'];

export default function SearchOverlay() {
  const searchOpen = useUIStore((s) => s.searchOpen);
  const setSearchOpen = useUIStore((s) => s.setSearchOpen);
  const setActiveStep = useUIStore((s) => s.setActiveStep);
  const pipeline = usePipelineStore();

  const [query, setQuery] = useState('');
  const [focusedIndex, setFocusedIndex] = useState(-1);
  const inputRef = useRef<HTMLInputElement>(null);
  const resultsRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (searchOpen) {
      setQuery('');
      setTimeout(() => inputRef.current?.focus(), 50);
    }
  }, [searchOpen]);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && searchOpen) {
        setSearchOpen(false);
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [searchOpen, setSearchOpen]);

  const allResults = useMemo<SearchResult[]>(() => {
    const results: SearchResult[] = [];

    // Processors from parsed data
    if (pipeline.parsed?.processors) {
      for (const p of pipeline.parsed.processors) {
        results.push({
          category: 'Processors',
          icon: '\u2699',
          name: p.name,
          type: p.type,
          tag: p.platform || 'processor',
          targetStep: 0,
        });
      }
    }

    // Mappings from assessment
    if (pipeline.assessment?.mappings) {
      for (const m of pipeline.assessment.mappings) {
        results.push({
          category: 'Mappings',
          icon: '\u2194',
          name: m.name,
          type: m.type,
          tag: m.category,
          targetStep: 2,
        });
      }
    }

    // Security findings from analysis
    if (pipeline.analysis?.securityFindings) {
      for (const f of pipeline.analysis.securityFindings) {
        const finding = f as Record<string, unknown>;
        results.push({
          category: 'Security Findings',
          icon: '\u26A0',
          name: String(finding.title || finding.name || finding.type || 'Finding'),
          type: String(finding.severity || finding.type || 'security'),
          tag: 'security',
          targetStep: 1,
        });
      }
    }

    // Gap playbook from report
    if (pipeline.report?.gapPlaybook) {
      for (const g of pipeline.report.gapPlaybook) {
        results.push({
          category: 'Gaps',
          icon: '\u2716',
          name: g.processor,
          type: g.severity,
          tag: g.gap,
          targetStep: 4,
        });
      }
    }

    return results;
  }, [pipeline.parsed, pipeline.assessment, pipeline.analysis, pipeline.report]);

  const filtered = useMemo(() => {
    if (!query.trim()) return allResults;
    const q = query.toLowerCase();
    return allResults.filter(
      (r) =>
        r.name.toLowerCase().includes(q) ||
        r.type.toLowerCase().includes(q) ||
        r.tag.toLowerCase().includes(q) ||
        r.category.toLowerCase().includes(q),
    );
  }, [allResults, query]);

  const grouped = useMemo(() => {
    const map = new Map<string, SearchResult[]>();
    for (const cat of CATEGORY_ORDER) {
      const items = filtered.filter((r) => r.category === cat);
      if (items.length > 0) map.set(cat, items.slice(0, 10));
    }
    return map;
  }, [filtered]);

  // Flat list of visible results for keyboard navigation
  const flatResults = useMemo(() => {
    const list: SearchResult[] = [];
    for (const [, items] of grouped) {
      list.push(...items);
    }
    return list;
  }, [grouped]);

  // Reset focused index when query or results change
  useEffect(() => {
    setFocusedIndex(-1);
  }, [query, flatResults.length]);

  const handleSelect = (result: SearchResult) => {
    setActiveStep(result.targetStep);
    setSearchOpen(false);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setFocusedIndex((prev) => (prev < flatResults.length - 1 ? prev + 1 : 0));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setFocusedIndex((prev) => (prev > 0 ? prev - 1 : flatResults.length - 1));
    } else if (e.key === 'Enter' && focusedIndex >= 0 && focusedIndex < flatResults.length) {
      e.preventDefault();
      handleSelect(flatResults[focusedIndex]);
    }
  };

  // Scroll focused item into view
  useEffect(() => {
    if (focusedIndex >= 0 && resultsRef.current) {
      const buttons = resultsRef.current.querySelectorAll('[data-search-item]');
      buttons[focusedIndex]?.scrollIntoView({ block: 'nearest' });
    }
  }, [focusedIndex]);

  if (!searchOpen) return null;

  return (
    <div className="fixed inset-0 z-[200] flex items-start justify-center pt-[15vh]">
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" onClick={() => setSearchOpen(false)} />

      {/* Modal */}
      <div className="relative w-full max-w-lg bg-gray-900 border border-border rounded-xl shadow-2xl overflow-hidden" onKeyDown={handleKeyDown}>
        {/* Search input */}
        <div className="flex items-center gap-3 px-4 py-3 border-b border-border">
          <svg className="w-5 h-5 text-gray-500 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
          </svg>
          <input
            ref={inputRef}
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search processors, mappings, findings, gaps..."
            className="flex-1 bg-transparent text-gray-100 text-sm outline-none placeholder:text-gray-500"
          />
          <kbd className="hidden sm:inline-block px-1.5 py-0.5 text-[10px] font-mono bg-gray-800 text-gray-500 rounded border border-gray-700">
            ESC
          </kbd>
        </div>

        {/* Results */}
        <div ref={resultsRef} className="max-h-80 overflow-y-auto p-2">
          {grouped.size === 0 ? (
            <div className="py-8 text-center text-sm text-gray-500">
              {allResults.length === 0
                ? 'No pipeline data loaded. Run the pipeline first.'
                : 'No results found.'}
            </div>
          ) : (
            (() => {
              let flatIdx = 0;
              return Array.from(grouped.entries()).map(([category, items]) => (
                <div key={category} className="mb-3">
                  <div className="px-2 py-1 text-[10px] font-semibold uppercase tracking-wider text-gray-500">
                    {category} ({items.length})
                  </div>
                  {items.map((item, i) => {
                    const currentIdx = flatIdx++;
                    return (
                      <button
                        key={`${category}-${i}`}
                        data-search-item
                        onClick={() => handleSelect(item)}
                        className={`w-full flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition text-left ${
                          currentIdx === focusedIndex ? 'bg-gray-800 ring-1 ring-primary/50' : 'hover:bg-gray-800'
                        }`}
                      >
                        <span className="text-base w-5 text-center shrink-0">{item.icon}</span>
                        <span className="flex-1 text-gray-200 truncate">{item.name}</span>
                        <span className="px-1.5 py-0.5 rounded text-[10px] bg-gray-800 text-gray-400 border border-gray-700">
                          {item.type}
                        </span>
                        <span className="px-1.5 py-0.5 rounded text-[10px] bg-primary/10 text-primary">
                          {item.tag}
                        </span>
                      </button>
                    );
                  })}
                </div>
              ));
            })()
          )}
        </div>

        {/* Footer hint */}
        <div className="px-4 py-2 border-t border-border text-[10px] text-gray-600 flex gap-4">
          <span><kbd className="px-1 bg-gray-800 rounded">&#x2191;&#x2193;</kbd> Navigate</span>
          <span><kbd className="px-1 bg-gray-800 rounded">&#x21B5;</kbd> Select</span>
          <span><kbd className="px-1 bg-gray-800 rounded">Esc</kbd> Close</span>
        </div>
      </div>
    </div>
  );
}
