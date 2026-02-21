import React, { useState, useMemo } from 'react';
import { ROLE_COLORS, classifyRole } from '../../utils/processorRoles';
import { normalizeConfidence } from '../../utils/confidence';

interface Processor { name: string; type: string; group: string }
interface Mapping { name: string; type: string; role: string; mapped: boolean; confidence: number }

export interface ProcessGroupDrillDownProps {
  processors: Processor[];
  mappings?: Mapping[];
  onSelectProcessor?: (name: string) => void;
}

export default function ProcessGroupDrillDown({ processors, mappings, onSelectProcessor }: ProcessGroupDrillDownProps) {
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [search, setSearch] = useState('');

  const mappingMap = useMemo(() => {
    const m = new Map<string, Mapping>();
    mappings?.forEach((mp) => m.set(mp.name, mp));
    return m;
  }, [mappings]);

  const groups = useMemo(() => {
    const gmap = new Map<string, Processor[]>();
    processors.forEach((p) => {
      const g = p.group || 'Default';
      if (!gmap.has(g)) gmap.set(g, []);
      gmap.get(g)!.push(p);
    });
    return gmap;
  }, [processors]);

  const filteredGroups = useMemo(() => {
    if (!search) return groups;
    const lower = search.toLowerCase();
    const filtered = new Map<string, Processor[]>();
    groups.forEach((procs, name) => {
      const matchingProcs = procs.filter((p) =>
        p.name.toLowerCase().includes(lower) ||
        p.type.toLowerCase().includes(lower) ||
        name.toLowerCase().includes(lower)
      );
      if (matchingProcs.length > 0) filtered.set(name, matchingProcs);
    });
    return filtered;
  }, [groups, search]);

  const toggleGroup = (name: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      next.has(name) ? next.delete(name) : next.add(name);
      return next;
    });
  };

  const expandAll = () => setExpanded(new Set(filteredGroups.keys()));
  const collapseAll = () => setExpanded(new Set());

  return (
    <div className="rounded-lg border border-border bg-gray-900/50 p-4">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-sm font-medium text-gray-300">Process Groups</h3>
        <div className="flex items-center gap-2">
          <input
            type="text"
            placeholder="Filter..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="px-2 py-1 rounded bg-gray-800 border border-border text-xs text-gray-300 placeholder-gray-500 w-36 focus:outline-none"
          />
          <button onClick={expandAll} className="text-[10px] text-gray-500 hover:text-gray-300 transition">Expand All</button>
          <button onClick={collapseAll} className="text-[10px] text-gray-500 hover:text-gray-300 transition">Collapse All</button>
        </div>
      </div>

      <div className="space-y-1">
        {[...filteredGroups.entries()].map(([name, procs]) => {
          const isOpen = expanded.has(name);
          return (
            <div key={name} className="rounded-lg border border-border bg-gray-800/30 overflow-hidden">
              {/* Group header */}
              <button
                onClick={() => toggleGroup(name)}
                className="w-full flex items-center justify-between px-3 py-2 hover:bg-gray-800/50 transition"
              >
                <div className="flex items-center gap-2">
                  <span className="text-[10px] text-gray-500">{isOpen ? '\u25BC' : '\u25B6'}</span>
                  <span className="text-sm text-gray-200 font-medium">{name}</span>
                </div>
                <span className="text-[10px] bg-gray-700 text-gray-400 rounded-full px-2 py-0.5 tabular-nums">
                  {procs.length}
                </span>
              </button>

              {/* Processor list */}
              {isOpen && (
                <div className="border-t border-border divide-y divide-border">
                  {procs.map((p) => {
                    const m = mappingMap.get(p.name);
                    const role = m?.role ?? classifyRole(p.type);
                    const conf = m ? normalizeConfidence(m.confidence) : 0;
                    const isMapped = m?.mapped ?? false;
                    return (
                      <div
                        key={p.name}
                        className="flex items-center gap-3 px-4 py-2 hover:bg-gray-800/40 cursor-pointer transition"
                        onClick={() => onSelectProcessor?.(p.name)}
                      >
                        {/* Role dot */}
                        <span
                          className="w-2.5 h-2.5 rounded-full shrink-0"
                          style={{ backgroundColor: ROLE_COLORS[role] }}
                          title={role}
                        />
                        {/* Name & type */}
                        <div className="flex-1 min-w-0">
                          <p className="text-xs text-gray-200 truncate">{p.name}</p>
                          <p className="text-[10px] text-gray-500 font-mono truncate">{p.type}</p>
                        </div>
                        {/* Type badge */}
                        <span className="text-[10px] text-gray-500 bg-gray-700/50 rounded px-1.5 py-0.5 shrink-0">{role}</span>
                        {/* Confidence bar */}
                        <div className="w-16 shrink-0">
                          <div className="h-1.5 bg-gray-700 rounded-full overflow-hidden">
                            <div
                              className={`h-full rounded-full ${conf >= 90 ? 'bg-green-500' : conf >= 70 ? 'bg-amber-500' : 'bg-red-500'}`}
                              style={{ width: `${conf}%` }}
                            />
                          </div>
                          <p className="text-[9px] text-gray-500 text-center mt-0.5 tabular-nums">{Math.round(conf)}%</p>
                        </div>
                        {/* Mapped indicator */}
                        <span className={`text-[10px] font-medium ${isMapped ? 'text-green-400' : 'text-red-400'}`}>
                          {isMapped ? 'M' : 'U'}
                        </span>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          );
        })}

        {filteredGroups.size === 0 && (
          <p className="text-sm text-gray-500 text-center py-4">No matching groups found</p>
        )}
      </div>
    </div>
  );
}
