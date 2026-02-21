import React, { useMemo, useState } from 'react';
import { ROLE_COLORS, classifyRole } from '../../utils/processorRoles';

interface Processor { name: string; type: string; group: string }
interface Connection { sourceName: string; destinationName: string; relationship: string }

export interface ConnectionExplorerProps {
  processors: Processor[];
  connections: Connection[];
}

export default function ConnectionExplorer({ processors, connections }: ConnectionExplorerProps) {
  const [filterRel, setFilterRel] = useState<string>('all');
  const [hoveredPath, setHoveredPath] = useState<number | null>(null);

  const procMap = useMemo(() => {
    const m = new Map<string, Processor>();
    processors.forEach((p) => m.set(p.name, p));
    return m;
  }, [processors]);

  const relationships = useMemo(() => {
    const set = new Set<string>();
    connections.forEach((c) => set.add(c.relationship));
    return ['all', ...set];
  }, [connections]);

  const filtered = useMemo(() => {
    if (filterRel === 'all') return connections;
    return connections.filter((c) => c.relationship === filterRel);
  }, [connections, filterRel]);

  // Group by source-tier -> dest-tier
  const tierFlows = useMemo(() => {
    const flows = new Map<string, { count: number; connections: Connection[] }>();
    filtered.forEach((c) => {
      const srcProc = procMap.get(c.sourceName);
      const dstProc = procMap.get(c.destinationName);
      if (!srcProc || !dstProc) return;
      const srcRole = classifyRole(srcProc.type);
      const dstRole = classifyRole(dstProc.type);
      const key = `${srcRole}->${dstRole}`;
      if (!flows.has(key)) flows.set(key, { count: 0, connections: [] });
      const entry = flows.get(key)!;
      entry.count++;
      entry.connections.push(c);
    });
    return flows;
  }, [filtered, procMap]);

  const maxFlow = useMemo(() => Math.max(...[...tierFlows.values()].map((f) => f.count), 1), [tierFlows]);

  const TIERS = ['source', 'transform', 'process', 'route', 'sink', 'utility'];
  const TIER_Y: Record<string, number> = {};
  TIERS.forEach((t, i) => { TIER_Y[t] = 30 + i * 50; });

  return (
    <div className="rounded-lg border border-border bg-gray-900/50 p-4">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-sm font-medium text-gray-300">Connection Explorer</h3>
        <select
          value={filterRel}
          onChange={(e) => setFilterRel(e.target.value)}
          className="text-[10px] px-2 py-1 rounded bg-gray-800 border border-border text-gray-300 focus:outline-none"
        >
          {relationships.map((r) => (
            <option key={r} value={r}>{r === 'all' ? 'All Relationships' : r}</option>
          ))}
        </select>
      </div>

      <svg width="100%" height={TIERS.length * 50 + 30} viewBox={`0 0 500 ${TIERS.length * 50 + 30}`}>
        {/* Left labels */}
        {TIERS.map((t) => (
          <g key={`left-${t}`}>
            <circle cx={80} cy={TIER_Y[t]} r={6} fill={ROLE_COLORS[t]} opacity={0.8} />
            <text x={70} y={TIER_Y[t] + 4} textAnchor="end" fill="#9CA3AF" fontSize={10}>{t}</text>
          </g>
        ))}

        {/* Right labels */}
        {TIERS.map((t) => (
          <g key={`right-${t}`}>
            <circle cx={420} cy={TIER_Y[t]} r={6} fill={ROLE_COLORS[t]} opacity={0.8} />
            <text x={430} y={TIER_Y[t] + 4} textAnchor="start" fill="#9CA3AF" fontSize={10}>{t}</text>
          </g>
        ))}

        {/* Flow lines */}
        {[...tierFlows.entries()].map(([key, flow], i) => {
          const [src, dst] = key.split('->');
          const y1 = TIER_Y[src] ?? 0;
          const y2 = TIER_Y[dst] ?? 0;
          const thickness = Math.max(1, (flow.count / maxFlow) * 8);
          const isHovered = hoveredPath === i;
          return (
            <g key={key}
              onMouseEnter={() => setHoveredPath(i)}
              onMouseLeave={() => setHoveredPath(null)}
              className="cursor-pointer"
            >
              <path
                d={`M 86 ${y1} C 200 ${y1}, 300 ${y2}, 414 ${y2}`}
                fill="none"
                stroke={isHovered ? '#fff' : ROLE_COLORS[src] ?? '#6B7280'}
                strokeWidth={thickness}
                opacity={hoveredPath !== null && !isHovered ? 0.15 : 0.6}
                className="transition-all duration-200"
              />
              {isHovered && (
                <text x={250} y={(y1 + y2) / 2 - 8} textAnchor="middle" fill="#fff" fontSize={10} fontWeight="bold">
                  {flow.count} connection{flow.count !== 1 ? 's' : ''}
                </text>
              )}
            </g>
          );
        })}
      </svg>

      <p className="text-[10px] text-gray-500 mt-2">
        {filtered.length} connection{filtered.length !== 1 ? 's' : ''} across {tierFlows.size} tier paths
      </p>
    </div>
  );
}
