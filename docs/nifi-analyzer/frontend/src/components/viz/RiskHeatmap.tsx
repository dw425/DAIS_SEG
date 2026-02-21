import React, { useMemo, useState } from 'react';
import { usePipelineStore } from '../../store/pipeline';
import { classifyRole } from '../../utils/processorRoles';
import { normalizeConfidence } from '../../utils/confidence';

const TIERS = ['source', 'transform', 'process', 'route', 'sink', 'utility'] as const;

function confColor(avg: number): string {
  if (avg >= 90) return 'bg-green-500/40 text-green-300';
  if (avg >= 70) return 'bg-yellow-500/40 text-yellow-300';
  if (avg > 0) return 'bg-red-500/40 text-red-300';
  return 'bg-gray-800/30 text-gray-600';
}

export interface RiskHeatmapProps {
  onCellClick?: (group: string, tier: string) => void;
}

export default function RiskHeatmap({ onCellClick }: RiskHeatmapProps) {
  const parsed = usePipelineStore((s) => s.parsed);
  const assessment = usePipelineStore((s) => s.assessment);
  const [hoveredCell, setHoveredCell] = useState<{ group: string; tier: string } | null>(null);

  const { groups, grid, truncated } = useMemo(() => {
    const mappingMap = new Map<string, { role: string; confidence: number }>();
    assessment?.mappings.forEach((m) => mappingMap.set(m.name, { role: m.role, confidence: m.confidence }));

    // Group processors
    const groupSet = new Set<string>();
    parsed?.processors.forEach((p) => groupSet.add(p.group || 'Default'));
    const allGroups = [...groupSet];
    const truncated = allGroups.length > 20;
    const groups = allGroups.slice(0, 20);

    // Build grid: group -> tier -> { sum, count }
    const grid = new Map<string, Map<string, { sum: number; count: number }>>();
    groups.forEach((g) => {
      const tierMap = new Map<string, { sum: number; count: number }>();
      TIERS.forEach((t) => tierMap.set(t, { sum: 0, count: 0 }));
      grid.set(g, tierMap);
    });

    parsed?.processors.forEach((p) => {
      const g = p.group || 'Default';
      if (!grid.has(g)) return;
      const m = mappingMap.get(p.name);
      const role = m?.role ?? classifyRole(p.type);
      const conf = m ? normalizeConfidence(m.confidence) : 0;
      const cell = grid.get(g)?.get(role);
      if (cell) {
        cell.sum += conf;
        cell.count += 1;
      }
    });

    return { groups, grid, truncated };
  }, [parsed, assessment]);

  if (!parsed || groups.length === 0) {
    return (
      <div className="rounded-lg border border-border bg-gray-800/30 p-6 text-center text-gray-500 text-sm">
        No data available for risk heatmap
      </div>
    );
  }

  return (
    <div className="rounded-lg border border-border bg-gray-900/50 p-4 overflow-x-auto">
      <div className="flex items-center gap-2 mb-3">
        <h3 className="text-sm font-medium text-gray-300">Risk Heatmap</h3>
        {truncated && (
          <span className="text-[10px] text-amber-400 bg-amber-500/10 rounded px-1.5 py-0.5">(showing top 20)</span>
        )}
      </div>

      <div className="inline-grid gap-1" style={{ gridTemplateColumns: `120px repeat(${groups.length}, minmax(60px, 1fr))` }}>
        {/* Header row */}
        <div className="text-xs text-gray-500" />
        {groups.map((g) => (
          <div key={g} className="text-[10px] text-gray-400 text-center truncate px-1" title={g}>
            {g.length > 10 ? g.slice(0, 9) + '..' : g}
          </div>
        ))}

        {/* Tier rows */}
        {TIERS.map((tier) => (
          <React.Fragment key={tier}>
            <div className="text-xs text-gray-400 capitalize flex items-center">{tier}</div>
            {groups.map((g) => {
              const cell = grid.get(g)?.get(tier);
              const avg = cell && cell.count > 0 ? Math.round(cell.sum / cell.count) : 0;
              const count = cell?.count ?? 0;
              const isHovered = hoveredCell?.group === g && hoveredCell?.tier === tier;
              return (
                <div
                  key={`${g}-${tier}`}
                  className={`relative rounded text-center text-[10px] font-medium py-2 cursor-pointer transition
                    ${confColor(count > 0 ? avg : -1)} ${isHovered ? 'ring-1 ring-white/30' : ''}`}
                  onClick={() => onCellClick?.(g, tier)}
                  onMouseEnter={() => setHoveredCell({ group: g, tier })}
                  onMouseLeave={() => setHoveredCell(null)}
                  title={`${g} / ${tier}: ${count} processors, avg ${avg}%`}
                >
                  {count > 0 ? `${avg}%` : '-'}
                  {isHovered && count > 0 && (
                    <div className="absolute z-20 bottom-full left-1/2 -translate-x-1/2 mb-1 px-2 py-1 rounded bg-gray-800 border border-border text-[10px] text-gray-300 whitespace-nowrap shadow-lg pointer-events-none">
                      {count} processor{count !== 1 ? 's' : ''}, avg {avg}%
                    </div>
                  )}
                </div>
              );
            })}
          </React.Fragment>
        ))}
      </div>

      {/* Legend */}
      <div className="flex items-center gap-3 mt-3 text-[10px] text-gray-500">
        <span className="flex items-center gap-1"><span className="w-3 h-3 rounded bg-green-500/40" /> 90%+</span>
        <span className="flex items-center gap-1"><span className="w-3 h-3 rounded bg-yellow-500/40" /> 70-89%</span>
        <span className="flex items-center gap-1"><span className="w-3 h-3 rounded bg-red-500/40" /> &lt;70%</span>
        <span className="flex items-center gap-1"><span className="w-3 h-3 rounded bg-gray-800/30 border border-border" /> Empty</span>
      </div>
    </div>
  );
}
