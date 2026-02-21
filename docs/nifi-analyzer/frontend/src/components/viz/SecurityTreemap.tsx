import React, { useMemo, useState } from 'react';
import { usePipelineStore } from '../../store/pipeline';

const SEVERITY_COLORS: Record<string, { bg: string; border: string; text: string }> = {
  critical: { bg: 'bg-red-500/30',    border: 'border-red-500/40',    text: 'text-red-300' },
  high:     { bg: 'bg-orange-500/30', border: 'border-orange-500/40', text: 'text-orange-300' },
  medium:   { bg: 'bg-yellow-500/30', border: 'border-yellow-500/40', text: 'text-yellow-300' },
  low:      { bg: 'bg-green-500/30',  border: 'border-green-500/40',  text: 'text-green-300' },
};

interface CategoryData {
  category: string;
  severity: string;
  count: number;
  findings: Array<Record<string, unknown>>;
}

export default function SecurityTreemap() {
  const analysis = usePipelineStore((s) => s.analysis);
  const [expandedCategory, setExpandedCategory] = useState<string | null>(null);

  const categories = useMemo(() => {
    if (!analysis?.securityFindings?.length) return [];

    const catMap = new Map<string, CategoryData>();
    analysis.securityFindings.forEach((f) => {
      const finding = f as Record<string, unknown>;
      const cat = String(finding.type ?? finding.category ?? 'Unknown');
      const sev = String(finding.severity ?? 'medium').toLowerCase();
      if (!catMap.has(cat)) {
        catMap.set(cat, { category: cat, severity: sev, count: 0, findings: [] });
      }
      const entry = catMap.get(cat)!;
      entry.count++;
      entry.findings.push(finding);
      // Keep highest severity
      const sevOrder = ['critical', 'high', 'medium', 'low'];
      if (sevOrder.indexOf(sev) < sevOrder.indexOf(entry.severity)) {
        entry.severity = sev;
      }
    });

    return [...catMap.values()].sort((a, b) => b.count - a.count);
  }, [analysis]);

  const totalFindings = useMemo(() => categories.reduce((s, c) => s + c.count, 0), [categories]);

  if (categories.length === 0) {
    return (
      <div className="rounded-lg border border-border bg-gray-800/30 p-6 text-center text-gray-500 text-sm">
        No security findings. Run Step 2 (Analyze) first.
      </div>
    );
  }

  // Compute grid spans based on count proportion
  const maxCount = Math.max(...categories.map((c) => c.count));

  return (
    <div className="rounded-lg border border-border bg-gray-900/50 p-4">
      <h3 className="text-sm font-medium text-gray-300 mb-3">
        Security Findings ({totalFindings})
      </h3>

      <div className="grid grid-cols-4 gap-1.5 auto-rows-min">
        {categories.map((cat) => {
          const colors = SEVERITY_COLORS[cat.severity] ?? SEVERITY_COLORS.medium;
          const span = cat.count === maxCount ? 2 : 1;
          const isExpanded = expandedCategory === cat.category;

          return (
            <div
              key={cat.category}
              className={`rounded-lg border ${colors.border} ${colors.bg} p-3 cursor-pointer transition hover:opacity-80
                ${isExpanded ? 'col-span-4' : span === 2 ? 'col-span-2' : 'col-span-1'}`}
              onClick={() => setExpandedCategory(isExpanded ? null : cat.category)}
            >
              <div className="flex items-center justify-between">
                <span className={`text-xs font-medium ${colors.text} truncate`}>{cat.category}</span>
                <span className={`text-[10px] font-bold ${colors.text} tabular-nums`}>{cat.count}</span>
              </div>
              <span className={`text-[10px] ${colors.text} opacity-70 capitalize`}>{cat.severity}</span>

              {/* Expanded findings */}
              {isExpanded && (
                <div className="mt-2 pt-2 border-t border-white/10 space-y-1.5">
                  {cat.findings.map((f, i) => (
                    <div key={i} className="text-[10px] text-gray-300 bg-gray-900/40 rounded px-2 py-1.5">
                      <span className="font-medium">{String(f.processor ?? 'Unknown')}: </span>
                      {String(f.finding ?? f.description ?? '')}
                    </div>
                  ))}
                </div>
              )}
            </div>
          );
        })}
      </div>

      {/* Legend */}
      <div className="flex items-center gap-3 mt-3 text-[10px] text-gray-500">
        {Object.entries(SEVERITY_COLORS).map(([sev, colors]) => (
          <span key={sev} className="flex items-center gap-1">
            <span className={`w-3 h-3 rounded ${colors.bg} border ${colors.border}`} />
            {sev.charAt(0).toUpperCase() + sev.slice(1)}
          </span>
        ))}
      </div>
    </div>
  );
}
