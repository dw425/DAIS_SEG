import React, { useMemo } from 'react';
import { usePipelineStore } from '../../store/pipeline';

const PHASE_COLORS: Record<string, { bg: string; border: string; text: string }> = {
  setup:     { bg: 'bg-blue-500/30',   border: 'border-blue-500/40',   text: 'text-blue-300' },
  automated: { bg: 'bg-green-500/30',  border: 'border-green-500/40',  text: 'text-green-300' },
  manual:    { bg: 'bg-amber-500/30',  border: 'border-amber-500/40',  text: 'text-amber-300' },
  testing:   { bg: 'bg-purple-500/30', border: 'border-purple-500/40', text: 'text-purple-300' },
  deploy:    { bg: 'bg-cyan-500/30',   border: 'border-cyan-500/40',   text: 'text-cyan-300' },
  default:   { bg: 'bg-gray-500/30',   border: 'border-gray-500/40',   text: 'text-gray-300' },
};

function guessPhaseType(phase: string): string {
  const lower = phase.toLowerCase();
  if (/setup|init|prep|config/i.test(lower)) return 'setup';
  if (/auto|convert|generat/i.test(lower)) return 'automated';
  if (/manual|custom|hand/i.test(lower)) return 'manual';
  if (/test|valid|verif|qa/i.test(lower)) return 'testing';
  if (/deploy|release|ship|prod/i.test(lower)) return 'deploy';
  return 'default';
}

export default function MigrationTimeline() {
  const report = usePipelineStore((s) => s.report);
  const valueAnalysis = usePipelineStore((s) => s.valueAnalysis);

  const phases = useMemo(() => {
    // Prefer report.estimatedTimeline, fall back to valueAnalysis.implementationRoadmap
    if (report?.estimatedTimeline && report.estimatedTimeline.length > 0) {
      return report.estimatedTimeline.map((p) => ({
        label: p.phase,
        weeks: p.weeks,
        type: guessPhaseType(p.phase),
      }));
    }
    if (valueAnalysis?.implementationRoadmap && valueAnalysis.implementationRoadmap.length > 0) {
      return valueAnalysis.implementationRoadmap.map((p) => ({
        label: p.phase,
        weeks: p.weeks,
        type: guessPhaseType(p.phase),
      }));
    }
    return [];
  }, [report, valueAnalysis]);

  const totalWeeks = useMemo(() => phases.reduce((s, p) => s + p.weeks, 0), [phases]);
  const maxWeeks = useMemo(() => Math.max(...phases.map((p) => p.weeks), 1), [phases]);

  if (phases.length === 0) {
    return (
      <div className="rounded-lg border border-border bg-gray-800/30 p-6 text-center text-gray-500 text-sm">
        No timeline data available. Run Step 5 (Report) first.
      </div>
    );
  }

  // Compute cumulative start weeks
  let cumulativeWeek = 0;
  const phasesWithStart = phases.map((p) => {
    const start = cumulativeWeek;
    cumulativeWeek += p.weeks;
    return { ...p, startWeek: start, endWeek: start + p.weeks };
  });

  return (
    <div className="rounded-lg border border-border bg-gray-900/50 p-4">
      <h3 className="text-sm font-medium text-gray-300 mb-4">Migration Timeline</h3>

      <div className="space-y-2">
        {phasesWithStart.map((phase, i) => {
          const colors = PHASE_COLORS[phase.type] || PHASE_COLORS.default;
          const widthPct = Math.max((phase.weeks / totalWeeks) * 100, 10);
          return (
            <div key={i} className="flex items-center gap-3">
              {/* Phase label */}
              <div className="w-40 shrink-0 text-right">
                <span className={`text-xs font-medium ${colors.text}`}>{phase.label}</span>
              </div>

              {/* Bar container */}
              <div className="flex-1 relative">
                <div className="h-8 bg-gray-800/50 rounded-lg overflow-hidden">
                  <div
                    className={`h-full rounded-lg ${colors.bg} border ${colors.border} flex items-center px-2 transition-all duration-500`}
                    style={{ width: `${widthPct}%`, marginLeft: `${(phase.startWeek / totalWeeks) * 100}%` }}
                  >
                    <span className="text-[10px] text-gray-300 whitespace-nowrap tabular-nums">
                      {phase.weeks}w
                    </span>
                  </div>
                </div>
              </div>

              {/* Week labels */}
              <div className="w-24 shrink-0 text-xs text-gray-500 tabular-nums">
                W{phase.startWeek + 1} - W{phase.endWeek}
              </div>
            </div>
          );
        })}
      </div>

      {/* Total */}
      <div className="mt-4 pt-3 border-t border-border flex items-center justify-between">
        <span className="text-xs text-gray-500">Total Duration</span>
        <span className="text-sm font-medium text-gray-200 tabular-nums">{totalWeeks} weeks</span>
      </div>

      {/* Legend */}
      <div className="flex items-center gap-3 mt-3 flex-wrap">
        {Object.entries(PHASE_COLORS).filter(([k]) => k !== 'default').map(([key, colors]) => (
          <span key={key} className="flex items-center gap-1 text-[10px] text-gray-500">
            <span className={`w-3 h-3 rounded ${colors.bg} border ${colors.border}`} />
            {key.charAt(0).toUpperCase() + key.slice(1)}
          </span>
        ))}
      </div>
    </div>
  );
}
