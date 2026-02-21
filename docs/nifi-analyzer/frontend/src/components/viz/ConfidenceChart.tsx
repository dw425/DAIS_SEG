import React, { useMemo } from 'react';
import { usePipelineStore } from '../../store/pipeline';

interface Bin {
  label: string;
  min: number;
  max: number;
  color: string;
  bgColor: string;
}

const BINS: Bin[] = [
  { label: 'High (90-100%)',    min: 90,  max: 100, color: 'text-green-400',  bgColor: 'bg-green-500/40' },
  { label: 'Medium (70-89%)',   min: 70,  max: 89,  color: 'text-amber-400',  bgColor: 'bg-amber-500/40' },
  { label: 'Low (50-69%)',      min: 50,  max: 69,  color: 'text-orange-400', bgColor: 'bg-orange-500/40' },
  { label: 'Very Low (<50%)',   min: 0,   max: 49,  color: 'text-red-400',    bgColor: 'bg-red-500/40' },
];

export interface ConfidenceChartProps {
  onBinClick?: (min: number, max: number) => void;
}

export default function ConfidenceChart({ onBinClick }: ConfidenceChartProps) {
  const assessment = usePipelineStore((s) => s.assessment);

  const binCounts = useMemo(() => {
    const counts = BINS.map(() => 0);
    assessment?.mappings.forEach((m) => {
      const conf = m.confidence <= 1 ? m.confidence * 100 : m.confidence;
      for (let i = 0; i < BINS.length; i++) {
        if (conf >= BINS[i].min && conf <= BINS[i].max) {
          counts[i]++;
          break;
        }
      }
    });
    return counts;
  }, [assessment]);

  const total = useMemo(() => binCounts.reduce((s, c) => s + c, 0), [binCounts]);
  const maxCount = useMemo(() => Math.max(...binCounts, 1), [binCounts]);

  if (!assessment || total === 0) {
    return (
      <div className="rounded-lg border border-border bg-gray-800/30 p-6 text-center text-gray-500 text-sm">
        No confidence data. Run Step 3 (Assess) first.
      </div>
    );
  }

  return (
    <div className="rounded-lg border border-border bg-gray-900/50 p-4">
      <h3 className="text-sm font-medium text-gray-300 mb-3">Confidence Distribution</h3>
      <div className="space-y-2">
        {BINS.map((bin, i) => {
          const count = binCounts[i];
          const pct = total > 0 ? Math.round((count / total) * 100) : 0;
          const barWidth = Math.max((count / maxCount) * 100, 2);
          return (
            <div
              key={bin.label}
              className="group cursor-pointer"
              onClick={() => onBinClick?.(bin.min, bin.max)}
            >
              <div className="flex items-center justify-between mb-0.5">
                <span className={`text-xs font-medium ${bin.color}`}>{bin.label}</span>
                <span className="text-xs text-gray-500 tabular-nums">{count} ({pct}%)</span>
              </div>
              <div className="h-5 bg-gray-800/50 rounded overflow-hidden">
                <div
                  className={`h-full rounded ${bin.bgColor} group-hover:opacity-80 transition-all duration-300`}
                  style={{ width: `${barWidth}%` }}
                />
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
