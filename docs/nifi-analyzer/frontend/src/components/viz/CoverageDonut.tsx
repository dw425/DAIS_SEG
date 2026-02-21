import React, { useMemo } from 'react';
import { usePipelineStore } from '../../store/pipeline';

interface Segment {
  label: string;
  count: number;
  color: string;
  strokeColor: string;
}

export default function CoverageDonut() {
  const assessment = usePipelineStore((s) => s.assessment);

  const { segments, total, coveragePct } = useMemo(() => {
    if (!assessment?.mappings?.length) {
      return { segments: [] as Segment[], total: 0, coveragePct: 0 };
    }

    let mapped = 0;
    let unmapped = 0;
    let lowConf = 0;

    assessment.mappings.forEach((m) => {
      const conf = m.confidence <= 1 ? m.confidence * 100 : m.confidence;
      if (!m.mapped) {
        unmapped++;
      } else if (conf < 70) {
        lowConf++;
      } else {
        mapped++;
      }
    });

    const total = mapped + unmapped + lowConf;
    const coveragePct = total > 0 ? Math.round((mapped / total) * 100) : 0;

    const segments: Segment[] = [
      { label: 'Mapped', count: mapped, color: '#22C55E', strokeColor: 'text-green-400' },
      { label: 'Low Confidence', count: lowConf, color: '#EAB308', strokeColor: 'text-amber-400' },
      { label: 'Unmapped', count: unmapped, color: '#EF4444', strokeColor: 'text-red-400' },
    ];

    return { segments, total, coveragePct };
  }, [assessment]);

  if (total === 0) {
    return (
      <div className="rounded-lg border border-border bg-gray-800/30 p-6 text-center text-gray-500 text-sm">
        No mapping data. Run Step 3 (Assess) first.
      </div>
    );
  }

  const radius = 40;
  const circumference = 2 * Math.PI * radius;
  let accumulated = 0;

  return (
    <div className="rounded-lg border border-border bg-gray-900/50 p-4 flex flex-col items-center">
      <h3 className="text-sm font-medium text-gray-300 mb-3 self-start">Coverage</h3>

      <svg width={100} height={100} className="-rotate-90">
        <circle cx={50} cy={50} r={radius} fill="none" stroke="#1f2937" strokeWidth={10} />
        {segments.filter((s) => s.count > 0).map((seg) => {
          const segLen = (seg.count / total) * circumference;
          const offset = circumference - accumulated;
          accumulated += segLen;
          return (
            <circle
              key={seg.label}
              cx={50} cy={50} r={radius}
              fill="none"
              stroke={seg.color}
              strokeWidth={10}
              strokeLinecap="round"
              strokeDasharray={`${segLen} ${circumference - segLen}`}
              strokeDashoffset={offset}
              className="transition-all duration-1000"
            />
          );
        })}
      </svg>

      <span className="text-xl font-bold text-gray-200 -mt-[64px] mb-[40px]">{coveragePct}%</span>

      {/* Legend */}
      <div className="flex items-center gap-4 mt-2">
        {segments.map((seg) => (
          <span key={seg.label} className="flex items-center gap-1 text-[10px] text-gray-400">
            <span className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: seg.color }} />
            {seg.label} ({seg.count})
          </span>
        ))}
      </div>
    </div>
  );
}
