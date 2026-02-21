import React from 'react';
import { useUIStore } from '../../store/ui';
import { useProgress } from '../../hooks/useProgress';

export default function PipelineStatusBadge() {
  const setActiveStep = useUIStore((s) => s.setActiveStep);
  const { completed, running, errored, total, percent } = useProgress();

  const color = errored > 0
    ? 'text-red-400 border-red-500/50'
    : running > 0
      ? 'text-amber-400 border-amber-500/50'
      : completed === total
        ? 'text-green-400 border-green-500/50'
        : 'text-gray-400 border-gray-600';

  // SVG arc calculation
  const radius = 10;
  const circumference = 2 * Math.PI * radius;
  const offset = circumference - (percent / 100) * circumference;

  const arcColor = errored > 0 ? '#f87171' : running > 0 ? '#fbbf24' : completed === total ? '#4ade80' : '#6b7280';

  return (
    <button
      onClick={() => setActiveStep(8)}
      className={`flex items-center gap-2 px-3 py-1.5 rounded-lg border ${color} bg-gray-800/50
        hover:bg-gray-800 transition text-xs font-medium`}
      title="Go to Summary"
    >
      <svg width="24" height="24" className="shrink-0">
        <circle cx="12" cy="12" r={radius} fill="none" stroke="#374151" strokeWidth="2" />
        <circle
          cx="12"
          cy="12"
          r={radius}
          fill="none"
          stroke={arcColor}
          strokeWidth="2"
          strokeLinecap="round"
          strokeDasharray={circumference}
          strokeDashoffset={offset}
          transform="rotate(-90 12 12)"
          className="transition-all duration-300"
        />
      </svg>
      <span>{completed}/{total} complete</span>
    </button>
  );
}
