import React from 'react';
import { useProgress } from '../../hooks/useProgress';

export default function ProgressBar() {
  const { completed, running, errored, total, percent } = useProgress();

  const color =
    errored > 0
      ? 'bg-red-500'
      : running > 0
        ? 'bg-primary'
        : completed === total
          ? 'bg-green-500'
          : 'bg-blue-500';

  return (
    <div className="h-8 shrink-0 bg-gray-900/80 border-t border-border flex items-center px-4 gap-3">
      <div className="flex-1 h-1.5 bg-gray-800 rounded-full overflow-hidden">
        <div
          className={`h-full rounded-full transition-all duration-500 ${color}`}
          style={{ width: `${percent}%` }}
        />
      </div>
      <span className="text-xs text-gray-500 tabular-nums">
        {completed}/{total} steps
      </span>
      <span className="text-xs text-gray-600 tabular-nums">
        {percent}%
      </span>
    </div>
  );
}
