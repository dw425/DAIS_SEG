import React from 'react';
import type { StepStatus } from '../../store/ui';

interface StepIndicatorProps {
  stepNumber: number;
  label: string;
  status: StepStatus;
  active: boolean;
  disabled: boolean;
  onClick: () => void;
}

const STATUS_ICON: Record<StepStatus, React.ReactNode> = {
  idle: (
    <div className="w-6 h-6 rounded-full border-2 border-gray-600 flex items-center justify-center text-xs text-gray-500" />
  ),
  running: (
    <div className="w-6 h-6 rounded-full border-2 border-primary flex items-center justify-center">
      <div className="w-3 h-3 rounded-full border-2 border-primary border-t-transparent animate-spin" />
    </div>
  ),
  done: (
    <div className="w-6 h-6 rounded-full bg-green-500/20 border-2 border-green-500 flex items-center justify-center">
      <svg className="w-3.5 h-3.5 text-green-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M5 13l4 4L19 7" />
      </svg>
    </div>
  ),
  error: (
    <div className="w-6 h-6 rounded-full bg-red-500/20 border-2 border-red-500 flex items-center justify-center">
      <svg className="w-3.5 h-3.5 text-red-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={3} d="M6 18L18 6M6 6l12 12" />
      </svg>
    </div>
  ),
};

export default function StepIndicator({ stepNumber, label, status, active, disabled, onClick }: StepIndicatorProps) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className={`
        w-full flex items-center gap-3 px-3 py-2.5 rounded-lg transition-all text-left
        ${active ? 'bg-primary/10 border border-primary/30' : 'hover:bg-gray-800/50 border border-transparent'}
        ${disabled ? 'opacity-40 cursor-not-allowed' : 'cursor-pointer'}
      `}
    >
      {STATUS_ICON[status]}
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2">
          <span className="text-xs text-gray-500 font-mono">{stepNumber}</span>
          <span className={`text-sm truncate ${active ? 'text-gray-100 font-medium' : 'text-gray-400'}`}>
            {label}
          </span>
        </div>
        {status === 'running' && (
          <div className="mt-1.5 h-1 bg-gray-800 rounded-full overflow-hidden">
            <div className="h-full bg-primary rounded-full animate-[progress_2s_ease-in-out_infinite]" style={{ width: '60%' }} />
          </div>
        )}
      </div>
    </button>
  );
}
