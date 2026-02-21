import React, { useRef, useState } from 'react';
import { usePipeline } from '../../hooks/usePipeline';
import { useUIStore, type StepStatus } from '../../store/ui';

const STEP_NAMES = ['Parse', 'Analyze', 'Assess', 'Convert', 'Report', 'Final Report', 'Validate', 'Value Analysis'];
const ACCEPTED = '.xml,.json,.gz,.zip,.dtsx,.py,.sql';

function StepIndicator({ status, label }: { status: StepStatus; label: string }) {
  return (
    <div className="flex items-center gap-2 text-xs">
      {status === 'done' && (
        <span className="w-4 h-4 rounded-full bg-green-500 flex items-center justify-center text-white text-[10px]">
          &#x2713;
        </span>
      )}
      {status === 'running' && (
        <span className="w-4 h-4 rounded-full border-2 border-primary border-t-transparent animate-spin" />
      )}
      {status === 'error' && (
        <span className="w-4 h-4 rounded-full bg-red-500 flex items-center justify-center text-white text-[10px]">
          &#x2716;
        </span>
      )}
      {status === 'idle' && (
        <span className="w-4 h-4 rounded-full border border-gray-600" />
      )}
      <span className={status === 'done' ? 'text-green-400' : status === 'running' ? 'text-primary' : status === 'error' ? 'text-red-400' : 'text-gray-500'}>
        {label}
      </span>
    </div>
  );
}

export default function AutoRunButton() {
  const fileRef = useRef<HTMLInputElement>(null);
  const { runAll } = usePipeline();
  const stepStatuses = useUIStore((s) => s.stepStatuses);
  const setStepStatus = useUIStore((s) => s.setStepStatus);
  const [isRunning, setIsRunning] = useState(false);

  const handleClick = () => {
    if (isRunning) return;
    fileRef.current?.click();
  };

  const handleFile = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    e.target.value = '';
    setIsRunning(true);
    await runAll(file);
    setIsRunning(false);
  };

  // Cancel resets UI state only. AbortController is intentionally NOT used here
  // because aborting in-flight requests caused React state update issues during
  // re-renders (stale closures over the controller). The backend will finish
  // processing any in-progress request in the background; this just resets the
  // client-side step indicators so the user can start a new run.
  const handleCancel = () => {
    for (let i = 0; i < 8; i++) {
      const st = stepStatuses[i];
      if (st === 'running' || st === 'idle') {
        setStepStatus(i, 'idle');
      }
    }
    setIsRunning(false);
  };

  const hasAnyRunning = stepStatuses.slice(0, 8).some((s) => s === 'running');

  return (
    <div className="relative">
      <input
        ref={fileRef}
        type="file"
        accept={ACCEPTED}
        className="hidden"
        onChange={handleFile}
      />

      <button
        onClick={handleClick}
        disabled={isRunning}
        className="flex items-center gap-2 px-4 py-2 rounded-lg bg-primary text-white font-medium text-sm
          hover:bg-primary/90 active:bg-primary/80 transition disabled:opacity-60 disabled:cursor-not-allowed"
      >
        <svg className="w-4 h-4" fill="currentColor" viewBox="0 0 24 24">
          <path d="M8 5v14l11-7z" />
        </svg>
        Run Full Pipeline
      </button>

      {/* Progress overlay */}
      {isRunning && (
        <div className="absolute top-full left-0 mt-2 w-64 bg-gray-900 border border-border rounded-lg shadow-xl p-4 z-50">
          <div className="flex items-center justify-between mb-3">
            <span className="text-xs font-medium text-gray-300">Pipeline Progress</span>
            <button
              onClick={handleCancel}
              className="text-xs text-red-400 hover:text-red-300 transition"
            >
              Cancel
            </button>
          </div>
          <div className="space-y-1.5">
            {STEP_NAMES.map((name, i) => (
              <StepIndicator key={i} status={stepStatuses[i]} label={`${i + 1}. ${name}`} />
            ))}
          </div>
          {hasAnyRunning && (
            <div className="mt-3 h-1 bg-gray-800 rounded-full overflow-hidden">
              <div className="h-full bg-primary rounded-full animate-pulse" style={{ width: '100%' }} />
            </div>
          )}
        </div>
      )}
    </div>
  );
}
