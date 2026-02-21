import React, { useEffect, useState } from 'react';
import { useUIStore, type StepStatus } from '../../store/ui';
import { usePipelineStore } from '../../store/pipeline';

const STEP_NAMES = [
  'Parse Flow',
  'Analyze',
  'Assess & Map',
  'Convert',
  'Migration Report',
  'Final Report',
  'Validate',
  'Value Analysis',
];

function formatElapsed(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  return `${mins}m ${secs}s`;
}

function StepProgressRow({ index, name, status, startTime }: { index: number; name: string; status: StepStatus; startTime: number | null }) {
  const [elapsed, setElapsed] = useState(0);

  useEffect(() => {
    if (status !== 'running' || !startTime) {
      setElapsed(0);
      return;
    }
    const interval = setInterval(() => {
      setElapsed(Math.floor((Date.now() - startTime) / 1000));
    }, 1000);
    return () => clearInterval(interval);
  }, [status, startTime]);

  const barColor =
    status === 'done' ? 'bg-green-500' :
    status === 'running' ? 'bg-cyan-500' :
    status === 'error' ? 'bg-red-500' :
    'bg-gray-700';

  const barWidth =
    status === 'done' ? '100%' :
    status === 'error' ? '100%' :
    '0%';

  const statusLabel =
    status === 'done' ? 'Complete' :
    status === 'running' ? `Running... ${formatElapsed(elapsed)}` :
    status === 'error' ? 'Failed' :
    'Pending';

  const statusColor =
    status === 'done' ? 'text-green-400' :
    status === 'running' ? 'text-cyan-400' :
    status === 'error' ? 'text-red-400' :
    'text-gray-500';

  return (
    <div className="flex items-center gap-4">
      <div className="w-6 text-right text-sm font-mono text-gray-500">{index + 1}</div>
      <div className="flex-1 min-w-0">
        <div className="flex items-center justify-between mb-1">
          <span className={`text-sm font-medium ${status === 'running' ? 'text-gray-100' : status === 'done' ? 'text-green-300' : status === 'error' ? 'text-red-300' : 'text-gray-400'}`}>
            {name}
            {status === 'running' && (
              <span className="ml-2 inline-block w-1.5 h-1.5 rounded-full bg-cyan-400 animate-ping" />
            )}
          </span>
          <span className={`text-xs font-mono ${statusColor}`}>{statusLabel}</span>
        </div>
        <div className="h-2.5 bg-gray-800 rounded-full overflow-hidden">
          {status === 'running' ? (
            /* Indeterminate animated bar for running state */
            <div className="h-full bg-cyan-500 rounded-full animate-indeterminate" />
          ) : (
            <div
              className={`h-full rounded-full transition-all duration-700 ${barColor}`}
              style={{ width: barWidth }}
            />
          )}
        </div>
      </div>
    </div>
  );
}

export default function PipelineProgress() {
  const stepStatuses = useUIStore((s) => s.stepStatuses);
  const progress = useUIStore((s) => s.progress);
  const pipelineRunning = useUIStore((s) => s.pipelineRunning);
  const fileName = usePipelineStore((s) => s.fileName);
  const platform = usePipelineStore((s) => s.platform);

  // Track when each step started running
  const [stepStartTimes, setStepStartTimes] = useState<(number | null)[]>(Array(8).fill(null));
  const [overallStart] = useState(() => Date.now());
  const [overallElapsed, setOverallElapsed] = useState(0);

  // Update start times when step statuses change
  useEffect(() => {
    setStepStartTimes((prev) => {
      const next = [...prev];
      for (let i = 0; i < 8; i++) {
        if (stepStatuses[i] === 'running' && !next[i]) {
          next[i] = Date.now();
        } else if (stepStatuses[i] !== 'running') {
          next[i] = null;
        }
      }
      return next;
    });
  }, [stepStatuses]);

  // Overall elapsed timer
  useEffect(() => {
    if (!pipelineRunning) return;
    const interval = setInterval(() => {
      setOverallElapsed(Math.floor((Date.now() - overallStart) / 1000));
    }, 1000);
    return () => clearInterval(interval);
  }, [pipelineRunning, overallStart]);

  const doneCount = stepStatuses.slice(0, 8).filter((s) => s === 'done').length;
  const hasError = stepStatuses.slice(0, 8).some((s) => s === 'error');
  const isRunning = stepStatuses.slice(0, 8).some((s) => s === 'running');

  return (
    <div className="max-w-2xl mx-auto">
      {/* Add CSS for indeterminate animation */}
      <style>{`
        @keyframes indeterminate {
          0% { width: 0%; margin-left: 0%; }
          50% { width: 40%; margin-left: 30%; }
          100% { width: 0%; margin-left: 100%; }
        }
        .animate-indeterminate {
          animation: indeterminate 1.5s ease-in-out infinite;
        }
      `}</style>

      <div className="rounded-xl border border-border bg-gray-900/60 p-8">
        {/* Header */}
        <div className="text-center mb-8">
          <div className="flex items-center justify-center gap-3 mb-2">
            {isRunning && (
              <div className="w-5 h-5 border-2 border-cyan-400 border-t-transparent rounded-full animate-spin" />
            )}
            <h2 className="text-xl font-semibold text-gray-100">
              {hasError ? 'Pipeline Error' : isRunning ? 'Pipeline Running' : 'Pipeline Complete'}
            </h2>
          </div>
          <p className="text-sm text-gray-400">
            {fileName && <>Processing <span className="text-gray-300 font-medium">{fileName}</span></>}
            {platform && <> ({platform})</>}
          </p>
          {pipelineRunning && (
            <p className="text-xs text-gray-500 mt-1 font-mono">Elapsed: {formatElapsed(overallElapsed)}</p>
          )}
        </div>

        {/* Overall progress */}
        <div className="mb-6">
          <div className="flex items-center justify-between mb-2">
            <span className="text-sm text-gray-400">Overall Progress</span>
            <span className="text-sm font-mono text-gray-300">{doneCount}/8 steps &middot; {progress}%</span>
          </div>
          <div className="h-3 bg-gray-800 rounded-full overflow-hidden">
            <div
              className={`h-full rounded-full transition-all duration-700 ${hasError ? 'bg-red-500' : 'bg-cyan-500'}`}
              style={{ width: `${progress}%` }}
            />
          </div>
        </div>

        {/* Per-step progress bars */}
        <div className="space-y-4">
          {STEP_NAMES.map((name, i) => (
            <StepProgressRow key={i} index={i} name={name} status={stepStatuses[i]} startTime={stepStartTimes[i]} />
          ))}
        </div>

        {hasError && (
          <div className="mt-6 p-4 rounded-lg bg-red-500/10 border border-red-500/20">
            <p className="text-sm text-red-300">A step failed. Check the error modal for details.</p>
            <p className="text-xs text-gray-500 mt-1">You can dismiss the error and retry, or check the server logs for more info.</p>
          </div>
        )}

        {isRunning && (
          <div className="mt-6 text-center">
            <p className="text-xs text-gray-500">Large flows (10,000+ processors) may take several minutes per step. The pipeline is still running.</p>
          </div>
        )}
      </div>
    </div>
  );
}
