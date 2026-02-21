import React, { useState, useEffect } from 'react';
import { getRunDetail } from '../../api/client';

interface RunDetailProps {
  runId: string;
  onBack: () => void;
}

export default function RunDetail({ runId, onBack }: RunDetailProps) {
  const [run, setRun] = useState<Record<string, unknown> | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    const fetchRun = async () => {
      setLoading(true);
      try {
        const data = await getRunDetail(runId);
        setRun(data as Record<string, unknown>);
      } catch (e) {
        setError(e instanceof Error ? e.message : 'Failed to load run');
      } finally {
        setLoading(false);
      }
    };
    fetchRun();
  }, [runId]);

  if (loading) return <p className="text-sm text-gray-500">Loading run details...</p>;
  if (error) return <p className="text-sm text-red-400">{error}</p>;
  if (!run) return null;

  const stepsCompleted = (run.stepsCompleted as string[]) || [];
  const summary = (run.resultSummary as Record<string, unknown>) || {};

  return (
    <div className="space-y-4">
      <button
        onClick={onBack}
        className="flex items-center gap-1 text-sm text-gray-400 hover:text-gray-200 transition"
        aria-label="Back to run history"
      >
        <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
        </svg>
        Back to History
      </button>

      <div className="rounded-lg border border-border bg-gray-900/50 overflow-hidden">
        <div className="px-4 py-3 bg-gray-900/80 border-b border-border">
          <h3 className="text-lg font-semibold text-gray-100">
            Run {String(run.id)}
          </h3>
          <p className="text-sm text-gray-400 mt-0.5">{String(run.fileName)}</p>
        </div>

        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 p-4">
          <div className="text-center">
            <div className="text-2xl font-bold text-primary">{String(run.processorCount)}</div>
            <div className="text-xs text-gray-500 uppercase tracking-wider">Processors</div>
          </div>
          <div className="text-center">
            <div className="text-2xl font-bold text-gray-200">{String(run.platform)}</div>
            <div className="text-xs text-gray-500 uppercase tracking-wider">Platform</div>
          </div>
          <div className="text-center">
            <div className="text-2xl font-bold text-gray-200">
              {Number(run.durationMs) < 1000
                ? `${run.durationMs}ms`
                : `${(Number(run.durationMs) / 1000).toFixed(1)}s`}
            </div>
            <div className="text-xs text-gray-500 uppercase tracking-wider">Duration</div>
          </div>
          <div className="text-center">
            <div
              className={`text-2xl font-bold ${
                run.status === 'completed' ? 'text-green-400' : 'text-red-400'
              }`}
            >
              {String(run.status)}
            </div>
            <div className="text-xs text-gray-500 uppercase tracking-wider">Status</div>
          </div>
        </div>

        {stepsCompleted.length > 0 && (
          <div className="px-4 pb-4">
            <h4 className="text-xs font-medium text-gray-500 uppercase tracking-wider mb-2">
              Steps Completed
            </h4>
            <div className="flex flex-wrap gap-1.5">
              {stepsCompleted.map((step, i) => (
                <span
                  key={i}
                  className="px-2 py-0.5 text-xs rounded bg-green-500/20 text-green-400"
                >
                  {step}
                </span>
              ))}
            </div>
          </div>
        )}

        {Object.keys(summary).length > 0 && (
          <div className="px-4 pb-4">
            <h4 className="text-xs font-medium text-gray-500 uppercase tracking-wider mb-2">
              Result Summary
            </h4>
            <pre className="text-xs text-gray-400 bg-gray-800/50 rounded p-3 overflow-auto max-h-48">
              {JSON.stringify(summary, null, 2)}
            </pre>
          </div>
        )}
      </div>
    </div>
  );
}
