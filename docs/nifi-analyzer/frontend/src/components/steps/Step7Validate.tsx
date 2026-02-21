import React from 'react';
import { usePipelineStore } from '../../store/pipeline';
import { useUIStore } from '../../store/ui';
import { usePipeline } from '../../hooks/usePipeline';
import type { ValidationScore } from '../../types/pipeline';

function toPct(score: number): number {
  return score <= 1 ? score * 100 : score;
}

function scoreColor(score: number): string {
  const pct = toPct(score);
  if (pct >= 90) return 'text-green-400';
  if (pct >= 70) return 'text-amber-400';
  return 'text-red-400';
}

function barColor(score: number): string {
  const pct = toPct(score);
  if (pct >= 90) return 'bg-green-500';
  if (pct >= 70) return 'bg-amber-500';
  return 'bg-red-500';
}

export default function Step7Validate() {
  const notebook = usePipelineStore((s) => s.notebook);
  const validation = usePipelineStore((s) => s.validation);
  const status = useUIStore((s) => s.stepStatuses[6]);
  const { runValidate } = usePipeline();

  const canRun = notebook && status !== 'running';

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold text-gray-100 flex items-center gap-3">
            <span className="w-8 h-8 rounded-lg bg-teal-500/20 flex items-center justify-center text-sm text-teal-400 font-mono">7</span>
            Validate
          </h2>
          <p className="mt-1 text-sm text-gray-400">
            Validation scores, gaps, and errors.
          </p>
        </div>
        <button
          onClick={() => runValidate()}
          disabled={!canRun}
          className="px-4 py-2 rounded-lg bg-primary text-white text-sm font-medium
            hover:bg-primary/80 disabled:opacity-40 disabled:cursor-not-allowed transition"
        >
          {status === 'running' ? 'Validating...' : 'Run Validation'}
        </button>
      </div>

      {!notebook ? (
        <div className="rounded-lg border border-border bg-gray-800/30 p-8 text-center text-gray-500 text-sm">
          Complete Step 4 (Convert) first
        </div>
      ) : status === 'running' ? (
        <div className="flex items-center gap-3 p-4 rounded-lg bg-gray-800/50 border border-border">
          <div className="w-5 h-5 rounded-full border-2 border-teal-400 border-t-transparent animate-spin" />
          <span className="text-sm text-gray-300">Running validation checks...</span>
        </div>
      ) : validation && status === 'done' ? (
        <div className="space-y-6">
          {/* Overall score */}
          <div className="flex items-center gap-6 rounded-lg bg-gray-800/50 border border-border p-6">
            <div className="text-center">
              <p className={`text-4xl font-bold tabular-nums ${scoreColor(validation.overallScore)}`}>
                {toPct(validation.overallScore).toFixed(0)}
              </p>
              <p className="text-xs text-gray-500 mt-1">Overall Score</p>
            </div>
            <div className="flex-1 h-4 bg-gray-800 rounded-full overflow-hidden">
              <div
                className={`h-full rounded-full transition-all ${barColor(validation.overallScore)}`}
                style={{ width: `${toPct(validation.overallScore)}%` }}
              />
            </div>
            <span className={`px-3 py-1 rounded-lg text-sm font-medium ${
              toPct(validation.overallScore) >= 85
                ? 'bg-green-500/20 text-green-400'
                : toPct(validation.overallScore) >= 60
                  ? 'bg-amber-500/20 text-amber-400'
                  : 'bg-red-500/20 text-red-400'
            }`}>
              {toPct(validation.overallScore) >= 85 ? 'READY' : toPct(validation.overallScore) >= 60 ? 'NEEDS WORK' : 'NOT READY'}
            </span>
          </div>

          {/* Score breakdown */}
          {validation.scores && validation.scores.length > 0 && (
            <div>
              <h3 className="text-sm font-medium text-gray-300 mb-3">Score Breakdown</h3>
              <div className="space-y-3">
                {validation.scores.map((s: ValidationScore, i: number) => (
                  <div key={i} className="rounded-lg bg-gray-800/30 border border-border p-3">
                    <div className="flex items-center justify-between mb-2">
                      <span className="text-sm text-gray-200">{s.dimension}</span>
                      <span className={`text-sm font-medium tabular-nums ${scoreColor(s.score)}`}>
                        {toPct(s.score).toFixed(0)}%
                      </span>
                    </div>
                    <div className="h-2 bg-gray-800 rounded-full overflow-hidden">
                      <div
                        className={`h-full rounded-full transition-all ${barColor(s.score)}`}
                        style={{ width: `${toPct(s.score)}%` }}
                      />
                    </div>
                    {s.details && <p className="text-xs text-gray-500 mt-1.5">{s.details}</p>}
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Gaps */}
          {validation.gaps && validation.gaps.length > 0 && (
            <div>
              <h3 className="text-sm font-medium text-gray-300 mb-3">
                Validation Gaps ({validation.gaps.length})
              </h3>
              <div className="space-y-2">
                {validation.gaps.map((gap, i: number) => {
                  const gapType = String(gap.type ?? 'unknown');
                  const gapMessage = String(gap.message ?? '');
                  const gapName = gap.name ? String(gap.name) : null;
                  const gapSource = gap.source ? String(gap.source) : null;
                  const gapDest = gap.destination ? String(gap.destination) : null;
                  const isMissing = gapType === 'missing_processor';
                  return (
                    <div key={i} className={`rounded-lg border p-4 ${isMissing ? 'border-amber-500/30 bg-amber-500/5' : 'border-border bg-gray-800/30'}`}>
                      <div className="flex items-center gap-2 mb-1">
                        <span className={`px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wide ${isMissing ? 'bg-amber-500/20 text-amber-400' : 'bg-blue-500/20 text-blue-400'}`}>
                          {gapType.replace(/_/g, ' ')}
                        </span>
                        {gapName && <span className="text-sm font-medium text-gray-200">{gapName}</span>}
                        {gapSource && gapDest && (
                          <span className="text-xs text-gray-400 font-mono">{gapSource} &rarr; {gapDest}</span>
                        )}
                      </div>
                      {gapMessage && <p className="text-sm text-gray-400 mt-1">{gapMessage}</p>}
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {/* Errors */}
          {validation.errors && validation.errors.length > 0 && (
            <div>
              <h3 className="text-sm font-medium text-gray-300 mb-3">
                Errors ({validation.errors.length})
              </h3>
              <div className="space-y-2">
                {validation.errors.map((err, i: number) => (
                  <div key={i} className="rounded-lg border border-red-500/30 bg-red-500/5 p-3">
                    <p className="text-sm text-red-400">{err}</p>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      ) : null}
    </div>
  );
}
