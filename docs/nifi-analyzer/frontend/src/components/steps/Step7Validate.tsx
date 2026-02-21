import React, { useState } from 'react';
import { usePipelineStore } from '../../store/pipeline';
import { useUIStore } from '../../store/ui';
import { usePipeline } from '../../hooks/usePipeline';
import type { ValidationScore, RunnableReport, CheckResult } from '../../types/pipeline';

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

const severityColors: Record<string, { badge: string; text: string }> = {
  critical: { badge: 'bg-red-500/20 text-red-400', text: 'text-red-400' },
  high: { badge: 'bg-orange-500/20 text-orange-400', text: 'text-orange-400' },
  medium: { badge: 'bg-amber-500/20 text-amber-400', text: 'text-amber-400' },
  low: { badge: 'bg-blue-500/20 text-blue-400', text: 'text-blue-400' },
};

export default function Step7Validate() {
  const notebook = usePipelineStore((s) => s.notebook);
  const validation = usePipelineStore((s) => s.validation);
  const status = useUIStore((s) => s.stepStatuses[6]);
  const { runValidate } = usePipeline();
  const [showRunnableDetails, setShowRunnableDetails] = useState(false);

  const canRun = notebook && status !== 'running';
  const runnableReport: RunnableReport | null = (validation as Record<string, unknown> | null)?.runnableReport as RunnableReport | null ?? null;

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
            Multi-dimensional validation scores and 12-point runnable quality gate.
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
          <span className="text-sm text-gray-300">Running validation checks + 12-point runnable gate...</span>
        </div>
      ) : validation && status === 'done' ? (
        <div className="space-y-6">
          {/* V6 Runnable Quality Gate */}
          {runnableReport && (
            <div className={`rounded-lg border p-4 ${
              runnableReport.is_runnable
                ? 'border-green-500/30 bg-green-500/5'
                : 'border-red-500/30 bg-red-500/5'
            }`}>
              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center gap-3">
                  <div className={`w-10 h-10 rounded-lg flex items-center justify-center ${
                    runnableReport.is_runnable ? 'bg-green-500/20' : 'bg-red-500/20'
                  }`}>
                    {runnableReport.is_runnable ? (
                      <svg className="w-6 h-6 text-green-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                      </svg>
                    ) : (
                      <svg className="w-6 h-6 text-red-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 14l2-2m0 0l2-2m-2 2l-2-2m2 2l2 2m7-2a9 9 0 11-18 0 9 9 0 0118 0z" />
                      </svg>
                    )}
                  </div>
                  <div>
                    <p className={`text-lg font-bold ${runnableReport.is_runnable ? 'text-green-400' : 'text-red-400'}`}>
                      {runnableReport.is_runnable ? 'RUNNABLE' : 'NOT RUNNABLE'}
                    </p>
                    <p className="text-xs text-gray-500">
                      12-Point Quality Gate: {runnableReport.passed_count}/{runnableReport.passed_count + runnableReport.failed_count} checks passed
                    </p>
                  </div>
                </div>
                <div className="text-center">
                  <p className={`text-3xl font-bold tabular-nums ${scoreColor(runnableReport.overall_score)}`}>
                    {toPct(runnableReport.overall_score).toFixed(0)}
                  </p>
                  <p className="text-[10px] text-gray-500">Gate Score</p>
                </div>
              </div>

              {/* Check grid */}
              <div className="grid grid-cols-3 sm:grid-cols-4 md:grid-cols-6 gap-1.5 mb-2">
                {runnableReport.checks.map((check: CheckResult) => (
                  <div
                    key={check.check_id}
                    className={`rounded px-2 py-1.5 text-center cursor-pointer transition hover:opacity-80 ${
                      check.passed ? 'bg-green-500/10 border border-green-500/20' : 'bg-red-500/10 border border-red-500/20'
                    }`}
                    title={`${check.name}: ${check.message}`}
                    onClick={() => setShowRunnableDetails(!showRunnableDetails)}
                  >
                    <span className={`text-[10px] ${check.passed ? 'text-green-400' : 'text-red-400'}`}>
                      {check.passed ? 'PASS' : 'FAIL'}
                    </span>
                    <p className="text-[9px] text-gray-500 truncate mt-0.5">{check.name.replace(/_/g, ' ')}</p>
                  </div>
                ))}
              </div>

              {/* Expandable details */}
              <button
                onClick={() => setShowRunnableDetails(!showRunnableDetails)}
                className="text-xs text-gray-400 hover:text-gray-200 transition"
              >
                {showRunnableDetails ? 'Hide details' : 'Show details'}
              </button>

              {showRunnableDetails && (
                <div className="mt-3 space-y-2">
                  {runnableReport.checks.map((check: CheckResult) => (
                    <div
                      key={check.check_id}
                      className={`rounded-lg border p-3 ${
                        check.passed ? 'border-green-500/20 bg-green-500/5' : 'border-red-500/20 bg-red-500/5'
                      }`}
                    >
                      <div className="flex items-center justify-between mb-1">
                        <div className="flex items-center gap-2">
                          <span className={`text-sm font-medium ${check.passed ? 'text-green-400' : 'text-red-400'}`}>
                            #{check.check_id} {check.name.replace(/_/g, ' ')}
                          </span>
                          <span className={`px-1.5 py-0.5 rounded text-[9px] font-bold uppercase ${
                            severityColors[check.severity]?.badge ?? 'bg-gray-500/20 text-gray-400'
                          }`}>
                            {check.severity}
                          </span>
                        </div>
                        <span className={`text-xs font-bold ${check.passed ? 'text-green-400' : 'text-red-400'}`}>
                          {check.passed ? 'PASS' : 'FAIL'}
                        </span>
                      </div>
                      <p className="text-xs text-gray-400">{check.message}</p>
                      {check.details && check.details.length > 0 && (
                        <ul className="mt-1 space-y-0.5">
                          {check.details.slice(0, 5).map((d, j) => (
                            <li key={j} className="text-[10px] text-gray-500 pl-2 border-l border-gray-700">{d}</li>
                          ))}
                          {check.details.length > 5 && (
                            <li className="text-[10px] text-gray-600 pl-2">...and {check.details.length - 5} more</li>
                          )}
                        </ul>
                      )}
                    </div>
                  ))}
                  {runnableReport.summary && (
                    <p className="text-xs text-gray-500 italic mt-2">{runnableReport.summary}</p>
                  )}
                </div>
              )}
            </div>
          )}

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
