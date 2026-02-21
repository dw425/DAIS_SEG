import React from 'react';
import { usePipelineStore } from '../../store/pipeline';
import { useUIStore } from '../../store/ui';
import { usePipeline } from '../../hooks/usePipeline';
import type { ValidationScore, ValidationGap } from '../../types/pipeline';

const SEVERITY_ORDER = { critical: 0, high: 1, medium: 2, low: 3 };
const SEVERITY_STYLES: Record<string, string> = {
  critical: 'bg-red-500/20 text-red-400',
  high: 'bg-orange-500/20 text-orange-400',
  medium: 'bg-amber-500/20 text-amber-400',
  low: 'bg-green-500/20 text-green-400',
};

function scoreColor(score: number, max: number): string {
  const pct = max > 0 ? (score / max) * 100 : 0;
  if (pct >= 90) return 'text-green-400';
  if (pct >= 70) return 'text-amber-400';
  return 'text-red-400';
}

function barColor(score: number, max: number): string {
  const pct = max > 0 ? (score / max) * 100 : 0;
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

  const sortedGaps = validation?.gaps
    ? [...validation.gaps].sort((a, b) => SEVERITY_ORDER[a.severity] - SEVERITY_ORDER[b.severity])
    : [];

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
            Validation scores, severity-ranked gaps, and remediation suggestions.
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
              <p className={`text-4xl font-bold tabular-nums ${validation.overallScore >= 90 ? 'text-green-400' : validation.overallScore >= 70 ? 'text-amber-400' : 'text-red-400'}`}>
                {validation.overallScore}
              </p>
              <p className="text-xs text-gray-500 mt-1">Overall Score</p>
            </div>
            <div className="flex-1 h-4 bg-gray-800 rounded-full overflow-hidden">
              <div
                className={`h-full rounded-full transition-all ${validation.overallScore >= 90 ? 'bg-green-500' : validation.overallScore >= 70 ? 'bg-amber-500' : 'bg-red-500'}`}
                style={{ width: `${validation.overallScore}%` }}
              />
            </div>
            <span className={`px-3 py-1 rounded-lg text-sm font-medium ${validation.passed ? 'bg-green-500/20 text-green-400' : 'bg-red-500/20 text-red-400'}`}>
              {validation.passed ? 'PASSED' : 'NEEDS WORK'}
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
                      <span className="text-sm text-gray-200">{s.category}</span>
                      <span className={`text-sm font-medium tabular-nums ${scoreColor(s.score, s.maxScore)}`}>
                        {s.score}/{s.maxScore}
                      </span>
                    </div>
                    <div className="h-2 bg-gray-800 rounded-full overflow-hidden">
                      <div
                        className={`h-full rounded-full transition-all ${barColor(s.score, s.maxScore)}`}
                        style={{ width: `${s.maxScore > 0 ? (s.score / s.maxScore) * 100 : 0}%` }}
                      />
                    </div>
                    {s.details && <p className="text-xs text-gray-500 mt-1.5">{s.details}</p>}
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Gaps */}
          {sortedGaps.length > 0 && (
            <div>
              <h3 className="text-sm font-medium text-gray-300 mb-3">
                Validation Gaps ({sortedGaps.length})
              </h3>
              <div className="space-y-2">
                {sortedGaps.map((gap: ValidationGap, i: number) => (
                  <div key={i} className="rounded-lg border border-border bg-gray-800/30 p-4">
                    <div className="flex items-center justify-between mb-1">
                      <span className="text-sm font-medium text-gray-200">{gap.processor}</span>
                      <span className={`px-2 py-0.5 rounded text-xs font-medium ${SEVERITY_STYLES[gap.severity]}`}>
                        {gap.severity}
                      </span>
                    </div>
                    <p className="text-sm text-gray-400">{gap.message}</p>
                    {gap.remediation && (
                      <div className="mt-2 pl-3 border-l-2 border-teal-500/30">
                        <p className="text-xs text-teal-300/70">Remediation: {gap.remediation}</p>
                      </div>
                    )}
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
