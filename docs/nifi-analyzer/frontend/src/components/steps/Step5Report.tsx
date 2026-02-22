import React from 'react';
import { usePipelineStore } from '../../store/pipeline';
import { useUIStore } from '../../store/ui';
import { usePipeline } from '../../hooks/usePipeline';
import MigrationTimeline from '../viz/MigrationTimeline';
import MermaidRenderer from '../viz/MermaidRenderer';
import type { GapItem } from '../../types/pipeline';

const SEVERITY_STYLES: Record<string, string> = {
  critical: 'bg-red-500/20 text-red-400 border-red-500/30',
  high: 'bg-orange-500/20 text-orange-400 border-orange-500/30',
  medium: 'bg-amber-500/20 text-amber-400 border-amber-500/30',
  low: 'bg-green-500/20 text-green-400 border-green-500/30',
};

export default function Step5Report() {
  const assessment = usePipelineStore((s) => s.assessment);
  const analysis = usePipelineStore((s) => s.analysis);
  const report = usePipelineStore((s) => s.report);
  const lineage = usePipelineStore((s) => s.lineage);
  const status = useUIStore((s) => s.stepStatuses[4]);
  const { runReport } = usePipeline();

  const canRun = assessment && status !== 'running';

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold text-gray-100 flex items-center gap-3">
            <span className="w-8 h-8 rounded-lg bg-pink-500/20 flex items-center justify-center text-sm text-pink-400 font-mono">5</span>
            Migration Report
          </h2>
          <p className="mt-1 text-sm text-gray-400">
            Gap playbook, risk matrix, and implementation timeline for the migration.
          </p>
        </div>
        <button
          onClick={() => runReport()}
          disabled={!canRun}
          className="px-4 py-2 rounded-lg bg-primary text-white text-sm font-medium
            hover:bg-primary/80 disabled:opacity-40 disabled:cursor-not-allowed transition"
        >
          {status === 'running' ? 'Generating...' : 'Generate Report'}
        </button>
      </div>

      {!assessment ? (
        <div className="rounded-lg border border-border bg-gray-800/30 p-8 text-center text-gray-500 text-sm">
          Complete Steps 1-3 first
        </div>
      ) : status === 'running' ? (
        <div className="flex items-center gap-3 p-4 rounded-lg bg-gray-800/50 border border-border">
          <div className="w-5 h-5 rounded-full border-2 border-pink-400 border-t-transparent animate-spin" />
          <span className="text-sm text-gray-300">Building migration report...</span>
        </div>
      ) : report && status === 'done' ? (
        <div className="space-y-6">
          {/* Summary */}
          {report.summary && (
            <div className="rounded-lg border border-border bg-gray-800/30 p-4">
              <h3 className="text-sm font-medium text-gray-300 mb-2">Summary</h3>
              <p className="text-sm text-gray-400 leading-relaxed">{report.summary}</p>
            </div>
          )}

          {/* Risk Matrix */}
          {report.riskMatrix && Object.keys(report.riskMatrix).length > 0 && (
            <div>
              <h3 className="text-sm font-medium text-gray-300 mb-3">Risk Matrix</h3>
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                {Object.entries(report.riskMatrix).map(([key, value]) => (
                  <div key={key} className="rounded-lg bg-gray-800/50 border border-border p-3 text-center">
                    <p className="text-lg font-bold text-gray-200 tabular-nums">{value}</p>
                    <p className="text-xs text-gray-500 capitalize mt-0.5">{key.replace(/_/g, ' ')}</p>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Gap Playbook */}
          {report.gapPlaybook && report.gapPlaybook.length > 0 && (
            <div>
              <h3 className="text-sm font-medium text-gray-300 mb-3">
                Gap Playbook ({report.gapPlaybook.length} items)
              </h3>
              <div className="space-y-2">
                {report.gapPlaybook.map((gap: GapItem, i: number) => (
                  <div key={i} className={`rounded-lg border p-4 ${SEVERITY_STYLES[gap.severity] || 'border-border'}`}>
                    <div className="flex items-center justify-between mb-2">
                      <span className="text-sm font-medium text-gray-200">{gap.processor}</span>
                      <span className={`px-2 py-0.5 rounded text-xs font-medium ${SEVERITY_STYLES[gap.severity]}`}>
                        {gap.severity}
                      </span>
                    </div>
                    <p className="text-sm text-gray-400">{gap.gap}</p>
                    {gap.remediation && (
                      <p className="text-sm text-gray-500 mt-2 italic">
                        Remediation: {gap.remediation}
                      </p>
                    )}
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Data Lineage Diagram */}
          {lineage?.mermaidMarkdown && (
            <div>
              <h3 className="text-sm font-medium text-gray-300 mb-3">Data Lineage</h3>
              <MermaidRenderer markdown={lineage.mermaidMarkdown} />
            </div>
          )}

          {/* Migration Timeline Visualization */}
          <MigrationTimeline />

          {/* Original timeline (kept as fallback detail) */}
          {report.estimatedTimeline && report.estimatedTimeline.length > 0 && (
            <div>
              <h3 className="text-sm font-medium text-gray-300 mb-3">Timeline Detail</h3>
              <div className="relative">
                {report.estimatedTimeline.map((phase, i) => (
                  <div key={i} className="flex items-start gap-4 mb-4 last:mb-0">
                    {/* Timeline dot + line */}
                    <div className="flex flex-col items-center">
                      <div className="w-3 h-3 rounded-full bg-primary shrink-0 mt-1" />
                      {i < report.estimatedTimeline.length - 1 && (
                        <div className="w-0.5 h-full bg-border min-h-[32px]" />
                      )}
                    </div>
                    <div className="flex-1 rounded-lg bg-gray-800/50 border border-border p-3">
                      <div className="flex items-center justify-between">
                        <span className="text-sm font-medium text-gray-200">{phase.phase}</span>
                        <span className="text-xs text-gray-500 tabular-nums">{phase.weeks} week(s)</span>
                      </div>
                    </div>
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
