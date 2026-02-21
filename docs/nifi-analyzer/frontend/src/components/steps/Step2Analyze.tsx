import React from 'react';
import { usePipelineStore } from '../../store/pipeline';
import { useUIStore } from '../../store/ui';
import { usePipeline } from '../../hooks/usePipeline';
import TierDiagram from '../shared/TierDiagram';
import TierDiagramSearch from '../viz/TierDiagramSearch';
import ConfidenceChart from '../viz/ConfidenceChart';
import SecurityTreemap from '../viz/SecurityTreemap';

export default function Step2Analyze() {
  const parsed = usePipelineStore((s) => s.parsed);
  const analysis = usePipelineStore((s) => s.analysis);
  const status = useUIStore((s) => s.stepStatuses[1]);
  const { runAnalyze } = usePipeline();

  const hasCycles = (analysis?.cycles?.length ?? 0) > 0;
  const depNodeCount = Object.keys(analysis?.dependencyGraph ?? {}).length;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold text-gray-100 flex items-center gap-3">
            <span className="w-8 h-8 rounded-lg bg-purple-500/20 flex items-center justify-center text-sm text-purple-400 font-mono">2</span>
            Analyze Flow
          </h2>
          <p className="mt-1 text-sm text-gray-400">
            Structural analysis of the parsed flow: dependency graph, external system detection, cycle detection, and security findings.
          </p>
        </div>
        <button
          onClick={() => runAnalyze()}
          disabled={!parsed || status === 'running'}
          className="px-4 py-2 rounded-lg bg-primary text-white text-sm font-medium
            hover:bg-primary/80 disabled:opacity-40 disabled:cursor-not-allowed transition"
        >
          {status === 'running' ? 'Analyzing...' : 'Run Analysis'}
        </button>
      </div>

      {/* No parsed data */}
      {!parsed && (
        <div className="rounded-lg border border-border bg-gray-800/30 p-8 text-center text-gray-500 text-sm">
          Parse a flow file first (Step 1)
        </div>
      )}

      {/* Loading */}
      {status === 'running' && (
        <div className="flex items-center gap-3 p-4 rounded-lg bg-gray-800/50 border border-border">
          <div className="w-5 h-5 rounded-full border-2 border-purple-400 border-t-transparent animate-spin" />
          <span className="text-sm text-gray-300">Running structural analysis...</span>
        </div>
      )}

      {/* Results */}
      {analysis && status === 'done' && (
        <div className="space-y-6">
          {/* Stats grid */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <MetricCard label="Graph Nodes" value={depNodeCount} color="text-blue-400" />
            <MetricCard label="External Systems" value={analysis.externalSystems.length} color="text-purple-400" />
            <MetricCard label="Stages" value={analysis.stages.length} color="text-cyan-400" />
            <MetricCard label="Security Findings" value={analysis.securityFindings.length} color="text-amber-400" />
          </div>

          {/* Flow diagram with search */}
          {parsed && parsed.processors.length > 0 && (
            <div>
              <TierDiagramSearch processors={parsed.processors} />
              <TierDiagram processors={parsed.processors} connections={parsed.connections} />
            </div>
          )}

          {/* Confidence & Security visualizations */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <ConfidenceChart />
            <SecurityTreemap />
          </div>

          {/* Cycles */}
          <div className={`rounded-lg border p-3 ${hasCycles ? 'border-red-500/30 bg-red-500/5' : 'border-green-500/30 bg-green-500/5'}`}>
            <span className={`text-sm font-medium ${hasCycles ? 'text-red-400' : 'text-green-400'}`}>
              {hasCycles ? `${analysis.cycles.length} cycle(s) detected in flow graph` : 'No cycles detected -- DAG structure confirmed'}
            </span>
          </div>

          {/* External systems */}
          {analysis.externalSystems && analysis.externalSystems.length > 0 && (
            <div>
              <h3 className="text-sm font-medium text-gray-300 mb-3">External Systems</h3>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
                {analysis.externalSystems.map((sys, i) => {
                  const name = String((sys as Record<string, unknown>).name ?? `System ${i + 1}`);
                  const type = String((sys as Record<string, unknown>).type ?? 'unknown');
                  return (
                    <div key={i} className="rounded-lg border border-border bg-gray-800/30 p-3 flex items-center gap-3">
                      <div className="w-8 h-8 rounded bg-amber-500/20 flex items-center justify-center text-amber-400 text-xs font-bold">
                        {type.slice(0, 2).toUpperCase()}
                      </div>
                      <div>
                        <p className="text-sm text-gray-200">{name}</p>
                        <p className="text-xs text-gray-500">{type}</p>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {/* Flow metrics */}
          {analysis.flowMetrics && Object.keys(analysis.flowMetrics).length > 0 && (
            <div>
              <h3 className="text-sm font-medium text-gray-300 mb-3">Flow Metrics</h3>
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                {Object.entries(analysis.flowMetrics).map(([key, value]) => (
                  <div key={key} className="rounded-lg bg-gray-800/50 border border-border p-3 text-center">
                    <p className="text-lg font-bold text-gray-200 tabular-nums">{String(value)}</p>
                    <p className="text-xs text-gray-500 capitalize mt-0.5">{key.replace(/_/g, ' ')}</p>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Security findings (detailed list) */}
          {analysis.securityFindings && analysis.securityFindings.length > 0 && (
            <div>
              <h3 className="text-sm font-medium text-gray-300 mb-3">Security Findings</h3>
              <div className="space-y-3">
                {analysis.securityFindings.map((f, i) => {
                  const finding = f as Record<string, unknown>;
                  const severity = String(finding.severity ?? 'MEDIUM').toUpperCase();
                  const processor = String(finding.processor ?? 'Unknown');
                  const processorType = String(finding.type ?? '');
                  const description = String(finding.finding ?? finding.description ?? '');
                  const snippet = finding.snippet ? String(finding.snippet) : null;

                  const severityStyles: Record<string, { border: string; bg: string; badge: string; text: string }> = {
                    CRITICAL: { border: 'border-red-500/40', bg: 'bg-red-500/5', badge: 'bg-red-500/20 text-red-400', text: 'text-red-300/80' },
                    HIGH:     { border: 'border-orange-500/40', bg: 'bg-orange-500/5', badge: 'bg-orange-500/20 text-orange-400', text: 'text-orange-300/80' },
                    MEDIUM:   { border: 'border-amber-500/40', bg: 'bg-amber-500/5', badge: 'bg-amber-500/20 text-amber-400', text: 'text-amber-300/80' },
                  };
                  const style = severityStyles[severity] ?? severityStyles.MEDIUM;

                  return (
                    <div key={i} className={`rounded-lg border ${style.border} ${style.bg} p-4`}>
                      <div className="flex items-start gap-3">
                        <span className={`shrink-0 px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wide ${style.badge}`}>
                          {severity}
                        </span>
                        <div className="min-w-0 flex-1">
                          <div className="flex items-center gap-2 mb-1">
                            <span className="text-sm font-medium text-gray-200">{processor}</span>
                            {processorType && (
                              <span className="text-[10px] text-gray-500 font-mono">{processorType}</span>
                            )}
                          </div>
                          {description && (
                            <p className={`text-xs ${style.text} mb-2`}>{description}</p>
                          )}
                          {snippet && (
                            <pre className="text-[11px] text-gray-400 bg-gray-900/60 rounded px-3 py-2 overflow-x-auto font-mono leading-relaxed border border-gray-700/30">
                              {snippet}
                            </pre>
                          )}
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function MetricCard({ label, value, color }: { label: string; value: number; color: string }) {
  return (
    <div className="rounded-lg bg-gray-800/50 border border-border p-4 text-center">
      <p className={`text-2xl font-bold tabular-nums ${color}`}>{value}</p>
      <p className="text-xs text-gray-500 mt-1">{label}</p>
    </div>
  );
}
