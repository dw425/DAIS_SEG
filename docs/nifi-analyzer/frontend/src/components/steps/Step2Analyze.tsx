import React from 'react';
import { usePipelineStore } from '../../store/pipeline';
import { useUIStore } from '../../store/ui';
import { usePipeline } from '../../hooks/usePipeline';
import TierDiagram from '../shared/TierDiagram';

export default function Step2Analyze() {
  const parsed = usePipelineStore((s) => s.parsed);
  const analysis = usePipelineStore((s) => s.analysis);
  const status = useUIStore((s) => s.stepStatuses[1]);
  const { runAnalyze } = usePipeline();

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
            Structural analysis of the parsed flow: complexity scoring, tier mapping, external system detection, and cycle detection.
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
            <MetricCard label="Processors" value={analysis.processorCount} color="text-blue-400" />
            <MetricCard label="Connections" value={analysis.connectionCount} color="text-purple-400" />
            <MetricCard label="Groups" value={analysis.processGroupCount} color="text-cyan-400" />
            <MetricCard
              label="Complexity"
              value={analysis.complexityScore}
              color={analysis.complexityScore > 70 ? 'text-red-400' : analysis.complexityScore > 40 ? 'text-amber-400' : 'text-green-400'}
            />
          </div>

          {/* Cycles */}
          <div className={`rounded-lg border p-3 ${analysis.hasCycles ? 'border-red-500/30 bg-red-500/5' : 'border-green-500/30 bg-green-500/5'}`}>
            <span className={`text-sm font-medium ${analysis.hasCycles ? 'text-red-400' : 'text-green-400'}`}>
              {analysis.hasCycles ? 'Cycles detected in flow graph' : 'No cycles detected -- DAG structure confirmed'}
            </span>
          </div>

          {/* External systems */}
          {analysis.externalSystems && analysis.externalSystems.length > 0 && (
            <div>
              <h3 className="text-sm font-medium text-gray-300 mb-3">External Systems</h3>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
                {analysis.externalSystems.map((sys, i) => (
                  <div key={i} className="rounded-lg border border-border bg-gray-800/30 p-3 flex items-center gap-3">
                    <div className="w-8 h-8 rounded bg-amber-500/20 flex items-center justify-center text-amber-400 text-xs font-bold">
                      {sys.type.slice(0, 2).toUpperCase()}
                    </div>
                    <div>
                      <p className="text-sm text-gray-200">{sys.name}</p>
                      <p className="text-xs text-gray-500">{sys.type} -- {sys.processors.length} processor(s)</p>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Tier diagram */}
          {analysis.tiers && Object.keys(analysis.tiers).length > 0 && (
            <div>
              <h3 className="text-sm font-medium text-gray-300 mb-3">Tier Visualization</h3>
              <TierDiagram tiers={analysis.tiers} width={700} height={280} />
            </div>
          )}

          {/* Warnings */}
          {analysis.warnings && analysis.warnings.length > 0 && (
            <div className="rounded-lg border border-amber-500/30 bg-amber-500/5 p-4">
              <h4 className="text-sm font-medium text-amber-400 mb-2">Warnings</h4>
              <ul className="space-y-1 text-xs text-amber-300/70">
                {analysis.warnings.map((w, i) => (
                  <li key={i}>&#x26A0; {w}</li>
                ))}
              </ul>
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
