import React, { useState } from 'react';
import { usePipelineStore } from '../../store/pipeline';
import { useUIStore } from '../../store/ui';
import { usePipeline } from '../../hooks/usePipeline';
import type { DroppableProcessor } from '../../types/pipeline';
import ROIDashboard from './ROIDashboard';

type TabKey = 'value' | 'roi';

export default function Step8ValueAnalysis() {
  const parsed = usePipelineStore((s) => s.parsed);
  const valueAnalysis = usePipelineStore((s) => s.valueAnalysis);
  const status = useUIStore((s) => s.stepStatuses[7]);
  const { runValueAnalysis } = usePipeline();
  const [activeTab, setActiveTab] = useState<TabKey>('value');

  const canRun = parsed && status !== 'running';

  const totalSavings = valueAnalysis?.droppableProcessors?.reduce((sum, p) => sum + p.savingsHours, 0) ?? 0;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold text-gray-100 flex items-center gap-3">
            <span className="w-8 h-8 rounded-lg bg-orange-500/20 flex items-center justify-center text-sm text-orange-400 font-mono">8</span>
            Value Analysis
          </h2>
          <p className="mt-1 text-sm text-gray-400">
            Identify droppable processors, complexity breakdown, ROI estimate, and implementation roadmap.
          </p>
        </div>
        <button
          onClick={() => runValueAnalysis()}
          disabled={!canRun}
          className="px-4 py-2 rounded-lg bg-primary text-white text-sm font-medium
            hover:bg-primary/80 disabled:opacity-40 disabled:cursor-not-allowed transition"
        >
          {status === 'running' ? 'Analyzing...' : 'Run Value Analysis'}
        </button>
      </div>

      {/* Tabs */}
      <div className="flex border-b border-border">
        <button
          onClick={() => setActiveTab('value')}
          className={`px-4 py-2 text-sm font-medium border-b-2 transition ${
            activeTab === 'value'
              ? 'border-primary text-primary'
              : 'border-transparent text-gray-500 hover:text-gray-300'
          }`}
        >
          Value Analysis
        </button>
        <button
          onClick={() => setActiveTab('roi')}
          className={`px-4 py-2 text-sm font-medium border-b-2 transition ${
            activeTab === 'roi'
              ? 'border-primary text-primary'
              : 'border-transparent text-gray-500 hover:text-gray-300'
          }`}
        >
          ROI Dashboard
        </button>
      </div>

      {/* Tab content */}
      {activeTab === 'roi' ? (
        <ROIDashboard />
      ) : (
        <>
          {!parsed ? (
            <div className="rounded-lg border border-border bg-gray-800/30 p-8 text-center text-gray-500 text-sm">
              Complete Step 1 first
            </div>
          ) : status === 'running' ? (
            <div className="flex items-center gap-3 p-4 rounded-lg bg-gray-800/50 border border-border">
              <div className="w-5 h-5 rounded-full border-2 border-orange-400 border-t-transparent animate-spin" />
              <span className="text-sm text-gray-300">Running value analysis...</span>
            </div>
          ) : valueAnalysis && status === 'done' ? (
            <div className="space-y-6">
              {/* ROI Calculator */}
              {valueAnalysis.roiEstimate && (
                <div>
                  <h3 className="text-sm font-medium text-gray-300 mb-3">ROI Estimate</h3>
                  <div className="grid grid-cols-3 gap-3">
                    <div className="rounded-lg bg-green-500/5 border border-green-500/20 p-4 text-center">
                      <p className="text-2xl font-bold text-green-400 tabular-nums">
                        ${valueAnalysis.roiEstimate.costSavings.toLocaleString()}
                      </p>
                      <p className="text-xs text-gray-500 mt-1">Cost Savings</p>
                    </div>
                    <div className="rounded-lg bg-blue-500/5 border border-blue-500/20 p-4 text-center">
                      <p className="text-2xl font-bold text-blue-400 tabular-nums">
                        {valueAnalysis.roiEstimate.timeSavings}h
                      </p>
                      <p className="text-xs text-gray-500 mt-1">Time Savings</p>
                    </div>
                    <div className="rounded-lg bg-purple-500/5 border border-purple-500/20 p-4 text-center">
                      <p className="text-2xl font-bold text-purple-400 tabular-nums">
                        {valueAnalysis.roiEstimate.riskReduction}%
                      </p>
                      <p className="text-xs text-gray-500 mt-1">Risk Reduction</p>
                    </div>
                  </div>
                </div>
              )}

              {/* Droppable processors */}
              {valueAnalysis.droppableProcessors && valueAnalysis.droppableProcessors.length > 0 && (
                <div>
                  <h3 className="text-sm font-medium text-gray-300 mb-3">
                    Droppable Processors ({valueAnalysis.droppableProcessors.length}) -- {totalSavings}h savings
                  </h3>
                  <div className="rounded-lg border border-border overflow-hidden">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="bg-gray-900/60 border-b border-border">
                          <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-500 uppercase">Processor</th>
                          <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-500 uppercase">Type</th>
                          <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-500 uppercase">Reason</th>
                          <th className="text-right px-4 py-2.5 text-xs font-medium text-gray-500 uppercase">Savings (h)</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-border">
                        {valueAnalysis.droppableProcessors.map((p: DroppableProcessor, i: number) => (
                          <tr key={i} className="hover:bg-gray-800/30 transition">
                            <td className="px-4 py-2.5 text-gray-200 font-medium">{p.name}</td>
                            <td className="px-4 py-2.5 text-gray-400 font-mono text-xs">{p.type}</td>
                            <td className="px-4 py-2.5 text-gray-400">{p.reason}</td>
                            <td className="px-4 py-2.5 text-right text-green-400 font-medium tabular-nums">{p.savingsHours}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {/* Complexity breakdown */}
              {valueAnalysis.complexityBreakdown && Object.keys(valueAnalysis.complexityBreakdown).length > 0 && (
                <div>
                  <h3 className="text-sm font-medium text-gray-300 mb-3">Complexity Breakdown</h3>
                  <div className="space-y-2">
                    {Object.entries(valueAnalysis.complexityBreakdown).map(([key, value]) => {
                      const total = Object.values(valueAnalysis.complexityBreakdown).reduce((a, b) => a + b, 0);
                      const pct = total > 0 ? (value / total) * 100 : 0;
                      return (
                        <div key={key} className="flex items-center gap-3">
                          <span className="text-xs text-gray-400 w-28 truncate capitalize">{key.replace(/_/g, ' ')}</span>
                          <div className="flex-1 h-2 bg-gray-800 rounded-full overflow-hidden">
                            <div className="h-full bg-orange-500 rounded-full" style={{ width: `${pct}%` }} />
                          </div>
                          <span className="text-xs text-gray-500 w-12 text-right tabular-nums">{value}</span>
                        </div>
                      );
                    })}
                  </div>
                </div>
              )}

              {/* Implementation roadmap */}
              {valueAnalysis.implementationRoadmap && valueAnalysis.implementationRoadmap.length > 0 && (
                <div>
                  <h3 className="text-sm font-medium text-gray-300 mb-3">Implementation Roadmap</h3>
                  <div className="space-y-3">
                    {valueAnalysis.implementationRoadmap.map((phase, i) => (
                      <div key={i} className="rounded-lg border border-border bg-gray-800/30 p-4">
                        <div className="flex items-center justify-between mb-2">
                          <span className="text-sm font-medium text-gray-200">{phase.phase}</span>
                          <span className="text-xs text-gray-500 tabular-nums">{phase.weeks} week(s)</span>
                        </div>
                        <ul className="space-y-1">
                          {phase.tasks.map((task, j) => (
                            <li key={j} className="text-xs text-gray-400 flex items-start gap-2">
                              <span className="text-gray-600 mt-0.5">&#x2022;</span>
                              {task}
                            </li>
                          ))}
                        </ul>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          ) : null}
        </>
      )}
    </div>
  );
}
