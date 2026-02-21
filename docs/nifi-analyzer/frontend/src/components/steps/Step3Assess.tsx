import React, { useState } from 'react';
import { usePipelineStore } from '../../store/pipeline';
import { useUIStore } from '../../store/ui';
import { usePipeline } from '../../hooks/usePipeline';
import TierDiagram from '../shared/TierDiagram';
import RiskHeatmap from '../viz/RiskHeatmap';
import type { MappingEntry } from '../../types/pipeline';

function confidenceColor(c: number): string {
  const pct = c <= 1 ? c * 100 : c;
  if (pct >= 90) return 'text-green-400';
  if (pct >= 70) return 'text-amber-400';
  return 'text-red-400';
}

function fmtConfidence(c: number): string {
  const pct = c <= 1 ? c * 100 : c;
  return `${pct.toFixed(0)}%`;
}

export default function Step3Assess() {
  const parsed = usePipelineStore((s) => s.parsed);
  const analysis = usePipelineStore((s) => s.analysis);
  const assessment = usePipelineStore((s) => s.assessment);
  const status = useUIStore((s) => s.stepStatuses[2]);
  const { runAssess } = usePipeline();
  const [filter, setFilter] = useState<'all' | 'mapped' | 'unmapped'>('all');
  const [searchTerm, setSearchTerm] = useState('');

  const canRun = parsed && analysis && status !== 'running';

  const filteredMappings = (assessment?.mappings || []).filter((m: MappingEntry) => {
    if (filter === 'mapped' && !m.mapped) return false;
    if (filter === 'unmapped' && m.mapped) return false;
    if (searchTerm && !m.name.toLowerCase().includes(searchTerm.toLowerCase()) &&
      !m.type.toLowerCase().includes(searchTerm.toLowerCase())) return false;
    return true;
  });

  const mappedCount = assessment?.mappings.filter((m) => m.mapped).length ?? 0;
  const totalCount = assessment?.mappings.length ?? 0;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold text-gray-100 flex items-center gap-3">
            <span className="w-8 h-8 rounded-lg bg-amber-500/20 flex items-center justify-center text-sm text-amber-400 font-mono">3</span>
            Assess & Map
          </h2>
          <p className="mt-1 text-sm text-gray-400">
            Map each processor to its Databricks equivalent with confidence scoring.
          </p>
        </div>
        <button
          onClick={() => runAssess()}
          disabled={!canRun}
          className="px-4 py-2 rounded-lg bg-primary text-white text-sm font-medium
            hover:bg-primary/80 disabled:opacity-40 disabled:cursor-not-allowed transition"
        >
          {status === 'running' ? 'Assessing...' : 'Run Assessment'}
        </button>
      </div>

      {!parsed || !analysis ? (
        <div className="rounded-lg border border-border bg-gray-800/30 p-8 text-center text-gray-500 text-sm">
          Complete Steps 1 and 2 first
        </div>
      ) : status === 'running' ? (
        <div className="flex items-center gap-3 p-4 rounded-lg bg-gray-800/50 border border-border">
          <div className="w-5 h-5 rounded-full border-2 border-amber-400 border-t-transparent animate-spin" />
          <span className="text-sm text-gray-300">Mapping processors to Databricks equivalents...</span>
        </div>
      ) : assessment && status === 'done' ? (
        <div className="space-y-4">
          {/* Summary cards */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <SummaryCard label="Total" value={String(totalCount)} color="text-gray-400" />
            <SummaryCard label="Mapped" value={String(mappedCount)} color="text-green-400" />
            <SummaryCard label="Unmapped" value={String(assessment.unmappedCount)} color="text-red-400" />
            <SummaryCard label="Packages" value={String(assessment.packages.length)} color="text-blue-400" />
          </div>

          {/* Tier Diagram with category enrichment */}
          {parsed && parsed.processors.length > 0 && (
            <TierDiagram
              processors={parsed.processors}
              connections={parsed.connections}
              mappings={assessment.mappings}
            />
          )}

          {/* Risk Heatmap */}
          <RiskHeatmap />

          {/* Filters */}
          <div className="flex items-center gap-3 flex-wrap">
            <input
              type="text"
              placeholder="Search processors..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="px-3 py-1.5 rounded-lg bg-gray-800 border border-border text-sm text-gray-200 placeholder-gray-500 focus:outline-none focus:border-gray-600 w-56"
            />
            <div className="flex gap-1">
              {(['all', 'mapped', 'unmapped'] as const).map((f) => (
                <button
                  key={f}
                  onClick={() => setFilter(f)}
                  className={`px-2.5 py-1 rounded text-xs font-medium transition
                    ${filter === f ? 'bg-primary/20 text-primary' : 'bg-gray-800 text-gray-400 hover:text-gray-200'}`}
                >
                  {f.charAt(0).toUpperCase() + f.slice(1)}
                </button>
              ))}
            </div>
            <span className="text-xs text-gray-500 ml-auto">{filteredMappings.length} processor(s)</span>
          </div>

          {/* Table */}
          <div className="rounded-lg border border-border overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="bg-gray-900/60 border-b border-border">
                    <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-500 uppercase">Processor</th>
                    <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-500 uppercase">Type</th>
                    <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-500 uppercase">Category</th>
                    <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-500 uppercase">Role</th>
                    <th className="text-center px-4 py-2.5 text-xs font-medium text-gray-500 uppercase">Confidence</th>
                    <th className="text-center px-4 py-2.5 text-xs font-medium text-gray-500 uppercase">Mapped</th>
                    <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-500 uppercase">Notes</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-border">
                  {filteredMappings.map((m: MappingEntry, i: number) => (
                    <tr key={i} className="hover:bg-gray-800/30 transition">
                      <td className="px-4 py-2.5 text-gray-200 font-medium">{m.name}</td>
                      <td className="px-4 py-2.5 text-gray-400 font-mono text-xs">{m.type}</td>
                      <td className="px-4 py-2.5 text-gray-400 text-xs">{m.category || '—'}</td>
                      <td className="px-4 py-2.5 text-gray-400">{m.role}</td>
                      <td className="px-4 py-2.5 text-center">
                        <span className={`font-medium tabular-nums ${confidenceColor(m.confidence)}`}>{fmtConfidence(m.confidence)}</span>
                      </td>
                      <td className="px-4 py-2.5 text-center">
                        <span className={`px-2 py-0.5 rounded text-xs font-medium ${m.mapped ? 'bg-green-500/20 text-green-400' : 'bg-red-500/20 text-red-400'}`}>
                          {m.mapped ? 'Yes' : 'No'}
                        </span>
                      </td>
                      <td className="px-4 py-2.5 text-gray-300">{m.notes}</td>
                    </tr>
                  ))}
                  {filteredMappings.length === 0 && (
                    <tr>
                      <td colSpan={7} className="px-4 py-8 text-center text-gray-500">No matching processors</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}

function SummaryCard({ label, value, color }: { label: string; value: string; color: string }) {
  return (
    <div className="rounded-lg bg-gray-800/50 border border-border p-3 text-center">
      <p className={`text-lg font-bold ${color}`}>{value}</p>
      <p className="text-xs text-gray-500 mt-0.5">{label}</p>
    </div>
  );
}
