import React, { useState } from 'react';
import { usePipelineStore } from '../../store/pipeline';
import { useUIStore } from '../../store/ui';
import { usePipeline } from '../../hooks/usePipeline';
import { exportDAB } from '../../api/client';
import JsonExplorer from '../shared/JsonExplorer';

export default function Step6FinalReport() {
  const finalReport = usePipelineStore((s) => s.finalReport);
  const status = useUIStore((s) => s.stepStatuses[5]);
  const { runFinalReport } = usePipeline();
  const report = usePipelineStore((s) => s.report);
  const [activeTab, setActiveTab] = useState<'summary' | 'sections' | 'compatibility' | 'effort' | 'json'>('summary');

  const parsed = usePipelineStore((s) => s.parsed);
  const assessment = usePipelineStore((s) => s.assessment);
  const [dabExporting, setDabExporting] = useState(false);
  const [dabError, setDabError] = useState('');

  const canRun = report && status !== 'running';

  const downloadDAB = async () => {
    if (!parsed || !assessment) return;
    setDabExporting(true);
    setDabError('');
    try {
      const blob = await exportDAB({ parsed, assessment });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'databricks_asset_bundle.zip';
      a.click();
      URL.revokeObjectURL(url);
    } catch (err) {
      console.error('DAB export failed:', err);
      setDabError(err instanceof Error ? err.message : 'DAB export failed. Please try again.');
    } finally {
      setDabExporting(false);
    }
  };

  const exportReport = (format: string) => {
    if (!finalReport) return;
    let content: string;
    let mimeType: string;
    let ext: string;

    if (format === 'json') {
      content = JSON.stringify(finalReport, null, 2);
      mimeType = 'application/json';
      ext = 'json';
    } else if (format === 'markdown') {
      const lines = [`# Migration Report\n`, `**Generated:** ${finalReport.generatedAt}\n`, `## Executive Summary\n`, finalReport.executiveSummary, ''];
      (finalReport.sections || []).forEach((s) => {
        lines.push(`## ${s.title}\n`, s.content, '');
      });
      content = lines.join('\n');
      mimeType = 'text/markdown';
      ext = 'md';
    } else {
      content = [finalReport.executiveSummary, '', ...(finalReport.sections || []).map((s) => `${s.title}\n${'-'.repeat(s.title.length)}\n${s.content}`)].join('\n\n');
      mimeType = 'text/plain';
      ext = 'txt';
    }

    const blob = new Blob([content], { type: mimeType });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `final_report.${ext}`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold text-gray-100 flex items-center gap-3">
            <span className="w-8 h-8 rounded-lg bg-indigo-500/20 flex items-center justify-center text-sm text-indigo-400 font-mono">6</span>
            Final Report
          </h2>
          <p className="mt-1 text-sm text-gray-400">
            Executive summary, detailed sections, and multi-format export.
          </p>
        </div>
        <button
          onClick={() => runFinalReport()}
          disabled={!canRun}
          className="px-4 py-2 rounded-lg bg-primary text-white text-sm font-medium
            hover:bg-primary/80 disabled:opacity-40 disabled:cursor-not-allowed transition"
        >
          {status === 'running' ? 'Generating...' : 'Generate Final Report'}
        </button>
      </div>

      {!report ? (
        <div className="rounded-lg border border-border bg-gray-800/30 p-8 text-center text-gray-500 text-sm">
          Complete Step 5 first
        </div>
      ) : status === 'running' ? (
        <div className="flex items-center gap-3 p-4 rounded-lg bg-gray-800/50 border border-border">
          <div className="w-5 h-5 rounded-full border-2 border-indigo-400 border-t-transparent animate-spin" />
          <span className="text-sm text-gray-300">Building final report...</span>
        </div>
      ) : finalReport && status === 'done' ? (
        <div className="space-y-4">
          {/* Export buttons */}
          <div className="flex items-center gap-2">
            <span className="text-xs text-gray-500">Export as:</span>
            {['json', 'markdown', 'text'].map((fmt) => (
              <button
                key={fmt}
                onClick={() => exportReport(fmt)}
                className="px-3 py-1 rounded-lg bg-gray-800 text-gray-300 text-xs hover:bg-gray-700 transition"
              >
                {fmt.toUpperCase()}
              </button>
            ))}
            <button
              onClick={downloadDAB}
              disabled={dabExporting || !parsed || !assessment}
              className="px-3 py-1 rounded-lg bg-orange-600/80 text-white text-xs hover:bg-orange-500 disabled:opacity-40 transition flex items-center gap-1.5"
            >
              <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M20 7l-8-4-8 4m16 0l-8 4m8-4v10l-8 4m0-10L4 7m8 4v10M4 7v10l8 4" />
              </svg>
              {dabExporting ? 'Exporting...' : 'Export DAB'}
            </button>
            {finalReport.generatedAt && (
              <span className="ml-auto text-xs text-gray-600">Generated: {finalReport.generatedAt}</span>
            )}
          </div>

          {dabError && (
            <div className="rounded-lg bg-red-500/10 border border-red-500/30 p-3 text-xs text-red-400">
              {dabError}
            </div>
          )}

          {/* Tab navigation */}
          <div className="flex gap-1 border-b border-border overflow-x-auto">
            {(['summary', 'sections', 'compatibility', 'effort', 'json'] as const).map((tab) => (
              <button
                key={tab}
                onClick={() => setActiveTab(tab)}
                className={`px-4 py-2 text-sm font-medium border-b-2 transition
                  ${activeTab === tab ? 'border-primary text-primary' : 'border-transparent text-gray-500 hover:text-gray-300'}`}
              >
                {tab.charAt(0).toUpperCase() + tab.slice(1)}
              </button>
            ))}
          </div>

          {/* Tab content */}
          {activeTab === 'summary' && (
            <div className="rounded-lg border border-border bg-gray-800/30 p-6">
              <h3 className="text-lg font-medium text-gray-200 mb-3">Executive Summary</h3>
              <p className="text-sm text-gray-400 leading-relaxed whitespace-pre-wrap">
                {finalReport.executiveSummary || 'No summary available.'}
              </p>
            </div>
          )}

          {activeTab === 'sections' && (
            <div className="space-y-4">
              {(finalReport.sections || []).map((section, i) => (
                <div key={i} className="rounded-lg border border-border bg-gray-800/30 p-4">
                  <h4 className="text-sm font-medium text-gray-200 mb-2">{section.title}</h4>
                  <p className="text-sm text-gray-400 leading-relaxed whitespace-pre-wrap">{section.content}</p>
                </div>
              ))}
              {(!finalReport.sections || finalReport.sections.length === 0) && (
                <p className="text-sm text-gray-500 text-center py-8">No sections available.</p>
              )}
            </div>
          )}

          {activeTab === 'compatibility' && (
            <CompatibilityMatrixView rawJson={finalReport.rawJson} />
          )}

          {activeTab === 'effort' && (
            <EffortEstimateView rawJson={finalReport.rawJson} />
          )}

          {activeTab === 'json' && (
            <JsonExplorer data={finalReport.rawJson || finalReport} rootLabel="finalReport" />
          )}
        </div>
      ) : null}
    </div>
  );
}

// ── Compatibility Matrix View ──

function CompatibilityMatrixView({ rawJson }: { rawJson: Record<string, unknown> }) {
  const matrix = (rawJson?.compatibility_matrix ?? rawJson?.compatibilityMatrix ?? {}) as Record<string, unknown>;
  const entries = (matrix.entries ?? []) as Array<Record<string, unknown>>;
  const summary = (matrix.summary ?? {}) as Record<string, number>;

  if (entries.length === 0) {
    return <p className="text-sm text-gray-500 text-center py-8">No compatibility data available. Re-generate the final report.</p>;
  }

  const levelColors: Record<string, string> = {
    native: 'bg-green-500/20 text-green-400',
    partial: 'bg-amber-500/20 text-amber-400',
    manual: 'bg-orange-500/20 text-orange-400',
    unsupported: 'bg-red-500/20 text-red-400',
  };

  return (
    <div className="space-y-4">
      {/* Summary bar */}
      <div className="grid grid-cols-4 gap-3">
        {Object.entries(summary).map(([level, count]) => (
          <div key={level} className="rounded-lg bg-gray-800/50 border border-border p-3 text-center">
            <p className={`text-2xl font-bold tabular-nums ${levelColors[level]?.split(' ')[1] ?? 'text-gray-400'}`}>{count}</p>
            <p className="text-xs text-gray-500 mt-1 capitalize">{level}</p>
          </div>
        ))}
      </div>

      {/* Matrix table */}
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border">
              <th className="text-left text-xs text-gray-500 py-2 px-3">Processor Type</th>
              <th className="text-left text-xs text-gray-500 py-2 px-3">Category</th>
              <th className="text-left text-xs text-gray-500 py-2 px-3">Target</th>
              <th className="text-left text-xs text-gray-500 py-2 px-3">Compatibility</th>
              <th className="text-left text-xs text-gray-500 py-2 px-3">Gap</th>
            </tr>
          </thead>
          <tbody>
            {entries.map((entry, i) => {
              const level = String(entry.compatibilityLevel ?? 'unknown');
              return (
                <tr key={i} className="border-b border-border/50 hover:bg-gray-800/30">
                  <td className="py-2 px-3 text-gray-200 font-mono text-xs">{String(entry.processorType)}</td>
                  <td className="py-2 px-3 text-gray-400 text-xs capitalize">{String(entry.sourceCategory)}</td>
                  <td className="py-2 px-3 text-gray-400 text-xs">{String(entry.targetEquivalent || '-')}</td>
                  <td className="py-2 px-3">
                    <span className={`px-2 py-0.5 rounded text-[10px] font-bold uppercase ${levelColors[level] ?? 'bg-gray-500/20 text-gray-400'}`}>
                      {level}
                    </span>
                  </td>
                  <td className="py-2 px-3 text-gray-500 text-xs max-w-xs truncate">{String(entry.gapDetails || '-')}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── Effort Estimate View ──

function EffortEstimateView({ rawJson }: { rawJson: Record<string, unknown> }) {
  const effort = (rawJson?.effort_estimate ?? rawJson?.effortEstimate ?? {}) as Record<string, unknown>;
  const entries = (effort.entries ?? []) as Array<Record<string, unknown>>;
  const totalHours = Number(effort.totalHours ?? 0);
  const criticalPathHours = Number(effort.criticalPathHours ?? 0);
  const teamSize = Number(effort.teamSizeRecommendation ?? 1);
  const weeks = Number(effort.estimatedWeeks ?? 0);
  const skills = (effort.skillRequirements ?? {}) as Record<string, number>;

  if (entries.length === 0) {
    return <p className="text-sm text-gray-500 text-center py-8">No effort data available. Re-generate the final report.</p>;
  }

  const complexityColors: Record<string, string> = {
    low: 'text-green-400',
    medium: 'text-amber-400',
    high: 'text-orange-400',
    critical: 'text-red-400',
  };

  return (
    <div className="space-y-4">
      {/* Summary metrics */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <div className="rounded-lg bg-gray-800/50 border border-border p-3 text-center">
          <p className="text-2xl font-bold tabular-nums text-blue-400">{totalHours}h</p>
          <p className="text-xs text-gray-500 mt-1">Total Hours</p>
        </div>
        <div className="rounded-lg bg-gray-800/50 border border-border p-3 text-center">
          <p className="text-2xl font-bold tabular-nums text-red-400">{criticalPathHours}h</p>
          <p className="text-xs text-gray-500 mt-1">Critical Path</p>
        </div>
        <div className="rounded-lg bg-gray-800/50 border border-border p-3 text-center">
          <p className="text-2xl font-bold tabular-nums text-purple-400">{teamSize}</p>
          <p className="text-xs text-gray-500 mt-1">Team Size</p>
        </div>
        <div className="rounded-lg bg-gray-800/50 border border-border p-3 text-center">
          <p className="text-2xl font-bold tabular-nums text-cyan-400">{weeks}w</p>
          <p className="text-xs text-gray-500 mt-1">Estimated Weeks</p>
        </div>
      </div>

      {/* Skill requirements */}
      {Object.keys(skills).length > 0 && (
        <div>
          <h4 className="text-xs text-gray-500 mb-2">Required Skills</h4>
          <div className="flex flex-wrap gap-1">
            {Object.entries(skills).sort(([, a], [, b]) => b - a).map(([skill, count]) => (
              <span key={skill} className="px-2 py-0.5 bg-purple-500/10 text-purple-400 rounded text-xs">
                {skill} ({count})
              </span>
            ))}
          </div>
        </div>
      )}

      {/* Per-processor effort table */}
      <div className="overflow-x-auto max-h-80 overflow-y-auto">
        <table className="w-full text-sm">
          <thead className="sticky top-0 bg-gray-900">
            <tr className="border-b border-border">
              <th className="text-left text-xs text-gray-500 py-2 px-3">Processor</th>
              <th className="text-left text-xs text-gray-500 py-2 px-3">Type</th>
              <th className="text-right text-xs text-gray-500 py-2 px-3">Hours</th>
              <th className="text-left text-xs text-gray-500 py-2 px-3">Complexity</th>
              <th className="text-left text-xs text-gray-500 py-2 px-3">Critical</th>
            </tr>
          </thead>
          <tbody>
            {entries.map((entry, i) => {
              const complexity = String(entry.complexity ?? 'low');
              return (
                <tr key={i} className="border-b border-border/50 hover:bg-gray-800/30">
                  <td className="py-1.5 px-3 text-gray-200 text-xs">{String(entry.processorName)}</td>
                  <td className="py-1.5 px-3 text-gray-400 text-xs font-mono">{String(entry.processorType)}</td>
                  <td className="py-1.5 px-3 text-right text-gray-300 text-xs tabular-nums">{String(entry.hoursEstimate)}h</td>
                  <td className="py-1.5 px-3">
                    <span className={`text-xs ${complexityColors[complexity] ?? 'text-gray-400'}`}>{complexity}</span>
                  </td>
                  <td className="py-1.5 px-3 text-xs">
                    {entry.isOnCriticalPath ? <span className="text-red-400">Yes</span> : <span className="text-gray-600">-</span>}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
