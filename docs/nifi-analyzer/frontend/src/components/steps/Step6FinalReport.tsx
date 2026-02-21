import React, { useState } from 'react';
import { usePipelineStore } from '../../store/pipeline';
import { useUIStore } from '../../store/ui';
import { usePipeline } from '../../hooks/usePipeline';
import JsonExplorer from '../shared/JsonExplorer';

export default function Step6FinalReport() {
  const finalReport = usePipelineStore((s) => s.finalReport);
  const status = useUIStore((s) => s.stepStatuses[5]);
  const { runFinalReport } = usePipeline();
  const report = usePipelineStore((s) => s.report);
  const [activeTab, setActiveTab] = useState<'summary' | 'sections' | 'json'>('summary');

  const canRun = report && status !== 'running';

  const exportReport = (format: string) => {
    if (!finalReport) return;
    let content: string;
    let mimeType: string;
    let ext: string;

    if (format === 'json') {
      content = JSON.stringify(finalReport.rawJson || finalReport, null, 2);
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
            {finalReport.generatedAt && (
              <span className="ml-auto text-xs text-gray-600">Generated: {finalReport.generatedAt}</span>
            )}
          </div>

          {/* Tab navigation */}
          <div className="flex gap-1 border-b border-border">
            {(['summary', 'sections', 'json'] as const).map((tab) => (
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

          {activeTab === 'json' && (
            <JsonExplorer data={finalReport.rawJson || finalReport} rootLabel="finalReport" />
          )}
        </div>
      ) : null}
    </div>
  );
}
