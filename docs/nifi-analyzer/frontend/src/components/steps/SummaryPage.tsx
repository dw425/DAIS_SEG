import React from 'react';
import { usePipelineStore } from '../../store/pipeline';
import { useUIStore } from '../../store/ui';
import { useProgress } from '../../hooks/useProgress';
import VizDashboard from '../viz/VizDashboard';

function ScoreRing({ value, label, color }: { value: number; label: string; color: string }) {
  const radius = 36;
  const circumference = 2 * Math.PI * radius;
  const dashOffset = circumference - (value / 100) * circumference;

  return (
    <div className="flex flex-col items-center">
      <svg width="88" height="88" className="-rotate-90">
        <circle cx="44" cy="44" r={radius} fill="none" stroke="#1f2937" strokeWidth="6" />
        <circle
          cx="44" cy="44" r={radius} fill="none"
          stroke={color} strokeWidth="6" strokeLinecap="round"
          strokeDasharray={circumference}
          strokeDashoffset={dashOffset}
          className="transition-all duration-1000"
        />
      </svg>
      <span className="text-lg font-bold text-gray-200 -mt-[58px] mb-[30px]">{value}%</span>
      <span className="text-xs text-gray-500 mt-1">{label}</span>
    </div>
  );
}

export default function SummaryPage() {
  const {
    fileName, platform, parsed, analysis, assessment, notebook,
    report, finalReport, validation, valueAnalysis,
  } = usePipelineStore();
  const stepStatuses = useUIStore((s) => s.stepStatuses);
  const { percent } = useProgress();

  const mappedCount = assessment?.mappings.filter((m) => m.mapped).length ?? 0;
  const totalCount = assessment?.mappings.length ?? 1;
  const confidenceScore = totalCount > 0 ? Math.round((mappedCount / totalCount) * 100) : 0;
  const rawValScore = validation?.overallScore ?? 0;
  const validationScore = rawValScore <= 1 ? Math.round(rawValScore * 100) : Math.round(rawValScore);

  const baseName = (fileName || 'export').replace(/\.[^.]+$/, '');

  const triggerDownload = (blob: Blob, name: string) => {
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = name;
    a.click();
    URL.revokeObjectURL(url);
  };

  const downloadAll = () => {
    const bundle = {
      fileName,
      platform,
      parsed,
      analysis,
      assessment,
      notebook: notebook ? { cellCount: notebook.cells.length } : null,
      report,
      finalReport,
      validation,
      valueAnalysis,
      exportedAt: new Date().toISOString(),
    };
    triggerDownload(
      new Blob([JSON.stringify(bundle, null, 2)], { type: 'application/json' }),
      `migration_summary_${baseName}.json`,
    );
  };

  const downloadNotebook = () => {
    if (!notebook) return;
    const content = JSON.stringify({
      nbformat: 4,
      nbformat_minor: 5,
      metadata: { kernelspec: { display_name: 'Python 3', language: 'python', name: 'python3' } },
      cells: notebook.cells.map((c) => ({
        cell_type: c.type,
        source: c.source.split('\n'),
        metadata: {},
        ...(c.type === 'code' ? { outputs: [], execution_count: null } : {}),
      })),
    }, null, 2);
    triggerDownload(new Blob([content], { type: 'application/json' }), `${baseName}_notebook.ipynb`);
  };

  const downloadPython = () => {
    if (!notebook) return;
    const pyContent = notebook.cells
      .filter((c) => c.type === 'code')
      .map((c) => c.source)
      .join('\n\n# ──────────────────────────────────────\n\n');
    triggerDownload(new Blob([pyContent], { type: 'text/plain' }), `${baseName}_notebook.py`);
  };

  const downloadReport = () => {
    if (!report && !finalReport) return;
    const data = { report, finalReport, exportedAt: new Date().toISOString() };
    triggerDownload(
      new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' }),
      `${baseName}_report.json`,
    );
  };

  const downloadValidation = () => {
    if (!validation) return;
    triggerDownload(
      new Blob([JSON.stringify(validation, null, 2)], { type: 'application/json' }),
      `${baseName}_validation.json`,
    );
  };

  const stepNames = ['Parse', 'Analyze', 'Assess', 'Convert', 'Report', 'Final Report', 'Validate', 'Value Analysis'];

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold text-gray-100 flex items-center gap-3">
            <span className="w-8 h-8 rounded-lg bg-gray-500/20 flex items-center justify-center text-sm text-gray-400">
              &#x03A3;
            </span>
            Pipeline Summary
          </h2>
          <p className="mt-1 text-sm text-gray-400">
            {fileName ? `Results for ${fileName}` : 'All pipeline outputs aggregated'}
          </p>
        </div>
        <button
          onClick={downloadAll}
          className="px-4 py-2 rounded-lg bg-primary text-white text-sm font-medium
            hover:bg-primary/80 transition"
        >
          Export All (JSON)
        </button>
      </div>

      {/* Download buttons */}
      <div className="rounded-lg bg-gray-800/50 border border-border p-4">
        <h3 className="text-sm font-medium text-gray-300 mb-3">Downloads</h3>
        <div className="flex flex-wrap gap-2">
          <button
            onClick={downloadNotebook}
            disabled={!notebook}
            className="px-3 py-2 rounded-lg bg-green-600/80 text-white text-xs font-medium
              hover:bg-green-500 disabled:opacity-30 disabled:cursor-not-allowed transition flex items-center gap-1.5"
          >
            <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
            </svg>
            Notebook (.ipynb)
          </button>
          <button
            onClick={downloadPython}
            disabled={!notebook}
            className="px-3 py-2 rounded-lg bg-blue-600/80 text-white text-xs font-medium
              hover:bg-blue-500 disabled:opacity-30 disabled:cursor-not-allowed transition flex items-center gap-1.5"
          >
            <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4" />
            </svg>
            Notebook (.py)
          </button>
          <button
            onClick={downloadReport}
            disabled={!report && !finalReport}
            className="px-3 py-2 rounded-lg bg-purple-600/80 text-white text-xs font-medium
              hover:bg-purple-500 disabled:opacity-30 disabled:cursor-not-allowed transition flex items-center gap-1.5"
          >
            <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 17v-2m3 2v-4m3 4v-6m2 10H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
            </svg>
            Migration Report
          </button>
          <button
            onClick={downloadValidation}
            disabled={!validation}
            className="px-3 py-2 rounded-lg bg-amber-600/80 text-white text-xs font-medium
              hover:bg-amber-500 disabled:opacity-30 disabled:cursor-not-allowed transition flex items-center gap-1.5"
          >
            <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            Validation Report
          </button>
        </div>
      </div>

      {/* Progress */}
      <div className="rounded-lg bg-gray-800/50 border border-border p-4">
        <div className="flex items-center justify-between mb-2">
          <span className="text-sm text-gray-300">Overall Progress</span>
          <span className="text-sm font-medium text-gray-200 tabular-nums">{percent}%</span>
        </div>
        <div className="h-3 bg-gray-800 rounded-full overflow-hidden">
          <div
            className={`h-full rounded-full transition-all duration-500 ${percent === 100 ? 'bg-green-500' : 'bg-primary'}`}
            style={{ width: `${percent}%` }}
          />
        </div>
      </div>

      {/* Score rings */}
      <div className="flex items-center justify-center gap-12 py-4">
        <ScoreRing value={confidenceScore} label="Confidence" color={confidenceScore >= 90 ? '#22c55e' : confidenceScore >= 70 ? '#eab308' : '#ef4444'} />
        <ScoreRing value={validationScore} label="Validation" color={validationScore >= 90 ? '#22c55e' : validationScore >= 70 ? '#eab308' : '#ef4444'} />
        <ScoreRing value={percent} label="Completion" color="#FF4B4B" />
      </div>

      {/* Visual Dashboard */}
      <VizDashboard />

      {/* Step status grid */}
      <div>
        <h3 className="text-sm font-medium text-gray-300 mb-3">Step Status</h3>
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
          {stepNames.map((name, i) => {
            const st = stepStatuses[i];
            const styles = {
              idle: 'border-border text-gray-500',
              running: 'border-primary/50 text-primary',
              done: 'border-green-500/50 text-green-400',
              error: 'border-red-500/50 text-red-400',
            };
            const icons = { idle: '\u25CB', running: '\u25D4', done: '\u2713', error: '\u2716' };
            return (
              <div key={i} className={`rounded-lg border bg-gray-800/30 p-3 flex items-center gap-2 ${styles[st]}`}>
                <span className="text-base">{icons[st]}</span>
                <div>
                  <p className="text-xs font-medium">{name}</p>
                  <p className="text-xs text-gray-600 capitalize">{st}</p>
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Key outputs */}
      <div>
        <h3 className="text-sm font-medium text-gray-300 mb-3">Key Outputs</h3>
        <div className="space-y-2">
          {analysis && (
            <OutputRow label="Analysis" detail={`${analysis.externalSystems.length} external systems, ${analysis.cycles.length} cycles`} />
          )}
          {assessment && (
            <OutputRow label="Assessment" detail={`${mappedCount} mapped, ${assessment.unmappedCount} unmapped`} />
          )}
          {notebook && (
            <OutputRow label="Notebook" detail={`${notebook.cells.length} cells`} />
          )}
          {validation && (
            <OutputRow label="Validation" detail={`Score: ${validationScore}%, ${validation.gaps.length} gaps, ${validation.errors.length} errors`} />
          )}
          {valueAnalysis && (
            <OutputRow
              label="Value Analysis"
              detail={`${valueAnalysis.droppableProcessors.length} droppable, $${valueAnalysis.roiEstimate.costSavings.toLocaleString()} savings`}
            />
          )}
          {!analysis && !assessment && !notebook && !validation && !valueAnalysis && (
            <p className="text-sm text-gray-500 text-center py-4">No outputs yet. Run the pipeline to see results.</p>
          )}
        </div>
      </div>
    </div>
  );
}

function OutputRow({ label, detail }: { label: string; detail: string }) {
  return (
    <div className="flex items-center justify-between rounded-lg bg-gray-800/30 border border-border px-4 py-2.5">
      <span className="text-sm font-medium text-gray-200">{label}</span>
      <span className="text-xs text-gray-400">{detail}</span>
    </div>
  );
}
