import React from 'react';
import { usePipelineStore } from '../../store/pipeline';
import { useUIStore } from '../../store/ui';
import { useProgress } from '../../hooks/useProgress';

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

  const confidenceScore = assessment?.overallConfidence ?? 0;
  const validationScore = validation?.overallScore ?? 0;

  const downloadAll = () => {
    const bundle = {
      fileName,
      platform,
      parsed,
      analysis,
      assessment,
      notebook: notebook ? { notebookName: notebook.notebookName, cellCount: notebook.cells.length } : null,
      report,
      finalReport,
      validation,
      valueAnalysis,
      exportedAt: new Date().toISOString(),
    };
    const blob = new Blob([JSON.stringify(bundle, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `migration_summary_${(fileName || 'export').replace(/\.[^.]+$/, '')}.json`;
    a.click();
    URL.revokeObjectURL(url);
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
            <OutputRow label="Analysis" detail={`${analysis.processorCount} processors, complexity ${analysis.complexityScore}`} />
          )}
          {assessment && (
            <OutputRow label="Assessment" detail={`${assessment.supportedCount} supported, ${assessment.unsupportedCount} unsupported, ${assessment.overallConfidence}% confidence`} />
          )}
          {notebook && (
            <OutputRow label="Notebook" detail={`${notebook.notebookName} -- ${notebook.cells.length} cells`} />
          )}
          {validation && (
            <OutputRow label="Validation" detail={`Score: ${validation.overallScore}, ${validation.passed ? 'PASSED' : 'NEEDS WORK'}, ${validation.gaps.length} gaps`} />
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
