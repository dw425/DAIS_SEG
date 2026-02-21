import React, { useState } from 'react';
import { usePipelineStore } from '../../store/pipeline';
import { useUIStore } from '../../store/ui';
import { usePipeline } from '../../hooks/usePipeline';
import { exportDAB } from '../../api/client';
import CodePreview from '../shared/CodePreview';
import type { ValidationSummary } from '../../types/pipeline';

export default function Step4Convert() {
  const parsed = usePipelineStore((s) => s.parsed);
  const assessment = usePipelineStore((s) => s.assessment);
  const notebook = usePipelineStore((s) => s.notebook);
  const status = useUIStore((s) => s.stepStatuses[3]);
  const { runConvert } = usePipeline();
  const [activeCell, setActiveCell] = useState(0);

  const canRun = parsed && assessment && status !== 'running';
  const isV6 = notebook?.version === 'v6';
  const validationSummary: ValidationSummary | null = notebook?.validationSummary ?? null;

  const downloadNotebook = () => {
    if (!notebook) return;
    const content = JSON.stringify({
      nbformat: 4,
      nbformat_minor: 5,
      metadata: { kernelspec: { display_name: 'Python 3', language: 'python', name: 'python3' } },
      cells: notebook.cells.map((c) => ({
        cell_type: c.type,
        source: c.source.split('\n'),
        metadata: c.metadata ?? {},
        ...(c.type === 'code' ? { outputs: [], execution_count: null } : {}),
      })),
    }, null, 2);
    const blob = new Blob([content], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'migration.ipynb';
    a.click();
    URL.revokeObjectURL(url);
  };

  const downloadPython = () => {
    if (!notebook) return;
    // V6 format: Databricks notebook with COMMAND separators
    const header = '# Databricks notebook source\n';
    const pyContent = notebook.cells
      .filter((c) => c.type === 'code')
      .map((c) => c.source)
      .join('\n\n# COMMAND ----------\n\n');
    const blob = new Blob([header + pyContent], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'migration.py';
    a.click();
    URL.revokeObjectURL(url);
  };

  const [dabExporting, setDabExporting] = useState(false);
  const [dabError, setDabError] = useState('');

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

  // Count code vs markdown cells
  const codeCells = notebook?.cells.filter((c) => c.type === 'code').length ?? 0;
  const mdCells = notebook?.cells.filter((c) => c.type === 'markdown').length ?? 0;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold text-gray-100 flex items-center gap-3">
            <span className="w-8 h-8 rounded-lg bg-green-500/20 flex items-center justify-center text-sm text-green-400 font-mono">4</span>
            Convert to Databricks
          </h2>
          <p className="mt-1 text-sm text-gray-400">
            Generate a runnable Databricks notebook with wired DataFrame pipelines and terminal writes.
          </p>
        </div>
        <button
          onClick={() => runConvert()}
          disabled={!canRun}
          className="px-4 py-2 rounded-lg bg-primary text-white text-sm font-medium
            hover:bg-primary/80 disabled:opacity-40 disabled:cursor-not-allowed transition"
        >
          {status === 'running' ? 'Generating...' : 'Generate V6 Notebook'}
        </button>
      </div>

      {!parsed || !assessment ? (
        <div className="rounded-lg border border-border bg-gray-800/30 p-8 text-center text-gray-500 text-sm">
          Complete Steps 1-3 first
        </div>
      ) : status === 'running' ? (
        <div className="flex items-center gap-3 p-4 rounded-lg bg-gray-800/50 border border-border">
          <div className="w-5 h-5 rounded-full border-2 border-green-400 border-t-transparent animate-spin" />
          <span className="text-sm text-gray-300">Running V6 generation pipeline (deep analysis + wiring + sink generation + validation)...</span>
        </div>
      ) : notebook && status === 'done' ? (
        <div className="space-y-4">
          {/* V6 Quality Gate Banner */}
          {isV6 && (
            <div className={`rounded-lg border p-4 flex items-center justify-between ${
              notebook.isRunnable
                ? 'border-green-500/30 bg-green-500/5'
                : 'border-amber-500/30 bg-amber-500/5'
            }`}>
              <div className="flex items-center gap-3">
                <div className={`w-10 h-10 rounded-lg flex items-center justify-center ${
                  notebook.isRunnable ? 'bg-green-500/20' : 'bg-amber-500/20'
                }`}>
                  {notebook.isRunnable ? (
                    <svg className="w-6 h-6 text-green-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                    </svg>
                  ) : (
                    <svg className="w-6 h-6 text-amber-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z" />
                    </svg>
                  )}
                </div>
                <div>
                  <p className={`text-sm font-medium ${notebook.isRunnable ? 'text-green-400' : 'text-amber-400'}`}>
                    {notebook.isRunnable ? 'Notebook is Runnable' : 'Notebook Needs Review'}
                  </p>
                  <p className="text-xs text-gray-500">
                    V6 Quality Gate {validationSummary
                      ? `-- ${validationSummary.checks_passed ?? '?'}/${validationSummary.checks_total ?? '?'} checks passed`
                      : ''}
                  </p>
                </div>
              </div>
              <span className={`px-3 py-1 rounded-lg text-sm font-bold ${
                notebook.isRunnable ? 'bg-green-500/20 text-green-400' : 'bg-amber-500/20 text-amber-400'
              }`}>
                V6
              </span>
            </div>
          )}

          {/* Notebook info + download */}
          <div className="flex items-center justify-between bg-gray-800/50 border border-border rounded-lg p-4">
            <div className="flex items-center gap-4">
              <div className="w-10 h-10 rounded-lg bg-green-500/20 flex items-center justify-center">
                <svg className="w-5 h-5 text-green-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                </svg>
              </div>
              <div>
                <p className="text-sm font-medium text-gray-200">migration_notebook</p>
                <p className="text-xs text-gray-500">
                  {notebook.cells.length} cells ({codeCells} code, {mdCells} markdown) -- python
                  {isV6 && ' -- V6 wired pipeline'}
                </p>
              </div>
            </div>
            <div className="flex gap-2">
              <button onClick={downloadNotebook} className="px-3 py-1.5 rounded-lg bg-gray-700 text-gray-200 text-xs hover:bg-gray-600 transition">
                Download .ipynb
              </button>
              <button onClick={downloadPython} className="px-3 py-1.5 rounded-lg bg-gray-700 text-gray-200 text-xs hover:bg-gray-600 transition">
                Download .py
              </button>
              <button
                onClick={downloadDAB}
                disabled={dabExporting}
                className="px-3 py-1.5 rounded-lg bg-orange-600/80 text-white text-xs hover:bg-orange-500 disabled:opacity-40 transition flex items-center gap-1.5"
              >
                <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M20 7l-8-4-8 4m16 0l-8 4m8-4v10l-8 4m0-10L4 7m8 4v10M4 7v10l8 4" />
                </svg>
                {dabExporting ? 'Exporting...' : 'Export DAB'}
              </button>
            </div>
          </div>

          {dabError && (
            <div className="rounded-lg bg-red-500/10 border border-red-500/30 p-3 text-xs text-red-400">
              {dabError}
            </div>
          )}

          {/* V6 Validation warnings */}
          {validationSummary?.warnings && validationSummary.warnings.length > 0 && (
            <div className="rounded-lg border border-amber-500/30 bg-amber-500/5 p-3">
              <p className="text-xs font-medium text-amber-400 mb-2">Generation Warnings</p>
              <ul className="space-y-1">
                {validationSummary.warnings.map((w, i) => (
                  <li key={i} className="text-xs text-amber-300/70">{w}</li>
                ))}
              </ul>
            </div>
          )}

          {/* Cell navigator */}
          <div className="flex gap-1 overflow-x-auto pb-2">
            {notebook.cells.map((cell, i) => (
              <button
                key={i}
                onClick={() => setActiveCell(i)}
                className={`px-3 py-1.5 rounded text-xs shrink-0 transition
                  ${i === activeCell ? 'bg-primary/20 text-primary' : 'bg-gray-800 text-gray-400 hover:text-gray-200'}`}
                title={cell.label || undefined}
              >
                {cell.label
                  ? cell.label.slice(0, 16) + (cell.label.length > 16 ? '...' : '')
                  : cell.type === 'code' ? `Code ${i + 1}` : `MD ${i + 1}`}
              </button>
            ))}
          </div>

          {/* Active cell preview */}
          {notebook.cells[activeCell] && (
            <div>
              {notebook.cells[activeCell].label && (
                <p className="text-xs text-gray-500 mb-1 font-mono">{notebook.cells[activeCell].label}</p>
              )}
              <CodePreview
                code={notebook.cells[activeCell].source}
                language={notebook.cells[activeCell].type === 'code' ? 'python' : 'markdown'}
                title={`Cell ${activeCell + 1} (${notebook.cells[activeCell].type})`}
              />
            </div>
          )}
        </div>
      ) : null}
    </div>
  );
}
