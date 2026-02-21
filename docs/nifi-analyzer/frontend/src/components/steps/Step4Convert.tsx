import React, { useState } from 'react';
import { usePipelineStore } from '../../store/pipeline';
import { useUIStore } from '../../store/ui';
import { usePipeline } from '../../hooks/usePipeline';
import CodePreview from '../shared/CodePreview';

export default function Step4Convert() {
  const parsed = usePipelineStore((s) => s.parsed);
  const assessment = usePipelineStore((s) => s.assessment);
  const notebook = usePipelineStore((s) => s.notebook);
  const status = useUIStore((s) => s.stepStatuses[3]);
  const { runConvert } = usePipeline();
  const [activeCell, setActiveCell] = useState(0);

  const canRun = parsed && assessment && status !== 'running';

  const downloadNotebook = () => {
    if (!notebook) return;
    const content = JSON.stringify({
      nbformat: 4,
      nbformat_minor: 5,
      metadata: { kernelspec: { display_name: 'Python 3', language: 'python', name: 'python3' } },
      cells: notebook.cells.map((c) => ({
        cell_type: c.cell_type,
        source: c.source.split('\n'),
        metadata: {},
        ...(c.cell_type === 'code' ? { outputs: [], execution_count: null } : {}),
      })),
    }, null, 2);
    const blob = new Blob([content], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${notebook.notebookName || 'migration'}.ipynb`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const downloadPython = () => {
    if (!notebook) return;
    const pyContent = notebook.cells
      .filter((c) => c.cell_type === 'code')
      .map((c) => c.source)
      .join('\n\n# ──────────────────────────────────────\n\n');
    const blob = new Blob([pyContent], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${notebook.notebookName || 'migration'}.py`;
    a.click();
    URL.revokeObjectURL(url);
  };

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
            Generate a Databricks notebook with the converted ETL pipeline code.
          </p>
        </div>
        <button
          onClick={() => runConvert()}
          disabled={!canRun}
          className="px-4 py-2 rounded-lg bg-primary text-white text-sm font-medium
            hover:bg-primary/80 disabled:opacity-40 disabled:cursor-not-allowed transition"
        >
          {status === 'running' ? 'Generating...' : 'Generate Notebook'}
        </button>
      </div>

      {!parsed || !assessment ? (
        <div className="rounded-lg border border-border bg-gray-800/30 p-8 text-center text-gray-500 text-sm">
          Complete Steps 1-3 first
        </div>
      ) : status === 'running' ? (
        <div className="flex items-center gap-3 p-4 rounded-lg bg-gray-800/50 border border-border">
          <div className="w-5 h-5 rounded-full border-2 border-green-400 border-t-transparent animate-spin" />
          <span className="text-sm text-gray-300">Generating Databricks notebook...</span>
        </div>
      ) : notebook && status === 'done' ? (
        <div className="space-y-4">
          {/* Notebook info + download */}
          <div className="flex items-center justify-between bg-gray-800/50 border border-border rounded-lg p-4">
            <div className="flex items-center gap-4">
              <div className="w-10 h-10 rounded-lg bg-green-500/20 flex items-center justify-center">
                <svg className="w-5 h-5 text-green-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                </svg>
              </div>
              <div>
                <p className="text-sm font-medium text-gray-200">{notebook.notebookName || 'migration_notebook'}</p>
                <p className="text-xs text-gray-500">{notebook.cells.length} cells -- {notebook.language || 'python'}</p>
              </div>
            </div>
            <div className="flex gap-2">
              <button onClick={downloadNotebook} className="px-3 py-1.5 rounded-lg bg-gray-700 text-gray-200 text-xs hover:bg-gray-600 transition">
                Download .ipynb
              </button>
              <button onClick={downloadPython} className="px-3 py-1.5 rounded-lg bg-gray-700 text-gray-200 text-xs hover:bg-gray-600 transition">
                Download .py
              </button>
            </div>
          </div>

          {/* Cell navigator */}
          <div className="flex gap-1 overflow-x-auto pb-2">
            {notebook.cells.map((cell, i) => (
              <button
                key={i}
                onClick={() => setActiveCell(i)}
                className={`px-3 py-1.5 rounded text-xs shrink-0 transition
                  ${i === activeCell ? 'bg-primary/20 text-primary' : 'bg-gray-800 text-gray-400 hover:text-gray-200'}`}
              >
                {cell.cell_type === 'code' ? `Code ${i + 1}` : `MD ${i + 1}`}
              </button>
            ))}
          </div>

          {/* Active cell preview */}
          {notebook.cells[activeCell] && (
            <CodePreview
              code={notebook.cells[activeCell].source}
              language={notebook.cells[activeCell].cell_type === 'code' ? notebook.language || 'python' : 'markdown'}
              title={`Cell ${activeCell + 1} (${notebook.cells[activeCell].cell_type})`}
            />
          )}
        </div>
      ) : null}
    </div>
  );
}
