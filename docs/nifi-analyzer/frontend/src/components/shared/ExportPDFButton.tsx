import React, { useState } from 'react';
import { usePipelineStore } from '../../store/pipeline';

export default function ExportPDFButton() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const finalReport = usePipelineStore((s) => s.finalReport);
  const fileName = usePipelineStore((s) => s.fileName);
  const platform = usePipelineStore((s) => s.platform);
  const parsed = usePipelineStore((s) => s.parsed);
  const validation = usePipelineStore((s) => s.validation);

  const handleExport = async () => {
    setLoading(true);
    setError('');
    try {
      const sections = finalReport?.sections || [];
      const res = await fetch('/api/export/pdf', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          title: `ETL Migration Report - ${fileName || 'Flow'}`,
          sections,
          summary: finalReport?.executiveSummary || '',
          platform: platform || parsed?.platform || '',
          fileName: fileName || '',
          processorCount: parsed?.processors?.length || 0,
          confidence: validation?.overallScore || 0,
        }),
      });

      if (!res.ok) throw new Error(`Export failed: ${res.status}`);

      const html = await res.text();
      // Open in new window for print-to-PDF
      const w = window.open('', '_blank');
      if (w) {
        w.document.write(html);
        w.document.close();
      }
    } catch (err) {
      console.error('PDF export error:', err);
      setError('PDF export failed. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <>
    {error && (
      <span className="text-xs text-red-400 mr-2">{error}</span>
    )}
    <button
      onClick={handleExport}
      disabled={loading}
      className="flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-lg bg-gray-800 border border-border text-gray-300 hover:bg-gray-700 hover:text-gray-100 transition disabled:opacity-50"
      aria-label="Export report as PDF"
    >
      <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path
          strokeLinecap="round"
          strokeLinejoin="round"
          strokeWidth={2}
          d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"
        />
      </svg>
      {loading ? 'Generating...' : 'Export PDF'}
    </button>
    </>
  );
}
