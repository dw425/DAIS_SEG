import React, { useState } from 'react';
import { compareFlows } from '../../api/client';
import ComparisonTable from './ComparisonTable';
import ComparisonRadar from './ComparisonRadar';

interface FlowEntry {
  label: string;
  data: Record<string, unknown>;
}

export default function ComparisonDashboard() {
  const [flows, setFlows] = useState<FlowEntry[]>([]);
  const [comparison, setComparison] = useState<Record<string, unknown> | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const handleFileDrop = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = e.target.files;
    if (!files) return;

    const newFlows: FlowEntry[] = [...flows];
    for (const file of Array.from(files)) {
      try {
        const text = await file.text();
        const parsed = JSON.parse(text);
        newFlows.push({ label: file.name, data: parsed });
      } catch {
        setError(`Failed to parse ${file.name}`);
      }
    }
    setFlows(newFlows);
  };

  const runComparison = async () => {
    if (flows.length < 2) {
      setError('Need at least 2 flows to compare');
      return;
    }
    setLoading(true);
    setError('');
    try {
      const data = await compareFlows(
        flows.map((f) => f.data),
        flows.map((f) => f.label),
      );
      setComparison(data as Record<string, unknown>);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Comparison failed');
    } finally {
      setLoading(false);
    }
  };

  const removeFlow = (idx: number) => {
    setFlows((prev) => prev.filter((_, i) => i !== idx));
    setComparison(null);
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-bold text-gray-100">Flow Comparison</h2>
        <div className="flex gap-2">
          <label className="px-3 py-1.5 text-sm rounded-lg bg-gray-800 text-gray-300 hover:bg-gray-700 transition cursor-pointer">
            Add Flows
            <input
              type="file"
              accept=".json"
              multiple
              onChange={handleFileDrop}
              className="hidden"
              aria-label="Upload flow files to compare"
            />
          </label>
          {flows.length >= 2 && (
            <button
              onClick={runComparison}
              disabled={loading}
              className="px-4 py-1.5 text-sm rounded-lg bg-primary text-white hover:bg-primary/80 transition disabled:opacity-50"
              aria-label="Run comparison"
            >
              {loading ? 'Comparing...' : 'Compare'}
            </button>
          )}
        </div>
      </div>

      {error && (
        <div className="px-4 py-2 rounded-lg bg-red-500/10 border border-red-500/30 text-sm text-red-400">
          {error}
        </div>
      )}

      {/* Flow chips */}
      {flows.length > 0 && (
        <div className="flex flex-wrap gap-2">
          {flows.map((f, i) => (
            <div
              key={i}
              className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-gray-800 border border-border"
            >
              <span className="text-sm text-gray-300">{f.label}</span>
              <button
                onClick={() => removeFlow(i)}
                className="text-gray-500 hover:text-red-400 transition"
                aria-label={`Remove ${f.label}`}
              >
                <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
          ))}
        </div>
      )}

      {flows.length === 0 && (
        <div className="text-center py-12 border-2 border-dashed border-border rounded-lg">
          <p className="text-gray-500 text-sm">Upload 2 or more parsed flow JSON files to compare</p>
        </div>
      )}

      {/* Comparison results */}
      {comparison && (
        <div className="space-y-6">
          <ComparisonTable data={comparison} />
          <ComparisonRadar data={comparison} />
        </div>
      )}
    </div>
  );
}
