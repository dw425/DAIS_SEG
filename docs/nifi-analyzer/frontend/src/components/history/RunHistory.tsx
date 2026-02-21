import React, { useState, useEffect } from 'react';
import { listRuns } from '../../api/client';
import RunDetail from './RunDetail';

interface RunEntry {
  id: string;
  fileName: string;
  platform: string;
  processorCount: number;
  status: string;
  durationMs: number;
  stepsCompleted: string[];
  createdAt: string;
}

export default function RunHistory() {
  const [runs, setRuns] = useState<RunEntry[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);

  useEffect(() => {
    const fetchRuns = async () => {
      setLoading(true);
      setError(null);
      try {
        const data = await listRuns(50);
        setRuns((data.runs as RunEntry[]) || []);
        setTotal(data.total || 0);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load run history');
      } finally {
        setLoading(false);
      }
    };
    fetchRuns();
  }, []);

  const formatDuration = (ms: number) => {
    if (ms < 1000) return `${ms}ms`;
    return `${(ms / 1000).toFixed(1)}s`;
  };

  const formatDate = (iso: string) => {
    try {
      return new Date(iso).toLocaleString();
    } catch {
      return iso;
    }
  };

  const statusColor = (status: string) => {
    switch (status) {
      case 'completed': return 'bg-green-500/20 text-green-400';
      case 'failed': return 'bg-red-500/20 text-red-400';
      case 'running': return 'bg-blue-500/20 text-blue-400 animate-pulse';
      default: return 'bg-gray-500/20 text-gray-400';
    }
  };

  if (selectedId) {
    return <RunDetail runId={selectedId} onBack={() => setSelectedId(null)} />;
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-bold text-gray-100">Run History</h2>
        <span className="text-sm text-gray-500">{total} total runs</span>
      </div>

      {loading && <p className="text-sm text-gray-500">Loading...</p>}

      {error && (
        <div className="px-4 py-2 rounded-lg bg-red-500/10 border border-red-500/30 text-sm text-red-400">
          {error}
        </div>
      )}

      {runs.length === 0 && !loading && !error && (
        <div className="text-center py-12 border-2 border-dashed border-border rounded-lg">
          <p className="text-gray-500 text-sm">No runs recorded yet</p>
        </div>
      )}

      {runs.length > 0 && (
        <div className="rounded-lg border border-border overflow-hidden">
          <table className="w-full text-sm" role="table" aria-label="Pipeline run history">
            <thead>
              <tr className="bg-gray-900/60 border-b border-border">
                <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">File</th>
                <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">Platform</th>
                <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">Processors</th>
                <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">Status</th>
                <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">Duration</th>
                <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">Date</th>
                <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase"></th>
              </tr>
            </thead>
            <tbody>
              {runs.map((run) => (
                <tr
                  key={run.id}
                  className="border-b border-border/50 hover:bg-gray-900/30 transition cursor-pointer"
                  onClick={() => setSelectedId(run.id)}
                >
                  <td className="px-4 py-2.5 text-gray-300 font-medium">{run.fileName}</td>
                  <td className="px-4 py-2.5 text-gray-400">{run.platform}</td>
                  <td className="px-4 py-2.5 text-gray-400">{run.processorCount}</td>
                  <td className="px-4 py-2.5">
                    <span className={`px-2 py-0.5 rounded text-xs font-medium ${statusColor(run.status)}`}>
                      {run.status}
                    </span>
                  </td>
                  <td className="px-4 py-2.5 text-gray-400 font-mono text-xs">
                    {formatDuration(run.durationMs)}
                  </td>
                  <td className="px-4 py-2.5 text-gray-500 text-xs">{formatDate(run.createdAt)}</td>
                  <td className="px-4 py-2.5">
                    <button
                      className="text-xs text-primary hover:text-primary/80 transition"
                      aria-label={`View details for run ${run.id}`}
                    >
                      Details
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
