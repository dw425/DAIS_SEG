import React, { useState, useEffect } from 'react';
import { diffVersions } from '../../api/client';

interface DiffResult {
  addedProcessors: string[];
  removedProcessors: string[];
  modifiedProcessors: string[];
  addedConnections: number;
  removedConnections: number;
  v1Label: string;
  v2Label: string;
}

interface VersionDiffProps {
  projectId?: string;
  v1: string;
  v2: string;
  onClose?: () => void;
}

export default function VersionDiff({ projectId = 'default', v1, v2, onClose }: VersionDiffProps) {
  const [diff, setDiff] = useState<DiffResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    const fetchDiff = async () => {
      setLoading(true);
      setError('');
      try {
        const data = await diffVersions(projectId, v1, v2);
        setDiff(data as DiffResult);
      } catch (e) {
        setError(e instanceof Error ? e.message : 'Failed to load diff');
      } finally {
        setLoading(false);
      }
    };
    fetchDiff();
  }, [projectId, v1, v2]);

  if (loading) {
    return <p className="text-sm text-gray-500">Computing diff...</p>;
  }

  if (error) {
    return <p className="text-sm text-red-400">{error}</p>;
  }

  if (!diff) return null;

  const totalChanges =
    diff.addedProcessors.length +
    diff.removedProcessors.length +
    diff.modifiedProcessors.length;

  return (
    <div className="rounded-lg border border-border bg-gray-900/50 overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-3 bg-gray-900/80 border-b border-border">
        <div className="flex items-center gap-3">
          <h4 className="text-sm font-semibold text-gray-100">Version Diff</h4>
          <span className="text-xs text-gray-400">
            {diff.v1Label} vs {diff.v2Label}
          </span>
        </div>
        {onClose && (
          <button
            onClick={onClose}
            className="p-1 text-gray-500 hover:text-gray-300 transition"
            aria-label="Close diff view"
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        )}
      </div>

      {/* Summary badges */}
      <div className="flex gap-3 px-4 py-3 border-b border-border">
        <span className="px-2 py-1 text-xs rounded bg-green-500/20 text-green-400">
          +{diff.addedProcessors.length} added
        </span>
        <span className="px-2 py-1 text-xs rounded bg-red-500/20 text-red-400">
          -{diff.removedProcessors.length} removed
        </span>
        <span className="px-2 py-1 text-xs rounded bg-amber-500/20 text-amber-400">
          ~{diff.modifiedProcessors.length} modified
        </span>
        <span className="px-2 py-1 text-xs rounded bg-blue-500/20 text-blue-400">
          {diff.addedConnections > 0 ? '+' : ''}{diff.addedConnections - diff.removedConnections} connections
        </span>
      </div>

      {/* Side-by-side changes */}
      <div className="grid grid-cols-2 divide-x divide-border">
        {/* Left: removals */}
        <div className="p-3">
          <p className="text-xs font-medium text-gray-500 uppercase tracking-wider mb-2">
            {diff.v1Label}
          </p>
          {diff.removedProcessors.length > 0 ? (
            <ul className="space-y-1">
              {diff.removedProcessors.map((p) => (
                <li key={p} className="text-xs text-red-400 flex items-center gap-1.5">
                  <span className="w-1.5 h-1.5 rounded-full bg-red-400" />
                  {p}
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-xs text-gray-600">No removals</p>
          )}
        </div>

        {/* Right: additions */}
        <div className="p-3">
          <p className="text-xs font-medium text-gray-500 uppercase tracking-wider mb-2">
            {diff.v2Label}
          </p>
          {diff.addedProcessors.length > 0 ? (
            <ul className="space-y-1">
              {diff.addedProcessors.map((p) => (
                <li key={p} className="text-xs text-green-400 flex items-center gap-1.5">
                  <span className="w-1.5 h-1.5 rounded-full bg-green-400" />
                  {p}
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-xs text-gray-600">No additions</p>
          )}
        </div>
      </div>

      {/* Modified processors */}
      {diff.modifiedProcessors.length > 0 && (
        <div className="px-4 py-3 border-t border-border">
          <p className="text-xs font-medium text-gray-500 uppercase tracking-wider mb-2">
            Modified Processors
          </p>
          <ul className="space-y-1">
            {diff.modifiedProcessors.map((p) => (
              <li key={p} className="text-xs text-amber-400 flex items-center gap-1.5">
                <span className="w-1.5 h-1.5 rounded-full bg-amber-400" />
                {p}
              </li>
            ))}
          </ul>
        </div>
      )}

      {totalChanges === 0 && (
        <div className="px-4 py-6 text-center">
          <p className="text-sm text-gray-500">No differences found between these versions</p>
        </div>
      )}
    </div>
  );
}
