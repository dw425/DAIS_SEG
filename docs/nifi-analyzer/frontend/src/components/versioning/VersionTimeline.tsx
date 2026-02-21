import React, { useState, useEffect, useCallback } from 'react';
import { listVersions } from '../../api/client';

interface VersionMeta {
  versionId: string;
  label: string;
  createdAt: string;
  processorCount: number;
  connectionCount: number;
}

interface VersionTimelineProps {
  projectId?: string;
  onSelectDiff?: (v1: string, v2: string) => void;
}

export default function VersionTimeline({ projectId = 'default', onSelectDiff }: VersionTimelineProps) {
  const [versions, setVersions] = useState<VersionMeta[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<string[]>([]);

  const fetchVersions = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await listVersions(projectId);
      setVersions((data.versions as VersionMeta[]) || []);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load versions');
    } finally {
      setLoading(false);
    }
  }, [projectId]);

  useEffect(() => {
    fetchVersions();
  }, [fetchVersions]);

  const toggleSelect = (id: string) => {
    setSelected((prev) => {
      if (prev.includes(id)) return prev.filter((v) => v !== id);
      if (prev.length >= 2) return [prev[1], id];
      return [...prev, id];
    });
  };

  const handleDiff = () => {
    if (selected.length === 2 && onSelectDiff) {
      onSelectDiff(selected[0], selected[1]);
    }
  };

  const formatDate = (iso: string) => {
    try {
      return new Date(iso).toLocaleString();
    } catch {
      return iso;
    }
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-lg font-semibold text-gray-100">Version History</h3>
        <div className="flex gap-2">
          <button
            onClick={fetchVersions}
            className="px-3 py-1.5 text-xs rounded-lg bg-gray-800 text-gray-300 hover:bg-gray-700 transition"
            aria-label="Refresh version history"
          >
            Refresh
          </button>
          {selected.length === 2 && (
            <button
              onClick={handleDiff}
              className="px-3 py-1.5 text-xs rounded-lg bg-primary/20 text-primary hover:bg-primary/30 transition"
              aria-label="Compare selected versions"
            >
              Compare Selected
            </button>
          )}
        </div>
      </div>

      {loading && <p className="text-sm text-gray-500">Loading versions...</p>}

      {error && (
        <div className="px-4 py-2 rounded-lg bg-red-500/10 border border-red-500/30 text-sm text-red-400">
          {error}
        </div>
      )}

      {versions.length === 0 && !loading && !error && (
        <p className="text-sm text-gray-500">No versions saved yet. Save a snapshot to start tracking.</p>
      )}

      {/* Timeline */}
      <div className="relative">
        {versions.length > 0 && (
          <div className="absolute left-4 top-0 bottom-0 w-0.5 bg-gray-700" />
        )}
        <div className="space-y-3">
          {versions.map((v, idx) => (
            <div
              key={v.versionId}
              className={`relative pl-10 cursor-pointer group ${
                selected.includes(v.versionId) ? 'opacity-100' : 'opacity-80 hover:opacity-100'
              }`}
              onClick={() => toggleSelect(v.versionId)}
              role="button"
              tabIndex={0}
              onKeyDown={(e) => e.key === 'Enter' && toggleSelect(v.versionId)}
              aria-label={`Version ${v.label}, created ${formatDate(v.createdAt)}`}
              aria-pressed={selected.includes(v.versionId)}
            >
              {/* Timeline dot */}
              <div
                className={`absolute left-2.5 top-3 w-3 h-3 rounded-full border-2 transition ${
                  selected.includes(v.versionId)
                    ? 'bg-primary border-primary'
                    : 'bg-gray-800 border-gray-600 group-hover:border-gray-400'
                }`}
              />

              <div
                className={`rounded-lg border p-3 transition ${
                  selected.includes(v.versionId)
                    ? 'border-primary/50 bg-primary/5'
                    : 'border-border bg-gray-900/50 hover:border-gray-600'
                }`}
              >
                <div className="flex items-center justify-between">
                  <span className="text-sm font-medium text-gray-200">{v.label}</span>
                  <span className="text-xs text-gray-500">{formatDate(v.createdAt)}</span>
                </div>
                <div className="flex gap-4 mt-1.5">
                  <span className="text-xs text-gray-400">
                    {v.processorCount} processors
                  </span>
                  <span className="text-xs text-gray-400">
                    {v.connectionCount} connections
                  </span>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {selected.length > 0 && selected.length < 2 && (
        <p className="text-xs text-gray-500">Select one more version to compare</p>
      )}
    </div>
  );
}
