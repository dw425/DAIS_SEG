import React, { useCallback, useState } from 'react';
import { usePipelineStore } from '../../store/pipeline';
import { useUIStore } from '../../store/ui';
import { usePipeline } from '../../hooks/usePipeline';
import FileUpload from '../shared/FileUpload';
import NiFiConnectModal from '../shared/NiFiConnectModal';
import type { ParseResult } from '../../types/pipeline';

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

export default function Step1Parse() {
  const parsed = usePipelineStore((s) => s.parsed);
  const fileName = usePipelineStore((s) => s.fileName);
  const fileSize = usePipelineStore((s) => s.fileSize);
  const platform = usePipelineStore((s) => s.platform);
  const status = useUIStore((s) => s.stepStatuses[0]);
  const { runAll } = usePipeline();
  const [showNiFiModal, setShowNiFiModal] = useState(false);

  const handleFile = useCallback(
    (file: File) => {
      runAll(file);
    },
    [runAll],
  );

  const handleNiFiExtract = useCallback(
    (extractedParsed: unknown) => {
      // Set the parsed result directly from NiFi live extraction
      const p = extractedParsed as ParseResult;
      usePipelineStore.getState().setParsed(p);
      usePipelineStore.getState().setFile('nifi-live-extraction', 0);
      if (p.platform) usePipelineStore.getState().setPlatform(p.platform);
      useUIStore.getState().setStepStatus(0, 'done');
      useUIStore.getState().setActiveStep(1);
    },
    [],
  );

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h2 className="text-xl font-semibold text-gray-100 flex items-center gap-3">
          <span className="w-8 h-8 rounded-lg bg-blue-500/20 flex items-center justify-center text-sm text-blue-400 font-mono">1</span>
          Parse Flow
        </h2>
        <p className="mt-1 text-sm text-gray-400">
          Upload your ETL flow definition file. The parser will auto-detect the platform and extract processors, connections, and process groups.
        </p>
      </div>

      {/* Upload zone + NiFi connect */}
      <div className="space-y-3">
        <FileUpload onFile={handleFile} disabled={status === 'running'} />
        <div className="flex items-center gap-3">
          <div className="flex-1 h-px bg-border" />
          <span className="text-xs text-gray-500">or</span>
          <div className="flex-1 h-px bg-border" />
        </div>
        <button
          onClick={() => setShowNiFiModal(true)}
          disabled={status === 'running'}
          className="w-full px-4 py-3 rounded-lg border border-dashed border-green-500/30 bg-green-500/5
            text-sm text-green-400 font-medium hover:bg-green-500/10 hover:border-green-500/50
            disabled:opacity-40 disabled:cursor-not-allowed transition flex items-center justify-center gap-2"
        >
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
          </svg>
          Connect to Live NiFi Instance
        </button>
      </div>

      {/* NiFi Connect Modal */}
      <NiFiConnectModal
        open={showNiFiModal}
        onClose={() => setShowNiFiModal(false)}
        onExtract={handleNiFiExtract}
      />

      {/* Loading */}
      {status === 'running' && (
        <div className="flex items-center gap-3 p-4 rounded-lg bg-gray-800/50 border border-border">
          <div className="w-5 h-5 rounded-full border-2 border-primary border-t-transparent animate-spin" />
          <span className="text-sm text-gray-300">Parsing {fileName}...</span>
        </div>
      )}

      {/* Results */}
      {parsed && status === 'done' && (
        <div className="space-y-4">
          {/* File info */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <InfoCard label="File" value={fileName || 'Unknown'} />
            <InfoCard label="Size" value={formatSize(fileSize)} />
            <InfoCard label="Version" value={parsed.version || 'Auto'} badge />
            <InfoCard label="Platform" value={platform || parsed.platform || 'Unknown'} badge />
          </div>

          {/* Parse summary */}
          <div className="grid grid-cols-3 gap-3">
            <StatCard label="Processors" value={parsed.processors?.length ?? 0} color="text-blue-400" />
            <StatCard label="Connections" value={parsed.connections?.length ?? 0} color="text-purple-400" />
            <StatCard label="Process Groups" value={parsed.processGroups?.length ?? 0} color="text-cyan-400" />
          </div>

          {/* Warnings */}
          {parsed.warnings && parsed.warnings.length > 0 && (
            <div className="rounded-lg border border-amber-500/30 bg-amber-500/5 p-4">
              <h4 className="text-sm font-medium text-amber-400 mb-2">
                Parse Warnings ({parsed.warnings.length})
              </h4>
              <ul className="space-y-1">
                {parsed.warnings.map((w, i: number) => (
                  <li key={i} className="text-xs text-amber-300/70 flex items-start gap-2">
                    <span className="text-amber-500 mt-0.5">&#x26A0;</span>
                    {w.message}
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function InfoCard({ label, value, badge }: { label: string; value: string; badge?: boolean }) {
  return (
    <div className="rounded-lg bg-gray-800/50 border border-border p-3">
      <p className="text-xs text-gray-500 mb-1">{label}</p>
      {badge ? (
        <span className="px-2 py-0.5 rounded bg-primary/10 text-primary text-sm font-medium">{value}</span>
      ) : (
        <p className="text-sm text-gray-200 truncate">{value}</p>
      )}
    </div>
  );
}

function StatCard({ label, value, color }: { label: string; value: number; color: string }) {
  return (
    <div className="rounded-lg bg-gray-800/50 border border-border p-4 text-center">
      <p className={`text-2xl font-bold ${color}`}>{value}</p>
      <p className="text-xs text-gray-500 mt-1">{label}</p>
    </div>
  );
}
