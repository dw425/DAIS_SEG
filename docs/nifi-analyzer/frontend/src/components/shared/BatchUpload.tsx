import React, { useState, useCallback, useRef } from 'react';

interface BatchFile {
  file: File;
  status: 'pending' | 'processing' | 'done' | 'error';
  platform: string | null;
  processorsFound: number;
  mappedPercent: number;
  confidence: number;
  error?: string;
}

interface BatchResult {
  fileName: string;
  processorsFound: number;
  mappedPercent: number;
  confidence: number;
}

interface BatchUploadProps {
  onProcess: (file: File) => Promise<{ processorsFound: number; mappedPercent: number; confidence: number; platform: string }>;
}

const EXT_BADGES: Record<string, string> = {
  xml: 'bg-blue-500/20 text-blue-400',
  json: 'bg-green-500/20 text-green-400',
  gz: 'bg-amber-500/20 text-amber-400',
  zip: 'bg-purple-500/20 text-purple-400',
  dtsx: 'bg-pink-500/20 text-pink-400',
  py: 'bg-yellow-500/20 text-yellow-400',
  sql: 'bg-cyan-500/20 text-cyan-400',
};

function getExtension(name: string): string {
  const dot = name.lastIndexOf('.');
  return dot >= 0 ? name.slice(dot + 1).toLowerCase() : '';
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function detectPlatform(name: string): string | null {
  const lower = name.toLowerCase();
  if (lower.includes('nifi') || lower.endsWith('.xml')) return 'NiFi';
  if (lower.endsWith('.dtsx')) return 'SSIS';
  if (lower.endsWith('.py')) return 'Airflow';
  if (lower.endsWith('.json')) return 'NiFi/JSON';
  if (lower.endsWith('.sql')) return 'SQL';
  return null;
}

export default function BatchUpload({ onProcess }: BatchUploadProps) {
  const [files, setFiles] = useState<BatchFile[]>([]);
  const [processing, setProcessing] = useState(false);
  const [complete, setComplete] = useState(false);
  const cancelRef = useRef(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const [dragging, setDragging] = useState(false);

  const addFiles = useCallback((newFiles: FileList | File[]) => {
    const items = Array.from(newFiles).map((file) => ({
      file,
      status: 'pending' as const,
      platform: detectPlatform(file.name),
      processorsFound: 0,
      mappedPercent: 0,
      confidence: 0,
    }));
    setFiles((prev) => [...prev, ...items]);
    setComplete(false);
  }, []);

  const onDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setDragging(false);
      if (e.dataTransfer.files.length > 0) addFiles(e.dataTransfer.files);
    },
    [addFiles],
  );

  const onDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setDragging(true);
  }, []);

  const onDragLeave = useCallback(() => setDragging(false), []);

  const onChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      if (e.target.files) addFiles(e.target.files);
    },
    [addFiles],
  );

  const processAll = useCallback(async () => {
    setProcessing(true);
    cancelRef.current = false;
    setComplete(false);

    for (let i = 0; i < files.length; i++) {
      if (cancelRef.current) break;
      if (files[i].status !== 'pending') continue;

      setFiles((prev) => prev.map((f, idx) => (idx === i ? { ...f, status: 'processing' } : f)));

      try {
        const result = await onProcess(files[i].file);
        setFiles((prev) =>
          prev.map((f, idx) =>
            idx === i
              ? {
                  ...f,
                  status: 'done',
                  processorsFound: result.processorsFound,
                  mappedPercent: result.mappedPercent,
                  confidence: result.confidence,
                  platform: result.platform || f.platform,
                }
              : f,
          ),
        );
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        setFiles((prev) =>
          prev.map((f, idx) => (idx === i ? { ...f, status: 'error', error: msg } : f)),
        );
      }
    }

    setProcessing(false);
    if (!cancelRef.current) setComplete(true);
  }, [files, onProcess]);

  const handleCancel = useCallback(() => {
    cancelRef.current = true;
  }, []);

  const exportResults = useCallback(() => {
    const results: BatchResult[] = files
      .filter((f) => f.status === 'done')
      .map((f) => ({
        fileName: f.file.name,
        processorsFound: f.processorsFound,
        mappedPercent: f.mappedPercent,
        confidence: f.confidence,
      }));
    const blob = new Blob([JSON.stringify(results, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'batch-results.json';
    a.click();
    URL.revokeObjectURL(url);
  }, [files]);

  const pendingCount = files.filter((f) => f.status === 'pending').length;
  const doneCount = files.filter((f) => f.status === 'done').length;

  return (
    <div className="space-y-4">
      {/* Drop zone */}
      <div
        onDrop={onDrop}
        onDragOver={onDragOver}
        onDragLeave={onDragLeave}
        onClick={() => !processing && inputRef.current?.click()}
        className={`
          relative cursor-pointer rounded-xl border-2 border-dashed p-10 text-center transition-all
          ${dragging ? 'border-primary bg-primary/5 scale-[1.01]' : 'border-border hover:border-gray-500'}
          ${processing ? 'opacity-50 cursor-not-allowed' : ''}
        `}
      >
        <input
          ref={inputRef}
          type="file"
          className="hidden"
          multiple
          onChange={onChange}
          disabled={processing}
        />
        <svg className="mx-auto w-12 h-12 text-gray-500 mb-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
        </svg>
        <p className="text-gray-300 font-medium">Drop multiple ETL flow files here</p>
        <p className="text-sm text-gray-500 mt-1">Supports NiFi, SSIS, Airflow, SQL, and more</p>
      </div>

      {/* File list */}
      {files.length > 0 && (
        <div className="rounded-lg border border-border overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-900/60 border-b border-border">
                <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-500 uppercase">File</th>
                <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-500 uppercase">Ext</th>
                <th className="text-right px-4 py-2.5 text-xs font-medium text-gray-500 uppercase">Size</th>
                <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-500 uppercase">Platform</th>
                <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-500 uppercase">Status</th>
                {complete && (
                  <>
                    <th className="text-right px-4 py-2.5 text-xs font-medium text-gray-500 uppercase">Procs</th>
                    <th className="text-right px-4 py-2.5 text-xs font-medium text-gray-500 uppercase">Mapped</th>
                    <th className="text-right px-4 py-2.5 text-xs font-medium text-gray-500 uppercase">Conf.</th>
                  </>
                )}
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {files.map((f, i) => {
                const ext = getExtension(f.file.name);
                const badgeClass = EXT_BADGES[ext] || 'bg-gray-500/20 text-gray-400';
                return (
                  <tr key={i} className="hover:bg-gray-800/30 transition">
                    <td className="px-4 py-2.5 text-gray-200 font-medium truncate max-w-[200px]">{f.file.name}</td>
                    <td className="px-4 py-2.5">
                      <span className={`px-2 py-0.5 rounded text-xs font-mono ${badgeClass}`}>.{ext}</span>
                    </td>
                    <td className="px-4 py-2.5 text-right text-gray-400 tabular-nums text-xs">{formatSize(f.file.size)}</td>
                    <td className="px-4 py-2.5 text-gray-400 text-xs">{f.platform || 'Auto'}</td>
                    <td className="px-4 py-2.5">
                      {f.status === 'pending' && <span className="text-xs text-gray-500">Pending</span>}
                      {f.status === 'processing' && (
                        <span className="flex items-center gap-1.5">
                          <span className="w-3 h-3 rounded-full border-2 border-primary border-t-transparent animate-spin" />
                          <span className="text-xs text-primary">Processing</span>
                        </span>
                      )}
                      {f.status === 'done' && (
                        <span className="flex items-center gap-1">
                          <svg className="w-3.5 h-3.5 text-green-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                          </svg>
                          <span className="text-xs text-green-400">Done</span>
                        </span>
                      )}
                      {f.status === 'error' && (
                        <span className="text-xs text-red-400" title={f.error}>Error</span>
                      )}
                    </td>
                    {complete && (
                      <>
                        <td className="px-4 py-2.5 text-right text-gray-300 tabular-nums">{f.status === 'done' ? f.processorsFound : '-'}</td>
                        <td className="px-4 py-2.5 text-right text-gray-300 tabular-nums">{f.status === 'done' ? `${f.mappedPercent}%` : '-'}</td>
                        <td className="px-4 py-2.5 text-right tabular-nums">
                          {f.status === 'done' ? (
                            <span className={f.confidence >= 0.9 ? 'text-green-400' : f.confidence >= 0.7 ? 'text-amber-400' : 'text-red-400'}>
                              {(f.confidence * 100).toFixed(0)}%
                            </span>
                          ) : '-'}
                        </td>
                      </>
                    )}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {/* Actions */}
      {files.length > 0 && (
        <div className="flex items-center gap-3">
          {!processing && !complete && (
            <button
              onClick={processAll}
              disabled={pendingCount === 0}
              className="px-4 py-2 rounded-lg bg-primary text-white text-sm font-medium hover:bg-primary/80 disabled:opacity-40 disabled:cursor-not-allowed transition"
            >
              Process All ({pendingCount} files)
            </button>
          )}
          {processing && (
            <button
              onClick={handleCancel}
              className="px-4 py-2 rounded-lg border border-red-500/30 text-red-400 text-sm font-medium hover:bg-red-500/10 transition"
            >
              Cancel
            </button>
          )}
          {complete && doneCount > 0 && (
            <button
              onClick={exportResults}
              className="px-4 py-2 rounded-lg bg-green-500/20 text-green-400 text-sm font-medium hover:bg-green-500/30 transition"
            >
              Export Batch Results (JSON)
            </button>
          )}
          {complete && (
            <span className="text-xs text-gray-500">
              {doneCount} of {files.length} processed successfully
            </span>
          )}
        </div>
      )}
    </div>
  );
}
