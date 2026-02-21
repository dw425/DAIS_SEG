import React, { useCallback, useRef, useState } from 'react';

interface FileUploadProps {
  onFile: (file: File) => void;
  accept?: string;
  disabled?: boolean;
}

const FORMAT_BADGES: Record<string, string> = {
  '.xml': 'bg-blue-500/20 text-blue-400',
  '.json': 'bg-green-500/20 text-green-400',
  '.gz': 'bg-amber-500/20 text-amber-400',
  '.zip': 'bg-purple-500/20 text-purple-400',
  '.dtsx': 'bg-pink-500/20 text-pink-400',
  '.py': 'bg-yellow-500/20 text-yellow-400',
  '.sql': 'bg-cyan-500/20 text-cyan-400',
};

function getExtension(name: string): string {
  const dot = name.lastIndexOf('.');
  return dot >= 0 ? name.slice(dot).toLowerCase() : '';
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

export default function FileUpload({ onFile, accept, disabled }: FileUploadProps) {
  const inputRef = useRef<HTMLInputElement>(null);
  const [dragging, setDragging] = useState(false);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);

  const handleFile = useCallback(
    (file: File) => {
      setSelectedFile(file);
      onFile(file);
    },
    [onFile],
  );

  const onDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setDragging(false);
      const file = e.dataTransfer.files?.[0];
      if (file) handleFile(file);
    },
    [handleFile],
  );

  const onDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setDragging(true);
  }, []);

  const onDragLeave = useCallback(() => setDragging(false), []);

  const onChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const file = e.target.files?.[0];
      if (file) handleFile(file);
    },
    [handleFile],
  );

  const ext = selectedFile ? getExtension(selectedFile.name) : '';
  const badgeClass = FORMAT_BADGES[ext] || 'bg-gray-500/20 text-gray-400';

  return (
    <div
      onDrop={onDrop}
      onDragOver={onDragOver}
      onDragLeave={onDragLeave}
      onClick={() => !disabled && inputRef.current?.click()}
      className={`
        relative cursor-pointer rounded-xl border-2 border-dashed p-8 text-center transition-all
        ${dragging ? 'border-primary bg-primary/5 scale-[1.01]' : 'border-border hover:border-gray-500'}
        ${disabled ? 'opacity-50 cursor-not-allowed' : ''}
      `}
    >
      <input
        ref={inputRef}
        type="file"
        className="hidden"
        accept={accept}
        onChange={onChange}
        disabled={disabled}
      />

      {selectedFile ? (
        <div className="space-y-2">
          <div className="flex items-center justify-center gap-3">
            <svg className="w-8 h-8 text-green-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            <span className="text-lg font-medium text-gray-100">{selectedFile.name}</span>
            <span className={`px-2 py-0.5 rounded text-xs font-mono ${badgeClass}`}>{ext}</span>
          </div>
          <p className="text-sm text-gray-400">{formatSize(selectedFile.size)}</p>
          <p className="text-xs text-gray-500">Click or drop another file to replace</p>
        </div>
      ) : (
        <div className="space-y-3">
          <svg className="mx-auto w-12 h-12 text-gray-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
          </svg>
          <p className="text-gray-300 font-medium">Drop your ETL flow file here</p>
          <p className="text-sm text-gray-500">
            Supports NiFi XML/JSON, SSIS .dtsx, Informatica XML, Airflow .py, SQL, and more
          </p>
        </div>
      )}
    </div>
  );
}
