import React, { useState } from 'react';

interface CodePreviewProps {
  code: string;
  language?: string;
  title?: string;
  maxHeight?: string;
}

export default function CodePreview({
  code,
  language = 'python',
  title,
  maxHeight = '500px',
}: CodePreviewProps) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="rounded-lg border border-border bg-gray-950 overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-2 bg-gray-900/60 border-b border-border">
        <div className="flex items-center gap-2">
          {title && <span className="text-sm font-medium text-gray-300">{title}</span>}
          <span className="text-xs text-gray-500 bg-gray-800 px-2 py-0.5 rounded">{language}</span>
        </div>
        <button
          onClick={handleCopy}
          className="text-xs text-gray-400 hover:text-gray-200 flex items-center gap-1 px-2 py-1 rounded hover:bg-gray-800 transition"
        >
          {copied ? (
            <>
              <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
              </svg>
              Copied
            </>
          ) : (
            <>
              <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
              </svg>
              Copy
            </>
          )}
        </button>
      </div>

      {/* Code body */}
      <div className="overflow-auto" style={{ maxHeight }}>
        <pre className="p-4 text-sm leading-relaxed">
          <code className="text-gray-300 whitespace-pre">{code}</code>
        </pre>
      </div>

      {/* Line count footer */}
      <div className="px-4 py-1 border-t border-border bg-gray-900/30">
        <span className="text-xs text-gray-600">
          {code.split('\n').length} lines
        </span>
      </div>
    </div>
  );
}
