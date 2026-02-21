import React, { useState } from 'react';
import { useUIStore } from '../../store/ui';

export default function ErrorModal() {
  const errorModal = useUIStore((s) => s.errorModal);
  const setErrorModal = useUIStore((s) => s.setErrorModal);
  const [showStack, setShowStack] = useState(false);

  if (!errorModal) return null;

  return (
    <div className="fixed inset-0 z-[100] flex items-center justify-center bg-black/60 backdrop-blur-sm" onClick={() => setErrorModal(null)}>
      <div
        className="bg-gray-900 border border-red-500/40 rounded-xl shadow-2xl max-w-lg w-full mx-4 overflow-hidden"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center gap-3 px-6 py-4 bg-red-500/10 border-b border-red-500/20">
          <div className="w-8 h-8 rounded-full bg-red-500/20 flex items-center justify-center shrink-0">
            <svg className="w-5 h-5 text-red-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L4.082 16.5c-.77.833.192 2.5 1.732 2.5z" />
            </svg>
          </div>
          <div className="flex-1 min-w-0">
            <h3 className="text-base font-semibold text-red-300">
              Step {errorModal.step} Failed: {errorModal.stepName}
            </h3>
          </div>
          <button
            onClick={() => setErrorModal(null)}
            className="text-gray-400 hover:text-gray-200 shrink-0"
          >
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Body */}
        <div className="px-6 py-5 space-y-4">
          {/* Status code */}
          {errorModal.status && (
            <div className="flex items-center gap-2">
              <span className="text-xs font-medium text-gray-500 uppercase tracking-wider">HTTP Status</span>
              <span className="px-2 py-0.5 rounded bg-red-500/20 text-red-300 text-sm font-mono">{errorModal.status}</span>
            </div>
          )}

          {/* Error message */}
          <div>
            <span className="text-xs font-medium text-gray-500 uppercase tracking-wider block mb-1">Error Message</span>
            <div className="bg-gray-800 rounded-lg p-3 text-sm text-gray-200 font-mono break-words">
              {errorModal.message}
            </div>
          </div>

          {/* Stack trace toggle */}
          {errorModal.stack && (
            <div>
              <button
                onClick={() => setShowStack(!showStack)}
                className="text-xs text-gray-500 hover:text-gray-300 underline"
              >
                {showStack ? 'Hide' : 'Show'} stack trace
              </button>
              {showStack && (
                <pre className="mt-2 bg-gray-800 rounded-lg p-3 text-xs text-gray-400 font-mono overflow-x-auto max-h-48 overflow-y-auto whitespace-pre-wrap">
                  {errorModal.stack}
                </pre>
              )}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="px-6 py-4 bg-gray-800/50 border-t border-border flex items-center justify-between">
          <button
            onClick={() => {
              navigator.clipboard.writeText(
                `Step ${errorModal.step}: ${errorModal.stepName}\nStatus: ${errorModal.status || 'N/A'}\nError: ${errorModal.message}\n${errorModal.stack || ''}`
              );
            }}
            className="text-xs text-gray-400 hover:text-gray-200 px-3 py-1.5 rounded bg-gray-700 hover:bg-gray-600 transition"
          >
            Copy to Clipboard
          </button>
          <button
            onClick={() => setErrorModal(null)}
            className="text-sm text-gray-200 px-4 py-2 rounded-lg bg-red-500/20 hover:bg-red-500/30 border border-red-500/30 transition"
          >
            Dismiss
          </button>
        </div>
      </div>
    </div>
  );
}
