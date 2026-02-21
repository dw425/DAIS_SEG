import React, { useState } from 'react';
import { useUIStore, type ErrorEntry } from '../../store/ui';

export default function ErrorPanel() {
  const errors = useUIStore((s) => s.errors);
  const clearErrors = useUIStore((s) => s.clearErrors);
  const removeError = useUIStore((s) => s.removeError);
  const [expanded, setExpanded] = useState(false);
  const [expandedStacks, setExpandedStacks] = useState<Set<string>>(new Set());

  if (errors.length === 0) return null;

  const toggleStack = (id: string) => {
    setExpandedStacks((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const severityIcon = (severity: ErrorEntry['severity']) => {
    switch (severity) {
      case 'error':
        return <span className="text-red-400">&#x2716;</span>;
      case 'warning':
        return <span className="text-amber-400">&#x26A0;</span>;
      default:
        return <span className="text-blue-400">&#x2139;</span>;
    }
  };

  return (
    <div className="fixed bottom-0 left-0 right-0 z-50 bg-gray-900 border-t border-red-500/30">
      {/* Header */}
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center justify-between px-4 py-2 hover:bg-gray-800/50 transition"
      >
        <div className="flex items-center gap-2">
          <span className="text-red-400 text-sm font-medium">
            {errors.length} error{errors.length !== 1 ? 's' : ''}
          </span>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={(e) => { e.stopPropagation(); clearErrors(); }}
            className="text-xs text-gray-500 hover:text-gray-300 px-2 py-0.5 rounded bg-gray-800"
          >
            Clear all
          </button>
          <svg
            className={`w-4 h-4 text-gray-400 transition-transform ${expanded ? 'rotate-180' : ''}`}
            fill="none" viewBox="0 0 24 24" stroke="currentColor"
          >
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 15l7-7 7 7" />
          </svg>
        </div>
      </button>

      {/* Body */}
      {expanded && (
        <div className="max-h-64 overflow-y-auto px-4 pb-3 space-y-2">
          {errors.map((err) => (
            <div key={err.id} className="flex items-start gap-2 bg-gray-800/50 rounded-lg p-3 text-sm">
              <div className="mt-0.5">{severityIcon(err.severity)}</div>
              <div className="flex-1 min-w-0">
                <p className="text-gray-200">{err.message}</p>
                <p className="text-xs text-gray-500 mt-1">
                  {new Date(err.timestamp).toLocaleTimeString()}
                </p>
                {err.stack && (
                  <>
                    <button
                      onClick={() => toggleStack(err.id)}
                      className="text-xs text-gray-500 hover:text-gray-300 mt-1 underline"
                    >
                      {expandedStacks.has(err.id) ? 'Hide' : 'Show'} stack trace
                    </button>
                    {expandedStacks.has(err.id) && (
                      <pre className="text-xs text-gray-500 mt-1 whitespace-pre-wrap font-mono bg-gray-900 rounded p-2 overflow-x-auto">
                        {err.stack}
                      </pre>
                    )}
                  </>
                )}
              </div>
              <button
                onClick={() => removeError(err.id)}
                className="text-gray-500 hover:text-gray-300 shrink-0"
              >
                &#x2715;
              </button>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
