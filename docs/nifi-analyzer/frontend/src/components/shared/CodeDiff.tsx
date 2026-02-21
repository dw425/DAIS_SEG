import React, { useState, useRef, useCallback } from 'react';
import type { Processor, MappingEntry } from '../../types/pipeline';

interface CodeDiffProps {
  processor: Processor;
  mapping: MappingEntry;
}

function sanitizeHtml(html: string): string {
  return html
    .replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, '')
    .replace(/on\w+\s*=\s*"[^"]*"/gi, '')
    .replace(/on\w+\s*=\s*'[^']*'/gi, '');
}

function highlightSyntax(code: string): React.ReactNode[] {
  // Simple regex-based syntax highlighting
  const lines = code.split('\n');
  return lines.map((line, i) => {
    let highlighted = line;

    // Comments (# or //)
    highlighted = highlighted.replace(/(#.*$|\/\/.*$)/gm, '<span class="text-gray-500 italic">$1</span>');

    // Strings
    highlighted = highlighted.replace(/(["'])(?:(?=(\\?))\2.)*?\1/g, '<span class="text-green-400">$&</span>');

    // Keywords
    const keywords = /\b(def|class|import|from|return|if|else|elif|for|while|try|except|finally|with|as|yield|lambda|not|and|or|in|is|True|False|None|spark|df|display)\b/g;
    highlighted = highlighted.replace(keywords, '<span class="text-blue-400 font-medium">$&</span>');

    // Numbers
    highlighted = highlighted.replace(/\b(\d+\.?\d*)\b/g, '<span class="text-amber-400">$1</span>');

    return (
      <div key={i} className="flex">
        <span className="w-10 text-right pr-3 text-gray-600 select-none text-xs leading-relaxed shrink-0">{i + 1}</span>
        <span className="flex-1 leading-relaxed" dangerouslySetInnerHTML={{ __html: sanitizeHtml(highlighted) }} />
      </div>
    );
  });
}

function formatProperties(properties: Record<string, string>): string {
  if (!properties || Object.keys(properties).length === 0) return '# No properties configured';
  return Object.entries(properties)
    .map(([key, value]) => `${key} = ${value}`)
    .join('\n');
}

function confidenceBadge(confidence: number) {
  const color = confidence >= 0.9 ? 'bg-green-500/20 text-green-400' : confidence >= 0.7 ? 'bg-amber-500/20 text-amber-400' : 'bg-red-500/20 text-red-400';
  return <span className={`px-2 py-0.5 rounded text-xs font-medium ${color}`}>{(confidence * 100).toFixed(0)}%</span>;
}

export default function CodeDiff({ processor, mapping }: CodeDiffProps) {
  const [copied, setCopied] = useState(false);
  const leftRef = useRef<HTMLDivElement>(null);
  const rightRef = useRef<HTMLDivElement>(null);

  const handleCopy = useCallback(async () => {
    if (mapping.code) {
      await navigator.clipboard.writeText(mapping.code);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  }, [mapping.code]);

  const handleScroll = useCallback((source: 'left' | 'right') => {
    const from = source === 'left' ? leftRef.current : rightRef.current;
    const to = source === 'left' ? rightRef.current : leftRef.current;
    if (from && to) {
      to.scrollTop = from.scrollTop;
    }
  }, []);

  const sourceCode = formatProperties(processor.properties);
  const generatedCode = mapping.code || '# No code generated';

  return (
    <div className="rounded-lg border border-border bg-gray-950 overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-4 py-2.5 bg-gray-900/60 border-b border-border">
        <div className="flex items-center gap-2">
          <span className="text-sm font-medium text-gray-200">{processor.name}</span>
          <span className="px-2 py-0.5 rounded text-xs font-mono bg-gray-800 text-gray-400">
            {processor.type.split('.').pop()}
          </span>
          {confidenceBadge(mapping.confidence)}
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
              Copy Generated Code
            </>
          )}
        </button>
      </div>

      {/* Split panes */}
      <div className="grid grid-cols-2 divide-x divide-border">
        {/* Left pane: Source */}
        <div>
          <div className="px-4 py-1.5 bg-gray-900/40 border-b border-border">
            <span className="text-xs text-gray-500 uppercase tracking-wider">Source Properties</span>
          </div>
          <div
            ref={leftRef}
            onScroll={() => handleScroll('left')}
            className="overflow-auto p-3 font-mono text-xs text-gray-300"
            style={{ maxHeight: '400px' }}
          >
            {highlightSyntax(sourceCode)}
          </div>
        </div>

        {/* Right pane: Generated */}
        <div>
          <div className="px-4 py-1.5 bg-gray-900/40 border-b border-border">
            <span className="text-xs text-gray-500 uppercase tracking-wider">Generated PySpark</span>
          </div>
          <div
            ref={rightRef}
            onScroll={() => handleScroll('right')}
            className="overflow-auto p-3 font-mono text-xs text-gray-300"
            style={{ maxHeight: '400px' }}
          >
            {highlightSyntax(generatedCode)}
          </div>
        </div>
      </div>

      {/* Footer */}
      <div className="flex items-center justify-between px-4 py-1.5 border-t border-border bg-gray-900/30">
        <span className="text-xs text-gray-600">
          {sourceCode.split('\n').length} lines | {Object.keys(processor.properties).length} properties
        </span>
        <span className="text-xs text-gray-600">
          {generatedCode.split('\n').length} lines
        </span>
      </div>

      {/* Notes */}
      {mapping.notes && (
        <div className="px-4 py-2 border-t border-border bg-gray-900/20">
          <p className="text-xs text-gray-500">
            <span className="text-gray-400 font-medium">Notes:</span> {mapping.notes}
          </p>
        </div>
      )}
    </div>
  );
}
