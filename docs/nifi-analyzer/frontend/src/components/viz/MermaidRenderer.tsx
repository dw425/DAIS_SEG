import React, { useEffect, useRef, useState } from 'react';

interface MermaidRendererProps {
  markdown: string;
  className?: string;
}

/**
 * MermaidRenderer — lazy-loads and renders Mermaid diagrams.
 *
 * Used for: flow topology, process group hierarchy, state diagrams.
 * Lazy-loads the mermaid library only when needed.
 */
export default function MermaidRenderer({ markdown, className = '' }: MermaidRendererProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [svg, setSvg] = useState<string>('');

  useEffect(() => {
    if (!markdown || !markdown.trim()) {
      setLoading(false);
      return;
    }

    let cancelled = false;

    async function renderMermaid() {
      try {
        setLoading(true);
        setError(null);

        // Lazy-load mermaid
        const mermaid = await import('mermaid');
        const mermaidApi = mermaid.default;

        mermaidApi.initialize({
          startOnLoad: false,
          theme: 'dark',
          themeVariables: {
            primaryColor: '#3b82f6',
            primaryTextColor: '#e5e7eb',
            primaryBorderColor: '#4b5563',
            lineColor: '#6b7280',
            secondaryColor: '#1f2937',
            tertiaryColor: '#111827',
            background: '#0f172a',
            mainBkg: '#1e293b',
            nodeBorder: '#4b5563',
          },
          flowchart: {
            useMaxWidth: true,
            htmlLabels: true,
            curve: 'basis',
          },
          securityLevel: 'strict',
        });

        const id = `mermaid-${Date.now()}`;
        const { svg: renderedSvg } = await mermaidApi.render(id, markdown);

        if (!cancelled) {
          setSvg(renderedSvg);
          setLoading(false);
        }
      } catch (err) {
        if (!cancelled) {
          console.error('Mermaid render error:', err);
          setError(err instanceof Error ? err.message : 'Failed to render diagram');
          setLoading(false);
        }
      }
    }

    renderMermaid();

    return () => {
      cancelled = true;
    };
  }, [markdown]);

  if (!markdown || !markdown.trim()) {
    return null;
  }

  if (loading) {
    return (
      <div className={`flex items-center justify-center p-8 rounded-lg bg-gray-800/30 border border-border ${className}`}>
        <div className="w-5 h-5 rounded-full border-2 border-blue-400 border-t-transparent animate-spin" />
        <span className="ml-3 text-sm text-gray-400">Rendering diagram...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className={`rounded-lg bg-red-500/5 border border-red-500/20 p-4 ${className}`}>
        <p className="text-xs text-red-400 mb-2">Diagram render error: {error}</p>
        <pre className="text-[10px] text-gray-500 bg-gray-900/60 rounded p-2 overflow-x-auto font-mono">
          {markdown}
        </pre>
      </div>
    );
  }

  return (
    <div
      ref={containerRef}
      className={`rounded-lg bg-gray-900/50 border border-border p-4 overflow-x-auto ${className}`}
      dangerouslySetInnerHTML={{ __html: svg }}
    />
  );
}
