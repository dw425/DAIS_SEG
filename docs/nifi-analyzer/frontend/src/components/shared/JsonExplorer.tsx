import React, { useState, useCallback } from 'react';

interface JsonExplorerProps {
  data: unknown;
  rootLabel?: string;
  initialExpanded?: boolean;
  maxDepth?: number;
}

interface NodeProps {
  label: string;
  value: unknown;
  depth: number;
  maxDepth: number;
}

function JsonNode({ label, value, depth, maxDepth }: NodeProps) {
  const [expanded, setExpanded] = useState(depth < 1);

  const toggle = useCallback(() => setExpanded((v) => !v), []);

  if (value === null) {
    return (
      <div className="flex items-center gap-1" style={{ paddingLeft: depth * 16 }}>
        <span className="text-gray-400">{label}:</span>
        <span className="text-gray-500 italic">null</span>
      </div>
    );
  }

  if (typeof value === 'boolean') {
    return (
      <div className="flex items-center gap-1" style={{ paddingLeft: depth * 16 }}>
        <span className="text-gray-400">{label}:</span>
        <span className="text-amber-400">{String(value)}</span>
      </div>
    );
  }

  if (typeof value === 'number') {
    return (
      <div className="flex items-center gap-1" style={{ paddingLeft: depth * 16 }}>
        <span className="text-gray-400">{label}:</span>
        <span className="text-cyan-400">{value}</span>
      </div>
    );
  }

  if (typeof value === 'string') {
    const truncated = value.length > 120 ? value.slice(0, 120) + '...' : value;
    return (
      <div className="flex items-start gap-1" style={{ paddingLeft: depth * 16 }}>
        <span className="text-gray-400 shrink-0">{label}:</span>
        <span className="text-green-400 break-all">"{truncated}"</span>
      </div>
    );
  }

  if (Array.isArray(value)) {
    if (depth >= maxDepth) {
      return (
        <div style={{ paddingLeft: depth * 16 }}>
          <span className="text-gray-400">{label}:</span>
          <span className="text-gray-500"> Array[{value.length}]</span>
        </div>
      );
    }
    return (
      <div>
        <button
          onClick={toggle}
          className="flex items-center gap-1 hover:bg-gray-800/50 rounded px-1 w-full text-left"
          style={{ paddingLeft: depth * 16 }}
        >
          <span className={`text-gray-500 text-xs transition-transform ${expanded ? 'rotate-90' : ''}`}>&#9654;</span>
          <span className="text-gray-400">{label}</span>
          <span className="text-gray-600 text-xs">Array[{value.length}]</span>
        </button>
        {expanded && value.map((item, i) => (
          <JsonNode key={i} label={String(i)} value={item} depth={depth + 1} maxDepth={maxDepth} />
        ))}
      </div>
    );
  }

  if (typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>);
    if (depth >= maxDepth) {
      return (
        <div style={{ paddingLeft: depth * 16 }}>
          <span className="text-gray-400">{label}:</span>
          <span className="text-gray-500"> {'{...}'} ({entries.length} keys)</span>
        </div>
      );
    }
    return (
      <div>
        <button
          onClick={toggle}
          className="flex items-center gap-1 hover:bg-gray-800/50 rounded px-1 w-full text-left"
          style={{ paddingLeft: depth * 16 }}
        >
          <span className={`text-gray-500 text-xs transition-transform ${expanded ? 'rotate-90' : ''}`}>&#9654;</span>
          <span className="text-gray-400">{label}</span>
          <span className="text-gray-600 text-xs">{'{}'} {entries.length} keys</span>
        </button>
        {expanded && entries.map(([k, v]) => (
          <JsonNode key={k} label={k} value={v} depth={depth + 1} maxDepth={maxDepth} />
        ))}
      </div>
    );
  }

  return (
    <div style={{ paddingLeft: depth * 16 }}>
      <span className="text-gray-400">{label}:</span>
      <span className="text-gray-300">{String(value)}</span>
    </div>
  );
}

export default function JsonExplorer({
  data,
  rootLabel = 'root',
  maxDepth = 8,
}: JsonExplorerProps) {
  return (
    <div className="rounded-lg border border-border bg-gray-950 p-3 overflow-auto max-h-[500px] font-mono text-xs leading-relaxed">
      <JsonNode label={rootLabel} value={data} depth={0} maxDepth={maxDepth} />
    </div>
  );
}
