import React, { useState, useRef, useEffect } from 'react';
import { useUIStore } from '../../store/ui';

interface ExportManagerProps {
  data: unknown;
  filename: string;
}

function downloadBlob(blob: Blob, name: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = name;
  a.click();
  URL.revokeObjectURL(url);
}

function toCSV(data: unknown): string {
  if (Array.isArray(data) && data.length > 0 && typeof data[0] === 'object' && data[0] !== null) {
    const headers = Object.keys(data[0] as Record<string, unknown>);
    const rows = data.map((item) => {
      const rec = item as Record<string, unknown>;
      return headers.map((h) => {
        const val = rec[h];
        const str = val == null ? '' : String(val);
        return str.includes(',') || str.includes('"') ? `"${str.replace(/"/g, '""')}"` : str;
      }).join(',');
    });
    return [headers.join(','), ...rows].join('\n');
  }
  return JSON.stringify(data, null, 2);
}

function toMarkdown(data: unknown): string {
  if (Array.isArray(data) && data.length > 0 && typeof data[0] === 'object' && data[0] !== null) {
    const headers = Object.keys(data[0] as Record<string, unknown>);
    const headerRow = `| ${headers.join(' | ')} |`;
    const sepRow = `| ${headers.map(() => '---').join(' | ')} |`;
    const rows = data.map((item) => {
      const rec = item as Record<string, unknown>;
      return `| ${headers.map((h) => String(rec[h] ?? '')).join(' | ')} |`;
    });
    return [headerRow, sepRow, ...rows].join('\n');
  }
  return '```json\n' + JSON.stringify(data, null, 2) + '\n```';
}

export default function ExportManager({ data, filename }: ExportManagerProps) {
  const [open, setOpen] = useState(false);
  const addToast = useUIStore((s) => s.addToast);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [open]);

  const exportJSON = () => {
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
    downloadBlob(blob, `${filename}.json`);
    addToast({ message: `Exported ${filename}.json`, type: 'success' });
    setOpen(false);
  };

  const exportMarkdown = () => {
    const blob = new Blob([toMarkdown(data)], { type: 'text/markdown' });
    downloadBlob(blob, `${filename}.md`);
    addToast({ message: `Exported ${filename}.md`, type: 'success' });
    setOpen(false);
  };

  const exportCSV = () => {
    const blob = new Blob([toCSV(data)], { type: 'text/csv' });
    downloadBlob(blob, `${filename}.csv`);
    addToast({ message: `Exported ${filename}.csv`, type: 'success' });
    setOpen(false);
  };

  const copyToClipboard = async () => {
    try {
      await navigator.clipboard.writeText(JSON.stringify(data, null, 2));
      addToast({ message: 'Copied to clipboard', type: 'success' });
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Clipboard access denied';
      addToast({ message: `Failed to copy: ${msg}`, type: 'error' });
    }
    setOpen(false);
  };

  const options = [
    { label: 'Export JSON', icon: '{ }', action: exportJSON },
    { label: 'Export Markdown', icon: 'MD', action: exportMarkdown },
    { label: 'Export CSV', icon: 'CSV', action: exportCSV },
    { label: 'Copy to Clipboard', icon: '\u2398', action: copyToClipboard },
  ];

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen(!open)}
        className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-gray-800 border border-border
          text-sm text-gray-300 hover:text-gray-100 hover:border-gray-600 transition"
      >
        <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
        </svg>
        Export
        <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>

      {open && (
        <div className="absolute top-full right-0 mt-1 w-48 bg-gray-900 border border-border rounded-lg shadow-xl z-50 overflow-hidden">
          {options.map((opt) => (
            <button
              key={opt.label}
              onClick={opt.action}
              className="w-full flex items-center gap-3 px-4 py-2.5 text-sm text-gray-300
                hover:bg-gray-800 hover:text-gray-100 transition text-left"
            >
              <span className="w-6 text-center text-xs font-mono text-gray-500">{opt.icon}</span>
              {opt.label}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
