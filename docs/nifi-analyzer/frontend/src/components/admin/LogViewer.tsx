import React, { useState, useEffect, useRef } from 'react';
import * as api from '../../api/client';

type LogLevel = 'all' | 'info' | 'warn' | 'error';

interface LogEntry {
  timestamp: string;
  level: string;
  message: string;
}

function parseLogLine(line: string): LogEntry {
  // Attempt to parse "[TIMESTAMP] [LEVEL] message" format
  const match = line.match(/^\[(.+?)\]\s*\[(\w+)\]\s*(.+)$/);
  if (match) {
    return { timestamp: match[1], level: match[2].toLowerCase(), message: match[3] };
  }
  return { timestamp: '', level: 'info', message: line };
}

const LEVEL_STYLES: Record<string, string> = {
  error: 'text-red-400',
  warn: 'text-amber-400',
  warning: 'text-amber-400',
  info: 'text-blue-400',
  debug: 'text-gray-500',
};

export default function LogViewer() {
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const [filter, setFilter] = useState<LogLevel>('all');
  const [search, setSearch] = useState('');
  const [autoScroll, setAutoScroll] = useState(true);
  const scrollRef = useRef<HTMLDivElement>(null);

  const fetchLogs = async () => {
    setLoading(true);
    try {
      const result = await api.getAdminLogs();
      const entries = (result.logs || []).map(parseLogLine);
      setLogs(entries);
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Unknown error';
      setLogs([{ timestamp: new Date().toISOString(), level: 'error', message: `Failed to fetch logs: ${msg}. Is the backend running?` }]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchLogs();
  }, []);

  useEffect(() => {
    if (autoScroll && scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [logs, autoScroll]);

  const filtered = logs.filter((log) => {
    if (filter !== 'all' && log.level !== filter) return false;
    if (search && !log.message.toLowerCase().includes(search.toLowerCase())) return false;
    return true;
  });

  return (
    <div className="space-y-3">
      {/* Controls */}
      <div className="flex items-center gap-3 flex-wrap">
        <input
          type="text"
          placeholder="Search logs..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="px-3 py-1.5 rounded-lg bg-gray-800 border border-border text-sm text-gray-200 placeholder-gray-500 focus:outline-none focus:border-gray-600 w-48"
        />
        <div className="flex gap-1">
          {(['all', 'info', 'warn', 'error'] as const).map((l) => (
            <button
              key={l}
              onClick={() => setFilter(l)}
              className={`px-2.5 py-1 rounded text-xs font-medium transition
                ${filter === l ? 'bg-primary/20 text-primary' : 'bg-gray-800 text-gray-400 hover:text-gray-200'}`}
            >
              {l.charAt(0).toUpperCase() + l.slice(1)}
            </button>
          ))}
        </div>
        <label className="flex items-center gap-1.5 text-xs text-gray-500 ml-auto cursor-pointer">
          <input
            type="checkbox"
            checked={autoScroll}
            onChange={(e) => setAutoScroll(e.target.checked)}
            className="rounded border-border"
          />
          Auto-scroll
        </label>
        <button
          onClick={fetchLogs}
          disabled={loading}
          className="px-3 py-1 rounded-lg bg-gray-800 text-gray-300 text-xs hover:bg-gray-700 transition disabled:opacity-50"
        >
          {loading ? 'Loading...' : 'Refresh'}
        </button>
      </div>

      {/* Log output */}
      <div
        ref={scrollRef}
        className="rounded-lg border border-border bg-gray-950 p-3 overflow-auto max-h-[500px] font-mono text-xs leading-relaxed"
      >
        {filtered.length === 0 ? (
          <p className="text-gray-600 text-center py-8">No logs to display</p>
        ) : (
          filtered.map((log, i) => (
            <div key={i} className="flex gap-2 hover:bg-gray-900/50 px-1 rounded">
              {log.timestamp && (
                <span className="text-gray-600 shrink-0">{log.timestamp}</span>
              )}
              <span className={`shrink-0 uppercase w-12 ${LEVEL_STYLES[log.level] || 'text-gray-400'}`}>
                [{log.level}]
              </span>
              <span className="text-gray-300 break-all">{log.message}</span>
            </div>
          ))
        )}
      </div>
      <p className="text-xs text-gray-600">{filtered.length} of {logs.length} entries</p>
    </div>
  );
}
