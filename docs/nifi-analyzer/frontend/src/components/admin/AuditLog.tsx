import React, { useEffect, useState } from 'react';
import { useAuthStore } from '../../store/auth';

interface AuditEntry {
  id: string;
  user_id: string | null;
  action: string;
  resource_type: string | null;
  resource_id: string | null;
  details: Record<string, unknown>;
  ip_address: string | null;
  created_at: string | null;
}

export default function AuditLog() {
  const [logs, setLogs] = useState<AuditEntry[]>([]);
  const [total, setTotal] = useState(0);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState('');
  const [actionFilter, setActionFilter] = useState('');
  const [offset, setOffset] = useState(0);
  const limit = 25;
  const token = useAuthStore((s) => s.token);

  const fetchLogs = async () => {
    setIsLoading(true);
    setError('');
    try {
      const params = new URLSearchParams({ limit: String(limit), offset: String(offset) });
      if (actionFilter) params.set('action', actionFilter);

      const res = await fetch(`/api/audit?${params}`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      if (!res.ok) throw new Error(`Failed to load audit logs (${res.status})`);
      const data = await res.json();
      setLogs(data.logs);
      setTotal(data.total);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchLogs();
  }, [offset, actionFilter]);

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-medium text-gray-300">Audit Trail</h3>
        <div className="flex items-center gap-2">
          <input
            type="text"
            placeholder="Filter by action..."
            value={actionFilter}
            onChange={(e) => { setActionFilter(e.target.value); setOffset(0); }}
            className="px-3 py-1.5 bg-gray-800 border border-gray-700 rounded-lg text-gray-200 text-xs
              placeholder-gray-500 focus:outline-none focus:border-primary transition w-48"
          />
          <button
            onClick={fetchLogs}
            className="px-3 py-1.5 rounded-lg bg-gray-800 text-gray-300 text-xs hover:bg-gray-700 transition"
          >
            Refresh
          </button>
        </div>
      </div>

      {error && (
        <div className="p-3 rounded-lg bg-red-500/10 border border-red-500/30 text-red-400 text-sm">{error}</div>
      )}

      {isLoading ? (
        <div className="text-center py-8 text-gray-500 text-sm">Loading audit logs...</div>
      ) : (
        <div className="overflow-x-auto rounded-lg border border-gray-800">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-900/80 border-b border-gray-800">
                <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-400 uppercase">Time</th>
                <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-400 uppercase">Action</th>
                <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-400 uppercase">Resource</th>
                <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-400 uppercase">IP</th>
                <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-400 uppercase">Status</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-800">
              {logs.map((log) => (
                <tr key={log.id} className="hover:bg-gray-800/50 transition">
                  <td className="px-4 py-2.5 text-xs text-gray-400 whitespace-nowrap">
                    {log.created_at ? new Date(log.created_at).toLocaleString() : '-'}
                  </td>
                  <td className="px-4 py-2.5 text-xs text-gray-200 font-mono">{log.action}</td>
                  <td className="px-4 py-2.5 text-xs text-gray-400">
                    {log.resource_type || '-'}{log.resource_id ? ` / ${log.resource_id.slice(0, 8)}...` : ''}
                  </td>
                  <td className="px-4 py-2.5 text-xs text-gray-500">{log.ip_address || '-'}</td>
                  <td className="px-4 py-2.5 text-xs">
                    {log.details && typeof log.details === 'object' && 'status_code' in log.details ? (
                      <span className={`px-1.5 py-0.5 rounded ${
                        (log.details.status_code as number) < 400 ? 'bg-green-500/20 text-green-400' : 'bg-red-500/20 text-red-400'
                      }`}>
                        {String(log.details.status_code)}
                      </span>
                    ) : '-'}
                  </td>
                </tr>
              ))}
              {logs.length === 0 && (
                <tr>
                  <td colSpan={5} className="px-4 py-8 text-center text-gray-500 text-sm">No audit logs found</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}

      {/* Pagination */}
      {total > limit && (
        <div className="flex items-center justify-between">
          <span className="text-xs text-gray-500">
            Showing {offset + 1}-{Math.min(offset + limit, total)} of {total}
          </span>
          <div className="flex gap-2">
            <button
              onClick={() => setOffset(Math.max(0, offset - limit))}
              disabled={offset === 0}
              className="px-3 py-1 rounded bg-gray-800 text-xs text-gray-300 hover:bg-gray-700 disabled:opacity-40 transition"
            >
              Previous
            </button>
            <button
              onClick={() => setOffset(offset + limit)}
              disabled={offset + limit >= total}
              className="px-3 py-1 rounded bg-gray-800 text-xs text-gray-300 hover:bg-gray-700 disabled:opacity-40 transition"
            >
              Next
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
