import React, { useEffect, useState } from 'react';
import { useAuthStore } from '../../store/auth';

interface APIKeyEntry {
  id: string;
  user_id: string;
  name: string;
  prefix: string;
  scopes: string;
  is_active: boolean;
  last_used_at: string | null;
  expires_at: string | null;
  created_at: string | null;
}

export default function APIKeyManager() {
  const [keys, setKeys] = useState<APIKeyEntry[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState('');
  const [showCreate, setShowCreate] = useState(false);
  const [newKeyName, setNewKeyName] = useState('');
  const [newKeyScopes, setNewKeyScopes] = useState('read');
  const [newKeyDays, setNewKeyDays] = useState<number | null>(null);
  const [createdKey, setCreatedKey] = useState<string | null>(null);
  const [isCreating, setIsCreating] = useState(false);
  const token = useAuthStore((s) => s.token);

  const headers = { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' };

  const fetchKeys = async () => {
    setIsLoading(true);
    setError('');
    try {
      const res = await fetch('/api/api-keys', { headers });
      if (!res.ok) throw new Error(`Failed to load API keys (${res.status})`);
      const data = await res.json();
      setKeys(data.api_keys);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchKeys();
  }, []);

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newKeyName.trim()) return;
    setIsCreating(true);
    setError('');
    try {
      const res = await fetch('/api/api-keys', {
        method: 'POST',
        headers,
        body: JSON.stringify({
          name: newKeyName.trim(),
          scopes: newKeyScopes,
          expires_in_days: newKeyDays,
        }),
      });
      if (!res.ok) throw new Error(`Failed to create API key (${res.status})`);
      const data = await res.json();
      setCreatedKey(data.api_key.key);
      setNewKeyName('');
      setNewKeyScopes('read');
      setNewKeyDays(null);
      fetchKeys();
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setIsCreating(false);
    }
  };

  const handleRevoke = async (id: string) => {
    if (!window.confirm('Revoke this API key? It will stop working immediately.')) return;
    try {
      const res = await fetch(`/api/api-keys/${id}`, { method: 'DELETE', headers });
      if (!res.ok) throw new Error(`Failed to revoke key (${res.status})`);
      fetchKeys();
    } catch (err) {
      setError((err as Error).message);
    }
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-medium text-gray-300">API Keys</h3>
        <button
          onClick={() => { setShowCreate(!showCreate); setCreatedKey(null); }}
          className="px-3 py-1.5 rounded-lg bg-primary/10 text-primary text-xs font-medium hover:bg-primary/20 transition"
        >
          {showCreate ? 'Cancel' : 'Generate Key'}
        </button>
      </div>

      {error && (
        <div className="p-3 rounded-lg bg-red-500/10 border border-red-500/30 text-red-400 text-sm">{error}</div>
      )}

      {/* Created key display */}
      {createdKey && (
        <div className="p-4 rounded-lg bg-green-500/10 border border-green-500/30">
          <p className="text-sm text-green-400 font-medium mb-2">API key created! Copy it now -- it will not be shown again.</p>
          <div className="flex items-center gap-2">
            <code className="flex-1 px-3 py-2 bg-gray-800 rounded text-xs text-gray-200 font-mono break-all">{createdKey}</code>
            <button
              onClick={() => { navigator.clipboard.writeText(createdKey); }}
              className="px-3 py-2 rounded bg-gray-700 text-xs text-gray-300 hover:bg-gray-600 transition shrink-0"
            >
              Copy
            </button>
          </div>
        </div>
      )}

      {/* Create form */}
      {showCreate && (
        <form onSubmit={handleCreate} className="p-4 rounded-lg bg-gray-800/50 border border-gray-700 space-y-3">
          <div>
            <label className="block text-xs font-medium text-gray-300 mb-1">Key name</label>
            <input
              type="text"
              value={newKeyName}
              onChange={(e) => setNewKeyName(e.target.value)}
              placeholder="e.g. CI Pipeline Key"
              className="w-full px-3 py-2 bg-gray-800 border border-gray-700 rounded-lg text-gray-100 text-sm
                placeholder-gray-500 focus:outline-none focus:border-primary transition"
            />
          </div>
          <div className="flex gap-3">
            <div className="flex-1">
              <label className="block text-xs font-medium text-gray-300 mb-1">Scopes</label>
              <select
                value={newKeyScopes}
                onChange={(e) => setNewKeyScopes(e.target.value)}
                className="w-full px-3 py-2 bg-gray-800 border border-gray-700 rounded-lg text-gray-100 text-sm focus:outline-none focus:border-primary transition"
              >
                <option value="read">Read only</option>
                <option value="read,write">Read + Write</option>
                <option value="read,write,delete">Full access</option>
              </select>
            </div>
            <div className="flex-1">
              <label className="block text-xs font-medium text-gray-300 mb-1">Expires in (days)</label>
              <input
                type="number"
                value={newKeyDays ?? ''}
                onChange={(e) => setNewKeyDays(e.target.value ? parseInt(e.target.value) : null)}
                placeholder="Never"
                min={1}
                className="w-full px-3 py-2 bg-gray-800 border border-gray-700 rounded-lg text-gray-100 text-sm
                  placeholder-gray-500 focus:outline-none focus:border-primary transition"
              />
            </div>
          </div>
          <button
            type="submit"
            disabled={isCreating || !newKeyName.trim()}
            className="px-4 py-2 bg-primary hover:bg-primary/90 disabled:opacity-50 text-white font-medium rounded-lg text-sm transition"
          >
            {isCreating ? 'Creating...' : 'Generate'}
          </button>
        </form>
      )}

      {/* Keys table */}
      {isLoading ? (
        <div className="text-center py-8 text-gray-500 text-sm">Loading API keys...</div>
      ) : (
        <div className="overflow-x-auto rounded-lg border border-gray-800">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-900/80 border-b border-gray-800">
                <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-400 uppercase">Name</th>
                <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-400 uppercase">Prefix</th>
                <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-400 uppercase">Scopes</th>
                <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-400 uppercase">Status</th>
                <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-400 uppercase">Last used</th>
                <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-400 uppercase">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-800">
              {keys.map((k) => (
                <tr key={k.id} className="hover:bg-gray-800/50 transition">
                  <td className="px-4 py-2.5 text-gray-200">{k.name}</td>
                  <td className="px-4 py-2.5 text-gray-400 font-mono text-xs">{k.prefix}...</td>
                  <td className="px-4 py-2.5 text-gray-400 text-xs">{k.scopes}</td>
                  <td className="px-4 py-2.5">
                    <span className={`px-1.5 py-0.5 rounded text-xs ${k.is_active ? 'bg-green-500/20 text-green-400' : 'bg-red-500/20 text-red-400'}`}>
                      {k.is_active ? 'Active' : 'Revoked'}
                    </span>
                  </td>
                  <td className="px-4 py-2.5 text-gray-500 text-xs">
                    {k.last_used_at ? new Date(k.last_used_at).toLocaleDateString() : 'Never'}
                  </td>
                  <td className="px-4 py-2.5">
                    {k.is_active && (
                      <button
                        onClick={() => handleRevoke(k.id)}
                        className="px-2 py-1 rounded text-xs text-red-400 hover:bg-red-500/10 transition"
                      >
                        Revoke
                      </button>
                    )}
                  </td>
                </tr>
              ))}
              {keys.length === 0 && (
                <tr>
                  <td colSpan={6} className="px-4 py-8 text-center text-gray-500 text-sm">No API keys found</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
