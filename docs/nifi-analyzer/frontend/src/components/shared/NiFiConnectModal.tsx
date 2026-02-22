import React, { useState } from 'react';

interface NiFiConnectModalProps {
  open: boolean;
  onClose: () => void;
  onExtract: (parsed: unknown) => void;
}

type AuthMethod = 'none' | 'basic' | 'token';

interface ConnectionStatus {
  state: 'idle' | 'connecting' | 'connected' | 'error';
  message?: string;
  version?: string;
}

interface ProcessGroup {
  id: string;
  name: string;
  depth: number;
  processorCount: number;
}

/**
 * NiFiConnectModal — dialog for connecting to a live NiFi instance
 * and extracting flows via the NiPyApi backend integration.
 */
export default function NiFiConnectModal({ open, onClose, onExtract }: NiFiConnectModalProps) {
  const [nifiUrl, setNifiUrl] = useState('');
  const [authMethod, setAuthMethod] = useState<AuthMethod>('none');
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [token, setToken] = useState('');
  const [verifySsl, setVerifySsl] = useState(true);
  const [connectionStatus, setConnectionStatus] = useState<ConnectionStatus>({ state: 'idle' });
  const [processGroups, setProcessGroups] = useState<ProcessGroup[]>([]);
  const [selectedGroup, setSelectedGroup] = useState('root');
  const [extracting, setExtracting] = useState(false);

  if (!open) return null;

  const getCredentials = () => {
    const creds: Record<string, unknown> = {
      nifiUrl: nifiUrl.trim(),
      verifySsl: verifySsl,
    };
    if (authMethod === 'basic') {
      creds.username = username;
      creds.password = password;
    } else if (authMethod === 'token') {
      creds.token = token;
    }
    return creds;
  };

  const handleTestConnection = async () => {
    if (!nifiUrl.trim()) return;

    setConnectionStatus({ state: 'connecting' });
    try {
      const res = await fetch('/api/nifi/connect', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(getCredentials()),
      });
      const data = await res.json();

      if (data.connected) {
        setConnectionStatus({
          state: 'connected',
          message: `Connected to NiFi ${data.version || ''}`,
          version: data.version,
        });
        // Auto-fetch process groups
        handleListGroups();
      } else {
        setConnectionStatus({
          state: 'error',
          message: data.error || data.detail || 'Connection failed',
        });
      }
    } catch (err) {
      setConnectionStatus({
        state: 'error',
        message: err instanceof Error ? err.message : 'Connection failed',
      });
    }
  };

  const handleListGroups = async () => {
    try {
      const res = await fetch('/api/nifi/process-groups', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(getCredentials()),
      });
      const data = await res.json();
      if (data.processGroups) {
        setProcessGroups(data.processGroups);
      }
    } catch {
      // Process group listing is optional
    }
  };

  const handleExtract = async () => {
    if (!nifiUrl.trim()) return;

    setExtracting(true);
    try {
      const res = await fetch('/api/nifi/extract', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ...getCredentials(),
          processGroupId: selectedGroup,
        }),
      });

      if (!res.ok) {
        const body = await res.text().catch(() => '');
        throw new Error(`Extract failed ${res.status}: ${body || res.statusText}`);
      }

      const parsed = await res.json();
      onExtract(parsed);
      onClose();
    } catch (err) {
      setConnectionStatus({
        state: 'error',
        message: err instanceof Error ? err.message : 'Extraction failed',
      });
    } finally {
      setExtracting(false);
    }
  };

  const statusColors: Record<string, string> = {
    idle: 'text-gray-500',
    connecting: 'text-blue-400',
    connected: 'text-green-400',
    error: 'text-red-400',
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
      <div className="w-full max-w-lg rounded-xl bg-gray-900 border border-border shadow-2xl">
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b border-border">
          <h3 className="text-lg font-semibold text-gray-100">Connect to NiFi</h3>
          <button
            onClick={onClose}
            className="w-8 h-8 rounded-lg hover:bg-gray-800 flex items-center justify-center text-gray-400 hover:text-gray-200 transition"
          >
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        {/* Body */}
        <div className="p-4 space-y-4">
          {/* NiFi URL */}
          <div>
            <label className="block text-xs text-gray-400 mb-1">NiFi URL</label>
            <input
              type="text"
              value={nifiUrl}
              onChange={(e) => setNifiUrl(e.target.value)}
              placeholder="https://nifi.example.com:8443"
              className="w-full px-3 py-2 rounded-lg bg-gray-800 border border-border text-sm text-gray-200 placeholder-gray-600 focus:border-primary focus:outline-none"
            />
          </div>

          {/* Auth Method */}
          <div>
            <label className="block text-xs text-gray-400 mb-1">Authentication</label>
            <div className="flex gap-2">
              {(['none', 'basic', 'token'] as const).map((method) => (
                <button
                  key={method}
                  onClick={() => setAuthMethod(method)}
                  className={`px-3 py-1.5 rounded-lg text-xs font-medium transition ${
                    authMethod === method
                      ? 'bg-primary/20 text-primary border border-primary/30'
                      : 'bg-gray-800 text-gray-400 border border-border hover:bg-gray-700'
                  }`}
                >
                  {method === 'none' ? 'No Auth' : method === 'basic' ? 'Username/Password' : 'Bearer Token'}
                </button>
              ))}
            </div>
          </div>

          {/* Basic Auth Fields */}
          {authMethod === 'basic' && (
            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="block text-xs text-gray-400 mb-1">Username</label>
                <input
                  type="text"
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  className="w-full px-3 py-2 rounded-lg bg-gray-800 border border-border text-sm text-gray-200 focus:border-primary focus:outline-none"
                />
              </div>
              <div>
                <label className="block text-xs text-gray-400 mb-1">Password</label>
                <input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  className="w-full px-3 py-2 rounded-lg bg-gray-800 border border-border text-sm text-gray-200 focus:border-primary focus:outline-none"
                />
              </div>
            </div>
          )}

          {/* Token Auth Field */}
          {authMethod === 'token' && (
            <div>
              <label className="block text-xs text-gray-400 mb-1">Bearer Token</label>
              <input
                type="password"
                value={token}
                onChange={(e) => setToken(e.target.value)}
                placeholder="Paste your NiFi access token"
                className="w-full px-3 py-2 rounded-lg bg-gray-800 border border-border text-sm text-gray-200 placeholder-gray-600 focus:border-primary focus:outline-none"
              />
            </div>
          )}

          {/* SSL Verification */}
          <label className="flex items-center gap-2 cursor-pointer">
            <input
              type="checkbox"
              checked={verifySsl}
              onChange={(e) => setVerifySsl(e.target.checked)}
              className="rounded border-gray-600"
            />
            <span className="text-xs text-gray-400">Verify SSL certificate</span>
          </label>

          {/* Connection Status */}
          {connectionStatus.state !== 'idle' && (
            <div className={`flex items-center gap-2 text-xs ${statusColors[connectionStatus.state]}`}>
              {connectionStatus.state === 'connecting' && (
                <div className="w-3 h-3 rounded-full border-2 border-blue-400 border-t-transparent animate-spin" />
              )}
              {connectionStatus.state === 'connected' && (
                <svg className="w-4 h-4 text-green-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                </svg>
              )}
              {connectionStatus.state === 'error' && (
                <svg className="w-4 h-4 text-red-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              )}
              <span>{connectionStatus.message}</span>
            </div>
          )}

          {/* Process Group Selection */}
          {processGroups.length > 0 && (
            <div>
              <label className="block text-xs text-gray-400 mb-1">Process Group</label>
              <select
                value={selectedGroup}
                onChange={(e) => setSelectedGroup(e.target.value)}
                className="w-full px-3 py-2 rounded-lg bg-gray-800 border border-border text-sm text-gray-200 focus:border-primary focus:outline-none"
              >
                <option value="root">Root (entire flow)</option>
                {processGroups.map((pg) => (
                  <option key={pg.id} value={pg.id}>
                    {'  '.repeat(pg.depth)}{pg.name} ({pg.processorCount} processors)
                  </option>
                ))}
              </select>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-3 p-4 border-t border-border">
          <button
            onClick={onClose}
            className="px-4 py-2 rounded-lg text-sm text-gray-400 hover:text-gray-200 hover:bg-gray-800 transition"
          >
            Cancel
          </button>
          <button
            onClick={handleTestConnection}
            disabled={!nifiUrl.trim() || connectionStatus.state === 'connecting'}
            className="px-4 py-2 rounded-lg bg-gray-700 text-white text-sm font-medium hover:bg-gray-600 disabled:opacity-40 disabled:cursor-not-allowed transition"
          >
            Test Connection
          </button>
          <button
            onClick={handleExtract}
            disabled={!nifiUrl.trim() || extracting || connectionStatus.state !== 'connected'}
            className="px-4 py-2 rounded-lg bg-primary text-white text-sm font-medium hover:bg-primary/80 disabled:opacity-40 disabled:cursor-not-allowed transition"
          >
            {extracting ? 'Extracting...' : 'Extract Flow'}
          </button>
        </div>
      </div>
    </div>
  );
}
