import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAuthStore } from '../store/auth';
import AdminConsole from '../components/admin/AdminConsole';
import AuditLog from '../components/admin/AuditLog';
import APIKeyManager from '../components/admin/APIKeyManager';

type AdminTab = 'console' | 'audit' | 'api-keys';

export default function AdminPage() {
  const [activeTab, setActiveTab] = useState<AdminTab>('console');
  const { user, logout } = useAuthStore();
  const navigate = useNavigate();

  const tabs: { id: AdminTab; label: string }[] = [
    { id: 'console', label: 'Console' },
    { id: 'audit', label: 'Audit Trail' },
    { id: 'api-keys', label: 'API Keys' },
  ];

  return (
    <div className="h-screen flex flex-col bg-gray-950 text-gray-100">
      {/* Admin header */}
      <header className="h-14 shrink-0 bg-gray-900/80 border-b border-gray-800 backdrop-blur-sm flex items-center px-4 gap-4 z-30">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-primary/20 flex items-center justify-center">
            <svg className="w-5 h-5 text-primary" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
            </svg>
          </div>
          <h1 className="text-base font-semibold text-gray-100">Admin</h1>
        </div>

        <div className="flex-1" />

        <button
          onClick={() => navigate('/dashboard')}
          className="px-3 py-1.5 rounded-lg text-sm text-gray-400 hover:text-gray-200 hover:bg-gray-800 transition"
        >
          Dashboard
        </button>
        <button
          onClick={() => navigate('/pipeline')}
          className="px-3 py-1.5 rounded-lg text-sm text-gray-400 hover:text-gray-200 hover:bg-gray-800 transition"
        >
          Pipeline
        </button>

        <div className="flex items-center gap-3">
          <div className="text-right">
            <div className="text-sm text-gray-200">{user?.name}</div>
            <div className="text-xs text-gray-500">{user?.role}</div>
          </div>
          <button
            onClick={() => { logout(); navigate('/login'); }}
            className="p-2 rounded-lg text-gray-400 hover:text-gray-200 hover:bg-gray-800 transition"
            title="Sign out"
          >
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1" />
            </svg>
          </button>
        </div>
      </header>

      {/* Tab bar */}
      <div className="shrink-0 border-b border-gray-800 bg-gray-900/50 px-4">
        <div className="flex gap-1">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`px-4 py-2.5 text-sm font-medium border-b-2 transition
                ${activeTab === tab.id ? 'border-primary text-primary' : 'border-transparent text-gray-500 hover:text-gray-300'}`}
            >
              {tab.label}
            </button>
          ))}
        </div>
      </div>

      {/* Content */}
      <main className="flex-1 overflow-y-auto p-6">
        <div className="max-w-5xl mx-auto">
          {activeTab === 'console' && <AdminConsole />}
          {activeTab === 'audit' && <AuditLog />}
          {activeTab === 'api-keys' && <APIKeyManager />}
        </div>
      </main>
    </div>
  );
}
