import React from 'react';
import { useNavigate } from 'react-router-dom';
import TopBar from '../components/layout/TopBar';
import ProjectList from '../components/projects/ProjectList';
import { useAuthStore } from '../store/auth';

export default function DashboardPage() {
  const { user, logout } = useAuthStore();
  const navigate = useNavigate();

  const handleLogout = () => {
    logout();
    navigate('/login');
  };

  return (
    <div className="h-screen flex flex-col bg-gray-950 text-gray-100">
      {/* Simplified top bar for dashboard */}
      <header className="h-14 shrink-0 bg-gray-900/80 border-b border-gray-800 backdrop-blur-sm flex items-center px-4 gap-4 z-30">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-primary/20 flex items-center justify-center">
            <svg className="w-5 h-5 text-primary" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
            </svg>
          </div>
          <h1 className="text-base font-semibold text-gray-100 hidden sm:block">ETL Migration Platform</h1>
        </div>

        <div className="flex-1" />

        {/* Quick link to pipeline (without project context) */}
        <button
          onClick={() => navigate('/pipeline')}
          className="px-3 py-1.5 rounded-lg text-sm text-gray-400 hover:text-gray-200 hover:bg-gray-800 transition"
        >
          Quick Pipeline
        </button>

        {user?.role === 'admin' && (
          <button
            onClick={() => navigate('/admin')}
            className="px-3 py-1.5 rounded-lg text-sm text-gray-400 hover:text-gray-200 hover:bg-gray-800 transition"
          >
            Admin
          </button>
        )}

        {/* User menu */}
        <div className="flex items-center gap-3">
          <div className="text-right">
            <div className="text-sm text-gray-200">{user?.name}</div>
            <div className="text-xs text-gray-500">{user?.role}</div>
          </div>
          <button
            onClick={handleLogout}
            className="p-2 rounded-lg text-gray-400 hover:text-gray-200 hover:bg-gray-800 transition"
            title="Sign out"
          >
            <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1" />
            </svg>
          </button>
        </div>
      </header>

      {/* Main content */}
      <main className="flex-1 overflow-y-auto p-6">
        <div className="max-w-6xl mx-auto">
          <ProjectList />
        </div>
      </main>
    </div>
  );
}
