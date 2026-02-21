import React, { useState, useEffect } from 'react';
import { useUIStore } from '../../store/ui';
import { usePipelineStore } from '../../store/pipeline';
import type { ETLPlatform } from '../../types/etl';
import { CATEGORY_COLORS } from '../../types/etl';
import AutoRunButton from '../shared/AutoRunButton';
import PipelineStatusBadge from './PipelineStatusBadge';
import NotificationCenter from '../shared/NotificationCenter';
import PresenceAvatars from '../shared/PresenceAvatars';
import ExportPDFButton from '../shared/ExportPDFButton';
import { useTheme } from '../../hooks/useTheme';
import { usePresence } from '../../hooks/usePresence';
import { getPlatforms } from '../../api/client';

// Hardcoded fallback used until the API responds (or if it fails)
const FALLBACK_PLATFORMS: ETLPlatform[] = [
  { id: 'nifi', name: 'Apache NiFi', formats: ['.xml', '.json', '.gz', '.zip', '.nar'], category: 'dataflow', versions: ['1.x', '2.x'] },
  { id: 'ssis', name: 'SQL Server SSIS', formats: ['.dtsx', '.ispac'], category: 'etl', versions: ['2012', '2014', '2016', '2017', '2019', '2022'] },
  { id: 'informatica', name: 'Informatica PowerCenter', formats: ['.xml', '.zip'], category: 'etl', versions: ['9.x', '10.x'] },
  { id: 'talend', name: 'Talend', formats: ['.item', '.properties', '.zip'], category: 'etl', versions: ['7.x', '8.x'] },
  { id: 'airflow', name: 'Apache Airflow', formats: ['.py'], category: 'orchestrator', versions: ['1.x', '2.x'] },
  { id: 'dbt', name: 'dbt', formats: ['.json', '.sql', '.yml'], category: 'transform', versions: ['1.x'] },
  { id: 'adf', name: 'Azure Data Factory', formats: ['.json'], category: 'orchestrator', versions: ['v2'] },
  { id: 'glue', name: 'AWS Glue', formats: ['.json', '.py'], category: 'etl', versions: ['3.0', '4.0'] },
  { id: 'snowflake', name: 'Snowflake', formats: ['.sql'], category: 'warehouse', versions: ['current'] },
  { id: 'spark', name: 'Existing Spark Jobs', formats: ['.py', '.scala', '.jar'], category: 'compute', versions: ['2.x', '3.x'] },
  { id: 'sql', name: 'Generic SQL', formats: ['.sql'], category: 'database', versions: ['ANSI', 'Oracle', 'MySQL', 'PostgreSQL', 'T-SQL'] },
];

export default function TopBar() {
  const sidebarMode = useUIStore((s) => s.sidebarMode);
  const setSidebarMode = useUIStore((s) => s.setSidebarMode);
  const setSettingsOpen = useUIStore((s) => s.setSettingsOpen);
  const setHelpOpen = useUIStore((s) => s.setHelpOpen);
  const setSearchOpen = useUIStore((s) => s.setSearchOpen);
  const platform = usePipelineStore((s) => s.platform);
  const setPlatform = usePipelineStore((s) => s.setPlatform);
  const [platformOpen, setPlatformOpen] = useState(false);
  const [platformsData, setPlatformsData] = useState<ETLPlatform[]>(FALLBACK_PLATFORMS);

  useEffect(() => {
    getPlatforms()
      .then((res) => {
        if (Array.isArray(res.platforms) && res.platforms.length > 0) {
          setPlatformsData(res.platforms as ETLPlatform[]);
        }
      })
      .catch(() => {
        // Keep using fallback platforms
      });
  }, []);

  const { mode: themeMode, setMode: setThemeMode } = useTheme();
  const { users: presenceUsers, connected: presenceConnected } = usePresence();

  const cycleTheme = () => {
    const modes: Array<'light' | 'dark' | 'system'> = ['dark', 'light', 'system'];
    const idx = modes.indexOf(themeMode);
    setThemeMode(modes[(idx + 1) % modes.length]);
  };

  const currentPlatform = platformsData.find((p) => p.id === platform);

  return (
    <header className="h-14 shrink-0 bg-gray-900/80 border-b border-border backdrop-blur-sm flex items-center px-4 gap-4 z-30">
      {/* Logo / Title */}
      <div className="flex items-center gap-3">
        <div className="w-8 h-8 rounded-lg bg-primary/20 flex items-center justify-center">
          <svg className="w-5 h-5 text-primary" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
          </svg>
        </div>
        <h1 className="text-base font-semibold text-gray-100 hidden sm:block">
          ETL Migration Platform
        </h1>
      </div>

      {/* Platform selector */}
      <div className="relative ml-4">
        <button
          onClick={() => setPlatformOpen(!platformOpen)}
          className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-gray-800 border border-border hover:border-gray-600 transition text-sm"
        >
          {currentPlatform ? (
            <>
              <span className={`px-1.5 py-0.5 rounded text-xs ${CATEGORY_COLORS[currentPlatform.category] || ''}`}>
                {currentPlatform.category}
              </span>
              <span className="text-gray-200">{currentPlatform.name}</span>
            </>
          ) : (
            <span className="text-gray-400">Auto-detect platform</span>
          )}
          <svg className="w-3.5 h-3.5 text-gray-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
          </svg>
        </button>

        {platformOpen && (
          <>
            <div className="fixed inset-0 z-40" onClick={() => setPlatformOpen(false)} />
            <div className="absolute top-full left-0 mt-1 w-64 bg-gray-900 border border-border rounded-lg shadow-xl z-50 max-h-80 overflow-y-auto">
              <button
                onClick={() => { setPlatform(null); setPlatformOpen(false); }}
                className="w-full text-left px-3 py-2 text-sm text-gray-400 hover:bg-gray-800 transition"
              >
                Auto-detect
              </button>
              {platformsData.map((p) => (
                <button
                  key={p.id}
                  onClick={() => { setPlatform(p.id); setPlatformOpen(false); }}
                  className={`
                    w-full text-left px-3 py-2 text-sm hover:bg-gray-800 transition flex items-center gap-2
                    ${platform === p.id ? 'bg-primary/10 text-gray-100' : 'text-gray-300'}
                  `}
                >
                  <span className={`px-1.5 py-0.5 rounded text-xs ${CATEGORY_COLORS[p.category] || ''}`}>
                    {p.category}
                  </span>
                  <span>{p.name}</span>
                </button>
              ))}
            </div>
          </>
        )}
      </div>

      {/* Auto-Run Button */}
      <AutoRunButton />

      {/* Spacer */}
      <div className="flex-1" />

      {/* Pipeline Status Badge */}
      <PipelineStatusBadge />

      {/* Search button */}
      <button
        onClick={() => setSearchOpen(true)}
        className="p-2 rounded-lg text-gray-400 hover:text-gray-200 hover:bg-gray-800 transition"
        title="Search (Ctrl+K)"
        aria-label="Open search overlay (Ctrl+K)"
      >
        <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
        </svg>
      </button>

      {/* Notification Center */}
      <NotificationCenter />

      {/* Presence Avatars */}
      <PresenceAvatars users={presenceUsers} connected={presenceConnected} />

      {/* PDF Export */}
      <ExportPDFButton />

      {/* Theme toggle */}
      <button
        onClick={cycleTheme}
        className="p-2 rounded-lg text-gray-400 hover:text-gray-200 hover:bg-gray-800 transition"
        title={`Theme: ${themeMode}`}
        aria-label={`Current theme: ${themeMode}. Click to change.`}
      >
        {themeMode === 'dark' ? (
          <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M20.354 15.354A9 9 0 018.646 3.646 9.003 9.003 0 0012 21a9.003 9.003 0 008.354-5.646z" />
          </svg>
        ) : themeMode === 'light' ? (
          <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 3v1m0 16v1m9-9h-1M4 12H3m15.364 6.364l-.707-.707M6.343 6.343l-.707-.707m12.728 0l-.707.707M6.343 17.657l-.707.707M16 12a4 4 0 11-8 0 4 4 0 018 0z" />
          </svg>
        ) : (
          <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
          </svg>
        )}
      </button>

      {/* Layout toggle */}
      <button
        onClick={() => setSidebarMode(!sidebarMode)}
        className="p-2 rounded-lg text-gray-400 hover:text-gray-200 hover:bg-gray-800 transition"
        title={sidebarMode ? 'Switch to tab layout' : 'Switch to sidebar layout'}
        aria-label={sidebarMode ? 'Switch to tab layout' : 'Switch to sidebar layout'}
      >
        {sidebarMode ? (
          <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h16" />
          </svg>
        ) : (
          <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 17V7m0 10a2 2 0 01-2 2H5a2 2 0 01-2-2V7a2 2 0 012-2h2a2 2 0 012 2m0 10a2 2 0 002 2h2a2 2 0 002-2M9 7a2 2 0 012-2h2a2 2 0 012 2m0 10V7" />
          </svg>
        )}
      </button>

      {/* Help */}
      <button
        onClick={() => setHelpOpen(true)}
        className="p-2 rounded-lg text-gray-400 hover:text-gray-200 hover:bg-gray-800 transition"
        title="Help"
        aria-label="Open help panel"
      >
        <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8.228 9c.549-1.165 2.03-2 3.772-2 2.21 0 4 1.343 4 3 0 1.4-1.278 2.575-3.006 2.907-.542.104-.994.54-.994 1.093m0 3h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
        </svg>
      </button>

      {/* Settings */}
      <button
        onClick={() => setSettingsOpen(true)}
        className="p-2 rounded-lg text-gray-400 hover:text-gray-200 hover:bg-gray-800 transition"
        title="Settings"
        aria-label="Open settings panel"
      >
        <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.066 2.573c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.573 1.066c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.066-2.573c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
        </svg>
      </button>
    </header>
  );
}
