import React, { useEffect, useState } from 'react';
import { useUIStore } from '../../store/ui';
import { usePipelineStore } from '../../store/pipeline';
import type { ETLPlatform } from '../../types/etl';
import { CATEGORY_COLORS } from '../../types/etl';

// Inline the platforms data; in production this would come from the API
const PLATFORMS_DATA: ETLPlatform[] = [
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
  const platform = usePipelineStore((s) => s.platform);
  const setPlatform = usePipelineStore((s) => s.setPlatform);
  const [platformOpen, setPlatformOpen] = useState(false);

  const currentPlatform = PLATFORMS_DATA.find((p) => p.id === platform);

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
              {PLATFORMS_DATA.map((p) => (
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

      {/* Spacer */}
      <div className="flex-1" />

      {/* Layout toggle */}
      <button
        onClick={() => setSidebarMode(!sidebarMode)}
        className="p-2 rounded-lg text-gray-400 hover:text-gray-200 hover:bg-gray-800 transition"
        title={sidebarMode ? 'Switch to tab layout' : 'Switch to sidebar layout'}
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

      {/* Settings */}
      <button className="p-2 rounded-lg text-gray-400 hover:text-gray-200 hover:bg-gray-800 transition">
        <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.066 2.573c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.573 1.066c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.066-2.573c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
        </svg>
      </button>
    </header>
  );
}
