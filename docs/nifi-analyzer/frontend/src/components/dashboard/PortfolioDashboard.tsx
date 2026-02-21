import React, { useState, useEffect } from 'react';
import { getDashboard } from '../../api/client';
import MetricCard from './MetricCard';

interface DashboardData {
  totalRuns: number;
  completedRuns: number;
  failedRuns: number;
  totalProcessors: number;
  platformBreakdown: Record<string, number>;
  avgDurationMs: number;
  successRate: number;
  recentRuns: Record<string, unknown>[];
}

const PLATFORM_COLORS: Record<string, string> = {
  nifi: '#3B82F6',
  ssis: '#A855F7',
  informatica: '#EC4899',
  talend: '#21C354',
  airflow: '#EAB308',
  dbt: '#F97316',
  adf: '#06B6D4',
  unknown: '#6B7280',
};

export default function PortfolioDashboard() {
  const [data, setData] = useState<DashboardData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchDashboard = async () => {
      setLoading(true);
      setError(null);
      try {
        const result = await getDashboard();
        setData(result as DashboardData);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Failed to load dashboard');
      } finally {
        setLoading(false);
      }
    };
    fetchDashboard();
  }, []);

  if (loading) return <p className="text-sm text-gray-500">Loading dashboard...</p>;

  if (error) {
    return (
      <div className="px-4 py-3 rounded-lg bg-red-500/10 border border-red-500/30 text-sm text-red-400">
        {error}
      </div>
    );
  }

  if (!data) {
    return (
      <div className="text-center py-12 border-2 border-dashed border-border rounded-lg">
        <p className="text-gray-500 text-sm">Dashboard data unavailable</p>
      </div>
    );
  }

  const platforms = Object.entries(data.platformBreakdown);
  const maxPlatformCount = Math.max(1, ...platforms.map(([, v]) => v));

  return (
    <div className="space-y-6">
      <h2 className="text-xl font-bold text-gray-100">Portfolio Dashboard</h2>

      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <MetricCard label="Total Runs" value={data.totalRuns} color="text-primary" />
        <MetricCard
          label="Success Rate"
          value={`${data.successRate}%`}
          color={data.successRate >= 90 ? 'text-green-400' : data.successRate >= 70 ? 'text-amber-400' : 'text-red-400'}
        />
        <MetricCard
          label="Avg Duration"
          value={data.avgDurationMs < 1000 ? `${data.avgDurationMs}ms` : `${(data.avgDurationMs / 1000).toFixed(1)}s`}
          color="text-blue-400"
        />
        <MetricCard label="Total Processors" value={data.totalProcessors} color="text-purple-400" />
      </div>

      {/* Platform breakdown chart */}
      {platforms.length > 0 && (
        <div className="rounded-lg border border-border bg-gray-900/50 p-4">
          <h3 className="text-sm font-semibold text-gray-200 mb-4">Platform Distribution</h3>
          <div className="space-y-2">
            {platforms.map(([platform, count]) => (
              <div key={platform} className="flex items-center gap-3">
                <span className="text-xs text-gray-400 w-24 text-right">{platform}</span>
                <div className="flex-1 h-6 bg-gray-800 rounded overflow-hidden">
                  <div
                    className="h-full rounded transition-all duration-500"
                    style={{
                      width: `${(count / maxPlatformCount) * 100}%`,
                      backgroundColor: PLATFORM_COLORS[platform] || PLATFORM_COLORS.unknown,
                    }}
                  />
                </div>
                <span className="text-xs text-gray-400 w-8">{count}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Success/Failure donut */}
      {data.totalRuns > 0 && (
        <div className="rounded-lg border border-border bg-gray-900/50 p-4">
          <h3 className="text-sm font-semibold text-gray-200 mb-4">Run Results</h3>
          <div className="flex items-center justify-center gap-8">
            <svg width="120" height="120" viewBox="0 0 120 120" role="img" aria-label="Success rate donut chart">
              {/* Background circle */}
              <circle cx="60" cy="60" r="50" fill="none" stroke="#374151" strokeWidth="12" />
              {/* Success arc */}
              <circle
                cx="60"
                cy="60"
                r="50"
                fill="none"
                stroke="#21C354"
                strokeWidth="12"
                strokeDasharray={`${(data.successRate / 100) * 314} 314`}
                strokeLinecap="round"
                transform="rotate(-90 60 60)"
              />
              {/* Center text */}
              <text x="60" y="56" textAnchor="middle" className="fill-gray-100 text-lg font-bold" fontSize="22">
                {data.successRate}%
              </text>
              <text x="60" y="72" textAnchor="middle" className="fill-gray-500" fontSize="10">
                success
              </text>
            </svg>
            <div className="space-y-2">
              <div className="flex items-center gap-2">
                <span className="w-3 h-3 rounded-full bg-green-400" />
                <span className="text-sm text-gray-300">{data.completedRuns} completed</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="w-3 h-3 rounded-full bg-red-400" />
                <span className="text-sm text-gray-300">{data.failedRuns} failed</span>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Recent runs */}
      {data.recentRuns.length > 0 && (
        <div className="rounded-lg border border-border bg-gray-900/50 overflow-hidden">
          <div className="px-4 py-2.5 bg-gray-900/80 border-b border-border">
            <h3 className="text-sm font-semibold text-gray-200">Recent Runs</h3>
          </div>
          <div className="divide-y divide-border/50">
            {data.recentRuns.map((run, i) => (
              <div key={i} className="flex items-center justify-between px-4 py-2.5">
                <div>
                  <span className="text-sm text-gray-300">{String(run.fileName)}</span>
                  <span className="text-xs text-gray-500 ml-2">{String(run.platform)}</span>
                </div>
                <span
                  className={`px-2 py-0.5 rounded text-xs font-medium ${
                    run.status === 'completed'
                      ? 'bg-green-500/20 text-green-400'
                      : 'bg-red-500/20 text-red-400'
                  }`}
                >
                  {String(run.status)}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
