import React, { useState } from 'react';
import CoverageDonut from './CoverageDonut';
import ConfidenceChart from './ConfidenceChart';
import RiskHeatmap from './RiskHeatmap';
import MigrationTimeline from './MigrationTimeline';
import SecurityTreemap from './SecurityTreemap';

type PanelKey = 'coverage' | 'confidence' | 'risk' | 'timeline' | 'security';

export default function VizDashboard() {
  const [expandedPanel, setExpandedPanel] = useState<PanelKey | null>(null);

  const toggleExpand = (key: PanelKey) => {
    setExpandedPanel((prev) => (prev === key ? null : key));
  };

  if (expandedPanel) {
    return (
      <div className="space-y-3">
        <div className="flex items-center justify-between">
          <h3 className="text-sm font-medium text-gray-300">
            {expandedPanel.charAt(0).toUpperCase() + expandedPanel.slice(1)} View
          </h3>
          <button
            onClick={() => setExpandedPanel(null)}
            className="text-xs text-gray-500 hover:text-gray-300 transition"
          >
            Back to Dashboard
          </button>
        </div>
        {expandedPanel === 'coverage' && <CoverageDonut />}
        {expandedPanel === 'confidence' && <ConfidenceChart />}
        {expandedPanel === 'risk' && <RiskHeatmap />}
        {expandedPanel === 'timeline' && <MigrationTimeline />}
        {expandedPanel === 'security' && <SecurityTreemap />}
      </div>
    );
  }

  return (
    <div className="space-y-3">
      <h3 className="text-sm font-medium text-gray-300">Visual Dashboard</h3>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
        {/* Top row: 3 panels */}
        <DashboardPanel title="Coverage" onExpand={() => toggleExpand('coverage')}>
          <CoverageDonut />
        </DashboardPanel>

        <DashboardPanel title="Confidence" onExpand={() => toggleExpand('confidence')}>
          <ConfidenceChart />
        </DashboardPanel>

        <DashboardPanel title="Risk Heatmap" onExpand={() => toggleExpand('risk')}>
          <RiskHeatmap />
        </DashboardPanel>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
        {/* Bottom row: timeline spans 2, security 1 */}
        <div className="md:col-span-2">
          <DashboardPanel title="Migration Timeline" onExpand={() => toggleExpand('timeline')}>
            <MigrationTimeline />
          </DashboardPanel>
        </div>

        <DashboardPanel title="Security" onExpand={() => toggleExpand('security')}>
          <SecurityTreemap />
        </DashboardPanel>
      </div>
    </div>
  );
}

function DashboardPanel({
  title,
  onExpand,
  children,
}: {
  title: string;
  onExpand: () => void;
  children: React.ReactNode;
}) {
  return (
    <div className="rounded-lg border border-border bg-gray-900/30 overflow-hidden">
      <div className="flex items-center justify-between px-3 py-2 border-b border-border bg-gray-800/30">
        <span className="text-xs font-medium text-gray-400">{title}</span>
        <button
          onClick={onExpand}
          className="text-[10px] text-gray-500 hover:text-gray-300 transition"
        >
          View Full
        </button>
      </div>
      <div className="p-2">{children}</div>
    </div>
  );
}
