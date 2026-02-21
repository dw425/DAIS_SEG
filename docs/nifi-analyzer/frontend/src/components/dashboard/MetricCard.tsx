import React from 'react';

interface MetricCardProps {
  label: string;
  value: string | number;
  sublabel?: string;
  color?: string;
  icon?: React.ReactNode;
}

export default function MetricCard({
  label,
  value,
  sublabel,
  color = 'text-primary',
  icon,
}: MetricCardProps) {
  return (
    <div className="rounded-lg border border-border bg-gray-900/50 p-4 hover:border-gray-600 transition">
      <div className="flex items-start justify-between">
        <div>
          <p className="text-xs font-medium text-gray-500 uppercase tracking-wider">{label}</p>
          <p className={`text-2xl font-bold mt-1 ${color}`}>{value}</p>
          {sublabel && <p className="text-xs text-gray-500 mt-0.5">{sublabel}</p>}
        </div>
        {icon && (
          <div className="w-10 h-10 rounded-lg bg-gray-800 flex items-center justify-center text-gray-400">
            {icon}
          </div>
        )}
      </div>
    </div>
  );
}
