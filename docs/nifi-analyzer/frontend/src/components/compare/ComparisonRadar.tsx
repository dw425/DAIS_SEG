import React from 'react';

interface FlowSummary {
  label: string;
  processorCount: number;
  connectionCount: number;
  processGroupCount: number;
  controllerServiceCount: number;
  uniqueTypes: number;
  warnings: number;
}

interface ComparisonRadarProps {
  data: Record<string, unknown>;
}

const COLORS = ['#FF4B4B', '#3B82F6', '#21C354', '#EAB308', '#A855F7', '#EC4899'];

const AXES = [
  { key: 'processorCount', label: 'Processors' },
  { key: 'connectionCount', label: 'Connections' },
  { key: 'processGroupCount', label: 'Groups' },
  { key: 'controllerServiceCount', label: 'Services' },
  { key: 'uniqueTypes', label: 'Types' },
  { key: 'warnings', label: 'Warnings' },
] as const;

export default function ComparisonRadar({ data }: ComparisonRadarProps) {
  const flows = (data.flows || []) as FlowSummary[];
  if (flows.length === 0) return null;

  const cx = 150;
  const cy = 150;
  const radius = 110;
  const levels = 5;
  const angleStep = (2 * Math.PI) / AXES.length;

  // Compute max for each axis for normalization
  const maxValues = AXES.map((axis) =>
    Math.max(1, ...flows.map((f) => Number((f as unknown as Record<string, unknown>)[axis.key]) || 0))
  );

  // Generate polygon points for each flow
  const getPolygonPoints = (flow: FlowSummary): string => {
    return AXES.map((axis, i) => {
      const val = Number((flow as unknown as Record<string, unknown>)[axis.key]) || 0;
      const normalized = val / maxValues[i];
      const angle = i * angleStep - Math.PI / 2;
      const x = cx + radius * normalized * Math.cos(angle);
      const y = cy + radius * normalized * Math.sin(angle);
      return `${x},${y}`;
    }).join(' ');
  };

  return (
    <div className="rounded-lg border border-border bg-gray-900/50 p-4">
      <h3 className="text-sm font-semibold text-gray-200 mb-4">Radar Comparison</h3>

      <div className="flex justify-center">
        <svg
          width="300"
          height="300"
          viewBox="0 0 300 300"
          className="max-w-full"
          role="img"
          aria-label="Radar chart comparing flow metrics"
        >
          {/* Grid levels */}
          {Array.from({ length: levels }, (_, l) => {
            const r = (radius * (l + 1)) / levels;
            const points = AXES.map((_, i) => {
              const angle = i * angleStep - Math.PI / 2;
              return `${cx + r * Math.cos(angle)},${cy + r * Math.sin(angle)}`;
            }).join(' ');
            return (
              <polygon
                key={l}
                points={points}
                fill="none"
                stroke="#374151"
                strokeWidth="0.5"
              />
            );
          })}

          {/* Axis lines and labels */}
          {AXES.map((axis, i) => {
            const angle = i * angleStep - Math.PI / 2;
            const x2 = cx + radius * Math.cos(angle);
            const y2 = cy + radius * Math.sin(angle);
            const lx = cx + (radius + 20) * Math.cos(angle);
            const ly = cy + (radius + 20) * Math.sin(angle);
            return (
              <g key={axis.key}>
                <line x1={cx} y1={cy} x2={x2} y2={y2} stroke="#4B5563" strokeWidth="0.5" />
                <text
                  x={lx}
                  y={ly}
                  textAnchor="middle"
                  dominantBaseline="middle"
                  className="fill-gray-500 text-[9px]"
                >
                  {axis.label}
                </text>
              </g>
            );
          })}

          {/* Flow polygons */}
          {flows.map((flow, fi) => (
            <polygon
              key={fi}
              points={getPolygonPoints(flow)}
              fill={COLORS[fi % COLORS.length]}
              fillOpacity="0.15"
              stroke={COLORS[fi % COLORS.length]}
              strokeWidth="1.5"
            />
          ))}

          {/* Flow data points */}
          {flows.map((flow, fi) =>
            AXES.map((axis, i) => {
              const val = Number((flow as unknown as Record<string, unknown>)[axis.key]) || 0;
              const normalized = val / maxValues[i];
              const angle = i * angleStep - Math.PI / 2;
              const x = cx + radius * normalized * Math.cos(angle);
              const y = cy + radius * normalized * Math.sin(angle);
              return (
                <circle
                  key={`${fi}-${i}`}
                  cx={x}
                  cy={y}
                  r="3"
                  fill={COLORS[fi % COLORS.length]}
                />
              );
            })
          )}
        </svg>
      </div>

      {/* Legend */}
      <div className="flex flex-wrap gap-4 justify-center mt-4">
        {flows.map((f, i) => (
          <div key={i} className="flex items-center gap-2">
            <span
              className="w-3 h-3 rounded-full"
              style={{ backgroundColor: COLORS[i % COLORS.length] }}
            />
            <span className="text-xs text-gray-400">{f.label}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
