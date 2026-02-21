import React from 'react';

interface FlowSummary {
  label: string;
  processorCount: number;
  connectionCount: number;
  processGroupCount: number;
  controllerServiceCount: number;
  uniqueTypes: number;
  platform: string;
  warnings: number;
}

interface ComparisonTableProps {
  data: Record<string, unknown>;
}

export default function ComparisonTable({ data }: ComparisonTableProps) {
  const flows = (data.flows || []) as FlowSummary[];

  if (flows.length === 0) return null;

  const metrics: { label: string; key: keyof FlowSummary; format?: (v: unknown) => string }[] = [
    { label: 'Platform', key: 'platform' },
    { label: 'Processors', key: 'processorCount' },
    { label: 'Connections', key: 'connectionCount' },
    { label: 'Process Groups', key: 'processGroupCount' },
    { label: 'Controller Services', key: 'controllerServiceCount' },
    { label: 'Unique Types', key: 'uniqueTypes' },
    { label: 'Warnings', key: 'warnings' },
  ];

  return (
    <div className="rounded-lg border border-border overflow-hidden">
      <div className="px-4 py-2.5 bg-gray-900/60 border-b border-border">
        <h3 className="text-sm font-semibold text-gray-200">Side-by-Side Metrics</h3>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm" role="table" aria-label="Flow comparison metrics">
          <thead>
            <tr className="border-b border-border bg-gray-900/40">
              <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Metric
              </th>
              {flows.map((f, i) => (
                <th
                  key={i}
                  className="px-4 py-2 text-left text-xs font-medium text-gray-400 uppercase tracking-wider"
                >
                  {f.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {metrics.map((m) => (
              <tr key={m.key} className="border-b border-border/50 hover:bg-gray-900/30 transition">
                <td className="px-4 py-2 text-gray-400 font-medium">{m.label}</td>
                {flows.map((f, i) => {
                  const val = f[m.key];
                  const isMax =
                    typeof val === 'number' &&
                    val === Math.max(...flows.map((ff) => Number(ff[m.key]) || 0));
                  return (
                    <td
                      key={i}
                      className={`px-4 py-2 ${
                        isMax ? 'text-primary font-semibold' : 'text-gray-300'
                      }`}
                    >
                      {String(val)}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
