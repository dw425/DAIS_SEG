import React, { useState, useCallback } from 'react';

interface CostInputFormProps {
  onApply: (params: Record<string, unknown>) => void;
  onClose: () => void;
}

interface CostParams {
  hourlyRate: number;
  monthlyInfraCost: number;
  teamSize: number;
  timeline: 'fast' | 'standard' | 'conservative';
}

const DEFAULTS: CostParams = {
  hourlyRate: 150,
  monthlyInfraCost: 5000,
  teamSize: 3,
  timeline: 'standard',
};

function estimateTotal(params: CostParams): number {
  const weeklyRate = params.hourlyRate * 40 * params.teamSize;
  const multiplier = params.timeline === 'fast' ? 0.7 : params.timeline === 'conservative' ? 1.4 : 1.0;
  return Math.round(weeklyRate * 4 * multiplier + params.monthlyInfraCost * 3);
}

export default function CostInputForm({ onApply, onClose }: CostInputFormProps) {
  const [params, setParams] = useState<CostParams>({ ...DEFAULTS });

  const update = useCallback(<K extends keyof CostParams>(key: K, value: CostParams[K]) => {
    setParams((prev) => ({ ...prev, [key]: value }));
  }, []);

  const handleApply = useCallback(() => {
    onApply({
      hourlyRate: params.hourlyRate,
      monthlyInfraCost: params.monthlyInfraCost,
      teamSize: params.teamSize,
      timeline: params.timeline,
    });
  }, [params, onApply]);

  const handleReset = useCallback(() => {
    setParams({ ...DEFAULTS });
  }, []);

  const totalEstimate = estimateTotal(params);

  return (
    <div className="rounded-lg border border-border bg-gray-900/80 p-5 space-y-5">
      <div className="flex items-center justify-between">
        <h4 className="text-sm font-semibold text-gray-200">Custom Cost Parameters</h4>
        <button onClick={onClose} className="text-gray-500 hover:text-gray-300 transition text-lg leading-none">&times;</button>
      </div>

      <div className="rounded-lg bg-blue-500/10 border border-blue-500/30 p-3 text-xs text-blue-400">
        Default values are provided as starting points. Customize these to match your organization's actual costs for more accurate estimates.
      </div>

      {/* Hourly Rate */}
      <div>
        <label className="flex items-center justify-between text-xs text-gray-400 mb-1">
          <span>Hourly Developer Rate</span>
          <span className="text-gray-300 font-medium tabular-nums">${params.hourlyRate}/hr</span>
        </label>
        <input
          type="range"
          min={50}
          max={300}
          step={10}
          value={params.hourlyRate}
          onChange={(e) => update('hourlyRate', Number(e.target.value))}
          className="w-full h-1.5 bg-gray-700 rounded-full appearance-none cursor-pointer accent-primary"
        />
        <div className="flex justify-between text-xs text-gray-600 mt-0.5">
          <span>$50</span>
          <span>$300</span>
        </div>
      </div>

      {/* Monthly Infra Cost */}
      <div>
        <label className="flex items-center justify-between text-xs text-gray-400 mb-1">
          <span>Current Infrastructure Monthly Cost</span>
          <span className="text-gray-300 font-medium tabular-nums">${params.monthlyInfraCost.toLocaleString()}/mo</span>
        </label>
        <input
          type="number"
          min={0}
          max={50000}
          step={500}
          value={params.monthlyInfraCost}
          onChange={(e) => update('monthlyInfraCost', Math.max(0, Math.min(50000, Number(e.target.value))))}
          placeholder="Default: $5,000/mo - customize to your costs"
          className="w-full px-3 py-2 rounded-lg bg-gray-800 border border-border text-sm text-gray-200 tabular-nums focus:outline-none focus:border-primary"
        />
      </div>

      {/* Team Size */}
      <div>
        <label className="flex items-center justify-between text-xs text-gray-400 mb-1">
          <span>Team Size</span>
          <span className="text-gray-300 font-medium tabular-nums">{params.teamSize} engineers</span>
        </label>
        <input
          type="number"
          min={0}
          max={20}
          value={params.teamSize}
          onChange={(e) => update('teamSize', Math.max(1, Math.min(20, Number(e.target.value))))}
          placeholder="Default: 3 engineers - adjust for your team"
          className="w-full px-3 py-2 rounded-lg bg-gray-800 border border-border text-sm text-gray-200 tabular-nums focus:outline-none focus:border-primary"
        />
      </div>

      {/* Timeline Preference */}
      <div>
        <p className="text-xs text-gray-400 mb-2">Migration Timeline Preference</p>
        <div className="flex gap-2">
          {(['fast', 'standard', 'conservative'] as const).map((opt) => (
            <button
              key={opt}
              onClick={() => update('timeline', opt)}
              className={`flex-1 px-3 py-2 rounded-lg text-xs font-medium transition border ${
                params.timeline === opt
                  ? 'bg-primary/20 border-primary text-primary'
                  : 'bg-gray-800 border-border text-gray-400 hover:text-gray-200'
              }`}
            >
              {opt === 'fast' ? 'Fast (Parallel)' : opt === 'standard' ? 'Standard' : 'Conservative'}
            </button>
          ))}
        </div>
      </div>

      {/* Cost Preview */}
      <div className="rounded-lg bg-gray-800/50 border border-border p-3 text-center">
        <p className="text-xs text-gray-500">Estimated Total Cost</p>
        <p className="text-xl font-bold text-gray-100 tabular-nums">${totalEstimate.toLocaleString()}</p>
      </div>

      {/* Actions */}
      <div className="flex gap-2">
        <button
          onClick={handleReset}
          className="flex-1 px-3 py-2 rounded-lg border border-border text-xs text-gray-400 hover:text-gray-200 hover:bg-gray-800 transition"
        >
          Reset to Defaults
        </button>
        <button
          onClick={handleApply}
          className="flex-1 px-3 py-2 rounded-lg bg-primary text-white text-xs font-medium hover:bg-primary/80 transition"
        >
          Apply
        </button>
      </div>
    </div>
  );
}
