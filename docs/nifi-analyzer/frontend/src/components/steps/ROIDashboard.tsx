import React, { useEffect, useState, useCallback } from 'react';
import { usePipelineStore } from '../../store/pipeline';
import { useUIStore } from '../../store/ui';
import { generateROIReport } from '../../api/client';
import type { FullROIReport } from '../../types/roi';
import CostInputForm from '../shared/CostInputForm';

// ── Fallback local estimation when API is unavailable ──

function estimateLocally(processorCount: number): FullROIReport {
  const base = processorCount * 2000;
  return {
    comparison: {
      liftAndShift: { cost: base, weeks: Math.ceil(processorCount * 0.5), risk: 0.3, description: 'Direct migration with minimal changes. Faster but retains legacy patterns.' },
      refactor: { cost: base * 1.6, weeks: Math.ceil(processorCount * 0.9), risk: 0.15, description: 'Full refactor to idiomatic Databricks patterns. Higher upfront cost, lower long-term risk.' },
      recommendation: processorCount > 20 ? 'Refactor recommended due to flow complexity' : 'Lift-and-shift is cost-effective for this flow size',
      breakEvenMonths: processorCount > 20 ? 8 : 14,
    },
    tco: {
      currentPlatform: { year1: 60000, year3: 180000, year5: 300000 },
      databricks: { year1: 42000, year3: 115000, year5: 180000 },
      migrationCost: base,
      savingsYear1: 18000 - base,
      savingsYear3: 65000,
      savingsYear5: 120000,
    },
    fteImpact: {
      roles: [
        { role: 'Data Engineer', hours: processorCount * 8, fteMonths: +(processorCount * 8 / 160).toFixed(1), tasks: ['Processor migration', 'Unit testing'] },
        { role: 'Senior DE', hours: processorCount * 4, fteMonths: +(processorCount * 4 / 160).toFixed(1), tasks: ['Architecture review', 'Code review'] },
        { role: 'Architect', hours: processorCount * 2, fteMonths: +(processorCount * 2 / 160).toFixed(1), tasks: ['Design patterns', 'Platform decisions'] },
        { role: 'QA Engineer', hours: processorCount * 3, fteMonths: +(processorCount * 3 / 160).toFixed(1), tasks: ['Integration testing', 'Validation'] },
      ],
      totalHours: processorCount * 17,
      totalFteMonths: +(processorCount * 17 / 160).toFixed(1),
      phases: [
        { phase: 'Discovery', roles: ['Architect', 'Senior DE'], weeks: 2 },
        { phase: 'Migration', roles: ['Data Engineer', 'Senior DE'], weeks: Math.ceil(processorCount * 0.4) },
        { phase: 'Testing', roles: ['QA Engineer', 'Data Engineer'], weeks: Math.ceil(processorCount * 0.2) },
        { phase: 'Cutover', roles: ['Architect', 'Senior DE', 'QA Engineer'], weeks: 1 },
      ],
    },
    licenseSavings: {
      currentLicense: 60000,
      databricksLicense: 42000,
      annualSavings: 18000,
      platforms: [
        { platform: 'NiFi Cluster', cost: 36000, notes: 'Hosting + maintenance' },
        { platform: 'Supporting Infrastructure', cost: 24000, notes: 'ZooKeeper, monitoring, storage' },
      ],
    },
    monteCarlo: {
      p10: base * 0.7,
      p25: base * 0.85,
      p50: base,
      p75: base * 1.2,
      p90: base * 1.5,
      mean: base * 1.05,
      std: base * 0.25,
      histogram: Array.from({ length: 20 }, (_, i) => {
        const binStart = base * 0.5 + (base * 1.2 / 20) * i;
        const binEnd = binStart + base * 1.2 / 20;
        const center = (i - 10) / 5;
        const count = Math.round(100 * Math.exp(-center * center / 2));
        return { binStart: Math.round(binStart), binEnd: Math.round(binEnd), count };
      }),
    },
    benchmarks: {
      flowSize: processorCount > 50 ? 'Large' : processorCount > 15 ? 'Medium' : 'Small',
      processorCount,
      percentile: Math.min(95, Math.round(processorCount * 2.5)),
      avgMigrationWeeks: processorCount > 50 ? 16 : processorCount > 15 ? 8 : 4,
      estimatedWeeks: Math.ceil(processorCount * 0.5),
      comparisonNotes: `This flow is in the ${processorCount > 50 ? 'top quartile' : processorCount > 15 ? 'median range' : 'lower quartile'} for complexity.`,
    },
    complexity: {
      overall: Math.min(100, processorCount * 3),
      distribution: {
        low: Math.round(processorCount * 0.4),
        medium: Math.round(processorCount * 0.3),
        high: Math.round(processorCount * 0.2),
        critical: Math.round(processorCount * 0.1),
      },
      processors: [],
    },
  };
}

// ── Subcomponents ──

function RiskBadge({ risk }: { risk: number }) {
  const label = risk < 0.2 ? 'Low' : risk < 0.4 ? 'Medium' : 'High';
  const color = risk < 0.2 ? 'text-green-400 bg-green-500/10' : risk < 0.4 ? 'text-amber-400 bg-amber-500/10' : 'text-red-400 bg-red-500/10';
  return <span className={`px-2 py-0.5 rounded text-xs font-medium ${color}`}>{label} ({(risk * 100).toFixed(0)}%)</span>;
}

function ScenarioCard({ title, scenario, recommended }: { title: string; scenario: { cost: number; weeks: number; risk: number; description: string }; recommended: boolean }) {
  return (
    <div className={`rounded-lg border p-5 ${recommended ? 'border-green-500/50 bg-green-500/5' : 'border-border bg-gray-800/30'}`}>
      <div className="flex items-center justify-between mb-3">
        <h4 className="text-sm font-semibold text-gray-200">{title}</h4>
        {recommended && <span className="px-2 py-0.5 rounded text-xs font-medium bg-green-500/20 text-green-400">Recommended</span>}
      </div>
      <div className="grid grid-cols-3 gap-3 mb-3">
        <div>
          <p className="text-xs text-gray-500">Cost</p>
          <p className="text-lg font-bold text-gray-100 tabular-nums">${scenario.cost.toLocaleString()}</p>
        </div>
        <div>
          <p className="text-xs text-gray-500">Duration</p>
          <p className="text-lg font-bold text-gray-100 tabular-nums">{scenario.weeks}w</p>
        </div>
        <div>
          <p className="text-xs text-gray-500">Risk</p>
          <RiskBadge risk={scenario.risk} />
        </div>
      </div>
      <p className="text-xs text-gray-400 leading-relaxed">{scenario.description}</p>
    </div>
  );
}

function TCOSection({ tco }: { tco: FullROIReport['tco'] }) {
  const maxVal = Math.max(tco.currentPlatform.year5, tco.databricks.year5);
  const bar = (val: number, color: string) => (
    <div className={`h-5 rounded ${color} flex items-center px-2`} style={{ width: `${Math.max(5, (val / maxVal) * 100)}%` }}>
      <span className="text-xs text-white font-medium tabular-nums whitespace-nowrap">${(val / 1000).toFixed(0)}K</span>
    </div>
  );

  const rows: [string, number, number, number][] = [
    ['Year 1', tco.currentPlatform.year1, tco.databricks.year1, tco.savingsYear1],
    ['Year 3', tco.currentPlatform.year3, tco.databricks.year3, tco.savingsYear3],
    ['Year 5', tco.currentPlatform.year5, tco.databricks.year5, tco.savingsYear5],
  ];

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-4 text-xs text-gray-500">
        <span className="flex items-center gap-1"><span className="w-3 h-3 rounded bg-red-500/70" /> Current Platform</span>
        <span className="flex items-center gap-1"><span className="w-3 h-3 rounded bg-blue-500/70" /> Databricks</span>
        <span className="flex items-center gap-1"><span className="w-3 h-3 rounded bg-green-500/70" /> Savings</span>
      </div>
      {rows.map(([label, current, db, savings]) => (
        <div key={label}>
          <p className="text-xs text-gray-400 mb-1">{label}</p>
          <div className="space-y-1">
            {bar(current, 'bg-red-500/70')}
            {bar(db, 'bg-blue-500/70')}
            {savings > 0 && bar(savings, 'bg-green-500/70')}
          </div>
        </div>
      ))}
      <div className="rounded-lg bg-gray-800/50 border border-border p-3 text-center">
        <p className="text-xs text-gray-500">One-time Migration Cost</p>
        <p className="text-lg font-bold text-amber-400 tabular-nums">${tco.migrationCost.toLocaleString()}</p>
      </div>
    </div>
  );
}

function FTESection({ fteImpact }: { fteImpact: FullROIReport['fteImpact'] }) {
  return (
    <div className="space-y-4">
      <div className="rounded-lg border border-border overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-gray-900/60 border-b border-border">
              <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-500 uppercase">Role</th>
              <th className="text-right px-4 py-2.5 text-xs font-medium text-gray-500 uppercase">Hours</th>
              <th className="text-right px-4 py-2.5 text-xs font-medium text-gray-500 uppercase">FTE Months</th>
              <th className="text-left px-4 py-2.5 text-xs font-medium text-gray-500 uppercase">Key Tasks</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {fteImpact.roles.map((r) => (
              <tr key={r.role} className="hover:bg-gray-800/30 transition">
                <td className="px-4 py-2.5 text-gray-200 font-medium">{r.role}</td>
                <td className="px-4 py-2.5 text-right text-gray-300 tabular-nums">{r.hours}</td>
                <td className="px-4 py-2.5 text-right text-gray-300 tabular-nums">{r.fteMonths}</td>
                <td className="px-4 py-2.5 text-gray-400 text-xs">{r.tasks.join(', ')}</td>
              </tr>
            ))}
            <tr className="bg-gray-900/40 font-semibold">
              <td className="px-4 py-2.5 text-gray-200">Total</td>
              <td className="px-4 py-2.5 text-right text-gray-200 tabular-nums">{fteImpact.totalHours}</td>
              <td className="px-4 py-2.5 text-right text-gray-200 tabular-nums">{fteImpact.totalFteMonths}</td>
              <td className="px-4 py-2.5" />
            </tr>
          </tbody>
        </table>
      </div>
      {/* Phase timeline */}
      <div className="space-y-2">
        <p className="text-xs text-gray-500 font-medium uppercase tracking-wider">Phase Timeline</p>
        {fteImpact.phases.map((p) => (
          <div key={p.phase} className="flex items-center gap-3">
            <span className="text-xs text-gray-400 w-24 truncate">{p.phase}</span>
            <div className="flex-1 h-6 bg-gray-800 rounded overflow-hidden flex items-center">
              <div className="h-full bg-primary/30 rounded flex items-center px-2" style={{ width: `${Math.max(10, (p.weeks / 20) * 100)}%` }}>
                <span className="text-xs text-gray-300 whitespace-nowrap">{p.weeks}w - {p.roles.join(', ')}</span>
              </div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function MonteCarloSection({ mc }: { mc: FullROIReport['monteCarlo'] }) {
  const maxCount = Math.max(...mc.histogram.map((b) => b.count), 1);
  return (
    <div className="space-y-3">
      {/* Histogram */}
      <div className="flex items-end gap-px h-40">
        {mc.histogram.map((bin, i) => {
          const pct = (bin.count / maxCount) * 100;
          const hue = 120 - (i / mc.histogram.length) * 120; // green to red
          return (
            <div key={i} className="flex-1 flex flex-col items-center justify-end h-full group relative">
              <div
                className="w-full rounded-t"
                style={{ height: `${Math.max(2, pct)}%`, backgroundColor: `hsl(${hue}, 60%, 45%)` }}
              />
              <div className="absolute bottom-full mb-1 hidden group-hover:block bg-gray-900 border border-border rounded px-2 py-1 text-xs text-gray-300 whitespace-nowrap z-10">
                ${(bin.binStart / 1000).toFixed(0)}K - ${(bin.binEnd / 1000).toFixed(0)}K: {bin.count} runs
              </div>
            </div>
          );
        })}
      </div>
      {/* Percentile markers */}
      <div className="grid grid-cols-5 gap-2 text-center">
        {(['p10', 'p25', 'p50', 'p75', 'p90'] as const).map((key) => (
          <div key={key} className="rounded bg-gray-800/50 border border-border p-2">
            <p className="text-xs text-gray-500 uppercase">{key}</p>
            <p className="text-sm font-bold text-gray-200 tabular-nums">${(mc[key] / 1000).toFixed(0)}K</p>
          </div>
        ))}
      </div>
      <div className="flex gap-4 text-xs text-gray-500">
        <span>Mean: ${(mc.mean / 1000).toFixed(0)}K</span>
        <span>Std Dev: ${(mc.std / 1000).toFixed(0)}K</span>
      </div>
    </div>
  );
}

function BenchmarkSection({ b }: { b: FullROIReport['benchmarks'] }) {
  return (
    <div className="space-y-3">
      <div className="grid grid-cols-3 gap-3">
        <div className="rounded-lg bg-gray-800/50 border border-border p-3 text-center">
          <p className="text-xs text-gray-500">Flow Size</p>
          <p className="text-sm font-bold text-gray-200">{b.flowSize}</p>
        </div>
        <div className="rounded-lg bg-gray-800/50 border border-border p-3 text-center">
          <p className="text-xs text-gray-500">Processors</p>
          <p className="text-sm font-bold text-gray-200 tabular-nums">{b.processorCount}</p>
        </div>
        <div className="rounded-lg bg-gray-800/50 border border-border p-3 text-center">
          <p className="text-xs text-gray-500">Percentile</p>
          <p className="text-sm font-bold text-gray-200 tabular-nums">{b.percentile}th</p>
        </div>
      </div>
      <div className="rounded-lg bg-gray-800/50 border border-border p-4">
        <p className="text-xs text-gray-500 mb-2">Migration Duration Comparison</p>
        <div className="flex items-center gap-3">
          <span className="text-xs text-gray-400 w-28">Industry Avg</span>
          <div className="flex-1 h-4 bg-gray-800 rounded overflow-hidden">
            <div className="h-full bg-gray-500/50 rounded" style={{ width: `${(b.avgMigrationWeeks / Math.max(b.avgMigrationWeeks, b.estimatedWeeks)) * 100}%` }} />
          </div>
          <span className="text-xs text-gray-400 tabular-nums w-12 text-right">{b.avgMigrationWeeks}w</span>
        </div>
        <div className="flex items-center gap-3 mt-2">
          <span className="text-xs text-gray-400 w-28">This Flow</span>
          <div className="flex-1 h-4 bg-gray-800 rounded overflow-hidden">
            <div className="h-full bg-primary/60 rounded" style={{ width: `${(b.estimatedWeeks / Math.max(b.avgMigrationWeeks, b.estimatedWeeks)) * 100}%` }} />
          </div>
          <span className="text-xs text-primary tabular-nums w-12 text-right">{b.estimatedWeeks}w</span>
        </div>
      </div>
      <p className="text-xs text-gray-400">{b.comparisonNotes}</p>
    </div>
  );
}

function LicenseSection({ ls }: { ls: FullROIReport['licenseSavings'] }) {
  return (
    <div className="space-y-3">
      <div className="grid grid-cols-3 gap-3">
        <div className="rounded-lg bg-red-500/5 border border-red-500/20 p-4 text-center">
          <p className="text-2xl font-bold text-red-400 tabular-nums">${(ls.currentLicense / 1000).toFixed(0)}K</p>
          <p className="text-xs text-gray-500 mt-1">Current License/yr</p>
        </div>
        <div className="rounded-lg bg-blue-500/5 border border-blue-500/20 p-4 text-center">
          <p className="text-2xl font-bold text-blue-400 tabular-nums">${(ls.databricksLicense / 1000).toFixed(0)}K</p>
          <p className="text-xs text-gray-500 mt-1">Databricks/yr</p>
        </div>
        <div className="rounded-lg bg-green-500/5 border border-green-500/20 p-4 text-center">
          <p className="text-2xl font-bold text-green-400 tabular-nums">${(ls.annualSavings / 1000).toFixed(0)}K</p>
          <p className="text-xs text-gray-500 mt-1">Annual Savings</p>
        </div>
      </div>
      {ls.platforms.length > 0 && (
        <div className="rounded-lg border border-border overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-900/60 border-b border-border">
                <th className="text-left px-4 py-2 text-xs font-medium text-gray-500 uppercase">Platform</th>
                <th className="text-right px-4 py-2 text-xs font-medium text-gray-500 uppercase">Cost/yr</th>
                <th className="text-left px-4 py-2 text-xs font-medium text-gray-500 uppercase">Notes</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {ls.platforms.map((p) => (
                <tr key={p.platform} className="hover:bg-gray-800/30 transition">
                  <td className="px-4 py-2 text-gray-200">{p.platform}</td>
                  <td className="px-4 py-2 text-right text-gray-300 tabular-nums">${p.cost.toLocaleString()}</td>
                  <td className="px-4 py-2 text-gray-400 text-xs">{p.notes}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ── Main ROI Dashboard Component ──

export default function ROIDashboard() {
  const parsed = usePipelineStore((s) => s.parsed);
  const assessment = usePipelineStore((s) => s.assessment);
  const roiReport = usePipelineStore((s) => s.roiReport);
  const setROIReport = usePipelineStore((s) => s.setROIReport);
  const valueAnalysis = usePipelineStore((s) => s.valueAnalysis);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [costFormOpen, setCostFormOpen] = useState(false);

  const processorCount = parsed?.processors?.length ?? 0;

  const fetchROI = useCallback(
    async (customParams?: Record<string, unknown>) => {
      setLoading(true);
      setError(null);
      try {
        const result = await generateROIReport({
          parsed,
          assessment,
          type: 'roi',
          ...customParams,
        });
        setROIReport(result);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        setError(msg);
        // Fallback to local estimation
        const fallback = estimateLocally(processorCount);
        setROIReport(fallback);
      } finally {
        setLoading(false);
      }
    },
    [parsed, assessment, processorCount, setROIReport],
  );

  useEffect(() => {
    if (parsed && !roiReport && !loading) {
      fetchROI();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [parsed]);

  const handleCustomParams = useCallback(
    (params: Record<string, unknown>) => {
      setCostFormOpen(false);
      fetchROI(params);
    },
    [fetchROI],
  );

  if (!parsed) {
    return (
      <div className="rounded-lg border border-border bg-gray-800/30 p-8 text-center text-gray-500 text-sm">
        Complete Step 1 first to generate ROI analysis.
      </div>
    );
  }

  if (loading) {
    return (
      <div className="flex items-center gap-3 p-4 rounded-lg bg-gray-800/50 border border-border">
        <div className="w-5 h-5 rounded-full border-2 border-orange-400 border-t-transparent animate-spin" />
        <span className="text-sm text-gray-300">Generating ROI analysis...</span>
      </div>
    );
  }

  if (!roiReport) return null;

  const isRefactorRecommended = roiReport.comparison.recommendation.toLowerCase().includes('refactor');
  const estimatedPrefix = error ? '(Estimated) ' : '';

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h3 className="text-lg font-semibold text-gray-100">ROI Dashboard</h3>
        <div className="flex gap-2">
          <button
            onClick={() => setCostFormOpen(!costFormOpen)}
            className="px-3 py-1.5 rounded-lg border border-border text-xs text-gray-400 hover:text-gray-200 hover:bg-gray-800 transition"
          >
            Customize Costs
          </button>
          <button
            onClick={() => fetchROI()}
            className="px-3 py-1.5 rounded-lg bg-primary text-white text-xs font-medium hover:bg-primary/80 transition"
          >
            Refresh
          </button>
        </div>
      </div>

      {error && (
        <div className="rounded-lg border border-red-500/40 bg-red-500/10 p-4 text-sm text-red-400 font-medium">
          <p className="mb-1">Estimated values only — backend unavailable.</p>
          <p className="text-xs text-red-400/70 font-normal">These numbers are rough approximations, not based on your actual flow data.</p>
        </div>
      )}

      {!valueAnalysis && (
        <div className="rounded-lg bg-amber-500/10 border border-amber-500/30 p-3 text-xs text-amber-400">
          Showing estimated sample data. Run the full pipeline for actual ROI analysis.
        </div>
      )}

      {costFormOpen && <CostInputForm onApply={handleCustomParams} onClose={() => setCostFormOpen(false)} />}

      {/* Section 1: Lift-and-Shift vs Refactor */}
      <div>
        <h4 className="text-sm font-medium text-gray-300 mb-3">{estimatedPrefix}Migration Strategy Comparison</h4>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <ScenarioCard title="Lift & Shift" scenario={roiReport.comparison.liftAndShift} recommended={!isRefactorRecommended} />
          <ScenarioCard title="Refactor" scenario={roiReport.comparison.refactor} recommended={isRefactorRecommended} />
        </div>
        <div className="mt-3 flex items-center gap-3">
          <p className="text-sm text-gray-400 flex-1">{roiReport.comparison.recommendation}</p>
          <span className="px-3 py-1 rounded-full bg-primary/10 text-primary text-xs font-medium whitespace-nowrap">
            Break-even: {roiReport.comparison.breakEvenMonths} months
          </span>
        </div>
      </div>

      {/* Section 2: TCO */}
      <div>
        <h4 className="text-sm font-medium text-gray-300 mb-3">{estimatedPrefix}Total Cost of Ownership</h4>
        <TCOSection tco={roiReport.tco} />
      </div>

      {/* Section 3: FTE Impact */}
      <div>
        <h4 className="text-sm font-medium text-gray-300 mb-3">{estimatedPrefix}FTE Impact</h4>
        <FTESection fteImpact={roiReport.fteImpact} />
      </div>

      {/* Section 4: Monte Carlo */}
      <div>
        <h4 className="text-sm font-medium text-gray-300 mb-3">{estimatedPrefix}Monte Carlo Cost Distribution</h4>
        <MonteCarloSection mc={roiReport.monteCarlo} />
      </div>

      {/* Section 5: Benchmark */}
      <div>
        <h4 className="text-sm font-medium text-gray-300 mb-3">{estimatedPrefix}Benchmark Comparison</h4>
        <BenchmarkSection b={roiReport.benchmarks} />
      </div>

      {/* Section 6: License Savings */}
      <div>
        <h4 className="text-sm font-medium text-gray-300 mb-3">{estimatedPrefix}License Savings</h4>
        <LicenseSection ls={roiReport.licenseSavings} />
      </div>
    </div>
  );
}
