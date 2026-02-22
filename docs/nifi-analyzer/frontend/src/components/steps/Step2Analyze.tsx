import React, { useState } from 'react';
import { usePipelineStore } from '../../store/pipeline';
import { useUIStore } from '../../store/ui';
import { usePipeline } from '../../hooks/usePipeline';
import TierDiagram from '../shared/TierDiagram';
import TierDiagramSearch from '../viz/TierDiagramSearch';
import ConfidenceChart from '../viz/ConfidenceChart';
import SecurityTreemap from '../viz/SecurityTreemap';
import LineageGraph from '../viz/LineageGraph';
import MermaidRenderer from '../viz/MermaidRenderer';
import type {
  DeepAnalysisResult,
  FunctionalReport,
  ProcessorReport,
  WorkflowReport,
  UpstreamReport,
  DownstreamReport,
  LineByLineReport,
  LintReport,
  LintFinding,
  LineageGraph as LineageGraphData,
} from '../../types/pipeline';

type PassName = 'functional' | 'processors' | 'workflow' | 'upstream' | 'downstream' | 'line_by_line';

const PASS_LABELS: Record<PassName, { title: string; icon: string; color: string }> = {
  functional:   { title: 'Functional Analysis',  icon: 'F', color: 'blue' },
  processors:   { title: 'Processor Analysis',   icon: 'P', color: 'purple' },
  workflow:     { title: 'Workflow Analysis',     icon: 'W', color: 'cyan' },
  upstream:     { title: 'Upstream Analysis',     icon: 'U', color: 'green' },
  downstream:   { title: 'Downstream Analysis',   icon: 'D', color: 'orange' },
  line_by_line: { title: 'Line-by-Line Analysis', icon: 'L', color: 'red' },
};

export default function Step2Analyze() {
  const parsed = usePipelineStore((s) => s.parsed);
  const analysis = usePipelineStore((s) => s.analysis);
  const deepAnalysis = usePipelineStore((s) => s.deepAnalysis);
  const lineage = usePipelineStore((s) => s.lineage);
  const status = useUIStore((s) => s.stepStatuses[1]);
  const { runAnalyze } = usePipeline();
  const [expandedPass, setExpandedPass] = useState<PassName | null>(null);
  const [lineageView, setLineageView] = useState<'graph' | 'mermaid'>('graph');

  const hasCycles = (analysis?.cycles?.length ?? 0) > 0;
  const depNodeCount = Object.keys(analysis?.dependencyGraph ?? {}).length;
  const deep = deepAnalysis ?? analysis?.deepAnalysis ?? null;

  const togglePass = (pass: PassName) => {
    setExpandedPass((prev) => (prev === pass ? null : pass));
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold text-gray-100 flex items-center gap-3">
            <span className="w-8 h-8 rounded-lg bg-purple-500/20 flex items-center justify-center text-sm text-purple-400 font-mono">2</span>
            Analyze Flow
          </h2>
          <p className="mt-1 text-sm text-gray-400">
            6-pass deep analysis: functional, processor, workflow, upstream, downstream, and line-by-line.
          </p>
        </div>
        <button
          onClick={() => runAnalyze()}
          disabled={!parsed || status === 'running'}
          className="px-4 py-2 rounded-lg bg-primary text-white text-sm font-medium
            hover:bg-primary/80 disabled:opacity-40 disabled:cursor-not-allowed transition"
        >
          {status === 'running' ? 'Analyzing...' : 'Run Deep Analysis'}
        </button>
      </div>

      {/* No parsed data */}
      {!parsed && (
        <div className="rounded-lg border border-border bg-gray-800/30 p-8 text-center text-gray-500 text-sm">
          Parse a flow file first (Step 1)
        </div>
      )}

      {/* Loading */}
      {status === 'running' && (
        <div className="flex items-center gap-3 p-4 rounded-lg bg-gray-800/50 border border-border">
          <div className="w-5 h-5 rounded-full border-2 border-purple-400 border-t-transparent animate-spin" />
          <span className="text-sm text-gray-300">Running 6-pass deep analysis...</span>
        </div>
      )}

      {/* Results */}
      {analysis && status === 'done' && (
        <div className="space-y-6">
          {/* Stats grid */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <MetricCard label="Graph Nodes" value={depNodeCount} color="text-blue-400" />
            <MetricCard label="External Systems" value={analysis.externalSystems.length} color="text-purple-400" />
            <MetricCard label="Stages" value={analysis.stages.length} color="text-cyan-400" />
            <MetricCard label="Security Findings" value={analysis.securityFindings.length} color="text-amber-400" />
          </div>

          {/* Deep analysis duration badge */}
          {deep && (
            <div className="flex items-center gap-3">
              <span className="px-3 py-1 rounded-full bg-purple-500/20 text-purple-400 text-xs font-medium">
                6-pass deep analysis completed in {deep.duration_ms.toFixed(0)}ms
              </span>
            </div>
          )}

          {/* 6-Pass Deep Analysis Panels */}
          {deep && (
            <div className="space-y-2">
              <h3 className="text-sm font-medium text-gray-300">Deep Analysis Results</h3>
              {(Object.keys(PASS_LABELS) as PassName[]).map((pass) => (
                <DeepAnalysisPanel
                  key={pass}
                  pass={pass}
                  deep={deep}
                  expanded={expandedPass === pass}
                  onToggle={() => togglePass(pass)}
                />
              ))}
            </div>
          )}

          {/* Flow Lint Panel */}
          {deep?.lint && deep.lint.findings.length > 0 && (
            <LintPanel lint={deep.lint} />
          )}

          {/* Data Lineage Visualization */}
          {lineage && lineage.nodes && lineage.nodes.length > 0 && (
            <div>
              <div className="flex items-center justify-between mb-3">
                <h3 className="text-sm font-medium text-gray-300">Data Lineage</h3>
                <div className="flex gap-1">
                  <button
                    onClick={() => setLineageView('graph')}
                    className={`px-3 py-1 rounded text-xs transition ${lineageView === 'graph' ? 'bg-blue-500/20 text-blue-400' : 'bg-gray-800 text-gray-400 hover:bg-gray-700'}`}
                  >Interactive Graph</button>
                  <button
                    onClick={() => setLineageView('mermaid')}
                    className={`px-3 py-1 rounded text-xs transition ${lineageView === 'mermaid' ? 'bg-blue-500/20 text-blue-400' : 'bg-gray-800 text-gray-400 hover:bg-gray-700'}`}
                  >Mermaid Diagram</button>
                </div>
              </div>
              <div className="grid grid-cols-4 gap-3 mb-3">
                <div className="rounded-lg bg-gray-800/50 border border-border p-3 text-center">
                  <p className="text-lg font-bold text-green-400 tabular-nums">{lineage.nodes.filter((n) => n.type === 'source').length}</p>
                  <p className="text-xs text-gray-500 mt-0.5">Sources</p>
                </div>
                <div className="rounded-lg bg-gray-800/50 border border-border p-3 text-center">
                  <p className="text-lg font-bold text-blue-400 tabular-nums">{lineage.nodes.filter((n) => n.type === 'transform').length}</p>
                  <p className="text-xs text-gray-500 mt-0.5">Transforms</p>
                </div>
                <div className="rounded-lg bg-gray-800/50 border border-border p-3 text-center">
                  <p className="text-lg font-bold text-orange-400 tabular-nums">{lineage.nodes.filter((n) => n.type === 'sink').length}</p>
                  <p className="text-xs text-gray-500 mt-0.5">Sinks</p>
                </div>
                <div className="rounded-lg bg-gray-800/50 border border-border p-3 text-center">
                  <p className="text-lg font-bold text-red-400 tabular-nums">{lineage.criticalPath.length}</p>
                  <p className="text-xs text-gray-500 mt-0.5">Critical Path</p>
                </div>
              </div>
              {lineageView === 'graph' ? (
                <LineageGraph data={lineage} />
              ) : (
                lineage.mermaidMarkdown ? (
                  <MermaidRenderer markdown={lineage.mermaidMarkdown} />
                ) : (
                  <div className="rounded-lg border border-border bg-gray-800/30 p-8 text-center text-gray-500 text-sm">
                    No Mermaid diagram available
                  </div>
                )
              )}
            </div>
          )}

          {/* Flow diagram with search */}
          {parsed && parsed.processors.length > 0 && (
            <div>
              <TierDiagramSearch processors={parsed.processors} />
              <TierDiagram processors={parsed.processors} connections={parsed.connections} />
            </div>
          )}

          {/* Confidence & Security visualizations */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <ConfidenceChart />
            <SecurityTreemap />
          </div>

          {/* Cycles */}
          <div className={`rounded-lg border p-3 ${hasCycles ? 'border-red-500/30 bg-red-500/5' : 'border-green-500/30 bg-green-500/5'}`}>
            <span className={`text-sm font-medium ${hasCycles ? 'text-red-400' : 'text-green-400'}`}>
              {hasCycles ? `${analysis.cycles.length} cycle(s) detected in flow graph` : 'No cycles detected -- DAG structure confirmed'}
            </span>
          </div>

          {/* External systems */}
          {analysis.externalSystems && analysis.externalSystems.length > 0 && (
            <div>
              <h3 className="text-sm font-medium text-gray-300 mb-3">External Systems</h3>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-2">
                {analysis.externalSystems.map((sys, i) => {
                  const name = String((sys as Record<string, unknown>).name ?? `System ${i + 1}`);
                  const type = String((sys as Record<string, unknown>).type ?? 'unknown');
                  return (
                    <div key={i} className="rounded-lg border border-border bg-gray-800/30 p-3 flex items-center gap-3">
                      <div className="w-8 h-8 rounded bg-amber-500/20 flex items-center justify-center text-amber-400 text-xs font-bold">
                        {type.slice(0, 2).toUpperCase()}
                      </div>
                      <div>
                        <p className="text-sm text-gray-200">{name}</p>
                        <p className="text-xs text-gray-500">{type}</p>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {/* Flow metrics */}
          {analysis.flowMetrics && Object.keys(analysis.flowMetrics).length > 0 && (
            <div>
              <h3 className="text-sm font-medium text-gray-300 mb-3">Flow Metrics</h3>
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
                {Object.entries(analysis.flowMetrics).map(([key, value]) => (
                  <div key={key} className="rounded-lg bg-gray-800/50 border border-border p-3 text-center">
                    <p className="text-lg font-bold text-gray-200 tabular-nums">{String(value)}</p>
                    <p className="text-xs text-gray-500 capitalize mt-0.5">{key.replace(/_/g, ' ')}</p>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Security findings (detailed list) */}
          {analysis.securityFindings && analysis.securityFindings.length > 0 && (
            <div>
              <h3 className="text-sm font-medium text-gray-300 mb-3">Security Findings</h3>
              <div className="space-y-3">
                {analysis.securityFindings.map((f, i) => {
                  const finding = f as Record<string, unknown>;
                  const severity = String(finding.severity ?? 'MEDIUM').toUpperCase();
                  const processor = String(finding.processor ?? 'Unknown');
                  const processorType = String(finding.type ?? '');
                  const description = String(finding.finding ?? finding.description ?? '');
                  const snippet = finding.snippet ? String(finding.snippet) : null;

                  const severityStyles: Record<string, { border: string; bg: string; badge: string; text: string }> = {
                    CRITICAL: { border: 'border-red-500/40', bg: 'bg-red-500/5', badge: 'bg-red-500/20 text-red-400', text: 'text-red-300/80' },
                    HIGH:     { border: 'border-orange-500/40', bg: 'bg-orange-500/5', badge: 'bg-orange-500/20 text-orange-400', text: 'text-orange-300/80' },
                    MEDIUM:   { border: 'border-amber-500/40', bg: 'bg-amber-500/5', badge: 'bg-amber-500/20 text-amber-400', text: 'text-amber-300/80' },
                  };
                  const style = severityStyles[severity] ?? severityStyles.MEDIUM;

                  return (
                    <div key={i} className={`rounded-lg border ${style.border} ${style.bg} p-4`}>
                      <div className="flex items-start gap-3">
                        <span className={`shrink-0 px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wide ${style.badge}`}>
                          {severity}
                        </span>
                        <div className="min-w-0 flex-1">
                          <div className="flex items-center gap-2 mb-1">
                            <span className="text-sm font-medium text-gray-200">{processor}</span>
                            {processorType && (
                              <span className="text-[10px] text-gray-500 font-mono">{processorType}</span>
                            )}
                          </div>
                          {description && (
                            <p className={`text-xs ${style.text} mb-2`}>{description}</p>
                          )}
                          {snippet && (
                            <pre className="text-[11px] text-gray-400 bg-gray-900/60 rounded px-3 py-2 overflow-x-auto font-mono leading-relaxed border border-gray-700/30">
                              {snippet}
                            </pre>
                          )}
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ── Metric Card ──

function MetricCard({ label, value, color }: { label: string; value: number; color: string }) {
  return (
    <div className="rounded-lg bg-gray-800/50 border border-border p-4 text-center">
      <p className={`text-2xl font-bold tabular-nums ${color}`}>{value}</p>
      <p className="text-xs text-gray-500 mt-1">{label}</p>
    </div>
  );
}

// ── Deep Analysis Panel (Collapsible) ──

function DeepAnalysisPanel({
  pass,
  deep,
  expanded,
  onToggle,
}: {
  pass: PassName;
  deep: DeepAnalysisResult;
  expanded: boolean;
  onToggle: () => void;
}) {
  const { title, icon, color } = PASS_LABELS[pass];
  const colorClasses: Record<string, string> = {
    blue: 'bg-blue-500/20 text-blue-400 border-blue-500/30',
    purple: 'bg-purple-500/20 text-purple-400 border-purple-500/30',
    cyan: 'bg-cyan-500/20 text-cyan-400 border-cyan-500/30',
    green: 'bg-green-500/20 text-green-400 border-green-500/30',
    orange: 'bg-orange-500/20 text-orange-400 border-orange-500/30',
    red: 'bg-red-500/20 text-red-400 border-red-500/30',
  };
  const cls = colorClasses[color] ?? colorClasses.blue;
  const [bgClass, textClass, borderClass] = cls.split(' ');

  return (
    <div className={`rounded-lg border ${borderClass} overflow-hidden`}>
      <button
        onClick={onToggle}
        className={`w-full flex items-center gap-3 p-3 ${bgClass} hover:opacity-80 transition text-left`}
      >
        <span className={`w-6 h-6 rounded flex items-center justify-center text-xs font-bold ${textClass}`}>
          {icon}
        </span>
        <span className={`text-sm font-medium ${textClass} flex-1`}>{title}</span>
        <svg className={`w-4 h-4 ${textClass} transition-transform ${expanded ? 'rotate-180' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>
      {expanded && (
        <div className="p-4 bg-gray-900/50 border-t border-gray-700/30">
          {pass === 'functional' && <FunctionalPassView report={deep.functional} />}
          {pass === 'processors' && <ProcessorPassView report={deep.processors} />}
          {pass === 'workflow' && <WorkflowPassView report={deep.workflow} />}
          {pass === 'upstream' && <UpstreamPassView report={deep.upstream} />}
          {pass === 'downstream' && <DownstreamPassView report={deep.downstream} />}
          {pass === 'line_by_line' && <LineByLinePassView report={deep.line_by_line} />}
        </div>
      )}
    </div>
  );
}

// ── Pass Views ──

function FunctionalPassView({ report }: { report: FunctionalReport }) {
  return (
    <div className="space-y-3 text-sm">
      <div>
        <span className="text-gray-500">Flow Purpose:</span>
        <p className="text-gray-200 mt-1">{report.flow_purpose}</p>
      </div>
      <div className="grid grid-cols-3 gap-3">
        <InfoCard label="Pattern" value={report.pipeline_pattern} />
        <InfoCard label="Complexity" value={report.estimated_complexity} />
        <InfoCard label="SLA Profile" value={report.sla_profile} />
      </div>
      {report.data_domains.length > 0 && (
        <div>
          <span className="text-gray-500 text-xs">Data Domains</span>
          <div className="flex flex-wrap gap-1 mt-1">
            {report.data_domains.map((d, i) => (
              <span key={i} className="px-2 py-0.5 bg-blue-500/10 text-blue-400 rounded text-xs">{d}</span>
            ))}
          </div>
        </div>
      )}
      {report.functional_zones.length > 0 && (
        <div>
          <span className="text-gray-500 text-xs">Functional Zones ({report.functional_zones.length})</span>
          <div className="space-y-2 mt-1">
            {report.functional_zones.map((z, i) => (
              <div key={i} className="rounded bg-gray-800/50 p-2">
                <p className="text-gray-200 text-xs font-medium">{z.zone_name}</p>
                <p className="text-gray-500 text-xs">{z.purpose}</p>
                <p className="text-gray-600 text-[10px] mt-1">{z.processor_names.join(', ')}</p>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function ProcessorPassView({ report }: { report: ProcessorReport }) {
  return (
    <div className="space-y-3 text-sm">
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <MetricCard label="Total Processors" value={report.total_processors} color="text-purple-400" />
        <MetricCard label="Categories" value={Object.keys(report.by_category).length} color="text-blue-400" />
        <MetricCard label="Unknown" value={report.unknown_processors.length} color="text-red-400" />
        <MetricCard label="Roles" value={Object.keys(report.by_role).length} color="text-cyan-400" />
      </div>
      {Object.keys(report.by_category).length > 0 && (
        <div>
          <span className="text-gray-500 text-xs">By Category</span>
          <div className="grid grid-cols-2 gap-2 mt-1">
            {Object.entries(report.by_category).sort(([, a], [, b]) => b - a).map(([cat, count]) => (
              <div key={cat} className="flex items-center justify-between bg-gray-800/50 rounded px-3 py-1.5">
                <span className="text-gray-300 text-xs capitalize">{cat.replace(/_/g, ' ')}</span>
                <span className="text-purple-400 text-xs font-mono">{count}</span>
              </div>
            ))}
          </div>
        </div>
      )}
      {Object.keys(report.by_conversion_complexity).length > 0 && (
        <div>
          <span className="text-gray-500 text-xs">Conversion Complexity</span>
          <div className="flex gap-2 mt-1">
            {Object.entries(report.by_conversion_complexity).map(([level, count]) => {
              const levelColors: Record<string, string> = { low: 'text-green-400', medium: 'text-amber-400', high: 'text-red-400' };
              return (
                <span key={level} className={`px-2 py-0.5 bg-gray-800/50 rounded text-xs ${levelColors[level] ?? 'text-gray-400'}`}>
                  {level}: {count}
                </span>
              );
            })}
          </div>
        </div>
      )}
      {report.unknown_processors.length > 0 && (
        <div>
          <span className="text-gray-500 text-xs">Unknown Processors</span>
          <div className="mt-1 rounded bg-red-500/5 border border-red-500/20 p-2">
            <p className="text-red-400 text-xs">{report.unknown_processors.join(', ')}</p>
          </div>
        </div>
      )}
    </div>
  );
}

function WorkflowPassView({ report }: { report: WorkflowReport }) {
  return (
    <div className="space-y-3 text-sm">
      <div className="grid grid-cols-3 gap-3">
        <InfoCard label="Execution Phases" value={String(report.total_execution_phases)} />
        <InfoCard label="Critical Path" value={String(report.critical_path_length)} />
        <InfoCard label="Parallelism" value={report.parallelism_factor.toFixed(2)} />
      </div>
      {report.topological_order.length > 0 && (
        <div>
          <span className="text-gray-500 text-xs">Topological Execution Order</span>
          <div className="flex flex-wrap gap-1 mt-1">
            {report.topological_order.map((name, i) => (
              <span key={i} className="flex items-center gap-1">
                <span className="px-2 py-0.5 bg-cyan-500/10 text-cyan-400 rounded text-[10px] font-mono">{name}</span>
                {i < report.topological_order.length - 1 && <span className="text-gray-600 text-xs">&rarr;</span>}
              </span>
            ))}
          </div>
        </div>
      )}
      {report.execution_phases.length > 0 && (
        <div>
          <span className="text-gray-500 text-xs">Execution Phases</span>
          <div className="space-y-1 mt-1">
            {report.execution_phases.map((phase, i) => (
              <div key={i} className="flex items-start gap-2 bg-gray-800/50 rounded px-3 py-1.5">
                <span className="text-cyan-400 text-xs font-mono shrink-0">Phase {phase.phase_number}</span>
                <span className="text-gray-400 text-xs">{phase.description || phase.processor_names.join(', ')}</span>
              </div>
            ))}
          </div>
        </div>
      )}
      {report.synchronization_points.length > 0 && (
        <div>
          <span className="text-gray-500 text-xs">Sync Points</span>
          <div className="flex flex-wrap gap-1 mt-1">
            {report.synchronization_points.map((s, i) => (
              <span key={i} className="px-2 py-0.5 bg-amber-500/10 text-amber-400 rounded text-xs">{s}</span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function UpstreamPassView({ report }: { report: UpstreamReport }) {
  return (
    <div className="space-y-3 text-sm">
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <MetricCard label="Data Sources" value={report.data_sources.length} color="text-green-400" />
        <MetricCard label="Transformations" value={report.content_transformations.length} color="text-blue-400" />
        <MetricCard label="Dependencies" value={report.external_dependencies.length} color="text-amber-400" />
        <MetricCard label="Parameters" value={report.parameter_injections.length} color="text-purple-400" />
      </div>
      {report.data_sources.length > 0 && (
        <div>
          <span className="text-gray-500 text-xs">Data Sources</span>
          <div className="space-y-1 mt-1">
            {report.data_sources.map((src, i) => (
              <div key={i} className="flex items-center gap-3 bg-gray-800/50 rounded px-3 py-2">
                <span className="px-2 py-0.5 bg-green-500/20 text-green-400 rounded text-[10px] font-bold uppercase">{src.source_type}</span>
                <div>
                  <p className="text-gray-200 text-xs">{src.processor_name}</p>
                  <p className="text-gray-500 text-[10px]">{src.protocol}</p>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
      {report.external_dependencies.length > 0 && (
        <div>
          <span className="text-gray-500 text-xs">External Dependencies</span>
          <div className="flex flex-wrap gap-1 mt-1">
            {report.external_dependencies.map((dep, i) => (
              <span key={i} className="px-2 py-0.5 bg-amber-500/10 text-amber-400 rounded text-xs">{dep}</span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function DownstreamPassView({ report }: { report: DownstreamReport }) {
  return (
    <div className="space-y-3 text-sm">
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <MetricCard label="Data Sinks" value={report.data_sinks.length} color="text-orange-400" />
        <MetricCard label="Error Routes" value={report.error_routing.length} color="text-red-400" />
        <MetricCard label="Multiplication" value={report.data_multiplication_points.length} color="text-amber-400" />
        <MetricCard label="Reduction" value={report.data_reduction_points.length} color="text-blue-400" />
      </div>
      {report.data_sinks.length > 0 && (
        <div>
          <span className="text-gray-500 text-xs">Data Sinks</span>
          <div className="space-y-1 mt-1">
            {report.data_sinks.map((sink, i) => (
              <div key={i} className="flex items-center gap-3 bg-gray-800/50 rounded px-3 py-2">
                <span className="px-2 py-0.5 bg-orange-500/20 text-orange-400 rounded text-[10px] font-bold uppercase">{sink.sink_type}</span>
                <div>
                  <p className="text-gray-200 text-xs">{sink.processor_name}</p>
                  <p className="text-gray-500 text-[10px]">{sink.protocol}</p>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
      {report.error_routing.length > 0 && (
        <div>
          <span className="text-gray-500 text-xs">Error Routing</span>
          <div className="space-y-1 mt-1">
            {report.error_routing.map((route, i) => (
              <div key={i} className="rounded bg-red-500/5 border border-red-500/20 px-3 py-1.5">
                <p className="text-red-400 text-xs">{JSON.stringify(route)}</p>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function LineByLinePassView({ report }: { report: LineByLineReport }) {
  const confidencePct = (report.overall_conversion_confidence * 100).toFixed(0);
  const confidenceColor = report.overall_conversion_confidence >= 0.9 ? 'text-green-400' : report.overall_conversion_confidence >= 0.7 ? 'text-amber-400' : 'text-red-400';

  return (
    <div className="space-y-3 text-sm">
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <MetricCard label="Properties Analyzed" value={report.total_properties_analyzed} color="text-red-400" />
        <MetricCard label="NEL Expressions" value={report.nel_expression_count} color="text-purple-400" />
        <MetricCard label="SQL Statements" value={report.sql_statement_count} color="text-blue-400" />
        <MetricCard label="Scripts" value={report.script_code_count} color="text-amber-400" />
      </div>
      <div className="flex items-center gap-4 bg-gray-800/50 rounded-lg p-4">
        <div className="text-center">
          <p className={`text-3xl font-bold tabular-nums ${confidenceColor}`}>{confidencePct}%</p>
          <p className="text-xs text-gray-500 mt-1">Conversion Confidence</p>
        </div>
        <div className="flex-1 h-3 bg-gray-800 rounded-full overflow-hidden">
          <div
            className={`h-full rounded-full transition-all ${report.overall_conversion_confidence >= 0.9 ? 'bg-green-500' : report.overall_conversion_confidence >= 0.7 ? 'bg-amber-500' : 'bg-red-500'}`}
            style={{ width: `${confidencePct}%` }}
          />
        </div>
      </div>
      {report.regex_count > 0 && (
        <div className="text-gray-500 text-xs">
          Also found: {report.regex_count} regex pattern(s)
        </div>
      )}
      {report.properties && report.properties.length > 0 && (
        <div>
          <span className="text-gray-500 text-xs">Property Analysis (showing first 20)</span>
          <div className="mt-1 max-h-80 overflow-y-auto space-y-1">
            {report.properties.slice(0, 20).map((prop, i) => (
              <div key={i} className="rounded bg-gray-800/50 p-2">
                <div className="flex items-center justify-between">
                  <span className="text-gray-200 text-xs font-mono">{prop.processor_name}.{prop.property_key}</span>
                  <span className={`px-2 py-0.5 rounded text-[10px] ${prop.needs_conversion ? 'bg-amber-500/20 text-amber-400' : 'bg-green-500/20 text-green-400'}`}>
                    {prop.needs_conversion ? 'needs conversion' : 'pass-through'}
                  </span>
                </div>
                <p className="text-gray-500 text-[10px] mt-0.5">{prop.value_type} &mdash; {prop.what_it_does}</p>
                {prop.why_its_there && (
                  <p className="text-gray-600 text-[10px]">{prop.why_its_there}</p>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// ── Flow Lint Panel ──

function LintPanel({ lint }: { lint: LintReport }) {
  const [expanded, setExpanded] = useState(false);
  const [filterSeverity, setFilterSeverity] = useState<string | null>(null);

  const filtered = filterSeverity
    ? lint.findings.filter((f) => f.severity === filterSeverity)
    : lint.findings;

  const severityStyles: Record<string, { badge: string; border: string; bg: string }> = {
    critical: { badge: 'bg-red-500/20 text-red-400', border: 'border-red-500/30', bg: 'bg-red-500/5' },
    high: { badge: 'bg-orange-500/20 text-orange-400', border: 'border-orange-500/30', bg: 'bg-orange-500/5' },
    medium: { badge: 'bg-amber-500/20 text-amber-400', border: 'border-amber-500/30', bg: 'bg-amber-500/5' },
    low: { badge: 'bg-blue-500/20 text-blue-400', border: 'border-blue-500/30', bg: 'bg-blue-500/5' },
    info: { badge: 'bg-gray-500/20 text-gray-400', border: 'border-gray-500/30', bg: 'bg-gray-500/5' },
  };

  return (
    <div className="rounded-lg border border-amber-500/30 overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center gap-3 p-3 bg-amber-500/10 hover:bg-amber-500/15 transition text-left"
      >
        <span className="w-6 h-6 rounded flex items-center justify-center text-xs font-bold text-amber-400 bg-amber-500/20">!</span>
        <span className="text-sm font-medium text-amber-400 flex-1">
          Flow Lint — {lint.counts.total} finding(s)
        </span>
        <div className="flex gap-1">
          {lint.counts.critical > 0 && <span className="px-1.5 py-0.5 rounded text-[10px] bg-red-500/20 text-red-400">{lint.counts.critical} critical</span>}
          {lint.counts.high > 0 && <span className="px-1.5 py-0.5 rounded text-[10px] bg-orange-500/20 text-orange-400">{lint.counts.high} high</span>}
          {lint.counts.medium > 0 && <span className="px-1.5 py-0.5 rounded text-[10px] bg-amber-500/20 text-amber-400">{lint.counts.medium} med</span>}
        </div>
        <svg className={`w-4 h-4 text-amber-400 transition-transform ${expanded ? 'rotate-180' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>
      {expanded && (
        <div className="p-4 bg-gray-900/50 border-t border-gray-700/30 space-y-3">
          <p className="text-xs text-gray-500">{lint.summary}</p>
          {/* Severity filter pills */}
          <div className="flex gap-1">
            <button
              onClick={() => setFilterSeverity(null)}
              className={`px-2 py-0.5 rounded text-[10px] transition ${!filterSeverity ? 'bg-gray-600 text-white' : 'bg-gray-800 text-gray-400 hover:bg-gray-700'}`}
            >All ({lint.counts.total})</button>
            {(['critical', 'high', 'medium', 'low', 'info'] as const).map((sev) => {
              const count = lint.counts[sev];
              if (count === 0) return null;
              return (
                <button
                  key={sev}
                  onClick={() => setFilterSeverity(filterSeverity === sev ? null : sev)}
                  className={`px-2 py-0.5 rounded text-[10px] transition ${filterSeverity === sev ? severityStyles[sev].badge : 'bg-gray-800 text-gray-400 hover:bg-gray-700'}`}
                >{sev} ({count})</button>
              );
            })}
          </div>
          {/* Findings list */}
          <div className="max-h-96 overflow-y-auto space-y-2">
            {filtered.map((f, i) => {
              const style = severityStyles[f.severity] ?? severityStyles.info;
              return (
                <div key={i} className={`rounded-lg border ${style.border} ${style.bg} p-3`}>
                  <div className="flex items-start gap-2">
                    <span className={`shrink-0 px-1.5 py-0.5 rounded text-[10px] font-bold uppercase ${style.badge}`}>
                      {f.severity}
                    </span>
                    <div className="min-w-0 flex-1">
                      <div className="flex items-center gap-2">
                        <span className="text-xs font-mono text-gray-500">{f.ruleId}</span>
                        <span className="text-xs text-gray-500">{f.category}</span>
                      </div>
                      <p className="text-sm text-gray-200 mt-0.5">{f.message}</p>
                      {f.processor && (
                        <p className="text-[10px] text-gray-500 mt-0.5 font-mono">Processor: {f.processor}</p>
                      )}
                      {f.suggestion && (
                        <p className="text-[10px] text-green-400/70 mt-1 italic">Fix: {f.suggestion}</p>
                      )}
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}

// ── Info Card ──

function InfoCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded bg-gray-800/50 p-2 text-center">
      <p className="text-gray-200 text-xs font-medium capitalize">{value}</p>
      <p className="text-gray-500 text-[10px] mt-0.5">{label}</p>
    </div>
  );
}
