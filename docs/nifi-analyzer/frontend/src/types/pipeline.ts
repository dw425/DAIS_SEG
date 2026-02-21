// ── Pipeline domain types — matches backend Pydantic models (camelCase) ──

export interface Processor {
  name: string;
  type: string;
  platform: string;
  properties: Record<string, string>;
  group: string;
  state: string;
  scheduling: Record<string, unknown> | null;
  resolvedServices: Record<string, Record<string, string>> | null; // resolved controller service properties
}

export interface Connection {
  sourceName: string;
  destinationName: string;
  relationship: string;
  backPressureObjectThreshold: number;
  backPressureDataSizeThreshold: string;
}

export interface ProcessGroup {
  name: string;
  processors: string[];
}

export interface ControllerService {
  name: string;
  type: string;
  properties: Record<string, string>;
}

export interface Warning {
  severity: string;
  message: string;
  source: string;
}

export interface ParameterEntry {
  key: string;
  value: string;
  sensitive: boolean;
  inferredType: string; // "string" | "numeric" | "secret"
  databricksVariable: string;
}

export interface ParameterContext {
  name: string;
  parameters: ParameterEntry[];
}

export interface ParseResult {
  platform: string;
  version: string;
  processors: Processor[];
  connections: Connection[];
  processGroups: ProcessGroup[];
  controllerServices: ControllerService[];
  parameterContexts: ParameterContext[];
  metadata: Record<string, unknown>;
  warnings: Warning[];
}

export interface CycleClassification {
  cycleNodes: string[];
  category: string; // "error_retry" | "data_reevaluation" | "pagination"
  description: string;
  databricksTranslation: string;
}

export interface TaskCluster {
  id: string;
  processors: string[];
  entryProcessor: string;
  exitProcessors: string[];
  connections: string[];
}

export interface BackpressureConfig {
  connectionSource: string;
  connectionDestination: string;
  nifiObjectThreshold: number;
  nifiDataSizeThreshold: string;
  databricksMaxFilesPerTrigger: number | null;
  databricksMaxBytesPerTrigger: string | null;
}

export interface AnalysisResult {
  dependencyGraph: Record<string, unknown>;
  externalSystems: Record<string, unknown>[];
  cycles: string[][];
  cycleClassifications: CycleClassification[];
  taskClusters: TaskCluster[];
  backpressureConfigs: BackpressureConfig[];
  flowMetrics: Record<string, unknown>;
  securityFindings: Record<string, unknown>[];
  stages: Record<string, unknown>[];
  // V6 deep analysis (returned from /analyze/deep)
  deepAnalysis?: DeepAnalysisResult | null;
}

export interface MappingEntry {
  name: string;
  type: string;
  role: string;
  category: string;
  mapped: boolean;
  confidence: number;
  code: string;
  notes: string;
}

export interface AssessmentResult {
  mappings: MappingEntry[];
  packages: string[];
  unmappedCount: number;
}

export interface NotebookCell {
  type: string;
  source: string;
  label: string;
  metadata?: Record<string, unknown>;
}

export interface NotebookResult {
  cells: NotebookCell[];
  workflow: Record<string, unknown>;
  // V6 additions
  deepAnalysis?: DeepAnalysisResult | null;
  validationSummary?: ValidationSummary | null;
  version?: string; // "v6" if V6 generated
  isRunnable?: boolean;
}

export interface ValidationScore {
  dimension: string;
  score: number;
  details: string;
}

export interface ValidationResult {
  overallScore: number;
  scores: ValidationScore[];
  gaps: Record<string, unknown>[];
  errors: string[];
  // V6: 12-point runnable checker
  runnableReport?: RunnableReport | null;
}

// ── V6 Deep Analysis Types ──

export interface FunctionalZone {
  zone_name: string;
  processor_names: string[];
  purpose: string;
}

export interface FunctionalReport {
  flow_purpose: string;
  functional_zones: FunctionalZone[];
  data_domains: string[];
  pipeline_pattern: string;
  estimated_complexity: string;
  sla_profile: string;
}

export interface ProcessorDetail {
  name: string;
  short_type: string;
  full_type: string;
  category: string;
  role: string;
  conversion_complexity: string;
  databricks_equivalent: string;
  is_known: boolean;
}

export interface ProcessorReport {
  total_processors: number;
  by_category: Record<string, number>;
  by_role: Record<string, number>;
  by_conversion_complexity: Record<string, number>;
  unknown_processors: string[];
  processors: ProcessorDetail[];
}

export interface ExecutionPhase {
  phase_number: number;
  processor_names: string[];
  description: string;
}

export interface WorkflowReport {
  execution_phases: ExecutionPhase[];
  process_groups: Record<string, unknown>[];
  cycles_detected: string[][];
  synchronization_points: string[];
  total_execution_phases: number;
  critical_path_length: number;
  parallelism_factor: number;
  topological_order: string[];
}

export interface DataSource {
  processor_name: string;
  source_type: string;
  protocol: string;
  details: Record<string, unknown>;
}

export interface UpstreamReport {
  data_sources: DataSource[];
  attribute_lineage: Record<string, unknown>;
  content_transformations: Record<string, unknown>[];
  external_dependencies: string[];
  parameter_injections: Record<string, unknown>[];
}

export interface DataSink {
  processor_name: string;
  sink_type: string;
  protocol: string;
  details: Record<string, unknown>;
}

export interface DownstreamReport {
  data_sinks: DataSink[];
  data_flow_map: Record<string, unknown>;
  error_routing: Record<string, unknown>[];
  volume_estimates: Record<string, unknown>;
  data_multiplication_points: string[];
  data_reduction_points: string[];
}

export interface PropertyAnalysis {
  processor_name: string;
  property_key: string;
  property_value: string;
  value_type: string;
  what_it_does: string;
  why_its_there: string;
  needs_conversion: boolean;
  conversion_confidence: number;
}

export interface LineByLineReport {
  total_properties_analyzed: number;
  nel_expression_count: number;
  sql_statement_count: number;
  script_code_count: number;
  regex_count: number;
  overall_conversion_confidence: number;
  properties: PropertyAnalysis[];
}

export interface DeepAnalysisResult {
  functional: FunctionalReport;
  processors: ProcessorReport;
  workflow: WorkflowReport;
  upstream: UpstreamReport;
  downstream: DownstreamReport;
  line_by_line: LineByLineReport;
  duration_ms: number;
  summary: string;
}

// ── V6 Runnable Checker Types ──

export interface CheckResult {
  check_id: number;
  name: string;
  passed: boolean;
  severity: string; // "critical" | "high" | "medium" | "low"
  message: string;
  details: string[];
}

export interface RunnableReport {
  checks: CheckResult[];
  passed_count: number;
  failed_count: number;
  is_runnable: boolean;
  overall_score: number;
  summary: string;
}

export interface ValidationSummary {
  is_runnable?: boolean;
  overall_score?: number;
  checks_passed?: number;
  checks_total?: number;
  warnings?: string[];
}

// ── Report types (returned as plain dicts from /report endpoint) ──

export interface GapItem {
  processor: string;
  gap: string;
  severity: 'critical' | 'high' | 'medium' | 'low';
  remediation: string;
}

export interface MigrationReport {
  gapPlaybook: GapItem[];
  riskMatrix: Record<string, number>;
  estimatedTimeline: { phase: string; weeks: number }[];
  summary: string;
}

export interface FinalReport {
  executiveSummary: string;
  sections: { title: string; content: string }[];
  exportFormats: string[];
  generatedAt: string;
  rawJson: Record<string, unknown>;
}

export interface DroppableProcessor {
  name: string;
  type: string;
  reason: string;
  savingsHours: number;
}

export interface ValueAnalysis {
  droppableProcessors: DroppableProcessor[];
  complexityBreakdown: Record<string, number>;
  roiEstimate: { costSavings: number; timeSavings: number; riskReduction: number };
  implementationRoadmap: { phase: string; tasks: string[]; weeks: number }[];
}
