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
}

export interface NotebookResult {
  cells: NotebookCell[];
  workflow: Record<string, unknown>;
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
