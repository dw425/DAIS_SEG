// ── Pipeline domain types ──

export interface Processor {
  id: string;
  name: string;
  type: string;
  properties: Record<string, string>;
  relationships: string[];
  position?: { x: number; y: number };
}

export interface Connection {
  id: string;
  sourceId: string;
  destinationId: string;
  name: string;
  selectedRelationships: string[];
}

export interface ProcessGroup {
  id: string;
  name: string;
  processors: Processor[];
  connections: Connection[];
  processGroups: ProcessGroup[];
}

export interface ParseResult {
  _nifi?: Record<string, unknown>;
  processors: Processor[];
  connections: Connection[];
  processGroups: ProcessGroup[];
  parse_warnings: string[];
  format: string;
  platform: string;
}

export interface ExternalSystem {
  name: string;
  type: string;
  processors: string[];
}

export interface AnalysisResult {
  processorCount: number;
  connectionCount: number;
  processGroupCount: number;
  externalSystems: ExternalSystem[];
  hasCycles: boolean;
  tiers: Record<string, string[]>;
  complexityScore: number;
  warnings: string[];
}

export interface MappingEntry {
  processorName: string;
  processorType: string;
  confidence: number;        // 0-100
  risk: 'low' | 'medium' | 'high';
  phase: number;
  mappedTo: string;
  notes: string;
}

export interface AssessmentResult {
  mappings: MappingEntry[];
  overallConfidence: number;
  overallRisk: 'low' | 'medium' | 'high';
  supportedCount: number;
  unsupportedCount: number;
  partialCount: number;
}

export interface NotebookCell {
  cell_type: 'code' | 'markdown';
  source: string;
}

export interface NotebookResult {
  cells: NotebookCell[];
  language: string;
  notebookName: string;
  downloadUrl?: string;
}

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

export interface ValidationScore {
  category: string;
  score: number;
  maxScore: number;
  details: string;
}

export interface ValidationGap {
  processor: string;
  severity: 'critical' | 'high' | 'medium' | 'low';
  message: string;
  remediation: string;
}

export interface ValidationResult {
  overallScore: number;
  scores: ValidationScore[];
  gaps: ValidationGap[];
  passed: boolean;
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
