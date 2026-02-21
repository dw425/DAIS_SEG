// ── ROI domain types — used by ROI Dashboard and value analysis ──

export interface ROIScenario {
  cost: number;
  weeks: number;
  risk: number;
  description: string;
}

export interface ROIComparison {
  liftAndShift: ROIScenario;
  refactor: ROIScenario;
  recommendation: string;
  breakEvenMonths: number;
}

export interface TCOResult {
  currentPlatform: { year1: number; year3: number; year5: number };
  databricks: { year1: number; year3: number; year5: number };
  migrationCost: number;
  savingsYear1: number;
  savingsYear3: number;
  savingsYear5: number;
}

export interface FTERole {
  role: string;
  hours: number;
  fteMonths: number;
  tasks: string[];
}

export interface FTEImpact {
  roles: FTERole[];
  totalHours: number;
  totalFteMonths: number;
  phases: { phase: string; roles: string[]; weeks: number }[];
}

export interface LicenseSavings {
  currentLicense: number;
  databricksLicense: number;
  annualSavings: number;
  platforms: { platform: string; cost: number; notes: string }[];
}

export interface MonteCarloResult {
  p10: number;
  p25: number;
  p50: number;
  p75: number;
  p90: number;
  mean: number;
  std: number;
  histogram: { binStart: number; binEnd: number; count: number }[];
}

export interface BenchmarkResult {
  flowSize: string;
  processorCount: number;
  percentile: number;
  avgMigrationWeeks: number;
  estimatedWeeks: number;
  comparisonNotes: string;
}

export interface ComplexityResult {
  overall: number;
  distribution: { low: number; medium: number; high: number; critical: number };
  processors: { name: string; type: string; score: number; factors: string[] }[];
}

export interface FullROIReport {
  comparison: ROIComparison;
  tco: TCOResult;
  fteImpact: FTEImpact;
  licenseSavings: LicenseSavings;
  monteCarlo: MonteCarloResult;
  benchmarks: BenchmarkResult;
  complexity: ComplexityResult;
}
