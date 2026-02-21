// ── ETL Platform types matching shared/etl_platforms.json ──

export interface ETLPlatform {
  id: string;
  name: string;
  formats: string[];
  category: 'dataflow' | 'etl' | 'orchestrator' | 'transform' | 'warehouse' | 'ingestion' | 'compute' | 'database';
  versions: string[];
}

export interface PlatformsResponse {
  platforms: ETLPlatform[];
}

export const CATEGORY_LABELS: Record<string, string> = {
  dataflow: 'Data Flow',
  etl: 'ETL',
  orchestrator: 'Orchestrator',
  transform: 'Transform',
  warehouse: 'Warehouse',
  ingestion: 'Ingestion',
  compute: 'Compute',
  database: 'Database',
};

export const CATEGORY_COLORS: Record<string, string> = {
  dataflow: 'bg-blue-500/20 text-blue-400',
  etl: 'bg-purple-500/20 text-purple-400',
  orchestrator: 'bg-amber-500/20 text-amber-400',
  transform: 'bg-green-500/20 text-green-400',
  warehouse: 'bg-cyan-500/20 text-cyan-400',
  ingestion: 'bg-pink-500/20 text-pink-400',
  compute: 'bg-orange-500/20 text-orange-400',
  database: 'bg-teal-500/20 text-teal-400',
};
