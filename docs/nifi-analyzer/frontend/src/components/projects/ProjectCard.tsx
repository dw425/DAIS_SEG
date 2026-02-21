import React from 'react';
import type { Project } from '../../store/projects';

interface ProjectCardProps {
  project: Project;
  onOpen: (id: string) => void;
  onDelete: (id: string) => void;
}

const PLATFORM_COLORS: Record<string, string> = {
  nifi: 'bg-blue-500/20 text-blue-400',
  ssis: 'bg-purple-500/20 text-purple-400',
  airflow: 'bg-green-500/20 text-green-400',
  dbt: 'bg-orange-500/20 text-orange-400',
  adf: 'bg-cyan-500/20 text-cyan-400',
  glue: 'bg-yellow-500/20 text-yellow-400',
  auto: 'bg-gray-500/20 text-gray-400',
};

export default function ProjectCard({ project, onOpen, onDelete }: ProjectCardProps) {
  const platformClass = PLATFORM_COLORS[project.platform] || PLATFORM_COLORS.auto;

  return (
    <div className="bg-gray-900 border border-gray-800 rounded-xl p-5 hover:border-gray-700 transition group">
      <div className="flex items-start justify-between mb-3">
        <div className="flex items-center gap-2">
          <span className={`px-2 py-0.5 rounded text-xs font-medium ${platformClass}`}>
            {project.platform}
          </span>
          {project.run_count > 0 && (
            <span className="px-2 py-0.5 rounded text-xs bg-gray-800 text-gray-400">
              {project.run_count} run{project.run_count !== 1 ? 's' : ''}
            </span>
          )}
        </div>
        <button
          onClick={(e) => { e.stopPropagation(); onDelete(project.id); }}
          className="p-1 rounded text-gray-600 hover:text-red-400 hover:bg-red-500/10 transition opacity-0 group-hover:opacity-100"
          title="Delete project"
        >
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
          </svg>
        </button>
      </div>

      <h3 className="text-base font-semibold text-gray-100 mb-1 truncate">{project.name}</h3>
      <p className="text-sm text-gray-400 mb-4 line-clamp-2">{project.description || 'No description'}</p>

      <div className="flex items-center justify-between">
        <span className="text-xs text-gray-500">
          {project.updated_at ? new Date(project.updated_at).toLocaleDateString() : 'N/A'}
        </span>
        <button
          onClick={() => onOpen(project.id)}
          className="px-3 py-1.5 rounded-lg bg-primary/10 text-primary text-xs font-medium hover:bg-primary/20 transition"
        >
          Open Pipeline
        </button>
      </div>
    </div>
  );
}
