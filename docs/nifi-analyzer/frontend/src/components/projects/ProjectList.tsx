import React, { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useProjectsStore } from '../../store/projects';
import ProjectCard from './ProjectCard';
import ProjectCreateModal from './ProjectCreateModal';

export default function ProjectList() {
  const { projects, isLoading, error, fetchProjects, createProject, deleteProject } = useProjectsStore();
  const [showCreate, setShowCreate] = useState(false);
  const navigate = useNavigate();

  useEffect(() => {
    fetchProjects();
  }, [fetchProjects]);

  const handleOpen = (id: string) => {
    navigate(`/pipeline?project=${id}`);
  };

  const handleDelete = async (id: string) => {
    if (window.confirm('Delete this project and all its runs? This cannot be undone.')) {
      await deleteProject(id);
    }
  };

  const handleCreate = async (name: string, description: string, platform: string) => {
    await createProject(name, description, platform);
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold text-gray-100">Projects</h2>
          <p className="text-sm text-gray-400 mt-1">Manage your ETL migration projects</p>
        </div>
        <button
          onClick={() => setShowCreate(true)}
          className="px-4 py-2 bg-primary hover:bg-primary/90 text-white font-medium rounded-lg text-sm transition flex items-center gap-2"
        >
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
          </svg>
          New Project
        </button>
      </div>

      {/* Error */}
      {error && (
        <div className="p-3 rounded-lg bg-red-500/10 border border-red-500/30 text-red-400 text-sm">
          {error}
        </div>
      )}

      {/* Loading */}
      {isLoading && projects.length === 0 && (
        <div className="text-center py-12 text-gray-500">Loading projects...</div>
      )}

      {/* Empty state */}
      {!isLoading && projects.length === 0 && (
        <div className="text-center py-16">
          <div className="w-16 h-16 rounded-2xl bg-gray-800 flex items-center justify-center mx-auto mb-4">
            <svg className="w-8 h-8 text-gray-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M3 7v10a2 2 0 002 2h14a2 2 0 002-2V9a2 2 0 00-2-2h-6l-2-2H5a2 2 0 00-2 2z" />
            </svg>
          </div>
          <h3 className="text-gray-300 font-medium mb-1">No projects yet</h3>
          <p className="text-sm text-gray-500 mb-4">Create your first migration project to get started.</p>
          <button
            onClick={() => setShowCreate(true)}
            className="px-4 py-2 bg-primary/10 text-primary rounded-lg text-sm font-medium hover:bg-primary/20 transition"
          >
            Create Project
          </button>
        </div>
      )}

      {/* Project grid */}
      {projects.length > 0 && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {projects.map((project) => (
            <ProjectCard
              key={project.id}
              project={project}
              onOpen={handleOpen}
              onDelete={handleDelete}
            />
          ))}
        </div>
      )}

      {/* Create modal */}
      <ProjectCreateModal
        isOpen={showCreate}
        onClose={() => setShowCreate(false)}
        onSubmit={handleCreate}
      />
    </div>
  );
}
