import React, { useState } from 'react';

interface ProjectCreateModalProps {
  isOpen: boolean;
  onClose: () => void;
  onSubmit: (name: string, description: string, platform: string) => Promise<void>;
}

const PLATFORMS = [
  { id: 'auto', label: 'Auto-detect' },
  { id: 'nifi', label: 'Apache NiFi' },
  { id: 'ssis', label: 'SQL Server SSIS' },
  { id: 'airflow', label: 'Apache Airflow' },
  { id: 'dbt', label: 'dbt' },
  { id: 'adf', label: 'Azure Data Factory' },
  { id: 'glue', label: 'AWS Glue' },
  { id: 'talend', label: 'Talend' },
  { id: 'informatica', label: 'Informatica' },
  { id: 'sql', label: 'Generic SQL' },
];

export default function ProjectCreateModal({ isOpen, onClose, onSubmit }: ProjectCreateModalProps) {
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [platform, setPlatform] = useState('auto');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState('');

  if (!isOpen) return null;

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!name.trim()) {
      setError('Project name is required');
      return;
    }
    setIsSubmitting(true);
    setError('');
    try {
      await onSubmit(name.trim(), description.trim(), platform);
      setName('');
      setDescription('');
      setPlatform('auto');
      onClose();
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" onClick={onClose} />
      <div className="relative bg-gray-900 border border-gray-800 rounded-2xl p-6 w-full max-w-lg shadow-2xl">
        <h2 className="text-lg font-semibold text-gray-100 mb-4">Create New Project</h2>

        {error && (
          <div className="mb-4 p-3 rounded-lg bg-red-500/10 border border-red-500/30 text-red-400 text-sm">
            {error}
          </div>
        )}

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1.5">Project name</label>
            <input
              type="text"
              value={name}
              onChange={(e) => setName(e.target.value)}
              className="w-full px-3 py-2.5 bg-gray-800 border border-gray-700 rounded-lg text-gray-100 text-sm
                placeholder-gray-500 focus:outline-none focus:border-primary focus:ring-1 focus:ring-primary transition"
              placeholder="My NiFi Migration"
              autoFocus
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1.5">Description</label>
            <textarea
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              rows={3}
              className="w-full px-3 py-2.5 bg-gray-800 border border-gray-700 rounded-lg text-gray-100 text-sm
                placeholder-gray-500 focus:outline-none focus:border-primary focus:ring-1 focus:ring-primary transition resize-none"
              placeholder="Brief description of the migration project..."
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1.5">Source platform</label>
            <select
              value={platform}
              onChange={(e) => setPlatform(e.target.value)}
              className="w-full px-3 py-2.5 bg-gray-800 border border-gray-700 rounded-lg text-gray-100 text-sm
                focus:outline-none focus:border-primary focus:ring-1 focus:ring-primary transition"
            >
              {PLATFORMS.map((p) => (
                <option key={p.id} value={p.id}>{p.label}</option>
              ))}
            </select>
          </div>

          <div className="flex justify-end gap-3 pt-2">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 rounded-lg text-sm text-gray-400 hover:text-gray-200 hover:bg-gray-800 transition"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={isSubmitting}
              className="px-4 py-2 bg-primary hover:bg-primary/90 disabled:opacity-50 text-white font-medium rounded-lg text-sm transition"
            >
              {isSubmitting ? 'Creating...' : 'Create Project'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
