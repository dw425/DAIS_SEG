import React, { useState } from 'react';
import LogViewer from './LogViewer';
import CodeValidator from './CodeValidator';
import JsonExplorer from '../shared/JsonExplorer';
import AutoRunButton from '../shared/AutoRunButton';
import { usePipelineStore } from '../../store/pipeline';
import { useUIStore } from '../../store/ui';

type Tab = 'logs' | 'state' | 'validator' | 'autorun';

export default function AdminConsole() {
  const [activeTab, setActiveTab] = useState<Tab>('logs');
  const pipelineState = usePipelineStore();
  const uiState = useUIStore();

  const stateSnapshot = {
    pipeline: {
      platform: pipelineState.platform,
      fileName: pipelineState.fileName,
      fileSize: pipelineState.fileSize,
      hasParsed: !!pipelineState.parsed,
      hasAnalysis: !!pipelineState.analysis,
      hasAssessment: !!pipelineState.assessment,
      hasNotebook: !!pipelineState.notebook,
      hasReport: !!pipelineState.report,
      hasFinalReport: !!pipelineState.finalReport,
      hasValidation: !!pipelineState.validation,
      hasValueAnalysis: !!pipelineState.valueAnalysis,
    },
    ui: {
      activeStep: uiState.activeStep,
      stepStatuses: uiState.stepStatuses,
      sidebarMode: uiState.sidebarMode,
      errorCount: uiState.errors.length,
      progress: uiState.progress,
    },
  };

  const tabs: { id: Tab; label: string }[] = [
    { id: 'logs', label: 'Log Viewer' },
    { id: 'state', label: 'State Inspector' },
    { id: 'validator', label: 'Code Validator' },
    { id: 'autorun', label: 'Auto-Run' },
  ];

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h2 className="text-xl font-semibold text-gray-100 flex items-center gap-3">
          <span className="w-8 h-8 rounded-lg bg-gray-600/20 flex items-center justify-center text-sm text-gray-400">
            &#x2699;
          </span>
          Admin Console
        </h2>
        <p className="mt-1 text-sm text-gray-400">
          Debug tools, state inspection, and log viewing.
        </p>
      </div>

      {/* Tab bar */}
      <div className="flex gap-1 border-b border-border">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`px-4 py-2 text-sm font-medium border-b-2 transition
              ${activeTab === tab.id ? 'border-primary text-primary' : 'border-transparent text-gray-500 hover:text-gray-300'}`}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      {activeTab === 'logs' && <LogViewer />}

      {activeTab === 'state' && (
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <h3 className="text-sm font-medium text-gray-300">Application State</h3>
            <button
              onClick={() => {
                pipelineState.resetAll();
                uiState.resetUI();
              }}
              className="px-3 py-1 rounded-lg bg-red-500/20 text-red-400 text-xs hover:bg-red-500/30 transition"
            >
              Reset All State
            </button>
          </div>
          <JsonExplorer data={stateSnapshot} rootLabel="appState" maxDepth={5} />
        </div>
      )}

      {activeTab === 'validator' && <CodeValidator />}

      {activeTab === 'autorun' && (
        <div className="space-y-4">
          <h3 className="text-sm font-medium text-gray-300">Auto-Run Pipeline</h3>
          <p className="text-xs text-gray-500">
            Select a file to automatically run all 8 pipeline steps sequentially.
          </p>
          <AutoRunButton />
        </div>
      )}
    </div>
  );
}
