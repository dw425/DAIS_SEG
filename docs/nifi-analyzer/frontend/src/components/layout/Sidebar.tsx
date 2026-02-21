import React, { useState } from 'react';
import { useUIStore } from '../../store/ui';
import StepIndicator from './StepIndicator';

const PIPELINE_STEPS = [
  'Parse Flow',
  'Analyze',
  'Assess & Map',
  'Convert',
  'Migration Report',
  'Final Report',
  'Validate',
  'Value Analysis',
];

const NAV_PAGES = [
  { label: 'Summary', icon: '\u03A3', idx: 8 },
  { label: 'Admin', icon: '\u2699', idx: 9 },
  { label: 'Dashboard', icon: '\u25A3', idx: 10 },
  { label: 'History', icon: '\u231A', idx: 11 },
  { label: 'Compare', icon: '\u2194', idx: 12 },
];

const APP_VERSION = import.meta.env.VITE_APP_VERSION || '5.0.0';

export default function Sidebar() {
  const activeStep = useUIStore((s) => s.activeStep);
  const stepStatuses = useUIStore((s) => s.stepStatuses);
  const setActiveStep = useUIStore((s) => s.setActiveStep);
  const [collapsed, setCollapsed] = useState(false);

  const canNavigate = (idx: number) => {
    if (idx >= 8 || idx === activeStep || idx === 0) return true;
    if (stepStatuses[idx] === 'done' || stepStatuses[idx] === 'error' || stepStatuses[idx] === 'running') return true;
    if (idx > 0 && stepStatuses[idx - 1] === 'done') return true;
    return false;
  };

  return (
    <aside
      className={`
        shrink-0 bg-gray-900/50 border-r border-border flex flex-col transition-all
        ${collapsed ? 'w-14' : 'w-60'}
      `}
    >
      {/* Collapse toggle */}
      <button
        onClick={() => setCollapsed(!collapsed)}
        className="p-2 text-gray-500 hover:text-gray-300 self-end"
        title={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
        aria-label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
      >
        <svg className={`w-4 h-4 transition-transform ${collapsed ? 'rotate-180' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M11 19l-7-7 7-7m8 14l-7-7 7-7" />
        </svg>
      </button>

      <nav className="flex-1 overflow-y-auto px-2 space-y-1 pb-4">
        {/* Pipeline steps (1-8) with status indicators */}
        {PIPELINE_STEPS.map((label, idx) => (
          <React.Fragment key={idx}>
            {collapsed ? (
              <button
                onClick={() => canNavigate(idx) && setActiveStep(idx)}
                disabled={!canNavigate(idx)}
                title={label}
                className={`
                  w-10 h-10 flex items-center justify-center rounded-lg text-xs font-mono transition
                  ${idx === activeStep ? 'bg-primary/10 text-primary' : 'text-gray-500 hover:bg-gray-800'}
                  ${!canNavigate(idx) ? 'opacity-40 cursor-not-allowed' : 'cursor-pointer'}
                `}
              >
                {idx + 1}
              </button>
            ) : (
              <StepIndicator
                stepNumber={idx + 1}
                label={label}
                status={stepStatuses[idx]}
                active={idx === activeStep}
                disabled={!canNavigate(idx)}
                onClick={() => setActiveStep(idx)}
              />
            )}
          </React.Fragment>
        ))}

        {/* Separator */}
        <hr className="border-border my-3" />

        {/* Navigation pages — plain buttons, no step indicators */}
        {NAV_PAGES.map((page) => (
          <React.Fragment key={page.idx}>
            {collapsed ? (
              <button
                onClick={() => setActiveStep(page.idx)}
                title={page.label}
                className={`
                  w-10 h-10 flex items-center justify-center rounded-lg text-sm transition cursor-pointer
                  ${page.idx === activeStep ? 'bg-primary/10 text-primary' : 'text-gray-500 hover:bg-gray-800 hover:text-gray-300'}
                `}
              >
                {page.icon}
              </button>
            ) : (
              <button
                onClick={() => setActiveStep(page.idx)}
                className={`
                  w-full flex items-center gap-3 px-3 py-2.5 rounded-lg transition-all text-left cursor-pointer
                  ${page.idx === activeStep ? 'bg-primary/10 border border-primary/30 text-gray-100' : 'hover:bg-gray-800/50 border border-transparent text-gray-400 hover:text-gray-200'}
                `}
              >
                <span className="w-6 h-6 flex items-center justify-center text-sm">{page.icon}</span>
                <span className="text-sm">{page.label}</span>
              </button>
            )}
          </React.Fragment>
        ))}
      </nav>

      {/* Version */}
      {!collapsed && (
        <div className="px-4 py-2 border-t border-border">
          <span className="text-xs text-gray-600">v{APP_VERSION}</span>
        </div>
      )}
    </aside>
  );
}
