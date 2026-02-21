import React, { useState } from 'react';
import { useUIStore } from '../../store/ui';
import StepIndicator from './StepIndicator';

const STEP_LABELS = [
  'Parse Flow',
  'Analyze',
  'Assess & Map',
  'Convert',
  'Migration Report',
  'Final Report',
  'Validate',
  'Value Analysis',
  'Summary',
  'Admin',
];

export default function Sidebar() {
  const activeStep = useUIStore((s) => s.activeStep);
  const stepStatuses = useUIStore((s) => s.stepStatuses);
  const setActiveStep = useUIStore((s) => s.setActiveStep);
  const [collapsed, setCollapsed] = useState(false);

  const canNavigate = (idx: number) => {
    // Can always go to admin (9) or current step
    if (idx === 9 || idx === activeStep) return true;
    // Summary (8) available when any step is done
    if (idx === 8) return stepStatuses.some((s) => s === 'done');
    // Can navigate to completed steps or current active step
    return stepStatuses[idx] === 'done' || stepStatuses[idx] === 'error' || idx === 0;
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
      >
        <svg className={`w-4 h-4 transition-transform ${collapsed ? 'rotate-180' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M11 19l-7-7 7-7m8 14l-7-7 7-7" />
        </svg>
      </button>

      {/* Steps */}
      <nav className="flex-1 overflow-y-auto px-2 space-y-1 pb-4">
        {STEP_LABELS.map((label, idx) => (
          <React.Fragment key={idx}>
            {idx === 8 && <hr className="border-border my-2" />}
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
                {idx < 8 ? idx + 1 : idx === 8 ? '\u03A3' : '\u2699'}
              </button>
            ) : (
              <StepIndicator
                stepNumber={idx < 8 ? idx + 1 : idx === 8 ? 0 : -1}
                label={label}
                status={idx < 10 ? stepStatuses[idx] : 'idle'}
                active={idx === activeStep}
                disabled={!canNavigate(idx)}
                onClick={() => setActiveStep(idx)}
              />
            )}
          </React.Fragment>
        ))}
      </nav>

      {/* Version */}
      {!collapsed && (
        <div className="px-4 py-2 border-t border-border">
          <span className="text-xs text-gray-600">v5.0.0</span>
        </div>
      )}
    </aside>
  );
}
