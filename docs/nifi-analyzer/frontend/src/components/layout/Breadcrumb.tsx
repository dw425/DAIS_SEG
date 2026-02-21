import React from 'react';
import { useUIStore } from '../../store/ui';

const STEP_NAMES = [
  'Parse', 'Analyze', 'Assess', 'Convert', 'Report',
  'Final Report', 'Validate', 'Value Analysis', 'Summary', 'Admin',
];

export default function Breadcrumb() {
  const activeStep = useUIStore((s) => s.activeStep);
  const setActiveStep = useUIStore((s) => s.setActiveStep);
  const stepName = STEP_NAMES[activeStep] || 'Unknown';
  const stepNum = activeStep < 8 ? `Step ${activeStep + 1}: ` : '';

  return (
    <nav className="flex items-center gap-1.5 text-sm text-gray-500">
      <button
        onClick={() => setActiveStep(8)}
        className="hover:text-gray-300 transition"
      >
        Pipeline
      </button>
      <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
      </svg>
      <span className="text-gray-200 font-medium">
        {stepNum}{stepName}
      </span>
    </nav>
  );
}
